/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class of Spark readers.
 *
 * @param <T> is the Java class returned by this reader whose objects contain one or more rows.
 */
abstract class BaseReader<T, TaskT extends ScanTask> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseReader.class);

  private final Table table;
  private final Schema tableSchema;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;
  private final ScanTaskGroup<TaskT> taskGroup;
  private final Iterator<TaskT> tasks;
  private final DeleteCounter counter;

  private Map<String, InputFile> lazyInputFiles;
  private CloseableIterator<T> currentIterator;
  private long nextMethodStartTime;
  private final long constructorInitiationTime;
  private Queue<CompletableFuture<CloseableIterator<T>>> iteratorFutures;
  // TODO: should this be a thread safe data structure?
  private LinkedBlockingQueue<CloseableIterator<T>> iterators = new LinkedBlockingQueue<>();
  private ExecutorService executorService;
  private long s3DownloadStartTime;
  private Map<String, File> s3ToLocal;
  private long s3DownloadEndTime;
  private Path tempDir;
  private Iterator<InputFile> inputFileIterator;
  private List<CompletableFuture<Object>> completableFutureList;
  private T current = null;
  private TaskT currentTask = null;

  private final AtomicBoolean isDownloaded = new AtomicBoolean(false);

  BaseReader(
      Table table,
      ScanTaskGroup<TaskT> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    LOG.info("at base reader constructor");
    this.table = table;
    this.taskGroup = taskGroup;
    this.tasks = taskGroup.tasks().iterator();
    this.currentIterator = CloseableIterator.empty();
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    this.counter = new DeleteCounter();
    this.s3ToLocal = Maps.newConcurrentMap();
    executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor)
                Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors(),
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("iceberg-base-reader-%d")
                        .build()));
    // this.iteratorFutures = prefetchInputFiles();
    constructorInitiationTime = System.currentTimeMillis();
    try {
      tempDir = Files.createTempDirectory(table.name());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Reading table: {} using scan task group: {}", table.name(), taskGroup);
  }

  protected abstract CloseableIterator<T> open(TaskT task);

  protected abstract Stream<ContentFile<?>> referencedFiles(TaskT task);

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected NameMapping nameMapping() {
    return nameMapping;
  }

  protected Table table() {
    return table;
  }

  protected DeleteCounter counter() {
    return counter;
  }

  // TODO: this might cause memory to explode
  private void iterateTasks() {
    // maybe adding pub sub will make it better
    Preconditions.checkState(iteratorFutures != null);
    if (iteratorFutures.peek() == null) {
      return;
    }
    //    iterators = iteratorFutures
    //        .parallelStream()
    //        .map(
    //            (iteratorFuture) -> {
    //              try {
    //                return iteratorFuture.join();
    //              } catch (CompletionException e) {
    //                LOG.error("exception encountered while attempting to join future: {}", e);
    //                throw e;
    //              }
    //            })
    //            .collect(Collectors.toCollection(LinkedBlockingQueue::new));
    CompletableFuture.supplyAsync(() -> iteratorFutures.poll())
        .thenApply(
            (iteratorFuture) -> {
              try {
                return iteratorFuture.join();
              } catch (CompletionException e) {
                LOG.error("exception encountered while attempting to join future: {}", e);
                throw e;
              }
            })
        .thenApply((iterator) -> iterators.add(iterator))
        .join();
  }

  // TODO: not sure why i see current iterator null here
  public boolean oldNext() throws IOException {
    while (true) {
      if (currentIterator.hasNext()) {
        this.current = currentIterator.next();
        return true;
      } else if (iterators.peek() != null) {
        currentIterator.close();
        currentIterator = iterators.poll();
      } else if (iterators.peek() == null && iteratorFutures.peek() == null) {
        currentIterator.close();
        LOG.info("finished iterating over all iterators");
        long nextMethodEndTime = System.currentTimeMillis();
        long duration = nextMethodEndTime - nextMethodStartTime;
        LOG.info("total time taken for next method: {} ms", duration);
        return false;
      } else {
        nextMethodStartTime = System.currentTimeMillis();
        LOG.info("iterating over files");
        iterateTasks();
      }
    }
  }

  public boolean next() throws IOException {
    try {
      LOG.info("at next method");
      try {
        // TODO: you can check if the files have been downloaded already
        completableFutureList = prefetchS3Files();
        LOG.info("attempting to get downloaded s3 files....might block");
        completableFutureList.stream().map(CompletableFuture::join).collect(Collectors.toList());
        s3DownloadEndTime = System.currentTimeMillis();
        LOG.info("total time to download s3 files: {}", s3DownloadEndTime - s3DownloadStartTime);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      while (true) {
        if (currentIterator.hasNext()) {
          this.current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          this.currentIterator.close();
          this.currentTask = tasks.next();
          this.currentIterator = open(currentTask);
        } else {
          this.currentIterator.close();
          return false;
        }
      }
    } catch (IOException | RuntimeException e) {
      if (currentTask != null && !currentTask.isDataTask()) {
        String filePaths =
            referencedFiles(currentTask)
                .map(file -> file.path().toString())
                .collect(Collectors.joining(", "));
        LOG.error("Error reading file(s): {}", filePaths, e);
      }
      throw e;
    }
  }

  public T get() {
    LOG.info("returning record: {}", current);
    return current;
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing reader");
    InputFileBlockHolder.unset();

    // close the current iterator
    // might have to iterate over all opened tasks and close it

    this.currentIterator.close();

    //    try {
    //      iterators.forEach(
    //          iterator -> {
    //            try {
    //              iterator.close();
    //            } catch (IOException e) {
    //              throw new RuntimeException(e);
    //            }
    //          });
    //    } catch (Exception e) {
    //      throw e;
    //    }

    long closeEndTime = System.currentTimeMillis();
    long duration = closeEndTime - constructorInitiationTime;
    LOG.info("total time taken in base reader: {}", duration);

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  private Map<String, InputFile> inputFiles() {
    if (lazyInputFiles == null) {
      Stream<EncryptedInputFile> encryptedFiles =
          taskGroup.tasks().stream().flatMap(this::referencedFiles).map(this::toEncryptedInputFile);

      // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
      Iterable<InputFile> decryptedFiles = table.encryption().decrypt(encryptedFiles::iterator);

      Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(taskGroup.tasks().size());
      decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
      LOG.info("assigning input files: {}", files);
      this.lazyInputFiles = ImmutableMap.copyOf(files);
    }

    return lazyInputFiles;
  }

  private List<CompletableFuture<Object>> prefetchS3Files() throws IOException {
    Preconditions.checkNotNull(inputFiles().values(), "input files should be non-null");

    LOG.info("starting to download s3 files");
    s3DownloadStartTime = System.currentTimeMillis();

    return inputFiles().values().stream()
        .map(
            inputFile ->
                CompletableFuture.supplyAsync(
                    () -> {
                      downloadFile(inputFile);
                      return null;
                    }))
        .collect(Collectors.toList());
  }

  private void downloadFile(InputFile inputFile) {
    LOG.info("supply async for input file: {}", inputFile.location());
    SeekableInputStream inputStream = inputFile.newStream();
    try {
      String filename = getFileName(inputFile.location());
      File parquetFile = new File(tempDir.toString(), FileFormat.PARQUET.addExtension(filename));
      if (parquetFile.exists()) {
        if (parquetFile.length() != 0) {
          LOG.info("file already exists: {}", parquetFile);
          return;
        }
      } else {
        if (!parquetFile.createNewFile()) {
          LOG.error("could not create file : {}", parquetFile.getName());
          throw new RuntimeException(
              String.format("could not create file : %s", parquetFile.getName()));
        }
      }
      FileOutputStream outputStream = new FileOutputStream(parquetFile.getAbsolutePath());

      LOG.info("writing file to output file: {}", parquetFile);

      ByteStreams.copy(inputStream, outputStream);
      // we need to keep a mapping of the s3 file location and the local file
      // we open the referenced files for a given task. A task has reference to the s3
      // files
      // this map will help us reference the s3 file to a local file
      this.s3ToLocal.put(inputFile.location(), parquetFile);
      outputStream.close();
      inputStream.close();
      // TODO: open the file and read and check
      if (parquetFile.length() == 0) {
        String errorMsg =
            String.format("file length still zero after writing: %s", parquetFile.getName());
        throw new RuntimeException(errorMsg);
      }
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private String getFileName(String fileLocation) throws URISyntaxException {
    URI uri = new URI(fileLocation);
    String path = uri.normalize().getPath();
    int idx = path.lastIndexOf("/");
    String filename = path;
    if (idx >= 0) {
      filename = path.substring(idx + 1, path.length());
    }
    return filename;
  }

  protected InputFile getInputFile(String location) {
    if (!this.s3ToLocal.containsKey(location)) {
      LOG.info("printing hashmap keys:{}", this.s3ToLocal.keySet());
      throw new RuntimeException(String.format("s3 file doesnt exist :%s", location));
    }
    File localFile = this.s3ToLocal.get(location);
    ResolvingFileIO fileIO = new ResolvingFileIO();
    Map<String, String> properties = Maps.newHashMap();
    fileIO.initialize(properties);
    fileIO.setConf(new Configuration());
    return fileIO.newInputFile("file:/" + localFile.getPath());
  }

  private Queue<CompletableFuture<CloseableIterator<T>>> prefetchInputFiles() {
    // in parallel, try to open the files for each task in task group
    // i'm not sure if we want to open, just loading should be fine

    // in parallel, call open() against each task and save all the iterables in a blocking queue or
    // something
    // and then also update the next() method to reference this queue instead of calling open again
    //   CompletableFuture.supplyAsync(() -> {
    //     Stream.of(this.tasks).
    //   });
    // you probably need to split the tasks iterator to be divided among threads
    this.iteratorFutures = new LinkedBlockingQueue<>();
    this.tasks.forEachRemaining(
        task -> {
          CompletableFuture<CloseableIterator<T>> iteratorFuture =
              CompletableFuture.supplyAsync(() -> open(task), executorService)
                  .whenComplete(
                      (result, exception) -> {
                        if (exception != null) {
                          // Exception observed here will be thrown as part of
                          // CompletionException when we join completable futures.
                          if (!task.isDataTask()) {
                            String filePaths =
                                referencedFiles(task)
                                    .map(file -> file.path().toString())
                                    .collect(Collectors.joining(", "));
                            LOG.error("Error reading file(s): {}", filePaths, exception);
                          }
                        }
                      });
          iteratorFutures.add(iteratorFuture);
        });
    return iteratorFutures;
  }

  private EncryptedInputFile toEncryptedInputFile(ContentFile<?> file) {
    InputFile inputFile = table.io().newInputFile(file.path().toString());
    return EncryptedFiles.encryptedInput(inputFile, file.keyMetadata());
  }

  protected Map<Integer, ?> constantsMap(ContentScanTask<?> task, Schema readSchema) {
    if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
      StructType partitionType = Partitioning.partitionType(table);
      return PartitionUtil.constantsMap(task, partitionType, BaseReader::convertConstant);
    } else {
      return PartitionUtil.constantsMap(task, BaseReader::convertConstant);
    }
  }

  protected static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case DECIMAL:
        return Decimal.apply((BigDecimal) value);
      case STRING:
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          return UTF8String.fromBytes(utf8.getBytes(), 0, utf8.getByteLength());
        }
        return UTF8String.fromString(value.toString());
      case FIXED:
        if (value instanceof byte[]) {
          return value;
        } else if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case BINARY:
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case STRUCT:
        StructType structType = (StructType) type;

        if (structType.fields().isEmpty()) {
          return new GenericInternalRow();
        }

        List<NestedField> fields = structType.fields();
        Object[] values = new Object[fields.size()];
        StructLike struct = (StructLike) value;

        for (int index = 0; index < fields.size(); index++) {
          NestedField field = fields.get(index);
          Type fieldType = field.type();
          values[index] =
              convertConstant(fieldType, struct.get(index, fieldType.typeId().javaClass()));
        }

        return new GenericInternalRow(values);
      default:
    }
    return value;
  }

  protected class SparkDeleteFilter extends DeleteFilter<InternalRow> {
    private final InternalRowWrapper asStructLike;

    SparkDeleteFilter(String filePath, List<DeleteFile> deletes, DeleteCounter counter) {
      super(filePath, deletes, tableSchema, expectedSchema, counter);
      this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    }

    @Override
    protected StructLike asStructLike(InternalRow row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return BaseReader.this.getInputFile(location);
    }

    @Override
    protected void markRowDeleted(InternalRow row) {
      if (!row.getBoolean(columnIsDeletedPosition())) {
        row.setBoolean(columnIsDeletedPosition(), true);
        counter().increment();
      }
    }
  }
}
