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
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
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
  private final long constructorInitiationTime;
  private long allS3FilesDownloadStartTime;
  private long s3DownloadStartTime;
  private long nextMethodStartTime;
  private Map<String, String> s3ToLocal;
  private long s3DownloadEndTime;
  private T current = null;
  private TaskT currentTask = null;
  private int count = 0;
  private ResolvingFileIO fileIO;
  private final Collection<TaskT> tasksCache = Lists.newArrayList();

  BaseReader(
      Table table,
      ScanTaskGroup<TaskT> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    LOG.info("at base reader constructor");
    this.table = table;
    this.taskGroup = taskGroup;
    this.tasksCache.addAll(taskGroup.tasks());
    LOG.info("tasks list size : {}", tasksCache.size());
    this.tasks = tasksCache.iterator();
    this.currentIterator = CloseableIterator.empty();
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    this.counter = new DeleteCounter();
    this.s3ToLocal = Maps.newConcurrentMap();

    constructorInitiationTime = System.currentTimeMillis();
    fileIO = new ResolvingFileIO();
    Map<String, String> properties = Maps.newHashMap();
    fileIO.initialize(properties);
    fileIO.setConf(new Configuration());
    try {
      LOG.info("attempting to download all files for the task group at: {}", new Date());
      prefetchS3Files();
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

  public boolean next() throws IOException {
    try {
      nextMethodStartTime = System.currentTimeMillis();
      // LOG.info("at next method");
      while (true) {
        // TODO: have to check if we got a CompletableException here
        if (currentIterator.hasNext()) {
          this.current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          this.currentTask = tasks.next();
          LOG.info("reading data for the task...: {}", this.currentTask);
          if (count >= 1) {
            // we prefetch the second task's file
            LOG.info(
                "attempting to get downloaded s3 files....might block at time: {}", new Date());
            prefetchS3FileForTask(this.currentTask).stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
            s3DownloadEndTime = System.currentTimeMillis();
            LOG.info("was able to get prefetched data at: {}", new Date());
            LOG.info(
                "total time to download s3 files: {}", s3DownloadEndTime - s3DownloadStartTime);
          } else {
            //  we don't prefetch the first task's file
            LOG.info("opening data for first task");
          }
          this.currentIterator.close();
          this.currentIterator = open(currentTask);
          count += 1;
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

    this.currentIterator.close();

    long closeEndTime = System.currentTimeMillis();
    long duration = closeEndTime - constructorInitiationTime;
    LOG.info("total time taken in base reader: {}", duration);

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  private List<CompletableFuture<Object>> prefetchS3FileForTask(TaskT task) throws IOException {
    LOG.info(
        "fetching s3 file for task: {}, starting to download s3 files at time: {}",
        task,
        new Date());
    s3DownloadStartTime = System.currentTimeMillis();
    return referencedFiles(task)
        .map(this::toEncryptedInputFile)
        .map(file -> table.encryption().decrypt(file))
        .map(
            inputFile ->
                CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        downloadFileToHadoop(
                            inputFile,
                            task.asFileScanTask().start(),
                            task.asFileScanTask().length());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return null;
                    }))
        .collect(Collectors.toList());
  }

  private List<CompletableFuture<Object>> prefetchS3Files() throws IOException {
    Preconditions.checkNotNull(inputFiles().values(), "input files should be non-null");

    // copy tasksCache to a list and remove first task, so that we don't prefetch data for the first
    // task
    List<TaskT> copy = Lists.newArrayList(tasksCache);
    copy.remove(0);

    // get all unique files
    List<InputFile> inputFiles =
        copy.stream()
            .flatMap(this::referencedFiles)
            .map(this::toEncryptedInputFile)
            .map(file -> table.encryption().decrypt(file))
            .collect(Collectors.toList());
    Set<InputFile> uniqueFiles = new HashSet<>(inputFiles);

    return uniqueFiles.stream()
        .map(
            inputFile ->
                CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        downloadFileToHadoop(inputFile, 0, inputFile.getLength());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return null;
                    }))
        .collect(Collectors.toList());
  }

  private void downloadFileToHadoop(InputFile inputFile, long start, long length)
      throws IOException {
    LOG.info("supply async for input file: {}", inputFile.location());
    SeekableInputStream inputStream = inputFile.newStream();
    try {
      String filename = getFileName(inputFile.location());
      Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), table.name());
      try {
        Files.createDirectory(tempDir);
      } catch (FileAlreadyExistsException e) {
        LOG.info("temp directory already created locally");
      }
      Path path = Paths.get(tempDir.toString(), FileFormat.PARQUET.addExtension(filename));
      LOG.info("checking if file already exists at path: {}", path);
      if (fileIO.newInputFile(path.toString()).exists()
          && fileIO.newInputFile(path.toString()).getLength() == inputFile.getLength()) {
        LOG.info("file: {} was already created, and full length has been written, returning", path);
        this.s3ToLocal.put(inputFile.location(), path.toString());
        inputStream.close();
        return;
      } else if (fileIO.newInputFile(path.toString()).exists()
          && fileIO.newInputFile(path.toString()).getLength() < inputFile.getLength()) {
        // TODO: this is super naive, the other thread could have died, we might have to do this
        // more intelligently
        long localFilelength = fileIO.newInputFile(path.toString()).getLength() / (1024 * 1024);
        long dataToBeRead = (start + length) / (1024 * 1024);
        if (localFilelength >= dataToBeRead) {
          LOG.info(
              "file: {} was created and has data of size: {}mb written.. data needed for this task: {}mb is ready to be read, returning",
              path,
              localFilelength,
              dataToBeRead);
        } else {
          LOG.info(
              "file: {} was created, blocking till all of the data is written.. local file length: {}mb, data to be read: {}mb",
              path,
              localFilelength,
              dataToBeRead);
          while (true) {
            if (fileIO.newInputFile(path.toString()).getLength() < dataToBeRead) {
              Thread.sleep(500);
            } else {
              break;
            }
          }
        }
        this.s3ToLocal.put(inputFile.location(), path.toString());
        inputStream.close();
        return;
      } else {
        LOG.info("file does not exist, creating new one at: {}", path);
        OutputFile outputFile = fileIO.newOutputFile(path.toString());
        PositionOutputStream os = outputFile.createOrOverwrite();
        long s3WriteToDiskStartTime = System.currentTimeMillis();
        LOG.info("copying all of input stream to the locally created file at: {}", path);
        ByteStreams.copy(inputStream, os);
        LOG.info(
            "total time to write s3 file to local disk: {}",
            System.currentTimeMillis() - s3WriteToDiskStartTime);
        this.s3ToLocal.put(inputFile.location(), outputFile.location());
        inputStream.close();
        os.close();
      }
    } catch (IOException | URISyntaxException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      inputStream.close();
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
    LOG.info("filename: {} for file location: {}", filename, fileLocation);
    return filename;
  }

  protected InputFile getInputFile(String location) {
    if (count == 0) {
      return inputFiles().get(location);
    }
    if (!this.s3ToLocal.containsKey(location)) {
      LOG.info("printing hashmap keys:{}", this.s3ToLocal.keySet());
      throw new RuntimeException(String.format("s3 file doesnt exist :%s", location));
    }
    return fileIO.newInputFile(this.s3ToLocal.get(location));
  }

  private Map<String, InputFile> inputFiles() {
    if (lazyInputFiles == null) {
      Stream<EncryptedInputFile> encryptedFiles =
          taskGroup.tasks().stream().flatMap(this::referencedFiles).map(this::toEncryptedInputFile);

      // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
      Iterable<InputFile> decryptedFiles = table.encryption().decrypt(encryptedFiles::iterator);

      Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(taskGroup.tasks().size());
      decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
      this.lazyInputFiles = ImmutableMap.copyOf(files);
    }

    return lazyInputFiles;
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
