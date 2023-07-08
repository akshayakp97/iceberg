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
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.iceberg.CacheType;
import org.apache.iceberg.CachedFileNameResolver;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileRangeCache;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
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
import org.apache.parquet.bytes.BytesUtils;
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
  private long s3DownloadStartTime;
  private long nextMethodStartTime;
  private Map<String, String> s3ToLocal;
  private long s3DownloadEndTime;
  private T current = null;
  private TaskT currentTask = null;
  private int count = 0;
  private final Collection<TaskT> tasksCache = Lists.newArrayList();
  private List<CompletableFuture<Object>> completableFutureList;

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

    FileRangeCache cache = new FileRangeCache(table);
    for (TaskT task : tasksCache) {
      String path = task.asFileScanTask().file().path().toString();
      InputFile inputFile = inputFiles().get(path);
      cache.putIfAbsent(
          path, task.asFileScanTask().start(), task.asFileScanTask().length(), CacheType.DISK);
      // footer bytes to be read from memory
      cache.putIfAbsent(path, inputFile.getLength() - 8, inputFile.getLength(), CacheType.MEMORY);
      // meta data index to be read from memory
      SeekableInputStream is = inputFile.newStream();
      int fileMetadataLength;
      try {
        is.seek(inputFile.getLength() - 8);
        fileMetadataLength = BytesUtils.readIntLittleEndian(is);
        if (!cache.doesFileExistInCache(inputFile.location())) {
          long fileMetadataIdx = inputFile.getLength() - 8 - fileMetadataLength;
          is.seek(fileMetadataIdx);
          byte[] cacheArr = new byte[fileMetadataLength + 8];
          IOUtil.readFully(is, cacheArr, 0, fileMetadataLength + 8);
          cache.setupCache(inputFile, cacheArr);
        }
        is.close();
      } catch (IOException e) {
        throw new RuntimeException("could not open input file to get metadata length");
      }
      // meta data start index should be read from memory
      cache.putIfAbsent(
          path,
          inputFile.getLength() - fileMetadataLength - 8,
          inputFile.getLength(),
          CacheType.MEMORY);
      task.asFileScanTask().setCache(cache);
    }

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

    try {
      LOG.info("attempting to download all files for the task group at: {}", new Date());
      completableFutureList = prefetchS3Files();
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
      while (true) {
        if (currentIterator.hasNext()) {
          this.current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          this.currentTask = tasks.next();
          LOG.info("reading data for the task...: {} at {}", this.currentTask, new Date());
          if (count >= 1) {
            // we skip prefetch for first task
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
    LOG.info("returning record: {}, at: {}", current, new Date());
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
    completableFutureList.stream().map(CompletableFuture::join).collect(Collectors.toList());
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

    // remove first task, since we skip prefetch for first task
    List<TaskT> copy = Lists.newArrayList(tasksCache);
    copy.remove(0);

    // get all unique files
    List<InputFile> inputFiles =
        copy.stream()
            .flatMap(this::referencedFiles)
            .map(this::toEncryptedInputFile)
            .map(file -> table.encryption().decrypt(file))
            .collect(Collectors.toList());

    Set<String> uniqueLocations = new HashSet<>();

    Set<InputFile> uniqueFiles =
        inputFiles.stream()
            .filter(
                inputFile -> {
                  if (uniqueLocations.contains(inputFile.location())) {
                    return false;
                  } else {
                    uniqueLocations.add(inputFile.location());
                    return true;
                  }
                })
            .collect(Collectors.toSet());

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
    long dataToBeRead = (start + length) / (1024 * 1024);
    LOG.info(
        "downloading input file: {} locally..total data to be read: {}mb",
        inputFile.location(),
        dataToBeRead);
    SeekableInputStream inputStream = inputFile.newStream();
    try {
      try {
        Files.createDirectory(CachedFileNameResolver.getCacheDirectoryPath(table));
      } catch (FileAlreadyExistsException e) {
        LOG.debug("temp directory already created locally");
      }
      Path path = CachedFileNameResolver.getCacheFileURI(table, inputFile.location());
      LOG.info("checking if file already exists at path: {}", path);
      File localFile = new File(path.toString());
      if (localFile.exists() && localFile.length() == inputFile.getLength()) {
        LOG.info("file: {} was already created, and full length has been written, returning", path);
        this.s3ToLocal.put(inputFile.location(), path.toString());
        inputStream.close();
        return;
      } else if (localFile.exists() && localFile.length() < inputFile.getLength()) {
        // TODO: this might block forever if the other thread died, setup timeout
        long localFilelength = localFile.length() / (1024 * 1024);
        if (localFilelength >= dataToBeRead) {
          LOG.info(
              "file: {} was created and has data of size: {}mb written.. data needed for this task: {}mb is ready to be read, returning",
              path,
              localFilelength,
              dataToBeRead);
        } else {
          long startOfData = start / (1024 * 1024);
          LOG.info(
              "file: {} was created, blocking till start of the data is written.. local file length: {}mb, start of data to be read: {}mb",
              path,
              localFilelength,
              startOfData);
          while (true) {
            localFilelength = localFile.length() / (1024 * 1024);
            if (localFilelength < startOfData) {
              Thread.sleep(50);
            } else {
              LOG.info("start of data write complete at : {}", new Date());
              break;
            }
          }
        }
        this.s3ToLocal.put(inputFile.location(), path.toString());
        inputStream.close();
        return;
      } else {
        LOG.info("file does not exist, creating new one at: {}", path);
        FileOutputStream os = new FileOutputStream(path.toString());
        long s3WriteToDiskStartTime = System.currentTimeMillis();
        LOG.info("copying all of input stream to the locally created file at: {}", path);
        ByteStreams.copy(inputStream, os);
        LOG.info(
            "total time to write s3 file to local disk: {}",
            System.currentTimeMillis() - s3WriteToDiskStartTime);
        this.s3ToLocal.put(inputFile.location(), path.toString());
        inputStream.close();
        os.close();
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      inputStream.close();
    }
  }

  // not sure where this method is used tbh..
  protected InputFile getInputFile(String location) {
    if (count == 0) {
      return inputFiles().get(location);
    }
    if (!this.s3ToLocal.containsKey(location)) {
      LOG.info("printing hashmap keys:{}", this.s3ToLocal.keySet());
      throw new RuntimeException(String.format("s3 file doesnt exist :%s", location));
    }
    return table.io().newInputFile(this.s3ToLocal.get(location));
  }

  protected InputFile getCachedInputFile(FileScanTask task, String location) {
    if (count == 0) {
      // for first task, we don't want to prefetch, so return the original input file
      return inputFiles().get(location);
    }
    if (!this.s3ToLocal.containsKey(location)) {
      LOG.info("file doesn't exist locally, printing hashmap keys: {}", this.s3ToLocal.keySet());
      throw new RuntimeException(String.format("s3 file doesnt exist :%s", location));
    }
    return new CachedInputFile(task.getCache(), inputFiles().get(location));
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
