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
package org.apache.iceberg.aws.s3;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.S3FileIOAwsClientFactories;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * FileIO implementation backed by S3.
 *
 * <p>Locations used must follow the conventions for S3 URIs (e.g. s3://bucket/path...). URIs with
 * schemes s3a, s3n, https are also treated as s3 file paths. Using this FileIO with other schemes
 * will result in {@link org.apache.iceberg.exceptions.ValidationException}.
 */
public class S3FileIO extends S3FileIOBase {
  private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);
  private S3FileIOBase delegate;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link S3FileIO#initialize(Map)} later.
   */
  public S3FileIO() {}

  /**
   * Constructor with custom S3 FileIO client factory and default S3 FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param clientFactory S3 FileIO AWS client factory to get S3 client
   */
  public S3FileIO(S3FileIOAwsClientFactory clientFactory) {
    this(clientFactory, new S3FileIOProperties());
  }

  /**
   * Constructor with custom s3 supplier and S3FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @deprecated Use S3FileIO(AwsClientFactory)
   * @param s3 S3Client factory
   */
  @Deprecated
  public S3FileIO(SerializableSupplier<S3Client> s3) {
    this(s3, new S3FileIOProperties());
  }

  /**
   * Creates a S3FileIO instance backed by the S3 synchronous client.
   *
   * @deprecated Use S3FileIO(S3FileIOAwsClientFactory, S3FileIOProperties)
   * @param s3 S3Client factory
   */
  @Deprecated
  public S3FileIO(SerializableSupplier<S3Client> s3, S3FileIOProperties s3FileIOProperties) {
    this.delegate = new S3SyncFileIO(s3, s3FileIOProperties);
  }

  /**
   * Constructor with custom s3 supplier and S3 FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3FileIOAwsClientFactory S3FileIOAwsClientFactory to vend S3 client
   * @param s3FileIOProperties S3 FileIO properties
   */
  public S3FileIO(S3FileIOAwsClientFactory s3FileIOAwsClientFactory, S3FileIOProperties s3FileIOProperties) {
    initializeDelegate(s3FileIOAwsClientFactory, s3FileIOProperties);
  }

  @Override
  public void initialize(Map<String, String> props) {
    final S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(props);
    Object clientFactory = S3FileIOAwsClientFactories.initialize(props);
    initializeDelegate(clientFactory, s3FileIOProperties);
    delegate().initialize(props);
  }


  private void initializeDelegate(Object clientFactory, S3FileIOProperties s3FileIOProperties) {
    if (this.delegate != null) {
      return;
    }
    if (s3FileIOProperties.isAsyncClientEnabled()) {
      LOG.info("Using S3AsyncClient for S3FileIO");
    } else {
      LOG.info("Using S3Client (sync) for S3FileIO");
    }
    if (clientFactory instanceof S3FileIOAwsClientFactory) {
      this.delegate = getDelegate((S3FileIOAwsClientFactory) clientFactory, s3FileIOProperties);
    }
    if (clientFactory instanceof AwsClientFactory) {
      this.delegate = getDelegate((AwsClientFactory) clientFactory, s3FileIOProperties);
    }
  }

  private S3FileIOBase getDelegate(S3FileIOAwsClientFactory clientFactory, S3FileIOProperties s3FileIOProperties) {
    if (s3FileIOProperties.isAsyncClientEnabled()) {
      return new S3AsyncFileIO(clientFactory::s3Async, s3FileIOProperties);
    }
    return new S3SyncFileIO(clientFactory::s3, s3FileIOProperties);
  }

  private S3FileIOBase getDelegate(AwsClientFactory clientFactory, S3FileIOProperties s3FileIOProperties) {
    if (s3FileIOProperties.isAsyncClientEnabled()) {
      return new S3AsyncFileIO(clientFactory::s3Async, s3FileIOProperties);
    }
    return new S3SyncFileIO(clientFactory::s3, s3FileIOProperties);
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return delegate.newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    delegate.deleteFiles(paths);
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return delegate.listPrefix(prefix);
  }

  /**
   * This method provides a "best-effort" to delete all objects under the given prefix.
   *
   * <p>Bulk delete operations are used and no reattempt is made for deletes if they fail, but will
   * log any individual objects that are not deleted as part of the bulk operation.
   *
   * @param prefix prefix to delete
   */
  @Override
  public void deletePrefix(String prefix) {
    delegate().deletePrefix(prefix);
  }

  @Override
  public String getCredential() {
    return delegate().getCredential();
  }

  @Override
  public void close() {
    delegate().close();
  }

  private S3FileIOBase delegate() {
    if (delegate != null) {
      return this.delegate;
    } else {
      throw new RuntimeException("S3FileIO has not been initialized");
    }
  }
}