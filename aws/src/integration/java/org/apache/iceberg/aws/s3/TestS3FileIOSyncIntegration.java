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

import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class TestS3FileIOSyncIntegration extends TestS3FileIOIntegrationBase {
  private static S3Client s3;

  @BeforeClass
  public static void beforeClass() {
    initialize();
    s3 = s3(true);
  }

  @AfterClass
  public static void afterClass() {
    cleanup();
    s3.close();
  }

  private static S3Client s3(boolean newClient) {
    if (newClient) {
      return clientFactory().s3();
    } else {
      return s3;
    }
  }

  @Override
  protected S3FileIO newS3FileIO(S3FileIOAwsClientFactory s3FileIOAwsClientFactory, S3FileIOProperties s3FileIOProperties) {
    s3FileIOProperties.setAsyncClientEnabled(false);
    return new S3FileIO(s3FileIOAwsClientFactory, s3FileIOProperties);
  }

  @Override
  protected void putObject(PutObjectRequest request, byte[] payload, boolean newClient) {
    s3(newClient).putObject(request, RequestBody.fromBytes(payload));
  }

  @Override
  protected ResponseInputStream<GetObjectResponse> getObject(
      GetObjectRequest request, boolean newClient) {
    return s3(newClient).getObject(request);
  }

  @Override
  protected GetObjectAclResponse getObjectAcl(GetObjectAclRequest request, boolean newClient) {
    return s3(newClient).getObjectAcl(request);
  }

  @Override
  protected void createRandomObjects(String objectPrefix, int count) {
    S3URI s3URI = new S3URI(objectPrefix);
    random()
        .ints(count)
        .parallel()
        .forEach(
            i ->
                s3.putObject(
                    builder -> builder.bucket(s3URI.bucket()).key(s3URI.key() + i).build(),
                    RequestBody.empty()));
  }
}
