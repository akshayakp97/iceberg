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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletionException;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.AttributeMap;

public class TestS3AsyncInputStream {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private S3AsyncClient s3;
  private final Random random = new Random(1);

  @Before
  public void before() {
    s3 =
        S3AsyncClient.builder()
            .region(Region.of("us-east-1"))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
            .endpointOverride(URI.create(S3_MOCK_RULE.getServiceEndpoint()))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .httpClient(
                NettyNioAsyncHttpClient.builder()
                    .buildWithDefaults(
                        AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
            .build();
    createBucket("bucket");
  }

  @Test
  public void testRead() throws Exception {

    S3URI uri = new S3URI("s3://bucket/path/to/read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeS3Data(uri, data);

    try (SeekableInputStream in = new S3AsyncInputStream(s3, uri)) {
      int readSize = 1024;
      byte[] actual = new byte[readSize];

      readAndCheck(in, in.getPos(), readSize, data, false);
      readAndCheck(in, in.getPos(), readSize, data, true);

      // Seek forward in current stream
      int seekSize = 1024;
      readAndCheck(in, in.getPos() + seekSize, readSize, data, false);
      readAndCheck(in, in.getPos() + seekSize, readSize, data, true);

      // Buffered read
      readAndCheck(in, in.getPos(), readSize, data, true);
      readAndCheck(in, in.getPos(), readSize, data, false);

      // Seek with new stream
      long seekNewStreamPosition = 2 * 1024 * 1024;
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, true);
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, false);

      // Backseek and read
      readAndCheck(in, 0, readSize, data, true);
      readAndCheck(in, 0, readSize, data, false);
    }
  }

  private void readAndCheck(
      SeekableInputStream in, long rangeStart, int size, byte[] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    assertEquals(rangeStart, in.getPos());

    long rangeEnd = rangeStart + size;
    byte[] actual = new byte[size];

    if (buffered) {
      IOUtils.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    assertEquals(rangeEnd, in.getPos());
    assertArrayEquals(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd), actual);
  }

  @Test
  public void testRangeRead() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/range-read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] expected = randomData(dataSize);
    byte[] actual = new byte[dataSize];

    long position;
    int offset;
    int length;

    writeS3Data(uri, expected);

    try (RangeReadable in = new S3AsyncInputStream(s3, uri)) {
      // first 1k
      position = 0;
      offset = 0;
      length = 1024;
      readAndCheckRanges(in, expected, position, actual, offset, length);

      // last 1k
      position = dataSize - 1024;
      offset = dataSize - 1024;
      readAndCheckRanges(in, expected, position, actual, offset, length);

      // middle 2k
      position = dataSize / 2 - 1024;
      offset = dataSize / 2 - 1024;
      length = 1024 * 2;
      readAndCheckRanges(in, expected, position, actual, offset, length);
    }
  }

  private void readAndCheckRanges(
      RangeReadable in, byte[] original, long position, byte[] buffer, int offset, int length)
      throws IOException {
    in.readFully(position, buffer, offset, length);

    assertArrayEquals(
        Arrays.copyOfRange(original, offset, offset + length),
        Arrays.copyOfRange(buffer, offset, offset + length));
  }

  @Test
  public void testClose() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/closed.dat");
    SeekableInputStream closed = new S3AsyncInputStream(s3, uri);
    closed.close();
    assertThrows(IllegalStateException.class, () -> closed.seek(0));
  }

  @Test
  public void testSeek() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/seek.dat");
    byte[] expected = randomData(1024 * 1024);

    writeS3Data(uri, expected);

    try (SeekableInputStream in = new S3AsyncInputStream(s3, uri)) {
      in.seek(expected.length / 2);
      byte[] actual = IOUtils.readFully(in, expected.length / 2);
      assertArrayEquals(Arrays.copyOfRange(expected, expected.length / 2, expected.length), actual);
    }
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void createBucket(String bucketName) {
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build()).join();
    } catch (CompletionException e) {
      if (e.getCause().getClass().equals(BucketAlreadyExistsException.class)) {
        // do nothing
        return;
      }
      throw e;
    }
  }

  private void writeS3Data(S3URI uri, byte[] data) throws IOException {
    s3.putObject(
            PutObjectRequest.builder()
                .bucket(uri.bucket())
                .key(uri.key())
                .contentLength((long) data.length)
                .build(),
            AsyncRequestBody.fromBytes(data))
        .join();
  }
}
