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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.io.IOUtils;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


public class TestS3AsyncInputStream {
  @Mock
  S3AsyncClient s3;
  @Mock
  CompletableFuture<Object> getObjectResponseCompletableFuture;
  private final Random random = new Random(1);

  @Before
  public void before() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testRead() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);
    InputStream empty = new InputStream() {

      @Override
      public int read() throws IOException {
        return 0;
      }
    };

    doReturn(getObjectResponseCompletableFuture).when(s3).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    doReturn(new ResponseInputStream<>(GetObjectResponse.builder().build(), AbortableInputStream.create(empty))).when(getObjectResponseCompletableFuture).join();

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

  //    @Test
  //    public void testRangeRead() throws Exception {
  //        S3URI uri = new S3URI("s3://bucket/path/to/range-read.dat");
  //        int dataSize = 1024 * 1024 * 10;
  //        byte[] expected = randomData(dataSize);
  //        byte[] actual = new byte[dataSize];
  //
  //        long position;
  //        int offset;
  //        int length;
  //
  //
  //        try (RangeReadable in = new S3AsyncInputStream(s3, uri)) {
  //            // first 1k
  //            position = 0;
  //            offset = 0;
  //            length = 1024;
  //            readAndCheckRanges(in, expected, position, actual, offset, length);
  //
  //            // last 1k
  //            position = dataSize - 1024;
  //            offset = dataSize - 1024;
  //            readAndCheckRanges(in, expected, position, actual, offset, length);
  //
  //            // middle 2k
  //            position = dataSize / 2 - 1024;
  //            offset = dataSize / 2 - 1024;
  //            length = 1024 * 2;
  //            readAndCheckRanges(in, expected, position, actual, offset, length);
  //        }
  //    }
  //
  //    private void readAndCheckRanges(
  //            RangeReadable in, byte[] original, long position, byte[] buffer, int offset, int
  // length)
  //            throws IOException {
  //        in.readFully(position, buffer, offset, length);
  //
  //        assertArrayEquals(
  //                Arrays.copyOfRange(original, offset, offset + length),
  //                Arrays.copyOfRange(buffer, offset, offset + length));
  //    }
  //
  //    @Test
  //    public void testClose() throws Exception {
  //        S3URI uri = new S3URI("s3://bucket/path/to/closed.dat");
  //        SeekableInputStream closed = new S3AsyncInputStream(s3, uri);
  //        closed.close();
  //        assertThrows(IllegalStateException.class, () -> closed.seek(0));
  //    }
  //
  //    @Test
  //    public void testSeek() throws Exception {
  //        S3URI uri = new S3URI("s3://bucket/path/to/seek.dat");
  //        byte[] expected = randomData(1024 * 1024);
  //
  //        try (SeekableInputStream in = new S3AsyncInputStream(s3, uri)) {
  //            in.seek(expected.length / 2);
  //            byte[] actual = IOUtils.readFully(in, expected.length / 2);
  //            assertArrayEquals(Arrays.copyOfRange(expected, expected.length / 2,
  // expected.length), actual);
  //        }
  //    }
  //
  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }
}
