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

import static java.lang.Thread.sleep;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.iceberg.CacheType;
import org.apache.iceberg.FileRangeCache;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(CachedInputStream.class);
  FileRangeCache fileRangeCache;
  InputFile inputFile;
  SeekableInputStream delegate;
  RandomAccessFile randomAccessFile;
  long currentPos;
  boolean isReadFromDisk = false;
  private static final long SLEEP_TIME = 500;

  public CachedInputStream(FileRangeCache fileRangeCache, InputFile inputFile) {
    this.fileRangeCache = fileRangeCache;
    this.inputFile = inputFile;
  }

  @Override
  public long getPos() throws IOException {
    return randomAccessFile.getFilePointer();
  }

  @Override
  public void seek(long newPos) throws IOException {
    LOG.info("CachingInputStream.. seeking position: {}", newPos);
    boolean flag = true;
    while (flag) {
      try {
        if (fileRangeCache.getCacheType(inputFile.location(), newPos).equals(CacheType.DISK)) {
          randomAccessFile =
              new RandomAccessFile(
                  String.valueOf(fileRangeCache.getCachedFilePath(inputFile.location())), "r");
          randomAccessFile.seek(newPos);
          isReadFromDisk = true;
        }
        flag = false;
      } catch (EOFException ignored) {
        randomAccessFile.close();
        try {
          sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
          // do nothing
        }
      }
    }
    currentPos = newPos;
    LOG.info("returning from seek at postition: {}", newPos);
  }

  @Override
  public int read() throws IOException {
    LOG.info("at cachedinput stream read without params");
    if (isReadFromDisk) {
      currentPos += 1;
      return randomAccessFile.read();
    }
    // return one byte from cache
    byte[] cachedData = fileRangeCache.getCachedFooter(inputFile.location());
    long cacheStartPos = fileRangeCache.getCacheStartPositionForFile(inputFile.location());
    int bytePos = (int) (currentPos - cacheStartPos);
    currentPos += 1;
    // read() method of an InputStream returns converts the byte to an int in range (-1, 255)
    return Byte.toUnsignedInt(cachedData[bytePos]);
  }

  // We need to somehow detect that this file has not been fully written and block here
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    LOG.info("at cachedinput stream read");
    boolean flag = true;
    while (flag && isReadFromDisk) {
      if (randomAccessFile.length() > len + randomAccessFile.getFilePointer()) {
        LOG.info(
            "local available: {}, to read: {}, left to read: {}",
            randomAccessFile.length(),
            len + randomAccessFile.getFilePointer(),
            (len + randomAccessFile.getFilePointer()) - randomAccessFile.length());
        flag = false;
      } else {
        try {
          sleep(SLEEP_TIME);
        } catch (InterruptedException ignored) {
          LOG.error("got exception while sleeping: {}", ignored);
        }
      }
    }

    // return cached data here
    if (!isReadFromDisk) {
      return readFromCache(b);
    }

    LOG.info("returning after reading: {} bytes", len);
    int bytesRead = randomAccessFile.read(b, off, len);
    currentPos += bytesRead;
    return bytesRead;
  }

  private int readFromCache(byte b[]) {
    if (!isReadFromDisk
        && currentPos >= fileRangeCache.getCacheStartPositionForFile(inputFile.location())) {
      byte[] cachedData = fileRangeCache.getCachedFooter(inputFile.location());
      int sourcePosition = 0;
      if (currentPos > fileRangeCache.getCacheStartPositionForFile(inputFile.location())) {
        sourcePosition =
            (int) (currentPos - fileRangeCache.getCacheStartPositionForFile(inputFile.location()));
      }
      System.arraycopy(cachedData, sourcePosition, b, 0, b.length);
    }
    return b.length;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    LOG.info("at ready fully");
    ((RangeReadable) delegate).readFully(position, buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    LOG.info("at read tail");
    return ((RangeReadable) delegate).readTail(buffer, offset, length);
  }
}
