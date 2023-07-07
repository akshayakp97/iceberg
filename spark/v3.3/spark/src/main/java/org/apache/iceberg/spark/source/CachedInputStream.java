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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileRangeCache;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(CachedInputStream.class);
  FileRangeCache fileRangeCache;
  InputFile inputFile;
  SeekableInputStream delegate;
  RandomAccessFile randomAccessFile;
  long currentPos;

  // yikes, this is needed because s3inputstream always returns 0 for available
  boolean isReadFromDisk = false;
  ResolvingFileIO fileIO = new ResolvingFileIO();

  private static final long SLEEP_TIME = 1000;

  public CachedInputStream(FileRangeCache fileRangeCache, InputFile inputFile) {
    this.fileRangeCache = fileRangeCache;
    this.inputFile = inputFile;
    Map<String, String> properties = Maps.newHashMap();
    fileIO.initialize(properties);
    fileIO.setConf(new Configuration());
  }

  @Override
  public long getPos() throws IOException {
    if (delegate == null) {
      throw new RuntimeException("currentInputStream wasn't set?");
    }
    return delegate.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    LOG.info("CachingInputStream.. seeking position: {}", newPos);
    boolean flag = true;
    while (flag) {
      try {
        switch (fileRangeCache.getCacheType(inputFile.location(), newPos, inputFile.getLength())) {
          case MEMORY:
            delegate = inputFile.newStream();
            delegate.seek(newPos);
            isReadFromDisk = false;
            break;
          default:
            randomAccessFile =
                new RandomAccessFile(
                    String.valueOf(fileRangeCache.getCachedFilePath(inputFile.location())), "r");
            randomAccessFile.seek(newPos);
            isReadFromDisk = true;
        }
        flag = false;
      } catch (EOFException ignored) {
        delegate.close();
        if (randomAccessFile != null) {
          randomAccessFile.close();
        }
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
    currentPos += 1;
    return delegate.read();
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
    if (!isReadFromDisk
        && currentPos >= fileRangeCache.getCacheStartPositionForFile(inputFile.location())) {
      byte[] cachedData = fileRangeCache.getCachedFooter(inputFile.location());
      int sourcePosition = 0;
      if (currentPos > fileRangeCache.getCacheStartPositionForFile(inputFile.location())) {
        sourcePosition =
            (int) (currentPos - fileRangeCache.getCacheStartPositionForFile(inputFile.location()));
      }
      System.arraycopy(cachedData, sourcePosition, b, 0, b.length);
      return b.length;
    }

    LOG.info("returning after reading: {} bytes", len);
    return randomAccessFile.read(b, off, len);
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
