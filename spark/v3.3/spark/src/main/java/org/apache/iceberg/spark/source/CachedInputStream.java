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

import java.io.IOException;
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
  long currentPos;

  public CachedInputStream(FileRangeCache fileRangeCache, InputFile inputFile) {
    this.fileRangeCache = fileRangeCache;
    this.inputFile = inputFile;
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
    switch (fileRangeCache.getCacheType(inputFile.location(), newPos, inputFile.getLength())) {
      case MEMORY:
        delegate = inputFile.newStream();
        break;
      default:
        ResolvingFileIO fileIO = new ResolvingFileIO();
        Map<String, String> properties = Maps.newHashMap();
        fileIO.initialize(properties);
        fileIO.setConf(new Configuration());
        delegate =
            fileIO
                .newInputFile(fileRangeCache.getCachedFilePath(inputFile.location()).toString())
                .newStream();
    }
    currentPos = newPos;
    delegate.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    currentPos += 1;
    return delegate.read();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    // return cached data here
    if (currentPos >= fileRangeCache.getCacheStartPositionForFile(inputFile.location())) {
      byte[] cachedData = fileRangeCache.getCachedFooter(inputFile.location());
      int sourcePosition = 0;
      if (currentPos > fileRangeCache.getCacheStartPositionForFile(inputFile.location())) {
        sourcePosition =
            (int) (currentPos - fileRangeCache.getCacheStartPositionForFile(inputFile.location()));
      }
      System.arraycopy(cachedData, sourcePosition, b, 0, b.length);
      return b.length;
    }
    return delegate.read(b, off, len);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    ((RangeReadable) delegate).readFully(position, buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    return ((RangeReadable) delegate).readTail(buffer, offset, length);
  }
}
