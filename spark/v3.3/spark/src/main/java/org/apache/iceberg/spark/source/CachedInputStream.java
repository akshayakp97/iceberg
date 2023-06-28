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
  SeekableInputStream currentInputStream;

  public CachedInputStream(FileRangeCache fileRangeCache, InputFile inputFile) {
    this.fileRangeCache = fileRangeCache;
    this.inputFile = inputFile;
  }

  @Override
  public long getPos() throws IOException {
    if (currentInputStream == null) {
      throw new RuntimeException("currentInputStream wasn't set?");
    }
    return currentInputStream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    LOG.info("CachingInputStream.. seeking position: {}", newPos);
    switch (fileRangeCache.get(inputFile.location(), newPos, inputFile.getLength())) {
      case MEMORY:
        currentInputStream = inputFile.newStream();
        break;
      default:
        ResolvingFileIO fileIO = new ResolvingFileIO();
        Map<String, String> properties = Maps.newHashMap();
        fileIO.initialize(properties);
        fileIO.setConf(new Configuration());
        currentInputStream =
            fileIO.newInputFile(fileRangeCache.getLocalFileName(inputFile.location())).newStream();
    }
    currentInputStream.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    return currentInputStream.read();
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    ((RangeReadable) currentInputStream).readFully(position, buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    return ((RangeReadable) currentInputStream).readTail(buffer, offset, length);
  }
}
