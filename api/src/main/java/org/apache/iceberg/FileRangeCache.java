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
package org.apache.iceberg;

import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class FileRangeCache {
  Table table;
  Map<String, CacheType> byteRangeToCacheType = Maps.newHashMap();

  Map<String, Long> fileToCacheStartPosition = Maps.newHashMap();
  Map<String, Long> fileToCacheSize = Maps.newHashMap();

  Map<String, byte[]> fileToCache = Maps.newHashMap();

  public FileRangeCache(Table table) {
    this.table = table;
  }

  public void putIfAbsent(String path, long start, long length, CacheType cacheType) {
    String key = getKey(path, start, length);
    byteRangeToCacheType.putIfAbsent(key, cacheType);
    if (cacheType.equals(CacheType.MEMORY)) {
      fileToCacheStartPosition.put(path, start);
      fileToCacheSize.put(path, length - start);
    }
  }

  public CacheType getCacheType(String path, long start) {
    if (fileToCacheStartPosition.containsKey(path)) {
      if (fileToCacheStartPosition.get(path) <= start) {
        return CacheType.MEMORY;
      }
    }
    return CacheType.DISK;
  }

  public void setupCache(InputFile inputFile, byte[] cache) {
    fileToCache.put(inputFile.location(), cache);
  }

  public Long getCacheStartPositionForFile(String path) {
    return fileToCacheStartPosition.get(path);
  }

  public Boolean doesFileExistInCache(String path) {
    return fileToCache.containsKey(path);
  }

  public byte[] getCachedFooter(String path) {
    if (!fileToCache.containsKey(path)) {
      throw new RuntimeException(String.format("cache not found for file: %s", path));
    }
    return fileToCache.get(path);
  }

  private String getKey(String path, long start, long length) {
    return path + "_" + start + "_" + length;
  }

  public Path getCachedFilePath(String inputFileURI) {
    return CachedFileNameResolver.getCacheFileURI(table, inputFileURI);
  }
}
