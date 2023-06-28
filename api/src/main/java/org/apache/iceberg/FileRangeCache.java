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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRangeCache {
  private static final Logger LOG = LoggerFactory.getLogger(FileRangeCache.class);
  Table table;
  Map<String, CacheType> map = Maps.newHashMap();

  public FileRangeCache(Table table) {
    this.table = table;
  }

  public void putIfAbsent(String path, long start, long length, CacheType cacheType) {
    String key = path + "_" + start + "_" + length;
    map.putIfAbsent(key, cacheType);
  }

  public CacheType get(String path, long start, long length) {
    String key = path + "_" + start + "_" + length;
    if (!map.containsKey(key)) {
      // return disk by default;
      return CacheType.DISK;
    }
    for (String k : map.keySet()) {
      if (k.contains(path)) {
        LOG.info("key and value for this file in the hash map: {}, {}", k, map.get(k));
      }
    }
    return map.get(key);
  }

  // this is common code with BaseReader
  public String getLocalFileName(String s3Filename) {
    String filename;
    try {
      filename = getFileName(s3Filename);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), table.name());
    Path path = Paths.get(tempDir.toString(), FileFormat.PARQUET.addExtension(filename));
    return path.toString();
  }

  private String getFileName(String fileLocation) throws URISyntaxException {
    URI uri = new URI(fileLocation);
    String path = uri.normalize().getPath();
    int idx = path.lastIndexOf("/");
    String filename = path;
    if (idx >= 0) {
      filename = path.substring(idx + 1, path.length());
    }
    return filename;
  }
}
