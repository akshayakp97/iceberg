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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper that stores the mapping between the locally created file for the s3 input file. */
public class CachedFileNameResolver {
  private static final Logger LOG = LoggerFactory.getLogger(CachedFileNameResolver.class);
  private static final Path LOCAL_TEMP_DIR_ROOT = Paths.get(System.getProperty("java.io.tmpdir"));

  public static Path getCacheDirectoryPath(Table table) {
    return Paths.get(LOCAL_TEMP_DIR_ROOT.toString(), table.name());
  }

  public static Path getCacheFileURI(Table table, String remoteFileURI) {
    String filename = "";
    try {
      filename = getFileNameFromURI(remoteFileURI);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return Paths.get(getCacheDirectoryPath(table).toString(), filename);
  }

  private static String getFileNameFromURI(String remoteFileURI) throws URISyntaxException {
    URI uri = new URI(remoteFileURI);
    String path = uri.normalize().getPath();
    int idx = path.lastIndexOf("/");
    String filename = path;
    if (idx >= 0) {
      filename = path.substring(idx + 1, path.length());
    }
    LOG.info("filename: {} for file location: {}", filename, remoteFileURI);
    return filename;
  }
}
