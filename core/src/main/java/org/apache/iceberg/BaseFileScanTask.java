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

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseFileScanTask extends BaseContentScanTask<FileScanTask, DataFile>
    implements FileScanTask {
  private final DeleteFile[] deletes;

  public BaseFileScanTask(
      DataFile file,
      DeleteFile[] deletes,
      String schemaString,
      String specString,
      ResidualEvaluator residuals) {
    super(file, schemaString, specString, residuals);
    this.deletes = deletes != null ? deletes : new DeleteFile[0];
  }

  @Override
  protected FileScanTask self() {
    return this;
  }

  @Override
  protected FileScanTask newSplitTask(FileScanTask parentTask, long offset, long length) {
    return new SplitScanTask(offset, length, parentTask);
  }

  @Override
  public List<DeleteFile> deletes() {
    return ImmutableList.copyOf(deletes);
  }

  @Override
  public Schema schema() {
    return super.schema();
  }

  @VisibleForTesting
  static final class SplitScanTask implements FileScanTask, MergeableScanTask<SplitScanTask> {
    private final long len;
    private final long offset;
    private final FileScanTask fileScanTask;

    SplitScanTask(long offset, long len, FileScanTask fileScanTask) {
      this.offset = offset;
      this.len = len;
      this.fileScanTask = fileScanTask;
    }

    @Override
    public DataFile file() {
      return fileScanTask.file();
    }

    @Override
    public List<DeleteFile> deletes() {
      return fileScanTask.deletes();
    }

    @Override
    public Schema schema() {
      return fileScanTask.schema();
    }

    @Override
    public PartitionSpec spec() {
      return fileScanTask.spec();
    }

    @Override
    public long start() {
      return offset;
    }

    @Override
    public long length() {
      return len;
    }

    @Override
    public long estimatedRowsCount() {
      return BaseContentScanTask.estimateRowsCount(len, fileScanTask.file());
    }

    @Override
    public Expression residual() {
      return fileScanTask.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      throw new UnsupportedOperationException("Cannot split a task which is already split");
    }

    @Override
    public boolean canMerge(ScanTask other) {
      if (other instanceof SplitScanTask) {
        SplitScanTask that = (SplitScanTask) other;
        return file().equals(that.file()) && offset + len == that.start();
      } else {
        return false;
      }
    }

    @Override
    public SplitScanTask merge(ScanTask other) {
      SplitScanTask that = (SplitScanTask) other;
      return new SplitScanTask(offset, len + that.length(), fileScanTask);
    }
  }
}
