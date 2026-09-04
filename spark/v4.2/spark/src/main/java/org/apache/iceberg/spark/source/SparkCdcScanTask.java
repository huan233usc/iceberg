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

import java.util.List;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

abstract class SparkCdcScanTask implements ChangelogScanTask, ContentScanTask<DataFile> {
  private final FileScanTask task;
  private final int changeOrdinal;
  private final long commitSnapshotId;

  private SparkCdcScanTask(
      FileScanTask task, int changeOrdinal, long commitSnapshotId) {
    this.task = task;
    this.changeOrdinal = changeOrdinal;
    this.commitSnapshotId = commitSnapshotId;
  }

  @Override
  public int changeOrdinal() {
    return changeOrdinal;
  }

  @Override
  public long commitSnapshotId() {
    return commitSnapshotId;
  }

  @Override
  public DataFile file() {
    return task.file();
  }

  @Override
  public PartitionSpec spec() {
    return task.spec();
  }

  @Override
  public long start() {
    return task.start();
  }

  @Override
  public long length() {
    return task.length();
  }

  @Override
  public Expression residual() {
    return task.residual();
  }

  static class AddedRows extends SparkCdcScanTask implements AddedRowsScanTask {
    private final List<DeleteFile> deletes;

    AddedRows(
        FileScanTask task,
        int changeOrdinal,
        long commitSnapshotId,
        List<DeleteFile> deletes) {
      super(task, changeOrdinal, commitSnapshotId);
      this.deletes = ImmutableList.copyOf(deletes);
    }

    @Override
    public List<DeleteFile> deletes() {
      return deletes;
    }
  }

  static class DeletedDataFile extends SparkCdcScanTask
      implements DeletedDataFileScanTask {
    private final List<DeleteFile> existingDeletes;

    DeletedDataFile(
        FileScanTask task,
        int changeOrdinal,
        long commitSnapshotId,
        List<DeleteFile> existingDeletes) {
      super(task, changeOrdinal, commitSnapshotId);
      this.existingDeletes = ImmutableList.copyOf(existingDeletes);
    }

    @Override
    public List<DeleteFile> existingDeletes() {
      return existingDeletes;
    }
  }

  static class DeletedRows extends SparkCdcScanTask implements DeletedRowsScanTask {
    private final List<DeleteFile> addedDeletes;
    private final List<DeleteFile> existingDeletes;

    DeletedRows(
        FileScanTask task,
        int changeOrdinal,
        long commitSnapshotId,
        List<DeleteFile> addedDeletes,
        List<DeleteFile> existingDeletes) {
      super(task, changeOrdinal, commitSnapshotId);
      this.addedDeletes = ImmutableList.copyOf(addedDeletes);
      this.existingDeletes = ImmutableList.copyOf(existingDeletes);
    }

    @Override
    public List<DeleteFile> addedDeletes() {
      return addedDeletes;
    }

    @Override
    public List<DeleteFile> existingDeletes() {
      return existingDeletes;
    }
  }
}
