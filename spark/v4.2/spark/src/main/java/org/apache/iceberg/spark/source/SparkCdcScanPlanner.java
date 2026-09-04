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
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

/**
 * Plans Spark CDC scans using regular file scan tasks.
 *
 * <p>For every snapshot, added data files produce inserts, removed data files produce deletes, and
 * delete files added by the snapshot are merged with their affected data files to recover deleted
 * rows. This follows the per-snapshot algorithm from the Iceberg CDC proposal.
 */
class SparkCdcScanPlanner {
  private final Table table;
  private final SparkReadConf readConf;
  private final Expression filter;

  SparkCdcScanPlanner(Table table, SparkReadConf readConf, Expression filter) {
    this.table = table;
    this.readConf = readConf;
    this.filter = filter;
  }

  List<ScanTaskGroup<ChangelogScanTask>> planTasks(
      Long fromSnapshotIdExclusive, Long toSnapshotIdInclusive) {
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      return Collections.emptyList();
    }

    long endSnapshotId =
        toSnapshotIdInclusive != null ? toSnapshotIdInclusive : currentSnapshot.snapshotId();
    Deque<Snapshot> snapshots = orderedSnapshots(fromSnapshotIdExclusive, endSnapshotId);
    List<ChangelogScanTask> tasks = Lists.newArrayList();

    int changeOrdinal = 0;
    for (Snapshot snapshot : snapshots) {
      planSnapshot(snapshot, changeOrdinal, tasks);
      changeOrdinal += 1;
    }

    return TableScanUtil.planTaskGroups(
        tasks, readConf.splitSize(), readConf.splitLookback(), readConf.splitOpenFileCost());
  }

  private void planSnapshot(
      Snapshot snapshot, int changeOrdinal, List<ChangelogScanTask> tasks) {
    Set<String> addedDataFiles = dataFileLocations(addedDataFiles(snapshot));
    Set<String> removedDataFiles = dataFileLocations(removedDataFiles(snapshot));
    Set<String> addedDeleteFiles = deleteFileKeys(addedDeleteFiles(snapshot));
    List<DeleteFile> removedDeleteFiles = removedDeleteFiles(snapshot);

    if (!addedDataFiles.isEmpty() || !addedDeleteFiles.isEmpty()) {
      for (FileScanTask task : planFiles(snapshot.snapshotId())) {
        String dataFileLocation = task.file().location();
        if (addedDataFiles.contains(dataFileLocation)) {
          addSplitTasks(
              task,
              split ->
                  new SparkCdcScanTask.AddedRows(
                      split, changeOrdinal, snapshot.snapshotId(), task.deletes()),
              tasks);

        } else {
          List<DeleteFile> addedDeletes =
              task.deletes().stream()
                  .filter(delete -> addedDeleteFiles.contains(deleteFileKey(delete)))
                  .collect(Collectors.toList());
          if (!addedDeletes.isEmpty()) {
            List<DeleteFile> existingDeletes =
                task.deletes().stream()
                    .filter(delete -> !addedDeleteFiles.contains(deleteFileKey(delete)))
                    .collect(Collectors.toCollection(ArrayList::new));
            addRemovedDVs(task.file(), removedDeleteFiles, existingDeletes);
            addSplitTasks(
                task,
                split ->
                    new SparkCdcScanTask.DeletedRows(
                        split,
                        changeOrdinal,
                        snapshot.snapshotId(),
                        addedDeletes,
                        existingDeletes),
                tasks);
          }
        }
      }
    }

    if (!removedDataFiles.isEmpty() && snapshot.parentId() != null) {
      for (FileScanTask task : planFiles(snapshot.parentId())) {
        if (removedDataFiles.contains(task.file().location())) {
          addSplitTasks(
              task,
              split ->
                  new SparkCdcScanTask.DeletedDataFile(
                      split, changeOrdinal, snapshot.snapshotId(), task.deletes()),
              tasks);
        }
      }
    }
  }

  private List<FileScanTask> planFiles(long snapshotId) {
    TableScan scan =
        table
            .newScan()
            .useSnapshot(snapshotId)
            .caseSensitive(readConf.caseSensitive())
            .filter(filter)
            .project(table.schema());

    try (CloseableIterable<FileScanTask> fileTasks = scan.planFiles()) {
      return Lists.newArrayList(fileTasks);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close file scan for snapshot " + snapshotId, e);
    }
  }

  private void addSplitTasks(
      FileScanTask task,
      Function<FileScanTask, ChangelogScanTask> createTask,
      List<ChangelogScanTask> tasks) {
    for (FileScanTask split : task.split(readConf.splitSize())) {
      tasks.add(createTask.apply(split));
    }
  }

  private Deque<Snapshot> orderedSnapshots(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {
    Deque<Snapshot> snapshots = new ArrayDeque<>();
    for (Snapshot snapshot :
        SnapshotUtil.ancestorsBetween(table, toSnapshotIdInclusive, fromSnapshotIdExclusive)) {
      if (!DataOperations.REPLACE.equals(snapshot.operation())) {
        snapshots.addFirst(snapshot);
      }
    }

    return snapshots;
  }

  private static void addRemovedDVs(
      DataFile dataFile, List<DeleteFile> removedDeleteFiles, List<DeleteFile> existingDeletes) {
    for (DeleteFile deleteFile : removedDeleteFiles) {
      if (ContentFileUtil.isDV(deleteFile)
          && dataFile
              .location()
              .equals(ContentFileUtil.referencedDataFileLocation(deleteFile))) {
        existingDeletes.add(deleteFile);
      }
    }
  }

  private static Set<String> dataFileLocations(Iterable<DataFile> dataFiles) {
    Set<String> locations = Sets.newHashSet();
    dataFiles.forEach(file -> locations.add(file.location()));
    return locations;
  }

  private static Set<String> deleteFileKeys(Iterable<DeleteFile> deleteFiles) {
    Set<String> keys = Sets.newHashSet();
    deleteFiles.forEach(file -> keys.add(deleteFileKey(file)));
    return keys;
  }

  private static String deleteFileKey(DeleteFile deleteFile) {
    return String.format(
        "%s:%s:%s",
        deleteFile.location(), deleteFile.contentOffset(), deleteFile.contentSizeInBytes());
  }

  @SuppressWarnings("deprecation")
  private Iterable<DataFile> addedDataFiles(Snapshot snapshot) {
    return snapshot.addedDataFiles(table.io());
  }

  @SuppressWarnings("deprecation")
  private Iterable<DataFile> removedDataFiles(Snapshot snapshot) {
    return snapshot.removedDataFiles(table.io());
  }

  @SuppressWarnings("deprecation")
  private Iterable<DeleteFile> addedDeleteFiles(Snapshot snapshot) {
    return snapshot.addedDeleteFiles(table.io());
  }

  @SuppressWarnings("deprecation")
  private List<DeleteFile> removedDeleteFiles(Snapshot snapshot) {
    return Lists.newArrayList(snapshot.removedDeleteFiles(table.io()));
  }
}
