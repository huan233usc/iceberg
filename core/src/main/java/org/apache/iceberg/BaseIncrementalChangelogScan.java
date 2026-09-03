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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  BaseIncrementalChangelogScan(Table table) {
    this(table, table.schema(), TableScanContext.empty());
  }

  private BaseIncrementalChangelogScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  protected IncrementalChangelogScan newRefinedScan(
      Table newTable, Schema newSchema, TableScanContext newContext) {
    return new BaseIncrementalChangelogScan(newTable, newSchema, newContext);
  }

  @Override
  protected CloseableIterable<ChangelogScanTask> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    Deque<Snapshot> changelogSnapshots =
        orderedChangelogSnapshots(fromSnapshotIdExclusive, toSnapshotIdInclusive);

    if (changelogSnapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    Set<Long> changelogSnapshotIds = toSnapshotIds(changelogSnapshots);

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toSet();

    ManifestGroup dataManifestGroup =
        new ManifestGroup(table().io(), newDataManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(entry -> changelogSnapshotIds.contains(entry.snapshotId()))
            .ignoreExisting()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      dataManifestGroup = dataManifestGroup.ignoreResiduals();
    }

    if (newDataManifests.size() > 1 && shouldPlanWithExecutor()) {
      dataManifestGroup = dataManifestGroup.planWith(planExecutor());
    }

    List<CloseableIterable<ChangelogScanTask>> taskIterables =
        Lists.newArrayList(
            dataManifestGroup.plan(new CreateDataFileChangeTasks(changelogSnapshots)));

    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(changelogSnapshots);
    for (Snapshot snapshot : changelogSnapshots) {
      List<ManifestFile> deleteManifests = snapshot.deleteManifests(table().io());
      boolean hasAddedDeleteFiles =
          deleteManifests.stream()
              .anyMatch(
                  manifest ->
                      manifest.snapshotId() == snapshot.snapshotId() && manifest.hasAddedFiles());
      if (hasAddedDeleteFiles) {
        List<ManifestFile> dataManifests = snapshot.dataManifests(table().io());
        ManifestGroup deleteManifestGroup =
            new ManifestGroup(table().io(), dataManifests, deleteManifests)
                .specsById(table().specs())
                .caseSensitive(isCaseSensitive())
                .select(scanColumns())
                .filterData(filter())
                .ignoreDeleted()
                .columnsToKeepStats(columnsToKeepStats());

        if (shouldIgnoreResiduals()) {
          deleteManifestGroup = deleteManifestGroup.ignoreResiduals();
        }

        if (dataManifests.size() > 1 && shouldPlanWithExecutor()) {
          deleteManifestGroup = deleteManifestGroup.planWith(planExecutor());
        }

        taskIterables.add(
            deleteManifestGroup.plan(
                new CreateDeletedRowsTasks(
                    snapshotOrdinals.get(snapshot.snapshotId()),
                    snapshot.snapshotId(),
                    snapshot.sequenceNumber())));
      }
    }

    return CloseableIterable.concat(taskIterables);
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ChangelogScanTask>> planTasks() {
    return TableScanUtil.planTaskGroups(
        planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  // builds a collection of changelog snapshots (oldest to newest)
  // the order of the snapshots is important as it is used to determine change ordinals
  private Deque<Snapshot> orderedChangelogSnapshots(Long fromIdExcl, long toIdIncl) {
    Deque<Snapshot> changelogSnapshots = new ArrayDeque<>();

    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table(), toIdIncl, fromIdExcl)) {
      if (!snapshot.operation().equals(DataOperations.REPLACE)) {
        changelogSnapshots.addFirst(snapshot);
      }
    }

    return changelogSnapshots;
  }

  private Set<Long> toSnapshotIds(Collection<Snapshot> snapshots) {
    return snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
  }

  private static Map<Long, Integer> computeSnapshotOrdinals(Deque<Snapshot> snapshots) {
    Map<Long, Integer> snapshotOrdinals = Maps.newHashMap();

    int ordinal = 0;

    for (Snapshot snapshot : snapshots) {
      snapshotOrdinals.put(snapshot.snapshotId(), ordinal++);
    }

    return snapshotOrdinals;
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

    private final Map<Long, Integer> snapshotOrdinals;

    CreateDataFileChangeTasks(Deque<Snapshot> snapshots) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.transform(
          entries,
          entry -> {
            long commitSnapshotId = entry.snapshotId();
            int changeOrdinal = snapshotOrdinals.get(commitSnapshotId);
            DataFile dataFile = entry.file().copy(context.shouldKeepStats());

            switch (entry.status()) {
              case ADDED:
                return new BaseAddedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    NO_DELETES,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                return new BaseDeletedDataFileScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    NO_DELETES,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          });
    }
  }

  private static class CreateDeletedRowsTasks implements CreateTasksFunction<ChangelogScanTask> {
    private final int changeOrdinal;
    private final long commitSnapshotId;
    private final long commitSequenceNumber;

    private CreateDeletedRowsTasks(
        int changeOrdinal, long commitSnapshotId, long commitSequenceNumber) {
      this.changeOrdinal = changeOrdinal;
      this.commitSnapshotId = commitSnapshotId;
      this.commitSequenceNumber = commitSequenceNumber;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {
      return CloseableIterable.filter(
          CloseableIterable.transform(entries, entry -> toDeletedRowsTask(entry, context)),
          Objects::nonNull);
    }

    private ChangelogScanTask toDeletedRowsTask(
        ManifestEntry<DataFile> entry, TaskContext context) {
      DeleteFile[] deletes = context.deletes().forEntry(entry);
      DeleteFile[] addedDeletes =
          Arrays.stream(deletes).filter(this::committedInSnapshot).toArray(DeleteFile[]::new);
      if (addedDeletes.length == 0) {
        return null;
      }

      if (Arrays.stream(addedDeletes).anyMatch(ContentFileUtil::isDV)) {
        throw new UnsupportedOperationException(
            "Deletion vectors are currently not supported in changelog scans");
      }

      DeleteFile[] existingDeletes =
          Arrays.stream(deletes)
              .filter(delete -> !committedInSnapshot(delete))
              .toArray(DeleteFile[]::new);
      return new BaseDeletedRowsScanTask(
          changeOrdinal,
          commitSnapshotId,
          entry.file().copy(context.shouldKeepStats()),
          addedDeletes,
          existingDeletes,
          context.schemaAsString(),
          context.specAsString(),
          context.residuals());
    }

    private boolean committedInSnapshot(DeleteFile delete) {
      return Long.valueOf(commitSequenceNumber).equals(delete.fileSequenceNumber());
    }
  }
}
