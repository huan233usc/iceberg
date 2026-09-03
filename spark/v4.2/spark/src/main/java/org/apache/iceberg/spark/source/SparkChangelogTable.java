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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogRange;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Iceberg changelog relation used both as {@code table.changes} and Spark 4.2 {@link Changelog}.
 *
 * <p>The Table API keeps Iceberg's changelog columns ({@code _change_type}, {@code
 * _change_ordinal}, {@code _commit_snapshot_id}). The Changelog API maps those onto Spark CDC
 * columns ({@code _change_type}, {@code _commit_version}, {@code _commit_timestamp}). {@link
 * ChangelogContext} is only set when this object is returned from {@code loadChangelog}.
 */
public class SparkChangelogTable
    implements org.apache.spark.sql.connector.catalog.Table,
        SupportsRead,
        SupportsMetadataColumns,
        Changelog {

  public static final String TABLE_NAME = "changes";

  static final String COMMIT_VERSION = "_commit_version";
  static final String COMMIT_TIMESTAMP = "_commit_timestamp";

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(TableCapability.BATCH_READ);

  private static final Types.NestedField COMMIT_VERSION_FIELD =
      Types.NestedField.required(
          Integer.MAX_VALUE - 109,
          COMMIT_VERSION,
          Types.LongType.get(),
          "Iceberg snapshot sequence number");
  private static final Types.NestedField COMMIT_TIMESTAMP_FIELD =
      Types.NestedField.required(
          Integer.MAX_VALUE - 110,
          COMMIT_TIMESTAMP,
          Types.TimestampType.withZone(),
          "Iceberg snapshot commit timestamp");

  private final Table table;
  private final Schema icebergChangelogSchema;
  private final ChangelogContext context;
  private final Schema sparkCdcSchema;
  private final Column[] sparkCdcColumns;
  private final Set<String> identifierFields;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;

  public SparkChangelogTable(Table table) {
    this(table, null);
  }

  public SparkChangelogTable(Table table, ChangelogContext context) {
    this.table = table;
    this.icebergChangelogSchema = ChangelogUtil.changelogSchema(table.schema());
    this.context = context;
    this.sparkCdcSchema =
        TypeUtil.join(
            table.schema(),
            new Schema(MetadataColumns.CHANGE_TYPE, COMMIT_VERSION_FIELD, COMMIT_TIMESTAMP_FIELD));
    this.sparkCdcColumns = toColumns(sparkCdcSchema);
    this.identifierFields = table.schema().identifierFieldNames();
  }

  @Override
  public String name() {
    return table.name() + "." + TABLE_NAME;
  }

  @Override
  public StructType schema() {
    if (lazySparkSchema == null) {
      this.lazySparkSchema = SparkSchemaUtil.convert(icebergChangelogSchema);
    }

    return lazySparkSchema;
  }

  @Override
  public Column[] columns() {
    return context != null ? sparkCdcColumns : toColumns(icebergChangelogSchema);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public boolean containsCarryoverRows() {
    // Iceberg CoW rewrites can emit identical delete/insert pairs, but Spark also requires
    // rowVersion() to drop them. Identifier fields alone are not a row version.
    return false;
  }

  @Override
  public boolean containsIntermediateChanges() {
    return !identifierFields.isEmpty();
  }

  @Override
  public boolean representsUpdateAsDeleteAndInsert() {
    // Incremental changelog emits INSERT/DELETE file changes, not Iceberg UPDATE_BEFORE/AFTER.
    // Spark still requires rowVersion() to reconstruct pre/post images.
    return false;
  }

  @Override
  public NamedReference[] rowId() {
    return Spark3Util.toNamedReferences(identifierFields);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (context == null) {
      return new SparkChangelogScanBuilder(spark(), table, icebergChangelogSchema, options);
    }

    RangeOptions range = rangeOptions();
    Map<String, String> scanOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    scanOptions.putAll(options.asCaseSensitiveMap());
    scanOptions.putAll(range.options());
    return new SparkChangelogScanBuilder(
        spark(),
        table,
        sparkCdcSchema,
        new CaseInsensitiveStringMap(scanOptions),
        true /* Spark CDC */,
        range.empty());
  }

  private SparkSession spark() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return new MetadataColumn[] {
      SparkMetadataColumns.SPEC_ID,
      SparkMetadataColumns.partition(table),
      SparkMetadataColumns.FILE_PATH,
      SparkMetadataColumns.ROW_POSITION,
      SparkMetadataColumns.IS_DELETED,
    };
  }

  private RangeOptions rangeOptions() {
    ChangelogRange range = context.range();
    if (range instanceof ChangelogRange.UnboundedRange) {
      return new RangeOptions(Collections.emptyMap(), false);
    } else if (range instanceof ChangelogRange.VersionRange) {
      return versionRangeOptions((ChangelogRange.VersionRange) range);
    } else if (range instanceof ChangelogRange.TimestampRange) {
      return timestampRangeOptions((ChangelogRange.TimestampRange) range);
    } else {
      throw new UnsupportedOperationException("Unsupported Spark changelog range: " + range);
    }
  }

  private RangeOptions versionRangeOptions(ChangelogRange.VersionRange range) {
    Snapshot start = snapshotWithSequenceNumber(range.startingVersion());
    Snapshot end =
        range.endingVersion().map(this::snapshotWithSequenceNumber).orElse(table.currentSnapshot());

    Long startExclusive = range.startingBoundInclusive() ? start.parentId() : start.snapshotId();
    Long endInclusive = range.endingBoundInclusive() ? end.snapshotId() : end.parentId();
    return snapshotRangeOptions(startExclusive, endInclusive);
  }

  private RangeOptions timestampRangeOptions(ChangelogRange.TimestampRange range) {
    List<Snapshot> snapshots = currentAncestorsInCommitOrder();
    Snapshot start =
        snapshots.stream()
            .filter(
                snapshot ->
                    range.startingBoundInclusive()
                        ? snapshot.timestampMillis() * 1000 >= range.startingTimestamp()
                        : snapshot.timestampMillis() * 1000 > range.startingTimestamp())
            .findFirst()
            .orElse(null);
    Snapshot end =
        snapshots.stream()
            .filter(
                snapshot ->
                    range.endingTimestamp().isEmpty()
                        || (range.endingBoundInclusive()
                            ? snapshot.timestampMillis() * 1000 <= range.endingTimestamp().get()
                            : snapshot.timestampMillis() * 1000 < range.endingTimestamp().get()))
            .reduce((left, right) -> right)
            .orElse(null);

    if (start == null || end == null || start.sequenceNumber() > end.sequenceNumber()) {
      return new RangeOptions(Collections.emptyMap(), true);
    }

    return snapshotRangeOptions(start.parentId(), end.snapshotId());
  }

  private RangeOptions snapshotRangeOptions(Long startExclusive, Long endInclusive) {
    if (endInclusive == null) {
      return new RangeOptions(Collections.emptyMap(), true);
    }

    Snapshot end = table.snapshot(endInclusive);
    if (end == null) {
      return new RangeOptions(Collections.emptyMap(), true);
    }

    if (startExclusive != null) {
      Snapshot start = table.snapshot(startExclusive);
      if (start != null && start.sequenceNumber() >= end.sequenceNumber()) {
        return new RangeOptions(Collections.emptyMap(), true);
      }
    }

    Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (startExclusive != null) {
      options.put(SparkReadOptions.START_SNAPSHOT_ID, String.valueOf(startExclusive));
    }
    options.put(SparkReadOptions.END_SNAPSHOT_ID, String.valueOf(endInclusive));
    return new RangeOptions(options, false);
  }

  private Snapshot snapshotWithSequenceNumber(String version) {
    long sequenceNumber;
    try {
      sequenceNumber = Long.parseLong(version);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid Iceberg snapshot sequence number: " + version, e);
    }

    return Lists.newArrayList(SnapshotUtil.currentAncestors(table)).stream()
        .filter(snapshot -> snapshot.sequenceNumber() == sequenceNumber)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Cannot find Iceberg snapshot with sequence number: " + version));
  }

  private List<Snapshot> currentAncestorsInCommitOrder() {
    List<Snapshot> snapshots = Lists.newArrayList(SnapshotUtil.currentAncestors(table));
    Collections.reverse(snapshots);
    return snapshots;
  }

  private static Column[] toColumns(Schema schema) {
    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    return Arrays.stream(sparkSchema.fields())
        .map(field -> Column.create(field.name(), field.dataType(), field.nullable()))
        .toArray(Column[]::new);
  }

  private static class RangeOptions {
    private final Map<String, String> options;
    private final boolean empty;

    private RangeOptions(Map<String, String> options, boolean empty) {
      this.options = options;
      this.empty = empty;
    }

    private Map<String, String> options() {
      return options;
    }

    private boolean empty() {
      return empty;
    }
  }
}
