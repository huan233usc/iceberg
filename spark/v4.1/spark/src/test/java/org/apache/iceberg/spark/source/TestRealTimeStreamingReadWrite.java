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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestRealTimeStreamingReadWrite {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.sql.shuffle.partitions", 2)
            .config("spark.sql.streaming.realTimeMode.allowlistCheck", false)
            .config(TestBase.DISABLE_UI)
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() throws Exception {
    if (spark != null) {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }

      spark.stop();
      spark = null;
    }
  }

  @Test
  void readsNewSnapshotsAndRestartsWithStableShards() throws Exception {
    File sourceLocation = temp.resolve("source").toFile();
    File destinationLocation = temp.resolve("destination").toFile();
    File checkpoint = temp.resolve("checkpoint").toFile();
    HadoopTables tables = new HadoopTables(CONF);
    Table source = tables.create(SCHEMA, PartitionSpec.unpartitioned(), sourceLocation.toString());
    Table destination =
        tables.create(SCHEMA, PartitionSpec.unpartitioned(), destinationLocation.toString());

    DataStreamWriter<Row> writer =
        newRealTimeWriter(sourceLocation, destinationLocation, checkpoint);

    StreamingQuery query = writer.start();
    try {
      append(sourceLocation, new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
      awaitRows(destinationLocation, 2L);
      query.stop();

      source.updateSchema().addColumn("extra", Types.StringType.get()).commit();
      source.updateSpec().addField(Expressions.bucket("id", 2)).commit();

      StreamingQuery restarted = writer.start();
      try {
        append(sourceLocation, new SimpleRecord(3, "c"), new SimpleRecord(4, "d"));
        awaitRows(destinationLocation, 4L);
      } finally {
        restarted.stop();
      }

      destination.refresh();
      assertThat(destination.snapshots())
          .as("one destination snapshot per source append")
          .hasSize(2);
      assertThat(readRows(destinationLocation))
          .containsExactly(
              new SimpleRecord(1, "a"),
              new SimpleRecord(2, "b"),
              new SimpleRecord(3, "c"),
              new SimpleRecord(4, "d"));
    } finally {
      if (query.isActive()) {
        query.stop();
      }
    }
  }

  @Test
  void rejectsDeleteSnapshotsByDefault() throws Exception {
    File sourceLocation = temp.resolve("delete-source").toFile();
    File destinationLocation = temp.resolve("delete-destination").toFile();
    File checkpoint = temp.resolve("delete-checkpoint").toFile();
    HadoopTables tables = new HadoopTables(CONF);
    Table source = tables.create(SCHEMA, PartitionSpec.unpartitioned(), sourceLocation.toString());
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), destinationLocation.toString());

    StreamingQuery query =
        newRealTimeWriter(sourceLocation, destinationLocation, checkpoint).start();
    try {
      append(sourceLocation, new SimpleRecord(1, "a"));
      awaitRows(destinationLocation, 1L);

      source.refresh();
      DataFile dataFile =
          Iterables.getOnlyElement(source.currentSnapshot().addedDataFiles(source.io()));

      source.newDelete().deleteFile(dataFile).commit();

      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(() -> assertThat(query.exception().isDefined()).isTrue());
      assertThat(query.exception().get()).hasStackTraceContaining("Cannot process delete snapshot");
    } finally {
      if (query.isActive()) {
        query.stop();
      }
    }
  }

  @Test
  void rejectsOverwriteSnapshotsByDefault() throws Exception {
    File sourceLocation = temp.resolve("overwrite-source").toFile();
    File destinationLocation = temp.resolve("overwrite-destination").toFile();
    File checkpoint = temp.resolve("overwrite-checkpoint").toFile();
    HadoopTables tables = new HadoopTables(CONF);
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), sourceLocation.toString());
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), destinationLocation.toString());

    StreamingQuery query =
        newRealTimeWriter(sourceLocation, destinationLocation, checkpoint).start();
    try {
      append(sourceLocation, new SimpleRecord(1, "a"));
      awaitRows(destinationLocation, 1L);

      write(sourceLocation, SaveMode.Overwrite, new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(() -> assertThat(query.exception().isDefined()).isTrue());
      assertThat(query.exception().get())
          .hasStackTraceContaining("Cannot process overwrite snapshot");
    } finally {
      if (query.isActive()) {
        query.stop();
      }
    }
  }

  @Test
  void failsWhenCheckpointSnapshotIsExpired() throws Exception {
    File sourceLocation = temp.resolve("expired-source").toFile();
    File destinationLocation = temp.resolve("expired-destination").toFile();
    File checkpoint = temp.resolve("expired-checkpoint").toFile();
    HadoopTables tables = new HadoopTables(CONF);
    Table source = tables.create(SCHEMA, PartitionSpec.unpartitioned(), sourceLocation.toString());
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), destinationLocation.toString());
    DataStreamWriter<Row> writer =
        newRealTimeWriter(sourceLocation, destinationLocation, checkpoint);

    StreamingQuery query = writer.start();
    try {
      append(sourceLocation, new SimpleRecord(1, "a"));
      awaitRows(destinationLocation, 1L);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(new File(checkpoint, "commits").listFiles()).isNotNull().isNotEmpty());
    } finally {
      query.stop();
    }

    source.refresh();
    long expiredSnapshotId = source.currentSnapshot().snapshotId();
    append(sourceLocation, new SimpleRecord(2, "b"));
    source.refresh();
    source.expireSnapshots().expireSnapshotId(expiredSnapshotId).commit();

    StreamingQuery restarted = writer.start();
    try {
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(() -> assertThat(restarted.exception().isDefined()).isTrue());
      assertThat(restarted.exception().get())
          .hasStackTraceContaining("Cannot load current offset at snapshot " + expiredSnapshotId);
      awaitRows(destinationLocation, 1L);
    } finally {
      if (restarted.isActive()) {
        restarted.stop();
      }
    }
  }

  private DataStreamWriter<Row> newRealTimeWriter(
      File sourceLocation, File destinationLocation, File checkpoint) {
    return spark
        .readStream()
        .format("iceberg")
        .option(SparkReadOptions.STREAMING_REAL_TIME_SHARDS, 2)
        .load(sourceLocation.toString())
        .writeStream()
        .outputMode("update")
        .format("iceberg")
        .option("checkpointLocation", checkpoint.toString())
        .option("path", destinationLocation.toString())
        .option(SparkWriteBuilder.REAL_TIME_MODE_ENABLED, true)
        .trigger(Trigger.RealTime(5_000L));
  }

  private void append(File location, SimpleRecord... records) {
    write(location, SaveMode.Append, records);
  }

  private void write(File location, SaveMode mode, SimpleRecord... records) {
    Dataset<Row> data =
        spark.createDataset(List.of(records), Encoders.bean(SimpleRecord.class)).toDF();
    DataFrameWriter<Row> writer = data.select("id", "data").write();
    writer.format("iceberg").mode(mode).save(location.toString());
  }

  private void awaitRows(File location, long expectedRows) {
    await()
        .atMost(Duration.ofSeconds(45))
        .untilAsserted(
            () ->
                assertThat(spark.read().format("iceberg").load(location.toString()).count())
                    .isEqualTo(expectedRows));
  }

  private List<SimpleRecord> readRows(File location) {
    return spark
        .read()
        .format("iceberg")
        .load(location.toString())
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }
}
