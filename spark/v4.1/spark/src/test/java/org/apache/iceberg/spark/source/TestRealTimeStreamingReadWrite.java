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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
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
        spark
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

  private void append(File location, SimpleRecord... records) {
    Dataset<Row> data =
        spark.createDataset(List.of(records), Encoders.bean(SimpleRecord.class)).toDF();
    DataFrameWriter<Row> writer = data.select("id", "data").write();
    writer.format("iceberg").mode(SaveMode.Append).save(location.toString());
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
