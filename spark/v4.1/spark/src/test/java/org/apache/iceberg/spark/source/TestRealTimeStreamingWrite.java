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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers(disabledWithoutDocker = true)
class TestRealTimeStreamingWrite {
  private static final String ALLOWLIST_CHECK = "spark.sql.streaming.realTimeMode.allowlistCheck";
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Container
  private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.9.2");

  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2,2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.sql.shuffle.partitions", 2)
            .config(ALLOWLIST_CHECK, false)
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
  void kafkaRealTimeAppendIsIdempotentAfterQueryRestart() throws Exception {
    String topic = "iceberg-rtm-" + UUID.randomUUID();
    createTopic(topic);
    send(topic, "1,a", "2,b", "3,c");

    File location = temp.resolve("table").toFile();
    File checkpoint = temp.resolve("checkpoint").toFile();
    Table table =
        new HadoopTables(CONF).create(SCHEMA, PartitionSpec.unpartitioned(), location.toString());
    DataStreamWriter<Row> writer = newRealTimeWriter(topic, location, checkpoint, false);

    StreamingQuery query = writer.start();
    try {
      awaitRows(location, 3L);
      query.stop();

      table.refresh();
      assertThat(table.snapshots()).as("one non-empty RTM epoch").hasSize(1);
      long committedSnapshotId = table.currentSnapshot().snapshotId();
      String committedQueryId =
          table.currentSnapshot().summary().get("spark.sql.streaming.queryId");
      long committedEpochId =
          Long.parseLong(table.currentSnapshot().summary().get("spark.sql.streaming.epochId"));

      File replayedCommit = removeCommitsFrom(checkpoint, committedEpochId);
      StreamingQuery restartedQuery = writer.start();
      try {
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(() -> assertThat(replayedCommit).exists());
      } finally {
        restartedQuery.stop();
      }

      awaitRows(location, 3L);
      table.refresh();
      assertThat(table.snapshots()).as("replayed epoch is not committed twice").hasSize(1);
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committedSnapshotId);
      assertThat(table.currentSnapshot().summary())
          .containsEntry("spark.sql.streaming.queryId", committedQueryId)
          .containsEntry("spark.sql.streaming.epochId", Long.toString(committedEpochId));
    } finally {
      if (query.isActive()) {
        query.stop();
      }
    }
  }

  @Test
  void emptyRealTimeEpochDoesNotCreateSnapshot() throws Exception {
    String topic = "iceberg-rtm-empty-" + UUID.randomUUID();
    createTopic(topic);

    File location = temp.resolve("empty-table").toFile();
    File checkpoint = temp.resolve("empty-checkpoint").toFile();
    Table table =
        new HadoopTables(CONF).create(SCHEMA, PartitionSpec.unpartitioned(), location.toString());

    StreamingQuery query = newRealTimeWriter(topic, location, checkpoint, false).start();
    try {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(numericCommitFiles(checkpoint)).isNotEmpty());

      table.refresh();
      assertThat(table.currentSnapshot()).isNull();
    } finally {
      query.stop();
    }
  }

  @Test
  void kafkaRealTimeAppendUsesAsyncProgressTrackingByDefault() throws Exception {
    String topic = "iceberg-rtm-async-" + UUID.randomUUID();
    createTopic(topic);
    send(topic, "1,a", "2,b", "3,c");

    File location = temp.resolve("async-table").toFile();
    File checkpoint = temp.resolve("async-checkpoint").toFile();
    Table table =
        new HadoopTables(CONF).create(SCHEMA, PartitionSpec.unpartitioned(), location.toString());

    StreamingQuery query = newRealTimeWriter(topic, location, checkpoint, true).start();
    try {
      awaitRows(location, 3L);

      table.refresh();
      assertThat(table.snapshots()).as("one non-empty RTM epoch").hasSize(1);
      assertThat(table.currentSnapshot().summary())
          .containsKeys("spark.sql.streaming.queryId", "spark.sql.streaming.epochId");
    } finally {
      query.stop();
    }
  }

  @Test
  void icebergRealTimeAppendRetriesFailedWriteTasks() throws Exception {
    File sourceLocation = temp.resolve("task-retry-source").toFile();
    File location = temp.resolve("task-retry-table").toFile();
    File checkpoint = temp.resolve("task-retry-checkpoint").toFile();
    HadoopTables tables = new HadoopTables(CONF);
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), sourceLocation.toString());
    Table table = tables.create(SCHEMA, PartitionSpec.unpartitioned(), location.toString());
    spark
        .createDataset(
            List.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c")),
            Encoders.bean(SimpleRecord.class))
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(sourceLocation.toString());

    Dataset<Row> input =
        spark
            .readStream()
            .format("iceberg")
            .option(SparkReadOptions.STREAMING_REAL_TIME_SHARDS, 1)
            .load(sourceLocation.toString());
    StructType inputSchema = input.schema();
    input =
        input.mapPartitions(
            (MapPartitionsFunction<Row, Row>) FailFirstTaskAttemptIterator::new,
            RowEncoder.encoderFor(inputSchema));

    StreamingQuery query =
        input
            .writeStream()
            .outputMode("update")
            .format("iceberg")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", location.toString())
            .option(SparkWriteBuilder.REAL_TIME_MODE_ENABLED, "true")
            .option("asyncProgressTrackingEnabled", "false")
            .trigger(Trigger.RealTime(5_000L))
            .start();
    try {
      awaitRows(location, 3L);

      table.refresh();
      assertThat(table.snapshots()).as("retried tasks produce one committed epoch").hasSize(1);
      assertThat(table.currentSnapshot().summary())
          .containsKeys("spark.sql.streaming.queryId", "spark.sql.streaming.epochId");
    } finally {
      query.stop();
    }
  }

  @Test
  void concurrentRealTimeQueriesCommitAllRows() throws Exception {
    String firstTopic = "iceberg-rtm-concurrent-1-" + UUID.randomUUID();
    String secondTopic = "iceberg-rtm-concurrent-2-" + UUID.randomUUID();
    createTopic(firstTopic);
    createTopic(secondTopic);
    send(firstTopic, "1,a", "2,b", "3,c");
    send(secondTopic, "4,d", "5,e", "6,f");

    File location = temp.resolve("concurrent-table").toFile();
    File firstCheckpoint = temp.resolve("concurrent-checkpoint-1").toFile();
    File secondCheckpoint = temp.resolve("concurrent-checkpoint-2").toFile();
    Table table =
        new HadoopTables(CONF).create(SCHEMA, PartitionSpec.unpartitioned(), location.toString());

    StreamingQuery first = newRealTimeWriter(firstTopic, location, firstCheckpoint, false).start();
    StreamingQuery second =
        newRealTimeWriter(secondTopic, location, secondCheckpoint, false).start();
    try {
      awaitRows(location, 6L);

      table.refresh();
      assertThat(table.snapshots()).as("one commit from each RTM query").hasSize(2);
      assertThat(
              spark.read().format("iceberg").load(location.toString()).select("id").collectAsList())
          .extracting(row -> row.getInt(0))
          .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
    } finally {
      first.stop();
      second.stop();
    }
  }

  private DataStreamWriter<Row> newRealTimeWriter(
      String topic, File location, File checkpoint, boolean asyncProgressTrackingEnabled) {
    Dataset<Row> input =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA.getBootstrapServers())
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
            .selectExpr("CAST(split(value, ',')[0] AS INT) AS id", "split(value, ',')[1] AS data");

    DataStreamWriter<Row> writer =
        input
            .writeStream()
            .outputMode("update")
            .format("iceberg")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", location.toString())
            .option(SparkWriteBuilder.REAL_TIME_MODE_ENABLED, "true");
    if (!asyncProgressTrackingEnabled) {
      writer.option("asyncProgressTrackingEnabled", "false");
    }

    return writer.trigger(Trigger.RealTime(5_000L));
  }

  private void awaitRows(File location, long expectedRows) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(spark.read().format("iceberg").load(location.toString()).count())
                    .isEqualTo(expectedRows));
  }

  private void createTopic(String topic) throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (Admin admin = Admin.create(properties)) {
      admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get();
    }
  }

  private void send(String topic, String... values) throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
      for (String value : values) {
        producer.send(new ProducerRecord<>(topic, value)).get();
      }
    }
  }

  private File removeCommitsFrom(File checkpoint, long firstCommitId) throws Exception {
    List<File> commits = numericCommitFiles(checkpoint);
    File firstCommit =
        commits.stream()
            .filter(commit -> commitId(commit) == firstCommitId)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No streaming commit found for epoch " + firstCommitId));
    for (File commit : commits) {
      long commitId = commitId(commit);
      if (commitId >= firstCommitId) {
        Files.delete(commit.toPath());
        Files.deleteIfExists(new File(commit.getParentFile(), "." + commitId + ".crc").toPath());
      }
    }

    return firstCommit;
  }

  private List<File> numericCommitFiles(File checkpoint) {
    File[] files =
        new File(checkpoint, "commits")
            .listFiles(file -> file.isFile() && file.getName().matches("\\d+"));
    return files != null ? List.of(files) : List.of();
  }

  private long commitId(File commit) {
    return Long.parseLong(commit.getName());
  }

  private static class FailFirstTaskAttemptIterator implements Iterator<Row> {
    private final Iterator<Row> rows;

    private FailFirstTaskAttemptIterator(Iterator<Row> rows) {
      this.rows = rows;
    }

    @Override
    public boolean hasNext() {
      return rows.hasNext();
    }

    @Override
    public Row next() {
      Row row = rows.next();
      if (TaskContext.get().attemptNumber() == 0) {
        throw new IllegalStateException("Injected failure after writing task rows");
      }

      return row;
    }
  }
}
