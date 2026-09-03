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

import static org.apache.iceberg.TestHelpers.row;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Table;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestSparkChangelog extends TestBaseWithCatalog {

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void readsChangesUsingSparkCdcSyntax() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long firstVersion = table.currentSnapshot().sequenceNumber();

    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    table.refresh();
    long secondVersion = table.currentSnapshot().sequenceNumber();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d ORDER BY id",
                tableName, firstVersion, secondVersion))
        .containsExactly(
            row(1L, "a", "insert", firstVersion),
            row(2L, "b", "insert", firstVersion),
            row(3L, "c", "insert", secondVersion));
  }

  @TestTemplate
  void streamsChangesUsingSparkCdcApi() throws Exception {
    String queryName = "iceberg_cdc_changes";
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    Dataset<Row> changes = spark.readStream().changes(tableName);
    StreamingQuery query =
        changes
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.AvailableNow())
            .start();
    query.awaitTermination();

    assertThat(sql("SELECT id, data, _change_type FROM %s ORDER BY id", queryName))
        .containsExactly(row(1L, "a", "insert"), row(2L, "b", "insert"));
    spark.catalog().dropTempView(queryName);
  }
}
