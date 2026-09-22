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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TransactionalCatalogPlugin;
import org.apache.spark.sql.connector.catalog.transactions.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkTransaction extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties()
      }
    };
  }

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testStagesChangesUntilCommit() {
    Transaction transaction = newTransaction();
    SparkTable table = loadTable(transaction);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(temp.resolve("data.parquet").toString())
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .build();

    table.table().newAppend().appendFile(file).commit();

    assertThat(validationCatalog.loadTable(tableIdent).currentSnapshot()).isNull();
    transaction.commit();
    transaction.close();
    assertThat(validationCatalog.loadTable(tableIdent).currentSnapshot()).isNotNull();
  }

  @TestTemplate
  public void testAbortDiscardsStagedChanges() {
    Transaction transaction = newTransaction();
    SparkTable table = loadTable(transaction);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(temp.resolve("aborted.parquet").toString())
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .build();

    table.table().newAppend().appendFile(file).commit();
    transaction.abort();
    transaction.close();

    assertThat(validationCatalog.loadTable(tableIdent).currentSnapshot()).isNull();
  }

  @TestTemplate
  public void testSelfReadAppendUsesTransactionTable() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("INSERT INTO %s SELECT id + 10, data FROM %s WHERE id = 1", tableName, tableName);

    assertThat(sql("SELECT * FROM %s ORDER BY id", tableName))
        .containsExactly(row(1L, "a"), row(2L, "b"), row(11L, "a"));
  }

  @TestTemplate
  public void testCachedSelfReadAppendRegistersScan() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Dataset<Row> cached = spark.table(tableName).where("id = 1").cache();
    cached.count();

    try {
      cached.selectExpr("id + 10 AS id", "data").writeTo(tableName).append();
    } finally {
      cached.unpersist();
    }

    assertThat(sql("SELECT * FROM %s ORDER BY id", tableName))
        .containsExactly(row(1L, "a"), row(2L, "b"), row(11L, "a"));
  }

  private Transaction newTransaction() {
    TransactionalCatalogPlugin catalog =
        (TransactionalCatalogPlugin)
            spark.sessionState().catalogManager().catalog(catalogName);
    return catalog.beginTransaction(() -> "test-transaction");
  }

  private SparkTable loadTable(Transaction transaction) {
    TableCatalog transactionCatalog = (TableCatalog) transaction.catalog();
    Identifier sparkIdentifier =
        Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    return (SparkTable) transactionCatalog.loadTable(sparkIdentifier);
  }
}
