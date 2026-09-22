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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalogCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** A Spark transaction backed by one Iceberg transaction per loaded table. */
public class SparkTransaction
    implements org.apache.spark.sql.connector.catalog.transactions.Transaction {

  private enum State {
    ACTIVE,
    COMMITTED,
    ABORTED
  }

  private final TransactionCatalog catalog;
  private State state = State.ACTIVE;
  private boolean closed = false;

  public SparkTransaction(TableCatalog delegate) {
    this.catalog = new TransactionCatalog(delegate);
  }

  @Override
  public CatalogPlugin catalog() {
    return catalog;
  }

  @Override
  public boolean registerScans(Scan[] scans) {
    checkActive();
    return catalog.registerScans(scans);
  }

  @Override
  public void commit() {
    checkActive();
    catalog.commit();
    state = State.COMMITTED;
  }

  @Override
  public void abort() {
    if (state != State.ACTIVE) {
      return;
    }

    state = State.ABORTED;
  }

  @Override
  public void close() {
    if (!closed) {
      catalog.close();
      closed = true;
    }
  }

  private void checkActive() {
    if (closed || state != State.ACTIVE) {
      throw new IllegalStateException(
          "Cannot use transaction in state " + state + " (closed=" + closed + ")");
    }
  }

  private static class TransactionCatalog implements TableCatalog {
    private final TableCatalog delegate;
    private final Map<Identifier, PinnedTable> tables = new ConcurrentHashMap<>();

    private TransactionCatalog(TableCatalog delegate) {
      this.delegate = delegate;
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      // The delegate is already initialized and the transaction catalog is operation-scoped.
    }

    @Override
    public Set<TableCatalogCapability> capabilities() {
      return delegate.capabilities();
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
      return delegate.listTables(namespace);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
      PinnedTable pinned = tables.get(ident);
      if (pinned == null) {
        PinnedTable newPinned = pin(ident);
        PinnedTable concurrent = tables.putIfAbsent(ident, newPinned);
        pinned = concurrent != null ? concurrent : newPinned;
      }

      return pinned.sparkTable();
    }

    @Override
    public Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
        throws NoSuchTableException {
      return loadTable(ident);
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
      return delegate.loadTable(ident, version);
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
      return delegate.loadTable(ident, timestamp);
    }

    @Override
    public Table createTable(
        Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
        throws TableAlreadyExistsException, NoSuchNamespaceException {
      return delegate.createTable(ident, schema, partitions, properties);
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
      return delegate.alterTable(ident, changes);
    }

    @Override
    public boolean dropTable(Identifier ident) {
      tables.remove(ident);
      return delegate.dropTable(ident);
    }

    @Override
    public boolean purgeTable(Identifier ident) {
      tables.remove(ident);
      return delegate.purgeTable(ident);
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
        throws NoSuchTableException, TableAlreadyExistsException {
      tables.remove(oldIdent);
      delegate.renameTable(oldIdent, newIdent);
    }

    @Override
    public void invalidateTable(Identifier ident) {
      // A transaction keeps table state pinned for its complete lifetime.
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof CatalogPlugin && name().equals(((CatalogPlugin) other).name());
    }

    @Override
    public int hashCode() {
      return name().hashCode();
    }

    private PinnedTable pin(Identifier ident) throws NoSuchTableException {
      Table loaded = delegate.loadTable(ident);
      if (!(loaded instanceof SparkTable)) {
        return new PinnedTable(loaded);
      }

      SparkTable sparkTable = (SparkTable) loaded;
      org.apache.iceberg.Transaction icebergTransaction = sparkTable.table().newTransaction();
      PinnedTable pinned = new PinnedTable(sparkTable, icebergTransaction);
      pinned.setSparkTable(
          sparkTable.copyWithTable(
              icebergTransaction.table(), pinned::recordScanRead, pinned::activateWrite));
      return pinned;
    }

    private boolean registerScans(Scan[] scans) {
      PinnedTable[] routed = new PinnedTable[scans.length];
      for (int index = 0; index < scans.length; index += 1) {
        Scan scan = scans[index];
        if (!(scan instanceof SparkBatchQueryScan)) {
          return false;
        }

        SparkBatchQueryScan icebergScan = (SparkBatchQueryScan) scan;
        PinnedTable match =
            tables.values().stream()
                .filter(table -> table.matches(icebergScan))
                .findFirst()
                .orElse(null);
        if (match == null) {
          return false;
        }

        routed[index] = match;
      }

      for (int index = 0; index < routed.length; index += 1) {
        routed[index].recordScanRead(scans[index]);
      }

      return true;
    }

    private void commit() {
      List<PinnedTable> writeTables = writeTables();
      writeTables.forEach(PinnedTable::validateReadSnapshot);
      writeTables.forEach(PinnedTable::commit);
    }

    private List<PinnedTable> writeTables() {
      List<Map.Entry<Identifier, PinnedTable>> entries = new ArrayList<>(tables.entrySet());
      entries.sort(Comparator.comparing(entry -> entry.getKey().toString()));

      List<PinnedTable> writeTables = new ArrayList<>();
      for (Map.Entry<Identifier, PinnedTable> entry : entries) {
        if (entry.getValue().writeActive()) {
          writeTables.add(entry.getValue());
        }
      }

      return writeTables;
    }

    private void close() {
      tables.clear();
    }
  }

  private static class PinnedTable {
    private final SparkTable original;
    private final org.apache.iceberg.Transaction transaction;
    private final List<Scan> readScans = new ArrayList<>();
    private Table sparkTable;
    private boolean writeActive = false;

    private PinnedTable(Table table) {
      this.original = null;
      this.transaction = null;
      this.sparkTable = table;
    }

    private PinnedTable(SparkTable original, org.apache.iceberg.Transaction transaction) {
      this.original = original;
      this.transaction = transaction;
    }

    private void setSparkTable(Table newSparkTable) {
      this.sparkTable = newSparkTable;
    }

    private Table sparkTable() {
      return sparkTable;
    }

    private synchronized void recordScanRead(Scan scan) {
      readScans.add(scan);
    }

    private synchronized void activateWrite() {
      this.writeActive = true;
    }

    private synchronized boolean writeActive() {
      return writeActive;
    }

    private boolean matches(SparkBatchQueryScan scan) {
      return original != null
          && original.id().equals(scan.table().uuid().toString())
          && Objects.equals(original.snapshotId(), scan.snapshotId());
    }

    private synchronized void validateReadSnapshot() {
      if (readScans.isEmpty() || !writeActive || original == null) {
        return;
      }

      org.apache.iceberg.Table table = original.table();
      table.refresh();
      Snapshot current =
          original.branch() != null ? table.snapshot(original.branch()) : table.currentSnapshot();
      Long currentId = current != null ? current.snapshotId() : null;
      ValidationException.check(
          Objects.equals(original.snapshotId(), currentId),
          "Cannot commit transaction: table %s changed from snapshot %s to %s after it was read",
          table.name(),
          original.snapshotId(),
          currentId);
    }

    private void commit() {
      if (transaction != null) {
        transaction.commitTransaction();
      }
    }
  }
}
