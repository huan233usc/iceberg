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
import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.rpc.ThreadSafeRpcEndpoint;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead;
import org.apache.spark.util.RpcUtils;
import scala.PartialFunction;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

class IcebergRealTimeCoordinator implements ThreadSafeRpcEndpoint {
  private final RpcEnv rpcEnv;
  private final SparkMicroBatchPlanner planner;
  private final int numShards;

  IcebergRealTimeCoordinator(RpcEnv rpcEnv, Table table, SparkReadConf readConf, int numShards) {
    this.rpcEnv = rpcEnv;
    this.planner = new SyncSparkMicroBatchPlanner(table, readConf, null);
    this.numShards = numShards;
  }

  @Override
  public RpcEnv rpcEnv() {
    return rpcEnv;
  }

  @Override
  public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
    return new AbstractPartialFunction<>() {
      @Override
      public boolean isDefinedAt(Object message) {
        return message instanceof NextRealTimeTask;
      }

      @Override
      public BoxedUnit apply(Object message) {
        context.reply(nextTask((NextRealTimeTask) message));
        return BoxedUnit.UNIT;
      }
    };
  }

  @Override
  public void onStop() {
    planner.stop();
  }

  private synchronized RealTimeTaskResponse nextTask(NextRealTimeTask request) {
    Preconditions.checkArgument(
        request.shardId() >= 0 && request.shardId() < numShards,
        "Invalid real-time reader shard: %s",
        request.shardId());

    StreamingOffset currentOffset = request.offset();
    while (true) {
      StreamingOffset nextOffset = planner.latestOffset(currentOffset, ReadLimit.maxFiles(1));
      if (nextOffset == null || nextOffset.equals(currentOffset)) {
        return new RealTimeTaskResponse(null, currentOffset);
      }

      List<FileScanTask> tasks = planner.planFiles(currentOffset, nextOffset);
      currentOffset = nextOffset;
      if (!tasks.isEmpty()) {
        Preconditions.checkState(
            tasks.size() == 1, "Expected one real-time file task but found %s", tasks.size());
        FileScanTask task = tasks.get(0);
        if (shard(task) == request.shardId()) {
          return new RealTimeTaskResponse(task, currentOffset);
        }
      }
    }
  }

  private int shard(FileScanTask task) {
    return Math.floorMod(task.file().location().hashCode(), numShards);
  }
}

class NextRealTimeTask implements Serializable {
  private final int shardId;
  private final StreamingOffset offset;

  NextRealTimeTask(int shardId, StreamingOffset offset) {
    this.shardId = shardId;
    this.offset = offset;
  }

  int shardId() {
    return shardId;
  }

  StreamingOffset offset() {
    return offset;
  }
}

class RealTimeTaskResponse implements Serializable {
  private final FileScanTask task;
  private final StreamingOffset nextOffset;

  RealTimeTaskResponse(FileScanTask task, StreamingOffset nextOffset) {
    this.task = task;
    this.nextOffset = nextOffset;
  }

  FileScanTask task() {
    return task;
  }

  StreamingOffset nextOffset() {
    return nextOffset;
  }
}

class SparkRealTimeInputPartition implements InputPartition, Serializable {
  private final String endpointName;
  private final RpcAddress endpointAddress;
  private final int shardId;
  private final StreamingOffset startOffset;
  private final Broadcast<Table> tableBroadcast;
  private final Broadcast<FileIO> fileIOBroadcast;
  private final String projection;
  private final boolean caseSensitive;
  private final boolean cacheDeleteFilesOnExecutors;

  SparkRealTimeInputPartition(
      String endpointName,
      RpcAddress endpointAddress,
      int shardId,
      StreamingOffset startOffset,
      int numShards,
      Broadcast<Table> tableBroadcast,
      Broadcast<FileIO> fileIOBroadcast,
      String projection,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    Preconditions.checkArgument(
        shardId >= 0 && shardId < numShards,
        "Invalid real-time reader shard %s of %s",
        shardId,
        numShards);
    this.endpointName = endpointName;
    this.endpointAddress = endpointAddress;
    this.shardId = shardId;
    this.startOffset = startOffset;
    this.tableBroadcast = tableBroadcast;
    this.fileIOBroadcast = fileIOBroadcast;
    this.projection = projection;
    this.caseSensitive = caseSensitive;
    this.cacheDeleteFilesOnExecutors = cacheDeleteFilesOnExecutors;
  }

  String endpointName() {
    return endpointName;
  }

  RpcAddress endpointAddress() {
    return endpointAddress;
  }

  int shardId() {
    return shardId;
  }

  StreamingOffset startOffset() {
    return startOffset;
  }

  Broadcast<Table> tableBroadcast() {
    return tableBroadcast;
  }

  Broadcast<FileIO> fileIOBroadcast() {
    return fileIOBroadcast;
  }

  String projection() {
    return projection;
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  boolean cacheDeleteFilesOnExecutors() {
    return cacheDeleteFilesOnExecutors;
  }
}

class SparkRealTimeReaderFactory implements PartitionReaderFactory {
  private final SparkRowReaderFactory boundedFactory = new SparkRowReaderFactory();

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
    if (inputPartition instanceof SparkRealTimeInputPartition) {
      return new SparkRealTimePartitionReader((SparkRealTimeInputPartition) inputPartition);
    }

    return boundedFactory.createReader(inputPartition);
  }
}

class SparkRealTimePartitionReader implements SupportsRealTimeRead<InternalRow> {
  private static final long NANOS_PER_MILLISECOND = 1_000_000L;
  private static final long POLL_INTERVAL_MS = 50L;
  private static final ClassTag<RealTimeTaskResponse> RESPONSE_CLASS_TAG =
      ClassTag$.MODULE$.apply(RealTimeTaskResponse.class);

  private final SparkRealTimeInputPartition partition;
  private final RpcEndpointRef endpoint;

  private StreamingOffset currentOffset;
  private StreamingOffset nextOffset;
  private RowDataReader currentReader;
  private InternalRow current;

  SparkRealTimePartitionReader(SparkRealTimeInputPartition partition) {
    this.partition = partition;
    this.endpoint =
        RpcUtils.makeDriverRef(
            partition.endpointName(),
            partition.endpointAddress().host(),
            partition.endpointAddress().port(),
            SparkEnv.get().rpcEnv());
    this.currentOffset = partition.startOffset();

    Preconditions.checkState(TaskContext.get() != null, "Task context was not set");
  }

  @Override
  public RecordStatus nextWithTimeout(Long timeoutMs) throws IOException {
    long startNanos = System.nanoTime();
    long remainingMs = timeoutMs;

    while (remainingMs >= 0L) {
      if (currentReader != null) {
        if (currentReader.next()) {
          this.current = currentReader.get();
          return RecordStatus.newStatusWithoutArrivalTime(true);
        }

        currentReader.close();
        this.currentReader = null;
        this.currentOffset = nextOffset;
      }

      RealTimeTaskResponse response =
          endpoint.askSync(
              new NextRealTimeTask(partition.shardId(), currentOffset), RESPONSE_CLASS_TAG);
      if (response.task() != null) {
        this.currentReader = new RowDataReader(inputPartition(response.task()));
        this.nextOffset = response.nextOffset();
      } else {
        this.currentOffset = response.nextOffset();
        if (remainingMs == 0L) {
          return RecordStatus.newStatusWithoutArrivalTime(false);
        }

        try {
          Thread.sleep(Math.min(POLL_INTERVAL_MS, remainingMs));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for a real-time Iceberg snapshot", e);
        }
      }

      remainingMs = timeoutMs - (System.nanoTime() - startNanos) / NANOS_PER_MILLISECOND;
    }

    return RecordStatus.newStatusWithoutArrivalTime(false);
  }

  @Override
  public boolean next() throws IOException {
    return nextWithTimeout(0L).hasRecord();
  }

  @Override
  public InternalRow get() {
    return current;
  }

  @Override
  public RealTimePartitionOffset getOffset() {
    return new RealTimePartitionOffset(partition.shardId(), currentOffset);
  }

  @Override
  public void close() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      this.currentReader = null;
    }
  }

  private SparkInputPartition inputPartition(FileScanTask task) {
    return new SparkInputPartition(
        Types.StructType.of(),
        new BaseCombinedScanTask(task),
        partition.tableBroadcast(),
        partition.fileIOBroadcast(),
        partition.projection(),
        partition.caseSensitive(),
        SparkPlanningUtil.NO_LOCATION_PREFERENCE,
        partition.cacheDeleteFilesOnExecutors());
  }
}
