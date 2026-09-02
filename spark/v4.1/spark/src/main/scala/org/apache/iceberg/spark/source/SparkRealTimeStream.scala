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
package org.apache.iceberg.spark.source

import org.apache.iceberg.{BaseCombinedScanTask, FileScanTask, Table}
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.SparkReadConf
import org.apache.iceberg.types.Types
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rpc.{
  RpcAddress,
  RpcEndpointRef,
  RpcEnv,
  ThreadSafeRpcEndpoint
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsRealTimeRead}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.util.RpcUtils

private[source] case class NextRealTimeTask(
    shardId: Int,
    offset: StreamingOffset)

private[source] case class RealTimeTaskResponse(
    task: Option[FileScanTask],
    nextOffset: StreamingOffset)

private[source] class IcebergRealTimeCoordinator(
    override val rpcEnv: RpcEnv,
    table: Table,
    readConf: SparkReadConf,
    numShards: Int)
    extends ThreadSafeRpcEndpoint {

  private val planner = new SyncSparkMicroBatchPlanner(table, readConf, null)

  override def receiveAndReply(context: org.apache.spark.rpc.RpcCallContext)
      : PartialFunction[Any, Unit] = {
    case request: NextRealTimeTask =>
      context.reply(nextTask(request))
  }

  override def onStop(): Unit = planner.stop()

  private def nextTask(request: NextRealTimeTask): RealTimeTaskResponse = synchronized {
    Preconditions.checkArgument(
      request.shardId >= 0 && request.shardId < numShards,
      "Invalid real-time reader shard: %s",
      request.shardId)

    var currentOffset = request.offset
    while (true) {
      val nextOffset = planner.latestOffset(currentOffset, ReadLimit.maxFiles(1))
      if (nextOffset == null) {
        return RealTimeTaskResponse(None, currentOffset)
      }

      val tasks = planner.planFiles(currentOffset, nextOffset)
      currentOffset = nextOffset
      if (tasks.size() > 0) {
        Preconditions.checkState(
          tasks.size() == 1,
          "Expected one real-time file task but found %s",
          tasks.size())
        val task = tasks.get(0)
        if (shard(task) == request.shardId) {
          return RealTimeTaskResponse(Some(task), currentOffset)
        }
      }
    }

    throw new IllegalStateException("Unreachable")
  }

  private def shard(task: FileScanTask): Int =
    Math.floorMod(task.file().location().hashCode, numShards)
}

private[source] case class SparkRealTimeInputPartition(
    endpointName: String,
    endpointAddress: RpcAddress,
    shardId: Int,
    startOffset: StreamingOffset,
    numShards: Int,
    tableBroadcast: Broadcast[Table],
    fileIOBroadcast: Broadcast[org.apache.iceberg.io.FileIO],
    projection: String,
    caseSensitive: Boolean,
    cacheDeleteFilesOnExecutors: Boolean)
    extends InputPartition

private[source] class SparkRealTimeReaderFactory extends PartitionReaderFactory {
  private val boundedFactory = new SparkRowReaderFactory()

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case realTime: SparkRealTimeInputPartition =>
        new SparkRealTimePartitionReader(realTime)
      case bounded =>
        boundedFactory.createReader(bounded)
    }
}

private[source] class SparkRealTimePartitionReader(partition: SparkRealTimeInputPartition)
    extends SupportsRealTimeRead[InternalRow] {

  private val endpoint: RpcEndpointRef = RpcUtils.makeDriverRef(
    partition.endpointName,
    partition.endpointAddress.host,
    partition.endpointAddress.port,
    SparkEnv.get.rpcEnv)

  private var currentOffset = partition.startOffset
  private var nextOffset: StreamingOffset = _
  private var currentReader: RowDataReader = _
  private var current: InternalRow = _

  if (TaskContext.get() == null) {
    throw new IllegalStateException("Task context was not set")
  }

  override def nextWithTimeout(timeoutMs: java.lang.Long): RecordStatus = {
    val startNanos = System.nanoTime()
    var remainingMs = timeoutMs.longValue()

    while (remainingMs >= 0L) {
      if (currentReader != null) {
        if (currentReader.next()) {
          current = currentReader.get()
          return RecordStatus.newStatusWithoutArrivalTime(true)
        }

        currentReader.close()
        currentReader = null
        currentOffset = nextOffset
      }

      val response = endpoint.askSync[RealTimeTaskResponse](
        NextRealTimeTask(partition.shardId, currentOffset))
      response.task match {
        case Some(task) =>
          currentReader = new RowDataReader(inputPartition(task))
          nextOffset = response.nextOffset

        case None =>
          currentOffset = response.nextOffset
          if (remainingMs == 0L) {
            return RecordStatus.newStatusWithoutArrivalTime(false)
          }

          Thread.sleep(Math.min(50L, remainingMs))
      }

      remainingMs = timeoutMs.longValue() - (System.nanoTime() - startNanos) / 1000000L
    }

    RecordStatus.newStatusWithoutArrivalTime(false)
  }

  override def next(): Boolean =
    nextWithTimeout(java.lang.Long.valueOf(0L)).hasRecord()

  override def get(): InternalRow = current

  override def getOffset(): RealTimePartitionOffset =
    new RealTimePartitionOffset(partition.shardId, currentOffset)

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
      currentReader = null
    }
  }

  private def inputPartition(task: FileScanTask): SparkInputPartition =
    new SparkInputPartition(
      Types.StructType.of(),
      new BaseCombinedScanTask(task),
      partition.tableBroadcast,
      partition.fileIOBroadcast,
      partition.projection,
      partition.caseSensitive,
      SparkPlanningUtil.NO_LOCATION_PREFERENCE,
      partition.cacheDeleteFilesOnExecutors)
}
