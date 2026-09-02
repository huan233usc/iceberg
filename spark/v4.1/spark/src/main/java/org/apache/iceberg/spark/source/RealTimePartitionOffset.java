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

import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

class RealTimePartitionOffset implements PartitionOffset {
  private final int shardId;
  private final StreamingOffset streamingOffset;

  RealTimePartitionOffset(int shardId, StreamingOffset streamingOffset) {
    this.shardId = shardId;
    this.streamingOffset = streamingOffset;
  }

  int shardId() {
    return shardId;
  }

  StreamingOffset streamingOffset() {
    return streamingOffset;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RealTimePartitionOffset) {
      RealTimePartitionOffset that = (RealTimePartitionOffset) other;
      return shardId == that.shardId && streamingOffset.equals(that.streamingOffset);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shardId, streamingOffset);
  }
}
