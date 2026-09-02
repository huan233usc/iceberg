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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.apache.spark.sql.connector.read.streaming.Offset;

class RealTimeOffset extends Offset {
  private static final int VERSION = 2;
  private static final String VERSION_FIELD = "version";
  private static final String SHARDS = "shards";
  private static final String SHARD_ID = "shard-id";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String POSITION = "position";
  private static final String SCAN_ALL_FILES = "scan-all-files";

  private final Map<Integer, RealTimePartitionOffset> offsets;

  RealTimeOffset(Map<Integer, RealTimePartitionOffset> offsets) {
    Preconditions.checkArgument(!offsets.isEmpty(), "Real-time offset must contain reader shards");

    TreeMap<Integer, RealTimePartitionOffset> sortedOffsets = new TreeMap<>(offsets);
    for (int shardId = 0; shardId < sortedOffsets.size(); shardId++) {
      Preconditions.checkArgument(
          sortedOffsets.containsKey(shardId), "Missing real-time reader shard: %s", shardId);
      Preconditions.checkArgument(
          sortedOffsets.get(shardId).shardId() == shardId,
          "Invalid real-time partition offset for shard %s: %s",
          shardId,
          sortedOffsets.get(shardId).shardId());
    }

    this.offsets = Collections.unmodifiableMap(sortedOffsets);
  }

  static RealTimeOffset initial(StreamingOffset offset, int numShards) {
    Map<Integer, RealTimePartitionOffset> offsets = new TreeMap<>();
    for (int shardId = 0; shardId < numShards; shardId++) {
      offsets.put(shardId, new RealTimePartitionOffset(shardId, offset));
    }

    return new RealTimeOffset(offsets);
  }

  static boolean isRealTimeOffset(String json) {
    try {
      JsonNode node = JsonUtil.mapper().readValue(json, JsonNode.class);
      return node.has(VERSION_FIELD) && node.get(VERSION_FIELD).asInt() == VERSION;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to inspect streaming offset JSON", e);
    }
  }

  static RealTimeOffset fromJson(String json) {
    Preconditions.checkNotNull(json, "Cannot parse real-time offset JSON: null");

    try {
      JsonNode node = JsonUtil.mapper().readValue(json, JsonNode.class);
      Preconditions.checkArgument(
          JsonUtil.getInt(VERSION_FIELD, node) == VERSION,
          "Unsupported real-time offset version: %s",
          JsonUtil.getInt(VERSION_FIELD, node));

      Map<Integer, RealTimePartitionOffset> offsets = new TreeMap<>();
      for (JsonNode shard : node.get(SHARDS)) {
        int shardId = JsonUtil.getInt(SHARD_ID, shard);
        StreamingOffset streamingOffset =
            new StreamingOffset(
                JsonUtil.getLong(SNAPSHOT_ID, shard),
                JsonUtil.getLong(POSITION, shard),
                JsonUtil.getBool(SCAN_ALL_FILES, shard));
        offsets.put(shardId, new RealTimePartitionOffset(shardId, streamingOffset));
      }

      return new RealTimeOffset(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to parse real-time offset JSON", e);
    }
  }

  int numShards() {
    return offsets.size();
  }

  RealTimePartitionOffset partitionOffset(int shardId) {
    RealTimePartitionOffset offset = offsets.get(shardId);
    Preconditions.checkArgument(offset != null, "Unknown real-time reader shard: %s", shardId);
    return offset;
  }

  Map<Integer, RealTimePartitionOffset> partitionOffsets() {
    return offsets;
  }

  @Override
  public String json() {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.writeStartObject();
      generator.writeNumberField(VERSION_FIELD, VERSION);
      generator.writeArrayFieldStart(SHARDS);
      for (RealTimePartitionOffset offset : offsets.values()) {
        StreamingOffset streamingOffset = offset.streamingOffset();
        generator.writeStartObject();
        generator.writeNumberField(SHARD_ID, offset.shardId());
        generator.writeNumberField(SNAPSHOT_ID, streamingOffset.snapshotId());
        generator.writeNumberField(POSITION, streamingOffset.position());
        generator.writeBooleanField(SCAN_ALL_FILES, streamingOffset.shouldScanAllFiles());
        generator.writeEndObject();
      }

      generator.writeEndArray();
      generator.writeEndObject();
      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write real-time offset JSON", e);
    }

    return writer.toString();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RealTimeOffset && offsets.equals(((RealTimeOffset) other).offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(offsets);
  }

  @Override
  public String toString() {
    return "RealTimeOffset" + offsets;
  }
}
