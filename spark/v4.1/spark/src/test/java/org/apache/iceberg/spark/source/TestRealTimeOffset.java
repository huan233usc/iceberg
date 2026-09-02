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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TestRealTimeOffset {

  @Test
  void roundTrip() {
    RealTimeOffset expected =
        new RealTimeOffset(
            Map.of(
                0,
                new RealTimePartitionOffset(0, new StreamingOffset(11L, 2L, false)),
                1,
                new RealTimePartitionOffset(1, new StreamingOffset(12L, 4L, true))));

    String json = expected.json();

    assertThat(RealTimeOffset.isRealTimeOffset(json)).isTrue();
    assertThat(RealTimeOffset.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void initialOffsetUsesStableShardCount() {
    StreamingOffset streamingOffset = new StreamingOffset(11L, 2L, false);

    RealTimeOffset offset = RealTimeOffset.initial(streamingOffset, 3);

    assertThat(offset.numShards()).isEqualTo(3);
    assertThat(offset.partitionOffsets().values())
        .extracting(RealTimePartitionOffset::streamingOffset)
        .containsOnly(streamingOffset);
  }

  @Test
  void rejectsMissingShard() {
    Map<Integer, RealTimePartitionOffset> offsets =
        Map.of(
            0,
            new RealTimePartitionOffset(0, StreamingOffset.START_OFFSET),
            2,
            new RealTimePartitionOffset(2, StreamingOffset.START_OFFSET));

    assertThatThrownBy(() -> new RealTimeOffset(offsets))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing real-time reader shard: 1");
  }
}
