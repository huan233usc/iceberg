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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDataFileContentStats {
  private static final long RECORD_COUNT = 10L;
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.BinaryType.get()));
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(SCHEMA, List.of(1, 2));

  @Test
  void builderOwnsContentStats() {
    ContentStats source = stats();
    DataFiles.Builder builder = builder().withContentStats(source);

    source.<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);

    DataFile file = builder.build();
    assertThat(file.contentStats().<ByteBuffer>statsFor(2).lowerBound()).isEqualTo(bytes(1));
    file.contentStats().<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);
    assertThat(builder.build().contentStats().<ByteBuffer>statsFor(2).lowerBound())
        .isEqualTo(bytes(1));
  }

  @Test
  void fullCopyOwnsContentStats() {
    DataFile original = builder().withContentStats(stats()).build();
    DataFile copy = original.copy();

    original.contentStats().<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);

    assertThat(copy.contentStats().fieldStats())
        .extracting(FieldStats::fieldId)
        .containsExactlyInAnyOrder(1, 2);
    assertThat(copy.contentStats().<ByteBuffer>statsFor(2).lowerBound()).isEqualTo(bytes(1));
  }

  @Test
  void projectedCopyKeepsOnlyRequestedStats() {
    DataFile original = builder().withContentStats(stats()).build();
    DataFile copy = original.copyWithStats(Set.of(2));

    original.contentStats().<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);

    assertThat(copy.contentStats().fieldStats()).extracting(FieldStats::fieldId).containsExactly(2);
    assertThat(copy.contentStats().<ByteBuffer>statsFor(2).lowerBound()).isEqualTo(bytes(1));
  }

  @Test
  void copiesCanDropNativeStats() {
    DataFile file = builder().withContentStats(stats()).build();

    assertThat(file.copyWithoutStats().contentStats()).isNull();
    assertThat(file.copyWithStats(Set.of()).contentStats()).isNull();
  }

  @Test
  void builderCopySnapshotsReaderOwnedStats() {
    DataFile source = builder().withContentStats(stats()).build();
    DataFiles.Builder copy = builder().copy(source);

    source.contentStats().<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);

    assertThat(copy.build().contentStats().<ByteBuffer>statsFor(2).lowerBound())
        .isEqualTo(bytes(1));
  }

  private static DataFiles.Builder builder() {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("data.parquet")
        .withRecordCount(RECORD_COUNT)
        .withFileSizeInBytes(128L);
  }

  private static ContentStatsStruct stats() {
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    FieldStatsStruct<Long> id = new FieldStatsStruct<>(STATS_TYPE.fieldType("id").asStructType());
    id.fromFieldMetrics(new FieldMetrics<>(1, RECORD_COUNT, 0L, 1L, RECORD_COUNT));
    stats.setStats(1, id);
    FieldStatsStruct<ByteBuffer> data =
        new FieldStatsStruct<>(STATS_TYPE.fieldType("data").asStructType());
    data.fromFieldMetrics(new FieldMetrics<>(2, RECORD_COUNT, 0L, bytes(1), bytes(8)));
    stats.setStats(2, data);
    return stats;
  }

  private static ByteBuffer bytes(int value) {
    return ByteBuffer.wrap(new byte[] {(byte) value});
  }
}
