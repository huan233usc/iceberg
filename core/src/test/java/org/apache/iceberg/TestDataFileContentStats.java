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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.iceberg.TestHelpers.RoundTripSerializer;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDataFileContentStats {
  private static final long RECORD_COUNT = 10L;
  private static final long NULL_COUNT = 2L;
  private static final long TOTAL_BYTES = 16L;
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.BinaryType.get()));
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(SCHEMA, List.of(1, 2));
  private static final List<Named<RoundTripSerializer<DataFile>>> SERIALIZERS =
      List.of(
          Named.of("Java", TestHelpers::roundTripSerialize),
          Named.of("Kryo", TestHelpers.KryoHelpers::roundTripSerialize));
  private static final List<Named<UnaryOperator<DataFile>>> WITHOUT_STATS =
      List.of(
          Named.of("copyWithoutStats", DataFile::copyWithoutStats),
          Named.of("empty projection", file -> file.copyWithStats(Set.of())));
  private static final List<Named<Consumer<DataFiles.Builder>>> REPLACE_STATS =
      List.of(
          Named.of("clear", DataFiles.Builder::clear),
          Named.of("withMetrics", builder -> builder.withMetrics(new Metrics(RECORD_COUNT))),
          Named.of("copy legacy file", builder -> builder.copy(builder().build())));

  @TempDir private Path temp;

  @Test
  void builderOwnsContentStats() {
    ContentStats source = stats();
    DataFile file = builder().withContentStats(source).build();

    source.<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);

    assertThat(file.contentStats().<ByteBuffer>statsFor(2).lowerBound()).isEqualTo(bytes(1));
  }

  @Test
  void nativeStatsSupplyLegacyReadViews() {
    Map<Integer, Long> columnSizes = Map.of(1, 32L, 2, 64L);
    DataFile file =
        builder()
            .withMetrics(new Metrics(RECORD_COUNT, columnSizes, null, null, null))
            .withContentStats(stats())
            .build();

    assertThat(file.valueCounts())
        .containsOnly(Map.entry(1, RECORD_COUNT), Map.entry(2, RECORD_COUNT));
    assertThat(file.nullValueCounts()).containsOnly(Map.entry(1, 0L), Map.entry(2, NULL_COUNT));
    assertThat(file.lowerBounds())
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), 1L)),
            Map.entry(2, bytes(1)));
    assertThat(file.upperBounds())
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), RECORD_COUNT)),
            Map.entry(2, bytes(8)));
    assertThat(file.avgValueSizes())
        .containsOnly(Map.entry(2, Math.toIntExact(TOTAL_BYTES / (RECORD_COUNT - NULL_COUNT))));
    assertThat(file.columnSizes()).isEqualTo(columnSizes);
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
    assertThat(copy.valueCounts()).containsOnly(Map.entry(2, RECORD_COUNT));
  }

  @ParameterizedTest
  @FieldSource("WITHOUT_STATS")
  void copiesWithoutStatsDropNativePayload(UnaryOperator<DataFile> copy) {
    DataFile file = copy.apply(builder().withContentStats(stats()).build());

    assertThat(file.contentStats()).isNull();
    assertThat(file.valueCounts()).isNull();
    assertThat(file.lowerBounds()).isNull();
    assertThat(file.avgValueSizes()).isNull();
  }

  @ParameterizedTest
  @FieldSource("SERIALIZERS")
  void serializationPreservesNativePayload(RoundTripSerializer<DataFile> serializer)
      throws Exception {
    DataFile original = builder().withContentStats(stats()).build();

    DataFile copy = serializer.apply(original);

    assertThat(copy.contentStats().type()).isEqualTo(original.contentStats().type());
    assertThat(copy.contentStats().<ByteBuffer>statsFor(2).lowerBound()).isEqualTo(bytes(1));
    assertThat(copy.contentStats().statsFor(2).totalBytes()).isEqualTo(TOTAL_BYTES);
    assertThat(copy.valueCounts()).isEqualTo(original.valueCounts());
    assertThat(copy.avgValueSizes()).isEqualTo(original.avgValueSizes());
  }

  @Test
  void builderCopySnapshotsReaderOwnedStats() {
    DataFile source = builder().withContentStats(stats()).build();
    DataFiles.Builder copy = builder().copy(source);

    source.contentStats().<ByteBuffer>statsFor(2).lowerBound().put(0, (byte) 9);

    DataFile file = copy.build();
    assertThat(file.contentStats().<ByteBuffer>statsFor(2).lowerBound()).isEqualTo(bytes(1));
    assertThat(file.lowerBounds()).containsEntry(2, bytes(1));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void legacyManifestsKeepNativeStatsReadViews(int formatVersion) throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    DataFile original = builder().withContentStats(stats()).build();
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            formatVersion, spec, Files.localOutput(temp.resolve("manifest.avro").toFile()), 1L);
    try (writer) {
      writer.add(original);
    }

    try (ManifestReader<DataFile> reader =
            ManifestFiles.read(
                writer.toManifestFile(),
                new TestTables.LocalFileIO(),
                Map.of(spec.specId(), spec));
        CloseableIterator<DataFile> files = reader.select(List.of("*")).iterator()) {
      DataFile restored = files.next();
      assertThat(restored.valueCounts()).isEqualTo(original.valueCounts());
      assertThat(restored.lowerBounds()).isEqualTo(original.lowerBounds());
      assertThat(restored.upperBounds()).isEqualTo(original.upperBounds());
      assertThat(restored.contentStats()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("REPLACE_STATS")
  void builderReuseDiscardsNativePayload(Consumer<DataFiles.Builder> replaceStats) {
    DataFiles.Builder builder = builder().withContentStats(stats());

    replaceStats.accept(builder);
    DataFile file =
        builder
            .withPath("next.parquet")
            .withRecordCount(RECORD_COUNT)
            .withFileSizeInBytes(128L)
            .build();

    assertThat(file.contentStats()).isNull();
    assertThat(file.valueCounts()).isNull();
    assertThat(file.avgValueSizes()).isNull();
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
    data.fromFieldMetrics(
        new FieldMetrics<>(
            2,
            RECORD_COUNT,
            NULL_COUNT,
            -1L,
            bytes(1),
            bytes(8),
            Types.BinaryType.get(),
            TOTAL_BYTES));
    stats.setStats(2, data);
    return stats;
  }

  private static ByteBuffer bytes(int value) {
    return ByteBuffer.wrap(new byte[] {(byte) value});
  }
}
