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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestMetricsUtil {

  @Test
  void contentStatsPreservesTypedBoundsAndLongByteTotals() {
    int fieldId = 1;
    Schema schema = new Schema(Types.NestedField.optional(fieldId, "data", Types.StringType.get()));
    long totalBytes = Integer.MAX_VALUE + 1L;
    ContentStats stats =
        MetricsUtil.contentStats(
            schema,
            MetricsConfig.getDefault(),
            ImmutableList.of(
                new FieldMetrics<>(
                    fieldId, 5L, 1L, -1L, "a", "z", Types.StringType.get(), totalBytes)));

    assertThat(stats.<String>statsFor(fieldId).lowerBound()).isEqualTo("a");
    assertThat(stats.<String>statsFor(fieldId).upperBound()).isEqualTo("z");
    assertThat(stats.statsFor(fieldId).totalBytes()).isEqualTo(totalBytes);
  }

  @Test
  void contentStatsConvertsGeospatialBoundsToStructs() {
    int fieldId = 1;
    Schema schema =
        new Schema(Types.NestedField.optional(fieldId, "geom", Types.GeometryType.crs84()));
    GeospatialBound lower = GeospatialBound.createXYM(1.0, 2.0, 3.0);
    GeospatialBound upper = GeospatialBound.createXYZM(4.0, 5.0, 6.0, 7.0);
    ContentStats stats =
        MetricsUtil.contentStats(
            schema,
            MetricsConfig.getDefault(),
            ImmutableList.of(
                new FieldMetrics<>(
                    fieldId, 1L, 0L, -1L, lower, upper, Types.GeometryType.crs84(), 10L)));

    FieldStats<StructLike> fieldStats = stats.statsFor(fieldId);
    assertThat(fieldStats.lowerBound().get(0, Double.class)).isEqualTo(lower.x());
    assertThat(fieldStats.lowerBound().get(1, Double.class)).isEqualTo(lower.y());
    assertThat(fieldStats.lowerBound().get(2, Double.class)).isNull();
    assertThat(fieldStats.lowerBound().get(3, Double.class)).isEqualTo(lower.m());
    assertThat(fieldStats.upperBound().get(2, Double.class)).isEqualTo(upper.z());
    assertThat(fieldStats.upperBound().get(3, Double.class)).isEqualTo(upper.m());
    assertThat(ContentStatsBackedMap.lowerBounds(stats))
        .containsEntry(fieldId, lower.toByteBuffer());
    assertThat(ContentStatsBackedMap.upperBounds(stats))
        .containsEntry(fieldId, upper.toByteBuffer());
  }

  @Test
  void contentStatsPreservesUnknownCounts() {
    int fieldId = 1;
    Schema schema = new Schema(Types.NestedField.optional(fieldId, "data", Types.StringType.get()));
    ContentStats stats =
        MetricsUtil.contentStats(
            schema,
            MetricsConfig.getDefault(),
            ImmutableList.of(new FieldMetrics<>(fieldId, -1L, -1L)));

    assertThat(stats.statsFor(fieldId).hasValueCount()).isFalse();
    assertThat(stats.statsFor(fieldId).hasNullValueCount()).isFalse();
  }

  @Test
  void copyWithoutFieldCountsDropsAvgValueSizesForExcludedFields() {
    Metrics copy =
        MetricsUtil.copyWithoutFieldCounts(metricsWithAvgValueSizes(), ImmutableSet.of(1));

    assertThat(copy.avgValueSizes()).isEqualTo(ImmutableMap.of(2, 20));
  }

  @Test
  void copyWithoutFieldCountsAndBoundsDropsAvgValueSizesForExcludedFields() {
    Metrics copy =
        MetricsUtil.copyWithoutFieldCountsAndBounds(metricsWithAvgValueSizes(), ImmutableSet.of(1));

    assertThat(copy.avgValueSizes()).isEqualTo(ImmutableMap.of(2, 20));
  }

  @Test
  void copyDropsAvgValueSizesEntirelyWhenAllFieldsExcluded() {
    Metrics copy =
        MetricsUtil.copyWithoutFieldCounts(metricsWithAvgValueSizes(), ImmutableSet.of(1, 2));

    assertThat(copy.avgValueSizes()).isNull();
  }

  @Test
  void copyWithStatsReturnsNullAvgValueSizesWhenNoRequestedColumnMatches() {
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withMetrics(metricsWithAvgValueSizes())
            .build();

    assertThat(file.copyWithStats(ImmutableSet.of(2)).avgValueSizes())
        .isEqualTo(ImmutableMap.of(2, 20));
    assertThat(file.copyWithStats(ImmutableSet.of(3)).avgValueSizes()).isNull();
  }

  @Test
  void deleteFileCopiesRetainLegacyAverageSizes() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    DeleteFile file =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes(1)
            .withPath("/path/to/delete.parquet")
            .withFileSizeInBytes(10)
            .withMetrics(metricsWithAvgValueSizes())
            .build();

    assertThat(FileMetadata.deleteFileBuilder(spec).copy(file).build().avgValueSizes())
        .isEqualTo(file.avgValueSizes());
    assertThat(file.copyWithStats(ImmutableSet.of(2)).avgValueSizes())
        .isEqualTo(ImmutableMap.of(2, 20));
    assertThat(file.copyWithoutStats().avgValueSizes()).isNull();
  }

  private static Metrics metricsWithAvgValueSizes() {
    return new Metrics(3L, null, null, null, null, null, null, ImmutableMap.of(1, 10, 2, 20), null);
  }
}
