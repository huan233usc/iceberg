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

import java.util.List;
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
            List.of(
                new FieldMetrics<>(
                    fieldId, 5L, 1L, -1L, "a", "z", Types.StringType.get(), totalBytes)));

    assertThat(stats.<String>statsFor(fieldId).lowerBound()).isEqualTo("a");
    assertThat(stats.<String>statsFor(fieldId).upperBound()).isEqualTo("z");
    assertThat(stats.statsFor(fieldId).totalBytes()).isEqualTo(totalBytes);
  }

  @Test
  void copyWithoutFieldCountsDropsTotalBytesForExcludedFields() {
    Metrics copy = MetricsUtil.copyWithoutFieldCounts(metricsWithTotalBytes(), ImmutableSet.of(1));

    assertThat(copy.totalBytes()).isEqualTo(ImmutableMap.of(2, 20L));
  }

  @Test
  void copyWithoutFieldCountsAndBoundsDropsTotalBytesForExcludedFields() {
    Metrics copy =
        MetricsUtil.copyWithoutFieldCountsAndBounds(metricsWithTotalBytes(), ImmutableSet.of(1));

    assertThat(copy.totalBytes()).isEqualTo(ImmutableMap.of(2, 20L));
  }

  @Test
  void copyDropsTotalBytesEntirelyWhenAllFieldsExcluded() {
    // once every tracked field is excluded the filtered map is empty, and copyWithoutKeys returns
    // null rather than an empty map, matching the "null otherwise" contract of totalBytes()
    Metrics copy =
        MetricsUtil.copyWithoutFieldCounts(metricsWithTotalBytes(), ImmutableSet.of(1, 2));

    assertThat(copy.totalBytes()).isNull();
  }

  @Test
  void copyWithStatsReturnsNullTotalBytesWhenNoRequestedColumnMatches() {
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withMetrics(metricsWithTotalBytes())
            .build();

    assertThat(file.copyWithStats(ImmutableSet.of(2)).totalBytes())
        .isEqualTo(ImmutableMap.of(2, 20L));
    assertThat(file.copyWithStats(ImmutableSet.of(3)).totalBytes()).isNull();
  }

  private static Metrics metricsWithTotalBytes() {
    return new Metrics(
        3L, null, null, null, null, null, null, ImmutableMap.of(1, 10L, 2, 20L), null);
  }
}
