/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;

class FileCommitterMetricsTest {
    static final Uuid TOPIC_ID = new Uuid(1, 2);
    static final TopicIdPartition T0 = new TopicIdPartition(TOPIC_ID, 0, "t");
    static final TopicIdPartition T1 = new TopicIdPartition(TOPIC_ID, 1, "t");
    static final TopicIdPartition T2 = new TopicIdPartition(TOPIC_ID, 2, "t");

    FileCommitterMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new FileCommitterMetrics(new MockTime());
    }

    @AfterEach
    void tearDown() throws IOException {
        metrics.close();
    }

    private static CommitBatchRequest request(final TopicIdPartition tp, final int byteOffset) {
        return CommitBatchRequest.of(0, tp, byteOffset, 10, 0, 0, 1000, TimestampType.CREATE_TIME);
    }

    /**
     * Builds a request list grouped by partition (as BatchBuffer.close() produces), given a fan-in per partition.
     **/
    private static List<CommitBatchRequest> grouped(final TopicIdPartition... partitionPerBatch) {
        final List<CommitBatchRequest> requests = new ArrayList<>();
        int byteOffset = 0;
        for (final TopicIdPartition tp : partitionPerBatch) {
            requests.add(request(tp, byteOffset));
            byteOffset += 10;
        }
        return requests;
    }

    @Test
    void emptyIsNoOp() {
        metrics.partitionFanInAdded(List.of());
        assertThat(metrics.partitionsPerCommitHistogram.count()).isZero();
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.count()).isZero();
    }

    @Test
    void singlePartitionSingleBatch() {
        metrics.partitionFanInAdded(grouped(T0));

        // 1 commit observed -> 1 partition; 1 partition observed -> fan-in 1.
        assertThat(metrics.partitionsPerCommitHistogram.count()).isEqualTo(1);
        assertThat(metrics.partitionsPerCommitHistogram.max()).isEqualTo(1.0);
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.count()).isEqualTo(1);
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.max()).isEqualTo(1.0);
    }

    @Test
    void singlePartitionManyBatchesCountsAsOneRunWithFullFanIn() {
        metrics.partitionFanInAdded(grouped(T0, T0, T0, T0, T0));

        assertThat(metrics.partitionsPerCommitHistogram.count()).isEqualTo(1);
        assertThat(metrics.partitionsPerCommitHistogram.max()).isEqualTo(1.0); // one distinct partition
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.count()).isEqualTo(1); // one partition observed
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.max()).isEqualTo(5.0); // fan-in 5
    }

    @Test
    void multiplePartitionsWithDifferentFanIn() {
        // Grouped by partition (the invariant from BatchBuffer.close()): T0 x2, T1 x1, T2 x3.
        metrics.partitionFanInAdded(grouped(T0, T0, T1, T2, T2, T2));

        // 3 distinct partitions in this one commit.
        assertThat(metrics.partitionsPerCommitHistogram.count()).isEqualTo(1);
        assertThat(metrics.partitionsPerCommitHistogram.max()).isEqualTo(3.0);

        // 3 partitions observed; fan-ins were 2, 1, 3 -> min 1, max 3.
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.count()).isEqualTo(3);
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.max()).isEqualTo(3.0);
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.min()).isEqualTo(1.0);
    }

    @Test
    void accumulatesAcrossCommits() {
        metrics.partitionFanInAdded(grouped(T0, T0));      // 1 partition, fan-in 2
        metrics.partitionFanInAdded(grouped(T0, T1, T2));  // 3 partitions, fan-in 1 each

        // Two commits observed.
        assertThat(metrics.partitionsPerCommitHistogram.count()).isEqualTo(2);
        assertThat(metrics.partitionsPerCommitHistogram.max()).isEqualTo(3.0);
        assertThat(metrics.partitionsPerCommitHistogram.min()).isEqualTo(1.0);

        // Partitions observed across both commits: 1 + 3 = 4; max fan-in was 2.
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.count()).isEqualTo(4);
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.max()).isEqualTo(2.0);
        assertThat(metrics.batchesPerPartitionPerCommitHistogram.min()).isEqualTo(1.0);
    }
}
