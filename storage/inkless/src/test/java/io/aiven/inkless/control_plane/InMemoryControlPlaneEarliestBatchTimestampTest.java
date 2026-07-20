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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In-memory mirror of {@link io.aiven.inkless.control_plane.postgres.LogsEarliestBatchTimestampTest}.
 * Verifies that LogInfo.earliestBatchTimestamp (the effective timestamp of the batch at log_start) is
 * maintained across every write path that changes the oldest batch: commit, delete, prune, and enforce.
 * The field is intentionally internal (not exposed on the ControlPlane interface), so it is read via
 * reflection; it exists only for parity with the Postgres control plane's short-circuit.
 */
class InMemoryControlPlaneEarliestBatchTimestampTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456789;
    static final String TOPIC = "topic0";
    static final Uuid TOPIC_ID = new Uuid(10, 12);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);

    MockTime time = new MockTime(0, 0L, 0L);
    InMemoryControlPlane controlPlane;

    @BeforeEach
    void setUp() {
        controlPlane = new InMemoryControlPlane(time);
        controlPlane.configure(Map.of());
        controlPlane.createTopicAndPartitions(
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 1)));
    }

    @Test
    void freshLogHasNullTimestamp() {
        assertThat(earliestTs(T0P0)).isNull();
    }

    @Test
    void firstCommitIntoEmptyLogSetsTimestampFromOldestBatch() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    @Test
    void logAppendTimeUsesCommitClock() {
        time.setCurrentTimeMs(7777L);
        // LOG_APPEND_TIME -> effective ts is the commit clock, not the request's max timestamp.
        commit("obj-lat", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.LOG_APPEND_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(7777L);
    }

    @Test
    void appendIntoNonEmptyLogLeavesTimestampUnchanged() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        // A later commit appends a NEWER batch at the head; the oldest batch is unchanged.
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 9000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    @Test
    void deleteAdvancingLogStartRecomputesFromNewOldestBatch() {
        // Two commits -> two batches: [0..4] ts 1000, [5..9] ts 2000.
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 2000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);

        delete(T0P0, 5L);  // removes [0..4]; [5..9] becomes the oldest
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
    }

    @Test
    void deleteThatDoesNotAdvanceLogStartLeavesTimestampUnchanged() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 2000L, TimestampType.CREATE_TIME));
        delete(T0P0, 5L);
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
        // Deleting to an offset at or below the current log_start is a no-op for the oldest batch.
        delete(T0P0, 5L);
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
    }

    @Test
    void deletingAllRecordsResetsTimestampToNull() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        delete(T0P0, -1L);  // -1 == high watermark, deletes everything
        assertThat(earliestTs(T0P0)).isNull();
    }

    @Test
    void pruneAdvancingLogStartRecomputesFromNewOldestBatch() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 2000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);

        prune(T0P0, 4L);  // removes the batch with last_offset <= 4; [5..9] becomes the oldest
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
    }

    @Test
    void pruneThatDeletesNothingLeavesTimestampUnchanged() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        prune(T0P0, 3L);  // no batch has last_offset <= 3 -> deletes nothing
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    @Test
    void enforceRetentionDeletingAllRecordsResetsTimestampToNull() {
        time.setCurrentTimeMs(5000L);
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        // retention_ms 0 -> everything older than "now" is deletable; the only batch (ts 1000 < 5000) goes.
        controlPlane.enforceRetention(List.of(new EnforceRetentionRequest(TOPIC_ID, 0, -1, 0)), 0);
        assertThat(earliestTs(T0P0)).isNull();
    }

    private void commit(final String objectKey, final CommitBatchRequest request) {
        controlPlane.commitFile(objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request));
    }

    private void delete(final TopicIdPartition partition, final long offset) {
        controlPlane.deleteRecords(List.of(new DeleteRecordsRequest(partition, offset)));
    }

    private void prune(final TopicIdPartition partition, final long highestTieredOffset) {
        controlPlane.pruneDisklessLogs(List.of(new PruneDisklessLogsRequest(partition, highestTieredOffset)));
    }

    private Long earliestTs(final TopicIdPartition partition) {
        try {
            final Field logsField = InMemoryControlPlane.class.getDeclaredField("logs");
            logsField.setAccessible(true);
            final Map<?, ?> logs = (Map<?, ?>) logsField.get(controlPlane);
            final Object logInfo = logs.get(partition);
            assertThat(logInfo).as("log for %s", partition).isNotNull();
            final Field tsField = logInfo.getClass().getDeclaredField("earliestBatchTimestamp");
            tsField.setAccessible(true);
            return (Long) tsField.get(logInfo);
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
