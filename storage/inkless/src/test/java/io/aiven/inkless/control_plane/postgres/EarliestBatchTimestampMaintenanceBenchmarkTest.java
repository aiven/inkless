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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

/**
 * Quantifies the write-path cost of maintaining {@code logs.earliest_batch_timestamp} (added so a future
 * retention check can skip the whole-partition scan when the oldest batch is still within retention).
 *
 * <p>This test is intentionally schema-agnostic: it drives only {@link CommitFileJob} and
 * {@link DeleteRecordsJob} and never references the new column, so the SAME file runs on {@code main}
 * (before the column exists) and on this branch (after), and the two runs form an A/B. Compare
 * {@code ms/op} per shape across the two checkouts; the maintenance is "minor" iff:
 * <ul>
 *   <li>{@code steady-commit} (append into a populated log, the dominant production path) is unchanged
 *       -- the {@code IS NULL} guard skips the recompute entirely; and</li>
 *   <li>{@code delete-advance} and {@code commit-into-empty} (where the recompute fires) grow by only a
 *       single indexed batch lookup, not proportionally to log depth.</li>
 * </ul>
 *
 * Wall-clock against a local container: indicative, not a microbenchmark.
 */
@Tag("benchmark")
@Testcontainers
class EarliestBatchTimestampMaintenanceBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 1_000_000;
    static final int BATCH_BYTES = 120;

    Time time = new MockTime();
    int topicSeq = 0;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void benchmarkMaintenanceCost() {
        final int partitions = 16;
        final int steadyWindows = 500;      // appends into already-populated logs (hot path)
        final int emptyCycles = 500;        // commit-then-delete-all cycles (empty -> non-empty)
        final int seedDepth = 2_000;        // batches per partition before the delete sweep
        final int deleteSteps = 500;        // log_start advances measured

        final double steadyMs = benchmarkSteadyCommit(partitions, steadyWindows);
        final double emptyMs = benchmarkCommitIntoEmpty(partitions, emptyCycles);
        final double deleteMs = benchmarkDeleteAdvance(partitions, seedDepth, deleteSteps);

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== WRITE-PATH COST of maintaining logs.earliest_batch_timestamp ==%n"));
        out.append(String.format("Run on `main` (no column) and on this branch, then diff ms/call per shape.%n"));
        out.append(String.format("Each call operates on %d partitions.%n", partitions));
        out.append(String.format("%-40s | %10s | %12s%n", "shape", "calls", "ms/call"));
        out.append("-".repeat(68)).append(String.format("%n"));
        out.append(String.format("%-40s | %10d | %12.3f%n",
            "steady-commit (append, guard skips)", steadyWindows, steadyMs));
        out.append(String.format("%-40s | %10d | %12.3f%n",
            "commit-into-empty (recompute fires)", emptyCycles, emptyMs));
        out.append(String.format("%-40s | %10d | %12.3f%n",
            "delete-advance (recompute fires)", deleteSteps, deleteMs));
        out.append(String.format("%nms/call is wall-clock per control-plane call against a local container; indicative only.%n"));
        System.out.println(out);
    }

    /**
     * Append windows into logs that already hold data. The first window populates the column; every
     * later window must hit the {@code IS NULL} guard and skip the recompute -- this is the path that
     * must show no regression.
     */
    private double benchmarkSteadyCommit(final int partitionCount, final int windows) {
        final List<TopicIdPartition> partitions = createTopic(partitionCount);
        commitOneBatchPerPartition(partitions, 0);  // warm-up: populates earliest_batch_timestamp

        long nanos = 0;
        int byteOffset = BATCH_BYTES;
        for (int w = 0; w < windows; w++) {
            final List<CommitBatchRequest> requests = new ArrayList<>(partitions.size());
            for (final TopicIdPartition partition : partitions) {
                requests.add(CommitBatchRequest.of(
                    0, partition, byteOffset, BATCH_BYTES, 0, 0, time.milliseconds(), TimestampType.CREATE_TIME));
            }
            byteOffset += BATCH_BYTES;
            final long start = System.nanoTime();
            commit(partitions.get(0).topicId(), "steady-" + w, requests);
            nanos += System.nanoTime() - start;
        }
        return nanos / 1_000_000.0 / windows;
    }

    /**
     * Commit one batch, delete everything, repeat. Each commit lands in an empty log, so the recompute
     * fires every time (worst realistic case for the commit path).
     */
    private double benchmarkCommitIntoEmpty(final int partitionCount, final int cycles) {
        final List<TopicIdPartition> partitions = createTopic(partitionCount);
        long nanos = 0;
        for (int c = 0; c < cycles; c++) {
            final List<CommitBatchRequest> requests = new ArrayList<>(partitions.size());
            for (final TopicIdPartition partition : partitions) {
                requests.add(CommitBatchRequest.of(
                    0, partition, 0, BATCH_BYTES, 0, 0, time.milliseconds(), TimestampType.CREATE_TIME));
            }
            final long start = System.nanoTime();
            commit(partitions.get(0).topicId(), "empty-" + c, requests);
            nanos += System.nanoTime() - start;

            // Delete everything so the next commit again lands in an empty log.
            final List<DeleteRecordsRequest> deletes = new ArrayList<>(partitions.size());
            for (final TopicIdPartition partition : partitions) {
                deletes.add(new DeleteRecordsRequest(partition, -1L));  // -1 == high watermark
            }
            new DeleteRecordsJob(time, pgContainer.getJooqCtx(), deletes, d -> {}).call();
        }
        return nanos / 1_000_000.0 / cycles;
    }

    /**
     * Seed a deep log (one non-coalesced batch row per offset via one file per batch), then advance
     * log_start one offset at a time. Every advance recomputes the column from the new oldest batch via
     * a single indexed lookup; cost must be flat in the number of remaining rows, not O(depth).
     */
    private double benchmarkDeleteAdvance(final int partitionCount, final int depth, final int steps) {
        final List<TopicIdPartition> partitions = createTopic(partitionCount);
        // Separate files keep rows un-coalesced so deletes actually remove rows and move the oldest.
        for (int i = 0; i < depth; i++) {
            final List<CommitBatchRequest> requests = new ArrayList<>(partitions.size());
            for (final TopicIdPartition partition : partitions) {
                requests.add(CommitBatchRequest.of(
                    0, partition, 0, BATCH_BYTES, 0, 0, time.milliseconds(), TimestampType.CREATE_TIME));
            }
            commit(partitions.get(0).topicId(), "seed-" + i, requests);
        }

        long nanos = 0;
        for (int s = 1; s <= steps; s++) {
            final List<DeleteRecordsRequest> deletes = new ArrayList<>(partitions.size());
            for (final TopicIdPartition partition : partitions) {
                deletes.add(new DeleteRecordsRequest(partition, (long) s));
            }
            final long start = System.nanoTime();
            new DeleteRecordsJob(time, pgContainer.getJooqCtx(), deletes, d -> {}).call();
            nanos += System.nanoTime() - start;
        }
        return nanos / 1_000_000.0 / steps;
    }

    private void commitOneBatchPerPartition(final List<TopicIdPartition> partitions, final int byteOffset) {
        final List<CommitBatchRequest> requests = new ArrayList<>(partitions.size());
        for (final TopicIdPartition partition : partitions) {
            requests.add(CommitBatchRequest.of(
                0, partition, byteOffset, BATCH_BYTES, 0, 0, time.milliseconds(), TimestampType.CREATE_TIME));
        }
        commit(partitions.get(0).topicId(), "warm", requests);
    }

    private void commit(final Uuid topicId, final String tag, final List<CommitBatchRequest> requests) {
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "file-" + topicId + "-" + tag,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, requests, true, d -> {}).call();
    }

    private List<TopicIdPartition> createTopic(final int partitionCount) {
        final Uuid topicId = new Uuid(7, ++topicSeq);
        final String topicName = "bench-" + topicSeq;
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, partitionCount)), d -> {}).run();
        final List<TopicIdPartition> partitions = new ArrayList<>();
        for (int p = 0; p < partitionCount; p++) {
            partitions.add(new TopicIdPartition(topicId, p, topicName));
        }
        return partitions;
    }
}
