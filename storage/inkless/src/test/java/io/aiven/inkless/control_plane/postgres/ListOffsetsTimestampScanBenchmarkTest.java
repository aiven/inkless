/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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

import org.jooq.impl.DSL;
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
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.apache.kafka.common.requests.ListOffsetsRequest.MAX_TIMESTAMP;
import static org.jooq.generated.Tables.BATCHES;

/**
 * Measures how the timestamp branches of {@code list_offsets_v1} scale with the number of retained batches
 * in a partition. EARLIEST, LATEST and friends read one {@code logs} row and are not measured.
 *
 * <p>Two branches scan {@code batches}: MAX_TIMESTAMP (a MAX over the partition, then an equality lookup on
 * the same expression) and a concrete timestamp (the {@code offsetsForTimes} path, {@code >= ts ORDER BY
 * batch_id LIMIT 1}). Both evaluate {@code batch_timestamp(...)} per row. The concrete-timestamp case is
 * swept across where the requested timestamp falls in the partition: at the start (first row matches),
 * near the end (most rows rejected), and past the end (every row rejected, nothing returned).
 *
 * <p>A/B usage: run without the timestamp index to capture the baseline, apply the index migration, re-run,
 * and compare the printed tables and plans. The tell is whether us/call for the past-the-end case and for
 * MAX_TIMESTAMP stays flat with depth (index range scan) or grows linearly (per-row function evaluation).
 */
@Tag("benchmark")
@Testcontainers
class ListOffsetsTimestampScanBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 100_000_000;
    static final int BATCH_BYTES = 120;
    static final int BATCHES_PER_WINDOW = 10_000;
    static final int REPS = 5;
    static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    static final long[] DEPTH_CHECKPOINTS = {25_000, 50_000, 100_000, 200_000, 400_000};

    Time time = new MockTime();
    int fileSeq = 0;

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
    void benchmarkTimestampBranches() {
        final TopicIdPartition partition = createSinglePartitionTopic();

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== list_offsets_v1 timestamp branches: us/call vs retained batches ==%n"));
        out.append(String.format("ts@start: first row matches. ts@90%%: 10%% of rows match. ts@end+1: no row matches.%n%n"));
        out.append(String.format("%12s | %12s | %12s | %12s | %12s%n",
            "depth (rows)", "MAX_TS", "ts@start", "ts@90%", "ts@end+1"));
        out.append("-".repeat(70)).append(String.format("%n"));

        long committed = 0;
        for (final long targetDepth : DEPTH_CHECKPOINTS) {
            committed += seedUntil(partition, committed, targetDepth);
            // Autovacuum does not run inside the checkpoint loop; give the planner current statistics.
            // The pool has autocommit off, so a bare execute() would be rolled back on connection return.
            pgContainer.getJooqCtx().transaction(conf -> DSL.using(conf).execute("ANALYZE batches"));
            final double maxTs = measure(partition, MAX_TIMESTAMP);
            final double atStart = measure(partition, BASE_TIMESTAMP);
            final double at90 = measure(partition, BASE_TIMESTAMP + committed * 9 / 10);
            final double pastEnd = measure(partition, BASE_TIMESTAMP + committed + 1);
            out.append(String.format("%12d | %12.1f | %12.1f | %12.1f | %12.1f%n",
                committed, maxTs, atStart, at90, pastEnd));
        }
        System.out.println(out);

        explainMaxTimestamp(partition);
        explainTimestampLookup(partition, BASE_TIMESTAMP + committed + 1, "past the end");
        explainTimestampLookup(partition, BASE_TIMESTAMP + committed * 9 / 10, "at 90%");
        explainTimestampLookup(partition, BASE_TIMESTAMP, "at the start");
        printPlannerStatistics();
    }

    /**
     * Row estimates in the plans depend on these. Without statistics the planner assumed a ~10-row partition
     * and chose plans that do not survive a real ANALYZE, so the plans above are only meaningful if
     * pg_stats has rows for batches and the timestamp index expression.
     */
    private void printPlannerStatistics() {
        System.out.println("\n== planner statistics ==");
        pgContainer.getJooqCtx().fetch(
            "SELECT relname, reltuples, relpages FROM pg_class WHERE relname IN ('batches', 'batches_by_timestamp_idx')")
            .forEach(row -> System.out.println(row.get(0) + " reltuples=" + row.get(1) + " relpages=" + row.get(2)));
        pgContainer.getJooqCtx().fetch(
            "SELECT tablename, attname, n_distinct FROM pg_stats "
                + "WHERE tablename IN ('batches', 'batches_by_timestamp_idx') ORDER BY 1, 2")
            .forEach(row -> System.out.println("pg_stats " + row.get(0) + "." + row.get(1) + " n_distinct=" + row.get(2)));
    }

    private double measure(final TopicIdPartition partition, final long timestamp) {
        long nanos = 0;
        for (int rep = 0; rep < REPS + 1; rep++) {
            final ListOffsetsJob job = new ListOffsetsJob(
                time, pgContainer.getJooqCtx(),
                List.of(new ListOffsetsRequest(partition, timestamp)),
                duration -> {
                }
            );
            final long start = System.nanoTime();
            final List<ListOffsetsResponse> responses = job.call();
            final long elapsed = System.nanoTime() - start;
            if (responses.size() != 1) {
                throw new IllegalStateException("Expected one response, got " + responses);
            }
            if (rep == 0) {
                continue;
            }
            nanos += elapsed;
        }
        return nanos / 1_000.0 / REPS;
    }

    /**
     * Plans are dumped for the standalone queries because the plpgsql body is opaque to EXPLAIN.
     * What matters is the plan shape: an index scan bounded by the timestamp expression, or a scan of
     * the whole partition with the expression evaluated as a filter on every row.
     */
    private void explainMaxTimestamp(final TopicIdPartition p) {
        final org.jooq.Result<?> plan = pgContainer.getJooqCtx().fetch(
            "EXPLAIN (ANALYZE, BUFFERS) "
                + "SELECT MAX(batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp)) "
                + "FROM batches WHERE topic_id = {0} AND partition = {1}",
            DSL.val(p.topicId(), BATCHES.TOPIC_ID.getDataType()),
            DSL.val(p.partition(), BATCHES.PARTITION.getDataType()));
        System.out.println("\n== EXPLAIN MAX_TIMESTAMP ==");
        plan.forEach(row -> System.out.println(row.get(0)));
    }

    private void explainTimestampLookup(final TopicIdPartition p, final long timestamp, final String label) {
        final org.jooq.Result<?> plan = pgContainer.getJooqCtx().fetch(
            "EXPLAIN (ANALYZE, BUFFERS) "
                + "SELECT MIN(base_offset) "
                + "FROM batches WHERE topic_id = {0} AND partition = {1} "
                + "  AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) >= {2}",
            DSL.val(p.topicId(), BATCHES.TOPIC_ID.getDataType()),
            DSL.val(p.partition(), BATCHES.PARTITION.getDataType()),
            DSL.val(timestamp, BATCHES.BATCH_MAX_TIMESTAMP.getDataType()));
        System.out.println("\n== EXPLAIN timestamp lookup (" + label + ") ==");
        plan.forEach(row -> System.out.println(row.get(0)));
    }

    /**
     * Commits windows until the partition holds at least targetDepth batches; returns how many were added.
     * Batch i carries timestamp BASE_TIMESTAMP + i so the requested timestamp maps onto a row position.
     */
    private long seedUntil(final TopicIdPartition partition, final long alreadyCommitted, final long targetDepth) {
        long added = 0;
        while (alreadyCommitted + added < targetDepth) {
            final int batchesThisWindow = (int) Math.min(BATCHES_PER_WINDOW, targetDepth - alreadyCommitted - added);
            final List<CommitBatchRequest> requests = new ArrayList<>(batchesThisWindow);
            int byteOffset = 0;
            for (int b = 0; b < batchesThisWindow; b++) {
                final long timestamp = BASE_TIMESTAMP + alreadyCommitted + added + b;
                requests.add(CommitBatchRequest.of(0, partition, byteOffset, BATCH_BYTES, 0, 0, timestamp, TimestampType.CREATE_TIME));
                byteOffset += BATCH_BYTES;
            }
            final String objectKey = "file-" + partition.topicId() + "-" + fileSeq++;
            new CommitFileJob(
                time,
                pgContainer.getJooqCtx(),
                objectKey,
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                BROKER_ID,
                FILE_SIZE,
                requests,
                false,
                duration -> {
                }
            ).call();
            added += batchesThisWindow;
        }
        return added;
    }

    private TopicIdPartition createSinglePartitionTopic() {
        final Uuid topicId = new Uuid(7, 2);
        final String topicName = "bench-list-offsets-ts";
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, 1)), duration -> {
        }).run();
        return new TopicIdPartition(topicId, 0, topicName);
    }
}
