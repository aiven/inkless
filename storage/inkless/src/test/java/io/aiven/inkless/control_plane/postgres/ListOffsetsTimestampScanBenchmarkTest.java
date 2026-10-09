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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>The {@code max_timestamp} lookup (a MAX over the partition, then an equality lookup on the same
 * expression) and the concrete-timestamp lookup (the {@code offsetsForTimes} path, {@code >= ts ORDER BY
 * batch_id LIMIT 1}) scan {@code batches}. Both evaluate {@code batch_timestamp(...)} per row. The
 * concrete-timestamp case varies where the requested timestamp falls in the partition: at the start (first row matches),
 * near the end (most rows rejected), and past the end (every row rejected, nothing returned).
 *
 * <p>The same seeded workload runs against three schema variants, each in its own database: v29 (the
 * baseline: no timestamp index, {@code ORDER BY batch_id LIMIT 1} lookups), v30 (the index alone), and v31
 * (index plus the {@code MIN(base_offset)} rewrite). Compare the printed tables and plans across variants.
 * With an index range scan, the microseconds per call for the past-the-end and at-90% cases hold constant as
 * depth grows. A walk with the predicate as a filter makes them grow linearly. Statistics are gathered after
 * every seed step. Without them the planner never chooses the plans production runs.
 *
 * <p>The write path and storage cost of the index are measured on the same runs. Bulk commits (10k batches per
 * file) and small commits (10 batches per file) report latency and WAL volume per call, and a size table
 * reports the heap and each index of {@code batches}. Compare the v29 and v30 rows to isolate the cost of
 * {@code batches_by_timestamp_idx}. v30 and v31 share the same write path. Each variant runs once in a
 * container, so treat differences below about 10% as noise.
 */
@Tag("benchmark")
@Testcontainers
class ListOffsetsTimestampScanBenchmarkTest {
    /** Flyway version of the first migration that rewrites the lookups as MIN(base_offset). */
    static final int MIN_OFFSET_LOOKUP_VERSION = 31;
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 100_000_000;
    static final int BATCH_BYTES = 120;
    static final int BATCHES_PER_WINDOW = 10_000;
    static final int REPS = 5;
    static final int PROBE_COMMITS = 20;
    static final int PROBE_BATCHES = 10;
    static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    static final long[] DEPTH_CHECKPOINTS = {25_000, 50_000, 100_000, 200_000, 400_000};

    Time time = new MockTime();
    int fileSeq = 0;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @ParameterizedTest(name = "schema V{0}")
    @ValueSource(strings = {"29", "30", "31"})
    void benchmarkTimestampBranches(final String schemaVersion) {
        pgContainer.migrate(schemaVersion);
        final boolean minOffsetLookup = Integer.parseInt(schemaVersion) >= MIN_OFFSET_LOOKUP_VERSION;
        final TopicIdPartition partition = createSinglePartitionTopic();

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== schema V%s: list_offsets_v1 timestamp branches: us/call vs retained batches ==%n", schemaVersion));
        out.append(String.format("ts@start: first row matches. ts@90%%: 10%% of rows match. ts@end+1: no row matches.%n%n"));
        out.append(String.format("%12s | %12s | %12s | %12s | %12s%n",
            "depth (rows)", "MAX_TS", "ts@start", "ts@90%", "ts@end+1"));
        out.append("-".repeat(70)).append(String.format("%n"));
        final StringBuilder writes = new StringBuilder();
        writes.append(String.format("%n== schema V%s: write path ==%n", schemaVersion));
        writes.append(String.format("bulk: %d batches per commit_file. small: %d batches per commit_file, %d commits.%n%n",
            BATCHES_PER_WINDOW, PROBE_BATCHES, PROBE_COMMITS));
        writes.append(String.format("%12s | %14s | %14s | %14s | %14s%n",
            "depth (rows)", "bulk us/batch", "bulk WAL B/batch", "small us/commit", "small WAL B/commit"));
        writes.append("-".repeat(86)).append(String.format("%n"));
        final StringBuilder sizes = new StringBuilder();
        sizes.append(String.format("%n== schema V%s: batches storage (MB) ==%n%n", schemaVersion));
        sizes.append(String.format("%12s | %9s | %9s | %9s | %9s | %9s | %12s%n",
            "depth (rows)", "heap", "pkey", "by_file", "covering", "by_ts", "by_ts B/row"));
        sizes.append("-".repeat(88)).append(String.format("%n"));

        long committed = 0;
        for (final long targetDepth : DEPTH_CHECKPOINTS) {
            final WriteCost bulk = seedUntil(partition, committed, targetDepth);
            committed += bulk.batches();
            final WriteCost small = probeSmallCommits(partition, committed);
            committed += small.batches();
            // Autovacuum does not run inside the checkpoint loop. Give the planner current statistics.
            // The pool has autocommit off, so a bare execute() would be rolled back on connection return.
            pgContainer.getJooqCtx().transaction(conf -> DSL.using(conf).execute("ANALYZE batches"));
            final double maxTs = measure(partition, MAX_TIMESTAMP);
            final double atStart = measure(partition, BASE_TIMESTAMP);
            final double at90 = measure(partition, BASE_TIMESTAMP + committed * 9 / 10);
            final double pastEnd = measure(partition, BASE_TIMESTAMP + committed + 1);
            out.append(String.format("%12d | %12.1f | %12.1f | %12.1f | %12.1f%n",
                committed, maxTs, atStart, at90, pastEnd));
            writes.append(String.format("%12d | %14.1f | %14.1f | %14.1f | %14.1f%n",
                committed,
                bulk.nanos() / 1_000.0 / bulk.batches(), (double) bulk.walBytes() / bulk.batches(),
                small.nanos() / 1_000.0 / PROBE_COMMITS, (double) small.walBytes() / PROBE_COMMITS));
            sizes.append(sizeRow(committed));
        }
        System.out.println(out);
        System.out.println(writes);
        System.out.println(sizes);

        explainMaxTimestamp(partition);
        explainTimestampLookup(partition, BASE_TIMESTAMP + committed + 1, "past the end", minOffsetLookup);
        explainTimestampLookup(partition, BASE_TIMESTAMP + committed * 9 / 10, "at 90%", minOffsetLookup);
        explainTimestampLookup(partition, BASE_TIMESTAMP, "at the start", minOffsetLookup);
        printPlannerStatistics();
    }

    /**
     * Row estimates in the plans depend on these. Without statistics the planner assumed a ~10-row partition
     * and chose plans that change after a real ANALYZE. The preceding plans are valid only if
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
     * The plan is either an index scan bounded by the timestamp expression, or a scan of
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

    private void explainTimestampLookup(final TopicIdPartition p, final long timestamp, final String label,
                                        final boolean minOffsetLookup) {
        final org.jooq.Result<?> plan = pgContainer.getJooqCtx().fetch(
            "EXPLAIN (ANALYZE, BUFFERS) "
                + (minOffsetLookup
                    ? "SELECT MIN(base_offset) "
                    : "SELECT batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp), base_offset ")
                + "FROM batches WHERE topic_id = {0} AND partition = {1} "
                + "  AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) >= {2}"
                + (minOffsetLookup ? "" : " ORDER BY batch_id LIMIT 1"),
            DSL.val(p.topicId(), BATCHES.TOPIC_ID.getDataType()),
            DSL.val(p.partition(), BATCHES.PARTITION.getDataType()),
            DSL.val(timestamp, BATCHES.BATCH_MAX_TIMESTAMP.getDataType()));
        System.out.println("\n== EXPLAIN timestamp lookup (" + label + ") ==");
        plan.forEach(row -> System.out.println(row.get(0)));
    }

    /** Latency and WAL volume of commit_file calls. */
    private record WriteCost(long batches, long nanos, long walBytes) {
        WriteCost plus(final WriteCost other) {
            return new WriteCost(batches + other.batches, nanos + other.nanos, walBytes + other.walBytes);
        }
    }

    /**
     * Commits windows until the partition contains at least targetDepth batches.
     * Batch N carries timestamp BASE_TIMESTAMP + N so the requested timestamp maps onto a row position.
     */
    private WriteCost seedUntil(final TopicIdPartition partition, final long alreadyCommitted, final long targetDepth) {
        WriteCost total = new WriteCost(0, 0, 0);
        while (alreadyCommitted + total.batches() < targetDepth) {
            final int batchesThisWindow =
                (int) Math.min(BATCHES_PER_WINDOW, targetDepth - alreadyCommitted - total.batches());
            total = total.plus(commit(partition, alreadyCommitted + total.batches(), batchesThisWindow));
        }
        return total;
    }

    /** Commits small files, the commit size of a low-throughput producer, and returns their summed cost. */
    private WriteCost probeSmallCommits(final TopicIdPartition partition, final long alreadyCommitted) {
        WriteCost total = new WriteCost(0, 0, 0);
        for (int c = 0; c < PROBE_COMMITS; c++) {
            total = total.plus(commit(partition, alreadyCommitted + total.batches(), PROBE_BATCHES));
        }
        return total;
    }

    private WriteCost commit(final TopicIdPartition partition, final long firstIndex, final int count) {
        final List<CommitBatchRequest> requests = new ArrayList<>(count);
        int byteOffset = 0;
        for (int b = 0; b < count; b++) {
            final long timestamp = BASE_TIMESTAMP + firstIndex + b;
            requests.add(CommitBatchRequest.of(0, partition, byteOffset, BATCH_BYTES, 0, 0, timestamp, TimestampType.CREATE_TIME));
            byteOffset += BATCH_BYTES;
        }
        final String objectKey = "file-" + partition.topicId() + "-" + fileSeq++;
        final long walBefore = walPosition();
        final long start = System.nanoTime();
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
        final long elapsed = System.nanoTime() - start;
        return new WriteCost(count, elapsed, walPosition() - walBefore);
    }

    private long walPosition() {
        return pgContainer.getJooqCtx()
            .fetchOne("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
            .get(0, Long.class);
    }

    /** Formats the size of the heap and each index of batches. A missing index prints as a dash. */
    private String sizeRow(final long depth) {
        final long heap = pgContainer.getJooqCtx().fetchOne("SELECT pg_relation_size('batches')").get(0, Long.class);
        final Map<String, Long> indexes = new HashMap<>();
        pgContainer.getJooqCtx().fetch(
            "SELECT indexrelid::regclass::text, pg_relation_size(indexrelid) FROM pg_index "
                + "WHERE indrelid = 'batches'::regclass")
            .forEach(row -> indexes.put(row.get(0, String.class), row.get(1, Long.class)));
        final Long byTimestamp = indexes.get("batches_by_timestamp_idx");
        return String.format("%12d | %9s | %9s | %9s | %9s | %9s | %12s%n",
            depth, megabytes(heap), megabytes(indexes.get("batches_pkey")), megabytes(indexes.get("batches_by_file")),
            megabytes(indexes.get("batches_by_last_offset_covering_idx")), megabytes(byTimestamp),
            byTimestamp == null ? "-" : String.format("%.1f", (double) byTimestamp / depth));
    }

    private static String megabytes(final Long bytes) {
        return bytes == null ? "-" : String.format("%.1f", bytes / 1_048_576.0);
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
