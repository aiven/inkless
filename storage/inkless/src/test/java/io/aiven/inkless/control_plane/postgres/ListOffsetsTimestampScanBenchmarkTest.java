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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

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
 * in a partition that shares {@code batches} with other partitions. EARLIEST, LATEST, and similar requests read one
 * {@code logs} row and are not measured.
 *
 * <p>The {@code max_timestamp} lookup (a MAX over the partition, then an equality lookup on the same
 * expression) and the concrete-timestamp lookup (the {@code offsetsForTimes} path) scan {@code batches} and
 * evaluate {@code batch_timestamp(...)} per row. The concrete-timestamp case varies where the requested
 * timestamp falls in the partition: at the start (first row matches), near the end (most rows rejected), and
 * past the end (every row rejected, nothing returned).
 *
 * <p>The same seeded workload runs against two schema variants, each in its own database: v29 (the baseline,
 * {@code ORDER BY batch_id LIMIT 1} lookups) and v30 ({@code MIN(base_offset)} lookups). Batches of
 * {@code PARTITIONS} partitions are interleaved, so the baseline walk of {@code batches_pkey} crosses the other
 * partitions' rows. Compare the printed tables and plans across variants. With a walk, the microseconds per call
 * grow with the whole table. With an index scan of one partition, they grow with that partition only.
 * Statistics are gathered after every seed step. Without them the planner never chooses the plans production runs.
 *
 * <p>The write path and storage are measured on the same runs, to check that the migration changes neither.
 * Bulk commits fill a {@code BULK_FILE_SIZE} file with {@code BATCH_BYTES} batches. Small commits contain
 * {@code PROBE_BATCHES} batches per partition. Both report latency and WAL volume per call, and a size table
 * reports the heap and each index of {@code batches}. Each variant runs once in a container, so treat
 * differences below about 10 to 30% as noise.
 */
@Tag("benchmark")
@Testcontainers
class ListOffsetsTimestampScanBenchmarkTest {
    /** Flyway version of the first migration that rewrites the lookups as MIN(base_offset). */
    static final int MIN_OFFSET_LOOKUP_VERSION = 30;
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long BULK_FILE_SIZE = 10_000_000;
    static final int BATCH_BYTES = 12_000;
    static final int PARTITIONS = 8;
    /** Batches per partition in one bulk file, which makes a file hold about BULK_FILE_SIZE bytes. */
    static final int BATCHES_PER_WINDOW = (int) (BULK_FILE_SIZE / BATCH_BYTES) / PARTITIONS;
    static final int REPS = 5;
    static final int PROBE_COMMITS = 20;
    static final int PROBE_BATCHES = 10;
    static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    static final long[] DEPTH_CHECKPOINTS = {12_500, 25_000, 50_000, 100_000};

    Time time = new MockTime();
    int fileSeq = 0;
    boolean minOffsetLookup;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @ParameterizedTest(name = "schema V{0}")
    @ValueSource(strings = {"29", "30"})
    void benchmarkTimestampBranches(final String schemaVersion) {
        pgContainer.migrate(schemaVersion);
        minOffsetLookup = Integer.parseInt(schemaVersion) >= MIN_OFFSET_LOOKUP_VERSION;
        final List<TopicIdPartition> partitions = createTopic();
        final TopicIdPartition partition = partitions.get(0);

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== schema V%s: list_offsets_v1 timestamp branches: us/call vs retained batches ==%n", schemaVersion));
        out.append(String.format("%d partitions with interleaved batches, partition 0 measured. ", PARTITIONS));
        out.append(String.format("ts@start: first row matches. ts@90%%: 10%% of rows match. ts@end+1: no row matches.%n%n"));
        out.append(String.format("%12s | %12s | %12s | %12s | %12s%n",
            "depth/part", "MAX_TS", "ts@start", "ts@90%", "ts@end+1"));
        out.append("-".repeat(70)).append(String.format("%n"));
        final StringBuilder writes = new StringBuilder();
        writes.append(String.format("%n== schema V%s: write path ==%n", schemaVersion));
        writes.append(String.format("bulk: %d batches per commit_file. small: %d batches per commit_file, %d commits.%n%n",
            BATCHES_PER_WINDOW, PROBE_BATCHES, PROBE_COMMITS));
        writes.append(String.format("%12s | %14s | %14s | %14s | %14s%n",
            "depth/part", "bulk us/batch", "bulk WAL B/batch", "small us/commit", "small WAL B/commit"));
        writes.append("-".repeat(86)).append(String.format("%n"));
        final StringBuilder sizes = new StringBuilder();
        sizes.append(String.format("%n== schema V%s: batches storage (MB) ==%n%n", schemaVersion));
        sizes.append(String.format("%12s | %9s | %9s | %9s | %9s%n",
            "depth/part", "heap", "pkey", "by_file", "covering"));
        sizes.append("-".repeat(54)).append(String.format("%n"));

        long committed = 0;
        for (final long targetDepth : DEPTH_CHECKPOINTS) {
            final WriteCost bulk = seedUntil(partitions, committed, targetDepth);
            committed += bulk.batches() / PARTITIONS;
            final WriteCost small = probeSmallCommits(partitions, committed);
            committed += small.batches() / PARTITIONS;
            vacuumAnalyze();
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
        explainTimestampLookup(partition, BASE_TIMESTAMP + committed + 1, "past the end");
        explainTimestampLookup(partition, BASE_TIMESTAMP + committed * 9 / 10, "at 90%");
        explainTimestampLookup(partition, BASE_TIMESTAMP, "at the start");
        printPlannerStatistics();
    }

    /**
     * Row estimates in the plans depend on these. Without statistics the planner assumed a ~10-row partition
     * and chose plans that change after a real ANALYZE. The preceding plans are valid only if
     * pg_stats has rows for batches.
     */
    private void printPlannerStatistics() {
        System.out.println("\n== planner statistics ==");
        pgContainer.getJooqCtx().fetch(
            "SELECT relname, reltuples, relpages FROM pg_class WHERE relname IN ('batches', 'batches_by_last_offset_covering_idx')")
            .forEach(row -> System.out.println(row.get(0) + " reltuples=" + row.get(1) + " relpages=" + row.get(2)));
        pgContainer.getJooqCtx().fetch(
            "SELECT tablename, attname, n_distinct FROM pg_stats "
                + "WHERE tablename = 'batches' ORDER BY 1, 2")
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

    private void explainTimestampLookup(final TopicIdPartition p, final long timestamp, final String label) {
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
     * Commits windows until every partition contains at least targetDepth batches. A window holds
     * BATCHES_PER_WINDOW batches per partition, interleaved across partitions as in a shared write-ahead file.
     * Batch N of a partition has timestamp BASE_TIMESTAMP + N so the requested timestamp maps onto a row
     * position.
     */
    private WriteCost seedUntil(final List<TopicIdPartition> partitions, final long alreadyCommitted, final long targetDepth) {
        WriteCost total = new WriteCost(0, 0, 0);
        while (alreadyCommitted + total.batches() / PARTITIONS < targetDepth) {
            final long done = alreadyCommitted + total.batches() / PARTITIONS;
            final int batchesThisWindow = (int) Math.min(BATCHES_PER_WINDOW, targetDepth - done);
            total = total.plus(commit(partitions, done, batchesThisWindow));
        }
        return total;
    }

    /** Commits small files, the commit size of a low-throughput producer, and returns their summed cost. */
    private WriteCost probeSmallCommits(final List<TopicIdPartition> partitions, final long alreadyCommitted) {
        WriteCost total = new WriteCost(0, 0, 0);
        for (int c = 0; c < PROBE_COMMITS; c++) {
            total = total.plus(commit(partitions, alreadyCommitted + total.batches() / PARTITIONS, PROBE_BATCHES));
        }
        return total;
    }

    /** Commits one file with count batches for each partition and returns its cost. */
    private WriteCost commit(final List<TopicIdPartition> partitions, final long firstIndex, final int count) {
        final List<CommitBatchRequest> requests = new ArrayList<>(count * partitions.size());
        int byteOffset = 0;
        for (int b = 0; b < count; b++) {
            final long timestamp = BASE_TIMESTAMP + firstIndex + b;
            for (final TopicIdPartition partition : partitions) {
                requests.add(CommitBatchRequest.of(0, partition, byteOffset, BATCH_BYTES, 0, 0, timestamp, TimestampType.CREATE_TIME));
                byteOffset += BATCH_BYTES;
            }
        }
        final String objectKey = "file-" + partitions.get(0).topicId() + "-" + fileSeq++;
        final long walBefore = walPosition();
        final long start = System.nanoTime();
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            (long) requests.size() * BATCH_BYTES,
            requests,
            false,
            duration -> {
            }
        ).call();
        final long elapsed = System.nanoTime() - start;
        return new WriteCost(requests.size(), elapsed, walPosition() - walBefore);
    }

    private long walPosition() {
        return pgContainer.getJooqCtx()
            .fetchOne("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
            .get(0, Long.class);
    }

    /**
     * Autovacuum does not run inside the checkpoint loop. Gives the planner current statistics and sets the
     * visibility map, which index-only scans need. VACUUM cannot run in a transaction and the pool has autocommit
     * off. The statement uses its own connection.
     */
    private void vacuumAnalyze() {
        try (Connection connection = pgContainer.getDataSource().getConnection();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            statement.execute("VACUUM (ANALYZE) batches");
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Formats the size of the heap and each index of batches. */
    private String sizeRow(final long depth) {
        final long heap = pgContainer.getJooqCtx().fetchOne("SELECT pg_relation_size('batches')").get(0, Long.class);
        final Map<String, Long> indexes = new HashMap<>();
        pgContainer.getJooqCtx().fetch(
            "SELECT indexrelid::regclass::text, pg_relation_size(indexrelid) FROM pg_index "
                + "WHERE indrelid = 'batches'::regclass")
            .forEach(row -> indexes.put(row.get(0, String.class), row.get(1, Long.class)));
        return String.format("%12d | %9s | %9s | %9s | %9s%n",
            depth, megabytes(heap), megabytes(indexes.get("batches_pkey")), megabytes(indexes.get("batches_by_file")),
            megabytes(indexes.get("batches_by_last_offset_covering_idx")));
    }

    private static String megabytes(final Long bytes) {
        return bytes == null ? "-" : String.format("%.1f", bytes / 1_048_576.0);
    }

    private List<TopicIdPartition> createTopic() {
        final Uuid topicId = new Uuid(7, 2);
        final String topicName = "bench-list-offsets-ts";
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, PARTITIONS)), duration -> {
        }).run();
        return IntStream.range(0, PARTITIONS).mapToObj(p -> new TopicIdPartition(topicId, p, topicName)).toList();
    }
}
