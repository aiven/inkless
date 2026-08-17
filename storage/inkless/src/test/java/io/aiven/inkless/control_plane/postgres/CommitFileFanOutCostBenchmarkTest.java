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

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

/**
 * Answers two questions about {@code commit_file}'s cost model that production metrics cannot separate,
 * and that decide whether write-path fan-out is worth managing at all (wiki G24).
 *
 * <p><b>Q1 — is commit cost proportional to fan-out, or fixed per transaction?</b> {@link #fanOutSweep()}
 * holds the number of batches per commit constant and varies only the number of distinct partitions they
 * spread over. The v1 leg is the controlled one: {@code commit_file_v1} writes one {@code batches} row per
 * request regardless of shape, so insert volume, file count and request-array size are all constant and
 * partition count is the only variable. Growth along that leg can only come from the per-partition work:
 * the {@code logs_tmp} seed width, the unindexed per-request {@code UPDATE logs_tmp} scan, the
 * {@code logs FOR UPDATE} set, and the final dump. A flat v1 leg means fixed overhead dominates and no
 * rotation policy can help; a rising leg means commit is superlinear in fan-out.
 *
 * <p><b>Q2 — what does the per-call temp table actually cost?</b> {@link #tempTableChurnFloor()} times
 * the {@code CREATE TEMPORARY TABLE ... AS SELECT} / {@code DROP} pair on its own, both empty (catalog
 * floor) and at production width, so the DDL can be priced against the whole-commit numbers instead of
 * being assumed expensive.
 *
 * <p>Timings are wall clock against a local container: indicative, and only comparable within a run.
 */
@Tag("benchmark")
@Testcontainers
class CommitFileFanOutCostBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private static final int BROKER_ID = 11;
    private static final long FILE_SIZE = 4 * 1024 * 1024;
    private static final int BATCH_BYTES = 2600;   // ~ prod p999: 4 MiB / 1600 batches
    private static final int BATCHES_PER_COMMIT = 1600;
    private static final int COMMITS_PER_SHAPE = 30;
    private static final int CHURN_ITERATIONS = 2000;

    private final Time time = new MockTime();
    private int topicSeq = 0;

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
    void fanOutSweep() {
        // Each shape carries BATCHES_PER_COMMIT batches; only the spread changes.
        final List<Integer> partitionCounts = List.of(16, 80, 320, 640, 800, 1600);

        msPerCommit(16, BATCHES_PER_COMMIT / 16, false);  // warm up JIT + PG caches before measuring

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== COMMIT COST vs FAN-OUT (constant %d batches per commit) ==%n",
            BATCHES_PER_COMMIT));
        out.append(String.format("%-30s | %12s | %12s | %12s | %12s | %16s%n",
            "shape", "v1 ms/commit", "v2 ms/commit", "v1 KiB WAL", "v2 KiB WAL", "tmp visits (M)"));
        out.append("-".repeat(113)).append(String.format("%n"));

        for (final int partitions : partitionCounts) {
            final int batchesPerPartition = BATCHES_PER_COMMIT / partitions;
            final Result v1 = measure(partitions, batchesPerPartition, false);
            final Result v2 = measure(partitions, batchesPerPartition, true);
            out.append(String.format("%-30s | %12.3f | %12.3f | %12.1f | %12.1f | %16.3f%n",
                partitions + " parts x " + batchesPerPartition,
                v1.msPerCommit, v2.msPerCommit,
                v1.walBytesPerCommit / 1024, v2.walBytesPerCommit / 1024,
                (double) partitions * BATCHES_PER_COMMIT / 1_000_000));
        }
        out.append(String.format("%nv1 leg is the controlled one (constant %d batches rows inserted per commit; "
            + "only partition count varies). v2 additionally collapses runs, so its batches-row count falls "
            + "as fan-out falls.%n"
            + "'tmp visits' = batches x partitions, the tuple-visit count implied by the unindexed "
            + "per-request UPDATE logs_tmp scan; compare its growth against the measured growth.%n"
            + "WAL is a pg_current_wal_lsn() delta over the shape's commits, so it includes background "
            + "activity (checkpoints, autovacuum) and is indicative only.%n",
            BATCHES_PER_COMMIT));
        System.out.println(out);
    }

    @Test
    void walFromLoopedUpdates() throws Exception {
        createTopic(630);
        try (Connection connection = pgContainer.getDataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE probe_perm AS SELECT * FROM logs");
            statement.execute("""
                CREATE OR REPLACE FUNCTION probe_looped_updates(arg_iterations INT, arg_permanent BOOLEAN)
                RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
                BEGIN
                    CREATE TEMPORARY TABLE probe_tmp ON COMMIT DROP AS SELECT * FROM logs;
                    FOR i IN 1..arg_iterations LOOP
                        IF arg_permanent THEN
                            UPDATE probe_perm SET byte_size = byte_size + 1 WHERE partition = i % 630;
                        ELSE
                            UPDATE probe_tmp SET byte_size = byte_size + 1 WHERE partition = i % 630;
                        END IF;
                    END LOOP;
                END;
                $$;
                """);
            connection.commit();
        }

        walForLoopedUpdates(false);  // discard: first call pays one-off plan/catalog costs
        final double tempWal = walForLoopedUpdates(false);
        final double permWal = walForLoopedUpdates(true);

        System.out.println(String.format("%n== WAL FROM %d LOOPED UPDATES (the statements the fix removes) ==%n"
                + "on a TEMPORARY table (today's logs_tmp):   %10.1f KiB%n"
                + "on a regular table (same rows, for scale): %10.1f KiB%n"
                + "The temporary leg still creates the temp table per call, so its figure is the "
                + "CREATE's catalog WAL, not the updates.%n",
            BATCHES_PER_COMMIT, tempWal / 1024, permWal / 1024));
    }

    private double walForLoopedUpdates(final boolean permanent) {
        final String lsnBefore = walLsn();
        pgContainer.getJooqCtx().transaction(cfg ->
            cfg.dsl().execute("SELECT probe_looped_updates({0}, {1})", BATCHES_PER_COMMIT, permanent));
        return walBytesSince(lsnBefore);
    }

    @Test
    void tempTableChurnFloor() throws Exception {
        try (Connection connection = pgContainer.getDataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                CREATE OR REPLACE FUNCTION probe_temp_table_churn(arg_iterations INT, arg_rows INT)
                RETURNS DOUBLE PRECISION LANGUAGE plpgsql VOLATILE AS $$
                DECLARE
                    l_start TIMESTAMP WITH TIME ZONE;
                BEGIN
                    l_start := clock_timestamp();
                    FOR i IN 1..arg_iterations LOOP
                        CREATE TEMPORARY TABLE probe_tmp AS
                            SELECT * FROM logs LIMIT arg_rows;
                        DROP TABLE probe_tmp;
                    END LOOP;
                    RETURN EXTRACT(EPOCH FROM (clock_timestamp() - l_start)) * 1000000 / arg_iterations;
                END;
                $$;
                """);
            connection.commit();  // pool hands out autoCommit=false connections
        }

        // Populate `logs` so the width-630 probe copies real rows.
        createTopic(630);

        final double emptyUs = probe(0);
        final double widthUs = probe(630);

        System.out.println(String.format("%n== TEMP TABLE CREATE+DROP COST (%d iterations each) ==%n"
                + "catalog floor (0 rows copied):   %8.1f us/pair%n"
                + "production width (630 rows):     %8.1f us/pair%n%n"
                + "Compare against v1/v2 ms/commit from fanOutSweep: this pair is paid once per commit.%n",
            CHURN_ITERATIONS, emptyUs, widthUs));
    }

    private double probe(final int rows) {
        return pgContainer.getJooqCtx()
            .fetchOne("SELECT probe_temp_table_churn({0}, {1})", CHURN_ITERATIONS, rows)
            .get(0, Double.class);
    }

    private record Result(double msPerCommit, double walBytesPerCommit) {
    }

    private double msPerCommit(final int partitions, final int batchesPerPartition, final boolean coalesce) {
        return measure(partitions, batchesPerPartition, coalesce).msPerCommit();
    }

    private Result measure(final int partitions, final int batchesPerPartition, final boolean coalesce) {
        final List<TopicIdPartition> topicPartitions = createTopic(partitions);
        final String lsnBefore = walLsn();
        long nanos = 0;
        for (int w = 0; w < COMMITS_PER_SHAPE; w++) {
            final List<CommitBatchRequest> requests = commitWindow(topicPartitions, batchesPerPartition);
            final CommitFileJob commit = new CommitFileJob(
                time, pgContainer.getJooqCtx(),
                "file-" + topicPartitions.get(0).topicId() + "-" + w,
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, requests, coalesce, duration -> { });
            final long start = System.nanoTime();
            commit.call();
            nanos += System.nanoTime() - start;
        }
        final double walBytes = walBytesSince(lsnBefore);
        return new Result(nanos / 1_000_000.0 / COMMITS_PER_SHAPE, walBytes / COMMITS_PER_SHAPE);
    }

    private String walLsn() {
        return pgContainer.getJooqCtx().fetchOne("SELECT pg_current_wal_lsn()::text").get(0, String.class);
    }

    private double walBytesSince(final String lsnBefore) {
        return pgContainer.getJooqCtx()
            .fetchOne("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), {0}::pg_lsn)", lsnBefore)
            .get(0, Double.class);
    }

    private List<TopicIdPartition> createTopic(final int partitionCount) {
        final Uuid topicId = new Uuid(11, ++topicSeq);
        final String topicName = "fanout-" + topicSeq;
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, partitionCount)), duration -> { }).run();
        final List<TopicIdPartition> partitions = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            partitions.add(new TopicIdPartition(topicId, p, topicName));
        }
        return partitions;
    }

    /** Batches grouped by partition and byte-contiguous, as {@code BatchBuffer.close()} lays them out. */
    private List<CommitBatchRequest> commitWindow(final List<TopicIdPartition> partitions,
                                                 final int batchesPerPartition) {
        final List<CommitBatchRequest> requests = new ArrayList<>(partitions.size() * batchesPerPartition);
        int byteOffset = 0;
        for (final TopicIdPartition partition : partitions) {
            for (int b = 0; b < batchesPerPartition; b++) {
                requests.add(CommitBatchRequest.of(
                    0, partition, byteOffset, BATCH_BYTES, 0, 0, time.milliseconds(), TimestampType.CREATE_TIME));
                byteOffset += BATCH_BYTES;
            }
        }
        return requests;
    }
}
