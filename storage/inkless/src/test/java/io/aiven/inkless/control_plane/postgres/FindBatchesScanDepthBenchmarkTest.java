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

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.jooq.generated.Tables.BATCHES;

/**
 * Measures how {@code find_batches_v2} query latency scales with the <em>scan depth</em> of a partition
 * (the offset distance from {@code log_start} to {@code high_watermark}) while the <em>result</em> -- a
 * fixed-byte fetch from {@code log_start} -- is held constant.
 *
 * <p>Before V19, the function computed {@code ROW_NUMBER()} / {@code SUM() OVER} windows over the whole
 * {@code [starting_offset, high_watermark)} range before trimming to the byte budget, so the scan could not
 * terminate early: latency was O(read-depth) rather than O(result). The signature of that cost is
 * <b>batches-returned stays flat while us/find grows ~linearly with depth</b>; a fetch budget-bounded scan
 * should instead keep us/find flat.
 *
 * <p>A/B usage: run on the pre-V19 version to capture the rising curve, apply the bounded rewrite, re-run,
 * and compare the printed tables. The fetch shape (1 MB from offset 0 over a multi-hundred-k-deep log)
 * mirrors a lagging consumer far behind the tail.
 *
 * <p>Seeded once, measured at each depth checkpoint along the way (not re-seeded per depth), so the whole
 * curve is one pass. Checkpoints are tunable; larger depths make the depth-vs-result gap more pronounced but
 * cost more seed time.
 */
@Tag("benchmark")
@Testcontainers
class FindBatchesScanDepthBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    // large enough to hold BATCHES_PER_WINDOW * BATCH_BYTES
    static final long FILE_SIZE = 100_000_000;
    // small single-record batch, typical low-throughput producer
    static final int BATCH_BYTES = 120;
    // batches per commit_file call (one file/window)
    static final int BATCHES_PER_WINDOW = 10_000;
    // the constant "result" budget: a 1 MB fetch (incident-faithful)
    static final int FETCH_BUDGET_BYTES = 1_000_000;
    // repeat find_batches to smooth timing noise (first rep discarded)
    static final int FIND_REPS = 5;

    // Depth checkpoints (total batches in the partition). Tune up to approach the incident's ~2.3M-row scan.
    static final long[] DEPTH_CHECKPOINTS = {25_000, 50_000, 100_000, 200_000, 400_000};

    Time time = new MockTime();
    int fileSeq = 0;   // monotonic across all commits so object keys never collide

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
    void benchmarkScanDepth() {
        final TopicIdPartition partition = createSinglePartitionTopic();

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== READ PATH: find_batches_v2 latency vs scan depth (fixed %d-byte fetch from log_start) ==%n",
            FETCH_BUDGET_BYTES));
        out.append(String.format("Gap signature (V15): batches-returned flat, us/find grows ~linearly with depth (O(depth)).%n"));
        out.append(String.format("A bounded rewrite should keep us/find flat (O(result)).%n%n"));
        out.append(String.format("%12s | %14s | %12s | %12s%n", "depth (rows)", "batches ret'd", "us/find", "ms/find"));
        out.append("-".repeat(60)).append(String.format("%n"));

        long committed = 0;
        for (final long targetDepth : DEPTH_CHECKPOINTS) {
            committed += seedUntil(partition, committed, targetDepth);
            final Measurement m = measureFind(partition);
            out.append(String.format("%12d | %14d | %12.1f | %12.3f%n",
                committed, m.batchesReturned, m.usPerFind, m.usPerFind / 1000.0));
        }

        out.append(String.format("%nNote: latency is wall-clock per find_batches call against a local container -- "
            + "indicative, not a microbenchmark. It measures the control-plane metadata query only, not object-storage fetch.%n"));
        System.out.println(out);

        explainInnerScan(partition, committed);
    }

    /**
     * Investigation: holds scan depth fixed and sweeps the fetch budget (= result size k). The plpgsql
     * rewrite builds the result via `arr := arr || elem` per row, which is O(k^2) if PostgreSQL copies the
     * array on each append. Tell: the `us/batch` column (us/find divided by k). Flat => linear O(k); rising
     * with k => quadratic append cost.
     */
    @Test
    void benchmarkResultSize() {
        final TopicIdPartition partition = createSinglePartitionTopic();
        final long depth = 200_000;
        seedUntil(partition, 0, depth);

        final int[] resultTargets = {1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000};

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== RESULT-SIZE SWEEP: find_batches_v2 latency vs batches returned (depth fixed at %d) ==%n", depth));
        out.append(String.format("us/batch flat => O(k) build; rising with k => O(k^2) array append.%n%n"));
        out.append(String.format("%12s | %14s | %12s | %12s%n", "budget bytes", "batches ret'd", "us/find", "us/batch"));
        out.append("-".repeat(60)).append(String.format("%n"));

        for (final int k : resultTargets) {
            final int budget = k * BATCH_BYTES;
            final Measurement m = measureFind(partition, budget);
            final double usPerBatch = m.batchesReturned == 0 ? 0 : m.usPerFind / m.batchesReturned;
            out.append(String.format("%12d | %14d | %12.1f | %12.3f%n",
                budget, m.batchesReturned, m.usPerFind, usPerBatch));
        }
        System.out.println(out);
    }

    private record Measurement(long batchesReturned, double usPerFind) {
    }

    /**
     * Dumps the plan of the inner per-partition scan the plpgsql function drives. We EXPLAIN the query
     * standalone (the function body is opaque to EXPLAIN). What matters is the plan SHAPE: an index scan
     * yielding last_offset order with NO Sort node above it -- that is what lets the cursor stop early
     * (O(result)). A Sort would force reading the whole range before row 1 (O(depth)).
     */
    private void explainInnerScan(final TopicIdPartition p, final long highWatermark) {
        final org.jooq.Result<?> plan = pgContainer.getJooqCtx().fetch(
            "EXPLAIN (ANALYZE, BUFFERS) "
                + "SELECT b.*, f.object_key FROM batches b "
                + "JOIN files f ON b.file_id = f.file_id "
                + "WHERE b.topic_id = {0} AND b.partition = {1} "
                + "  AND b.last_offset >= 0 AND b.base_offset < {2} "
                + "ORDER BY b.last_offset",
            DSL.val(p.topicId(), BATCHES.TOPIC_ID.getDataType()),
            DSL.val(p.partition(), BATCHES.PARTITION.getDataType()),
            DSL.val(highWatermark, BATCHES.BASE_OFFSET.getDataType()));
        System.out.println("\n== EXPLAIN inner find_batches scan (depth=" + highWatermark + ") ==");
        plan.forEach(row -> System.out.println(row.get(0)));
    }

    /**
     * Runs the same bounded fetch FIND_REPS times (first discarded as warmup); returns mean us and result size.
     */
    private Measurement measureFind(final TopicIdPartition partition) {
        return measureFind(partition, FETCH_BUDGET_BYTES);
    }

    private Measurement measureFind(final TopicIdPartition partition, final int budget) {
        long batchesReturned = 0;
        long findNanos = 0;
        for (int rep = 0; rep < FIND_REPS + 1; rep++) {
            final FindBatchesJob find = new FindBatchesJob(
                time,
                pgContainer.getJooqCtx(),
                List.of(new FindBatchRequest(partition, 0L, budget)),
                budget,
                0,
                duration -> {
                }
            );
            final long start = System.nanoTime();
            final List<FindBatchResponse> responses = find.call();
            final long elapsed = System.nanoTime() - start;
            if (rep == 0) {  // warmup: prime plan cache / OS cache, don't time
                batchesReturned = responses.stream().filter(r -> r.batches() != null)
                    .mapToLong(r -> r.batches().size()).sum();
                continue;
            }
            findNanos += elapsed;
        }
        return new Measurement(batchesReturned, findNanos / 1_000.0 / FIND_REPS);
    }

    /**
     * Commits windows until the partition holds at least targetDepth batches; returns how many were added.
     */
    private long seedUntil(final TopicIdPartition partition, final long alreadyCommitted, final long targetDepth) {
        long added = 0;
        while (alreadyCommitted + added < targetDepth) {
            final int batchesThisWindow = (int) Math.min(BATCHES_PER_WINDOW, targetDepth - alreadyCommitted - added);
            final List<CommitBatchRequest> requests = new ArrayList<>(batchesThisWindow);
            int byteOffset = 0;
            for (int b = 0; b < batchesThisWindow; b++) {
                requests.add(
                    CommitBatchRequest.of(
                        0,
                        partition,
                        byteOffset,
                        BATCH_BYTES,
                        0,
                        0,
                        time.milliseconds(),
                        TimestampType.CREATE_TIME
                    )
                );
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
        final Uuid topicId = new Uuid(7, 1);
        final String topicName = "bench-scan-depth";
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, 1)), duration -> {
        }).run();
        return new TopicIdPartition(topicId, 0, topicName);
    }

    @SuppressWarnings("unused")
    private long batchRowCount(final Uuid topicId) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.selectCount()
                .from(BATCHES)
                .where(BATCHES.TOPIC_ID.eq(topicId))
                .fetchOne(0, long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
