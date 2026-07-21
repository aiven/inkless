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

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.BATCHES;

/**
 * Measures {@code commit_file_v2} (coalescing) vs {@code commit_file_v1} on two axes: write path
 * ({@code batches}-row reduction + commit latency) and read path ({@code find_batches} query latency +
 * batch_info entries returned). Read savings are metadata-only — object-storage byte I/O is unaffected,
 * as the fetch planner already merges contiguous ranges.
 *
 * Compares against {@code commit_file_v1}, so retire or adapt it when v1 is dropped.
 */
@Tag("benchmark")
@Testcontainers
class CommitFileCoalescingBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 1_000_000;
    static final int BATCH_BYTES = 120;     // small batch typical of low-throughput producers
    static final int FIND_REPS = 5;         // repeat find_batches to smooth out timing noise

    Time time = new MockTime();
    int topicSeq = 0;                       // unique topic id per runShape, so runs are isolated

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    /**
     * One workload shape.
     *
     * @param name                       human-readable label
     * @param partitions                 number of partitions active per commit window
     * @param batchesPerPartitionPerFile number of physical batches each partition contributes per window
     * @param windows                    number of commit windows (files) to simulate
     */
    private record Shape(String name, int partitions, int batchesPerPartitionPerFile, int windows) {
    }

    @Test
    void benchmarkRowReduction() {
        final List<Shape> shapes = List.of(
            // High fan-in: many producers funnel small batches into few partitions.
            new Shape("high-fan-in (4 parts x 16 batches)", 4, 16, 200),
            // Few producers, frequent tiny batches into a handful of partitions.
            new Shape("frequent-tiny (8 parts x 8 batches)", 8, 8, 200),
            // Many partitions, ~1 batch each: the worst case for coalescing (floor = 1 row/partition).
            new Shape("sparse-wide (64 parts x 1 batch)", 64, 1, 100),
            // Moderate: 16 partitions x 4 batches.
            new Shape("moderate (16 parts x 4 batches)", 16, 4, 150)
        );

        final StringBuilder write = new StringBuilder();
        write.append(String.format("%n== WRITE PATH: coalescing (commit_file_v2) vs baseline (commit_file_v1) ==%n"));
        write.append(String.format("Row reduction ratio equals batches-per-partition-per-window; "
            + "sparse-wide is the 1-row-per-partition floor where coalescing cannot help.%n%n"));
        write.append(String.format("%-40s | %10s | %10s | %7s | %12s | %12s%n",
            "shape", "v1 rows", "v2 rows", "ratio", "v1 ms/file", "v2 ms/file"));
        write.append("-".repeat(104)).append(String.format("%n"));

        final StringBuilder read = new StringBuilder();
        read.append(String.format("%n== READ PATH: find_batches metadata query (whole-log scan per partition) ==%n"));
        read.append(String.format("Coalescing returns K x fewer batch_info entries, shrinking the scan + payload. "
            + "Object-storage byte I/O is NOT affected (ranges are already merged), so this is the metadata path only.%n%n"));
        read.append(String.format("%-40s | %12s | %12s | %7s | %12s | %12s%n",
            "shape", "v1 batches", "v2 batches", "ratio", "v1 us/find", "v2 us/find"));
        read.append("-".repeat(110)).append(String.format("%n"));

        for (final Shape shape : shapes) {
            final Result v1 = runShape(shape, false);
            final Result v2 = runShape(shape, true);

            // Regression guard: coalescing must never write more rows or return more batches than v1.
            assertThat(v2.rowsWritten).isLessThanOrEqualTo(v1.rowsWritten);
            assertThat(v2.batchesReturned).isLessThanOrEqualTo(v1.batchesReturned);

            final double rowRatio = v2.rowsWritten == 0 ? 0 : (double) v1.rowsWritten / v2.rowsWritten;
            write.append(String.format("%-40s | %10d | %10d | %6.2fx | %12.3f | %12.3f%n",
                shape.name, v1.rowsWritten, v2.rowsWritten, rowRatio, v1.msPerCommit(), v2.msPerCommit()));

            final double batchRatio = v2.batchesReturned == 0 ? 0 : (double) v1.batchesReturned / v2.batchesReturned;
            read.append(String.format("%-40s | %12d | %12d | %6.2fx | %12.3f | %12.3f%n",
                shape.name, v1.batchesReturned, v2.batchesReturned, batchRatio, v1.usPerFind(), v2.usPerFind()));
        }
        write.append(String.format("%nNote: rows are per-topic and exact; commit latency is wall-clock per commit "
            + "against a local container and is indicative, not a microbenchmark.%n"));
        read.append(String.format("%nNote: batch counts are exact; find latency is wall-clock per find_batches call "
            + "(whole-log, offset 0) and is indicative. It measures only the control-plane metadata query, "
            + "not object-storage fetch.%n"));

        System.out.println(write);
        System.out.println(read);
    }

    private record Result(long rowsWritten, double msPerCommit, long batchesReturned, double usPerFind) {
    }

    private Result runShape(final Shape shape, final boolean coalesce) {
        final List<TopicIdPartition> partitions = createTopic(shape.partitions);

        // 1. WRITE: commit `windows` files, each carrying `batchesPerPartitionPerFile` byte-contiguous
        //    batches per partition (the layout BatchBuffer.close() produces). Measure commit time.
        long commitNanos = 0;
        for (int w = 0; w < shape.windows; w++) {
            final List<CommitBatchRequest> requests = commitWindow(partitions, shape.batchesPerPartitionPerFile);
            final CommitFileJob commit = new CommitFileJob(
                time, pgContainer.getJooqCtx(), "file-" + partitions.get(0).topicId() + "-" + w,
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, requests, coalesce, duration -> {});
            final long start = System.nanoTime();
            commit.call();
            commitNanos += System.nanoTime() - start;
        }

        // 2. READ: find_batches over the whole log per partition (offset 0, unbounded fetch). Measure
        //    query time and count the batch_info entries returned (coalescing returns K x fewer).
        long findNanos = 0;
        long findCalls = 0;
        long batchesReturned = 0;
        for (int rep = 0; rep < FIND_REPS; rep++) {
            for (final TopicIdPartition partition : partitions) {
                final FindBatchesJob find = new FindBatchesJob(time, pgContainer.getJooqCtx(),
                    List.of(new FindBatchRequest(partition, 0L, Integer.MAX_VALUE)), Integer.MAX_VALUE, 0, duration -> {});
                final long start = System.nanoTime();
                final List<FindBatchResponse> responses = find.call();
                findNanos += System.nanoTime() - start;
                findCalls++;
                if (rep == 0) {  // counts are identical across reps; count once
                    batchesReturned += responses.stream().filter(r -> r.batches() != null)
                        .mapToLong(r -> r.batches().size()).sum();
                }
            }
        }

        return new Result(
            batchRowCount(partitions.get(0).topicId()),
            commitNanos / 1_000_000.0 / shape.windows,
            batchesReturned,
            findNanos / 1_000.0 / findCalls);
    }

    /** Creates a fresh topic (unique id per call so runs are isolated) and returns its partitions. */
    private List<TopicIdPartition> createTopic(final int partitionCount) {
        final Uuid topicId = new Uuid(7, ++topicSeq);
        final String topicName = "bench-" + topicSeq;
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, partitionCount)), duration -> {}).run();
        final List<TopicIdPartition> partitions = new ArrayList<>();
        for (int p = 0; p < partitionCount; p++) {
            partitions.add(new TopicIdPartition(topicId, p, topicName));
        }
        return partitions;
    }

    /** One commit window: `batchesPerPartition` single-record, byte-contiguous batches per partition. */
    private List<CommitBatchRequest> commitWindow(final List<TopicIdPartition> partitions, final int batchesPerPartition) {
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
