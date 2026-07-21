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
import org.apache.kafka.common.protocol.Errors;
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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.BATCHES;

/**
 * Measures commit latency while enforce_retention_v2 is running, in two shapes.
 *
 * <p>{@code benchmarkCommitBlockingDuringRetention} isolates the scan-under-lock effect: enforce and
 * commit target the SAME single partition, so the commit tail reflects the boundary scan holding that
 * partition's log row lock.
 *
 * <p>{@code benchmarkCommitBlockingMultiPartition} isolates the lock-accumulation-across-partitions
 * effect: enforce runs over N partitions in one call while a commit targets only the first one. Because
 * the baseline locks all partitions in a single transaction, the first partition's lock is held through
 * every other partition's scan, so the commit to it waits for the WHOLE enforce call. This is the case
 * that distinguishes lock-late alone (still blocks here) from lock-late plus per-request transactions
 * (does not).
 *
 * <p>A/B usage: run on the current implementation to capture the lock-coupled baseline, apply the
 * lock-decoupling rewrite, then re-run and compare the commit max/p95 columns. Both tests drive the
 * public {@link EnforceRetentionJob} API, so they run unchanged against either implementation.
 */
@Tag("benchmark")
@Testcontainers
class EnforceRetentionCommitBlockingBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 100_000_000;
    static final int BATCH_BYTES = 120;
    static final int BATCHES_PER_WINDOW = 10_000;
    static final int MAX_BATCHES_PER_ENFORCE = 1;
    static final long COMMIT_START_DELAY_MS = 10;
    static final long WARMUP_DEPTH = 100_000;
    static final long[] DEPTH_CHECKPOINTS = {200_000, 400_000, 800_000};

    static final int MULTI_PARTITION_COUNT = 8;
    static final long MULTI_WARMUP_DEPTH = 25_000;
    static final long[] MULTI_DEPTH_CHECKPOINTS = {50_000, 100_000};

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
    void benchmarkCommitBlockingDuringRetention() throws Exception {
        final TopicIdPartition partition = createSinglePartitionTopic();

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== WRITE PATH: commit latency while enforce_retention_v2 scans the same partition ==%n"));
        out.append(String.format("Retention uses retentionBytes=0 and maxBatchesPerRequest=%d so delete work is tiny; "
            + "the measured tail is lock coupling, not delete volume.%n%n", MAX_BATCHES_PER_ENFORCE));
        out.append(String.format("%12s | %12s | %12s | %8s | %13s | %13s | %13s | %13s%n",
            "target rows", "start rows", "enforce ms", "commits", "commit p50 ms", "commit p95 ms", "commit max ms", "max/enforce"));
        out.append("-".repeat(122)).append(String.format("%n"));

        seedUntil(partition, WARMUP_DEPTH);
        measureRetentionWithConcurrentCommits(singleRequest(partition), partition);

        for (final long targetDepth : DEPTH_CHECKPOINTS) {
            seedUntil(partition, targetDepth);
            final long startRows = partitionRowCount(partition);
            final Measurement measurement = measureRetentionWithConcurrentCommits(singleRequest(partition), partition);
            out.append(String.format("%12d | %12d | %12.3f | %8d | %13.3f | %13.3f | %13.3f | %12.2fx%n",
                targetDepth,
                startRows,
                measurement.enforceMs,
                measurement.commitStats.count,
                measurement.commitStats.p50Ms,
                measurement.commitStats.p95Ms,
                measurement.commitStats.maxMs,
                measurement.maxCommitToEnforceRatio()));
        }

        out.append(String.format("%nNote: latency is wall-clock against a local container and is indicative, not a microbenchmark. "
            + "Run the same benchmark before and after the lock-decoupling change and compare the commit max/p95 columns.%n"));
        System.out.println(out);
    }

    @Test
    void benchmarkCommitBlockingMultiPartition() throws Exception {
        final List<TopicIdPartition> partitions = createTopic(MULTI_PARTITION_COUNT);
        // The baseline SQL loop orders by (topic_id, partition), so the lowest partition is locked first
        // and held longest. Committing to it exposes the worst-case cross-partition blocking.
        final TopicIdPartition commitTarget = partitions.get(0);
        final List<EnforceRetentionRequest> enforceRequests = enforceRequestsFor(partitions);

        final StringBuilder out = new StringBuilder();
        out.append(String.format("%n== WRITE PATH: commit latency to ONE partition while enforce_retention_v2 scans %d partitions ==%n",
            MULTI_PARTITION_COUNT));
        out.append(String.format("Enforce runs over all %d partitions in one call; the commit targets only the first. "
            + "The baseline holds the first partition's lock through every other partition's scan, so the commit waits "
            + "for the whole enforce call.%n%n", MULTI_PARTITION_COUNT));
        out.append(String.format("%14s | %10s | %12s | %12s | %8s | %13s | %13s | %13s | %13s%n",
            "rows/partition", "partitions", "total rows", "enforce ms", "commits", "commit p50 ms", "commit p95 ms", "commit max ms", "max/enforce"));
        out.append("-".repeat(140)).append(String.format("%n"));

        seedAllPartitions(partitions, MULTI_WARMUP_DEPTH);
        measureRetentionWithConcurrentCommits(enforceRequests, commitTarget);

        for (final long depthPerPartition : MULTI_DEPTH_CHECKPOINTS) {
            seedAllPartitions(partitions, depthPerPartition);
            final long totalRows = batchRowCount(commitTarget.topicId());
            final Measurement measurement = measureRetentionWithConcurrentCommits(enforceRequests, commitTarget);
            out.append(String.format("%14d | %10d | %12d | %12.3f | %8d | %13.3f | %13.3f | %13.3f | %12.2fx%n",
                depthPerPartition,
                MULTI_PARTITION_COUNT,
                totalRows,
                measurement.enforceMs,
                measurement.commitStats.count,
                measurement.commitStats.p50Ms,
                measurement.commitStats.p95Ms,
                measurement.commitStats.maxMs,
                measurement.maxCommitToEnforceRatio()));
        }

        out.append(String.format("%nNote: latency is wall-clock against a local container and is indicative, not a microbenchmark. "
            + "Baseline max/enforce approaches 1.00x (commit blocked through the whole call); per-request transactions should "
            + "collapse it toward one partition's delete.%n"));
        System.out.println(out);
    }

    private record Measurement(double enforceMs, CommitStats commitStats) {
        double maxCommitToEnforceRatio() {
            return enforceMs == 0 ? 0 : commitStats.maxMs / enforceMs;
        }
    }

    private record CommitStats(int count, double p50Ms, double p95Ms, double maxMs) {
        static CommitStats fromNanos(final List<Long> samples) {
            if (samples.isEmpty()) {
                return new CommitStats(0, 0, 0, 0);
            }
            final List<Long> sorted = samples.stream().sorted(Comparator.naturalOrder()).toList();
            return new CommitStats(
                sorted.size(),
                nanosToMillis(percentile(sorted, 0.50)),
                nanosToMillis(percentile(sorted, 0.95)),
                nanosToMillis(sorted.get(sorted.size() - 1))
            );
        }

        private static long percentile(final List<Long> sorted, final double percentile) {
            final int index = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(percentile * sorted.size()) - 1));
            return sorted.get(index);
        }
    }

    private Measurement measureRetentionWithConcurrentCommits(final List<EnforceRetentionRequest> enforceRequests,
                                                              final TopicIdPartition commitTarget) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<Long> enforceFuture = executor.submit(() -> {
                final long start = System.nanoTime();
                final List<EnforceRetentionResponse> responses = new EnforceRetentionJob(
                    time,
                    pgContainer.getJooqCtx(),
                    enforceRequests,
                    MAX_BATCHES_PER_ENFORCE,
                    duration -> {
                    }
                ).call();
                assertThat(responses).hasSize(enforceRequests.size());
                assertThat(responses).allSatisfy(r -> assertThat(r.errors()).isEqualTo(Errors.NONE));
                return System.nanoTime() - start;
            });
            final Future<List<Long>> commitFuture = executor.submit(() -> measureCommitsWhileEnforceRuns(commitTarget, enforceFuture));

            final long enforceNanos = get(enforceFuture);
            final List<Long> commitNanos = get(commitFuture);
            return new Measurement(nanosToMillis(enforceNanos), CommitStats.fromNanos(commitNanos));
        } finally {
            executor.shutdownNow();
        }
    }

    private List<Long> measureCommitsWhileEnforceRuns(final TopicIdPartition partition,
                                                      final Future<Long> enforceFuture) throws Exception {
        Thread.sleep(COMMIT_START_DELAY_MS);
        final List<Long> commitNanos = new ArrayList<>();
        while (!enforceFuture.isDone()) {
            final long start = System.nanoTime();
            final List<CommitBatchResponse> responses = commitOneBatch(partition);
            commitNanos.add(System.nanoTime() - start);
            assertThat(responses).hasSize(1);
            assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        }
        return commitNanos;
    }

    private List<CommitBatchResponse> commitOneBatch(final TopicIdPartition partition) {
        final String objectKey = "commit-blocking-" + partition.topicId() + "-" + fileSeq++;
        return new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            FILE_SIZE,
            List.of(batch(partition, 0)),
            false,
            duration -> {
            }
        ).call();
    }

    private void seedUntil(final TopicIdPartition partition, final long targetDepth) {
        while (partitionRowCount(partition) < targetDepth) {
            final long currentRows = partitionRowCount(partition);
            final int batchesThisWindow = (int) Math.min(BATCHES_PER_WINDOW, targetDepth - currentRows);
            final List<CommitBatchRequest> requests = new ArrayList<>(batchesThisWindow);
            int byteOffset = 0;
            for (int b = 0; b < batchesThisWindow; b++) {
                requests.add(batch(partition, byteOffset));
                byteOffset += BATCH_BYTES;
            }
            final String objectKey = "seed-" + partition.topicId() + "-" + partition.partition() + "-" + fileSeq++;
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
        }
    }

    private void seedAllPartitions(final List<TopicIdPartition> partitions, final long depthPerPartition) {
        for (final TopicIdPartition partition : partitions) {
            seedUntil(partition, depthPerPartition);
        }
    }

    private List<EnforceRetentionRequest> singleRequest(final TopicIdPartition partition) {
        return List.of(new EnforceRetentionRequest(partition.topicId(), partition.partition(), 0, -1));
    }

    private List<EnforceRetentionRequest> enforceRequestsFor(final List<TopicIdPartition> partitions) {
        return partitions.stream()
            .map(p -> new EnforceRetentionRequest(p.topicId(), p.partition(), 0, -1))
            .toList();
    }

    private CommitBatchRequest batch(final TopicIdPartition partition, final int byteOffset) {
        return CommitBatchRequest.of(
            0,
            partition,
            byteOffset,
            BATCH_BYTES,
            0,
            0,
            time.milliseconds(),
            TimestampType.CREATE_TIME
        );
    }

    private TopicIdPartition createSinglePartitionTopic() {
        final Uuid topicId = new Uuid(7, 1);
        final String topicName = "bench-retention-blocking";
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, 1)), duration -> {
            }).run();
        return new TopicIdPartition(topicId, 0, topicName);
    }

    private List<TopicIdPartition> createTopic(final int partitionCount) {
        final Uuid topicId = new Uuid(7, 2);
        final String topicName = "bench-retention-blocking-multi";
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topicName, partitionCount)), duration -> {
            }).run();
        final List<TopicIdPartition> partitions = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            partitions.add(new TopicIdPartition(topicId, p, topicName));
        }
        return partitions;
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

    private long partitionRowCount(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.selectCount()
                .from(BATCHES)
                .where(BATCHES.TOPIC_ID.eq(partition.topicId()).and(BATCHES.PARTITION.eq(partition.partition())))
                .fetchOne(0, long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static double nanosToMillis(final long nanos) {
        return nanos / 1_000_000.0;
    }

    private static <T> T get(final Future<T> future) throws Exception {
        try {
            return future.get();
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new RuntimeException(cause);
        }
    }
}
