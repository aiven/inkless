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

import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.generated.udt.PruneBatchesBelowHighestTieredOffsetResponseV1;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetRequestV1Record;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetResponseV1Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.LOGS;
import static org.jooq.generated.Tables.PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V1;

/**
 * Compares commit latency under the old unbounded prune ({@code v1}, one lock held for the whole
 * tiered backlog) against the capped prune ({@code v2}, the same backlog cleared through many
 * short locked passes).
 *
 * <p>Both scenarios delete the same total batch count from identically seeded partitions. Postgres
 * clears an indexed range fast either way. The comparison that counts is the longest lock hold, the
 * worst case a produce commit can stall behind, not total prune time. {@code v1} holds the lock for
 * one pass over the whole backlog. {@code v2} holds it for one capped pass at a time and releases it
 * between passes. A background thread commits continuously across the whole measurement window. It
 * pools every commit latency, not just the couple that land inside one short pass.
 *
 * <p>The control plane uses a five-second socket timeout by default. Every prune pass and every
 * commit waiting behind its partition lock must stay inside that budget.
 */
@Tag("benchmark")
@Testcontainers
class PruneDisklessLogsCommitBlockingBenchmarkTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final int TIERED_BATCH_COUNT = 200_000;
    static final int UNTIERED_BATCH_COUNT = 200_000;
    static final int BATCHES_PER_SEED_FILE = 10_000;
    static final int BATCH_BYTES = 120;
    static final int MAX_BATCHES_PER_PRUNE = 2_000;
    static final int V2_PASS_COUNT = TIERED_BATCH_COUNT / MAX_BATCHES_PER_PRUNE;
    static final long CONTROL_PLANE_SOCKET_TIMEOUT_MS = 5_000;
    static final double MIN_V1_TO_V2_MAX_PASS_RATIO = 2;

    static final Uuid TOPIC_ID_V1 = new Uuid(7, 3);
    static final String TOPIC_V1 = "bench-prune-blocking-v1";
    static final TopicIdPartition PARTITION_V1 = new TopicIdPartition(TOPIC_ID_V1, 0, TOPIC_V1);

    static final Uuid TOPIC_ID_V2 = new Uuid(7, 4);
    static final String TOPIC_V2 = "bench-prune-blocking-v2";
    static final TopicIdPartition PARTITION_V2 = new TopicIdPartition(TOPIC_ID_V2, 0, TOPIC_V2);

    private final MockTime time = new MockTime();
    private int fileSequence;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(
                new CreateTopicAndPartitionsRequest(TOPIC_ID_V1, TOPIC_V1, 1),
                new CreateTopicAndPartitionsRequest(TOPIC_ID_V2, TOPIC_V2, 1)
            ),
            ignored -> {}
        ).run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void comparesUnboundedV1AgainstCappedV2UnderTheSameBacklog() throws Exception {
        seedBatches(PARTITION_V1, TIERED_BATCH_COUNT + UNTIERED_BATCH_COUNT);
        seedBatches(PARTITION_V2, TIERED_BATCH_COUNT + UNTIERED_BATCH_COUNT);
        setRemoteLogStartOffset(PARTITION_V1, 0L);
        setRemoteLogStartOffset(PARTITION_V2, 0L);

        final ScenarioResult v1Result = measureUnboundedV1Pass();
        final ScenarioResult v2Result = measureRepeatedCappedV2Passes();

        System.out.printf(
            "%n== WRITE PATH: commit latency during consolidation prune (v1 unbounded vs. v2 capped) ==%n"
                + "Tiered batches deleted by each scenario: %d; untiered tail: %d; v2 cap: %d batches "
                + "over %d passes; control-plane socket timeout: %d ms%n"
                + "%24s | %10s | %13s | %13s | %8s | %13s | %13s%n"
                + "%24s | %10.3f | %13.3f | %13.3f | %8d | %13.3f | %13.3f%n"
                + "%24s | %10.3f | %13.3f | %13.3f | %8d | %13.3f | %13.3f%n%n"
                + "v1's one lock hold is %.1fx as long as v2's single worst-case lock hold, for the "
                + "same total batch count.%n%n",
            TIERED_BATCH_COUNT,
            UNTIERED_BATCH_COUNT,
            MAX_BATCHES_PER_PRUNE,
            V2_PASS_COUNT,
            CONTROL_PLANE_SOCKET_TIMEOUT_MS,
            "scenario", "total ms", "pass p50 ms", "pass max ms", "commits", "commit p50 ms", "commit max ms",
            "v1 (one pass)", v1Result.totalPruneMs, v1Result.passStats.p50Ms, v1Result.passStats.maxMs,
            v1Result.commitStats.count, v1Result.commitStats.p50Ms, v1Result.commitStats.maxMs,
            "v2 (" + V2_PASS_COUNT + " passes)", v2Result.totalPruneMs, v2Result.passStats.p50Ms,
            v2Result.passStats.maxMs, v2Result.commitStats.count, v2Result.commitStats.p50Ms,
            v2Result.commitStats.maxMs,
            v1Result.passStats.maxMs / v2Result.passStats.maxMs
        );

        assertThat(v1Result.commitStats.count).isPositive();
        assertThat(v2Result.commitStats.count).isPositive();
        assertThat(v2Result.passStats.maxMs).isLessThan((double) CONTROL_PLANE_SOCKET_TIMEOUT_MS);

        assertThat(v1Result.passStats.maxMs).isGreaterThan(v2Result.passStats.maxMs * MIN_V1_TO_V2_MAX_PASS_RATIO);

        assertThat(singleLog(PARTITION_V1).getLogStartOffset()).isEqualTo((long) TIERED_BATCH_COUNT);
        assertThat(singleLog(PARTITION_V2).getLogStartOffset()).isEqualTo((long) TIERED_BATCH_COUNT);
    }

    private ScenarioResult measureUnboundedV1Pass() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean pruneStarted = new AtomicBoolean(false);
        try {
            final Future<Long> pruneResult = executor.submit(() -> {
                pruneStarted.set(true);
                final long start = System.nanoTime();
                final PruneBatchesBelowHighestTieredOffsetResponseV1Record response = pruneV1(
                    PARTITION_V1, TIERED_BATCH_COUNT - 1L);
                final long elapsed = System.nanoTime() - start;
                assertThat(response.getLogStartOffset()).isEqualTo((long) TIERED_BATCH_COUNT);
                return elapsed;
            });

            final List<Long> commitNanos = commitContinuouslyWhile(pruneStarted, pruneResult::isDone, PARTITION_V1);
            final long passNanos = pruneResult.get(30, TimeUnit.SECONDS);
            return new ScenarioResult(
                nanosToMillis(passNanos), DurationStats.fromNanos(List.of(passNanos)),
                DurationStats.fromNanos(commitNanos));
        } finally {
            executor.shutdownNow();
        }
    }

    private ScenarioResult measureRepeatedCappedV2Passes() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean pruneStarted = new AtomicBoolean(false);
        try {
            final Future<List<Long>> pruneResult = executor.submit(() -> {
                pruneStarted.set(true);
                final List<Long> passNanos = new ArrayList<>(V2_PASS_COUNT);
                for (int pass = 0; pass < V2_PASS_COUNT; pass++) {
                    final long passStart = System.nanoTime();
                    final List<PruneDisklessLogsResponse> responses = new PruneDisklessLogsJob(
                        time,
                        pgContainer.getJooqCtx(),
                        List.of(new PruneDisklessLogsRequest(PARTITION_V2, TIERED_BATCH_COUNT - 1L)),
                        MAX_BATCHES_PER_PRUNE,
                        ignored -> {}
                    ).call();
                    passNanos.add(System.nanoTime() - passStart);
                    assertThat(responses).singleElement().satisfies(response -> {
                        assertThat(response.batchesDeleted()).isEqualTo(MAX_BATCHES_PER_PRUNE);
                        assertThat(response.bytesDeleted()).isEqualTo((long) MAX_BATCHES_PER_PRUNE * BATCH_BYTES);
                    });
                }
                return passNanos;
            });

            final List<Long> commitNanos = commitContinuouslyWhile(pruneStarted, pruneResult::isDone, PARTITION_V2);
            final List<Long> passNanos = pruneResult.get(30, TimeUnit.SECONDS);
            final double totalMs = nanosToMillis(passNanos.stream().mapToLong(Long::longValue).sum());
            return new ScenarioResult(
                totalMs, DurationStats.fromNanos(passNanos), DurationStats.fromNanos(commitNanos));
        } finally {
            executor.shutdownNow();
        }
    }

    private List<Long> commitContinuouslyWhile(
        final AtomicBoolean pruneStarted,
        final BooleanSupplier pruneDone,
        final TopicIdPartition partition
    ) throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<List<Long>> commitResult = executor.submit(() -> {
                while (!pruneStarted.get()) {
                    Thread.onSpinWait();
                }
                final List<Long> commitNanos = new ArrayList<>();
                do {
                    final long start = System.nanoTime();
                    final List<CommitBatchResponse> responses = commitOneBatch(partition);
                    commitNanos.add(System.nanoTime() - start);
                    assertThat(responses).singleElement();
                } while (!pruneDone.getAsBoolean());
                return commitNanos;
            });
            return commitResult.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private PruneBatchesBelowHighestTieredOffsetResponseV1Record pruneV1(
        final TopicIdPartition partition,
        final long highestTieredOffset
    ) {
        final PruneBatchesBelowHighestTieredOffsetRequestV1Record[] requests = {
            new PruneBatchesBelowHighestTieredOffsetRequestV1Record(
                partition.topicId(), partition.partition(), highestTieredOffset)
        };
        final List<PruneBatchesBelowHighestTieredOffsetResponseV1Record> responses =
            pgContainer.getJooqCtx().transactionResult(conf -> conf.dsl().select(
                    PruneBatchesBelowHighestTieredOffsetResponseV1.TOPIC_ID,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.PARTITION,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.LOG_START_OFFSET,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.ERROR
                ).from(PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V1.call(Instant.EPOCH, requests))
                .fetchInto(PruneBatchesBelowHighestTieredOffsetResponseV1Record.class));
        assertThat(responses).hasSize(1);
        return responses.get(0);
    }

    private List<CommitBatchResponse> commitOneBatch(final TopicIdPartition partition) {
        return new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            "prune-commit-" + fileSequence++,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            BATCH_BYTES,
            List.of(CommitBatchRequest.of(
                0,
                partition,
                0,
                BATCH_BYTES,
                0,
                0,
                time.milliseconds(),
                TimestampType.CREATE_TIME
            )),
            false,
            ignored -> {}
        ).call();
    }

    private void seedBatches(final TopicIdPartition partition, final int batchCount) {
        for (int seeded = 0; seeded < batchCount; seeded += BATCHES_PER_SEED_FILE) {
            final int batchesInFile = Math.min(BATCHES_PER_SEED_FILE, batchCount - seeded);
            final List<CommitBatchRequest> requests = new ArrayList<>(batchesInFile);
            for (int batch = 0; batch < batchesInFile; batch++) {
                requests.add(CommitBatchRequest.of(
                    batch,
                    partition,
                    batch * BATCH_BYTES,
                    BATCH_BYTES,
                    0,
                    0,
                    time.milliseconds(),
                    TimestampType.CREATE_TIME
                ));
            }

            new CommitFileJob(
                time,
                pgContainer.getJooqCtx(),
                "prune-seed-" + partition.topic() + "-" + seeded,
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                BROKER_ID,
                (long) batchesInFile * BATCH_BYTES,
                requests,
                false,
                ignored -> {}
            ).call();
        }
    }

    private LogsRecord singleLog(final TopicIdPartition partition) {
        return pgContainer.getJooqCtx().selectFrom(LOGS)
            .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
            .fetchOne();
    }

    private void setRemoteLogStartOffset(final TopicIdPartition partition, final long offset) {
        pgContainer.getJooqCtx().transaction(conf -> conf.dsl()
            .update(LOGS)
            .set(LOGS.REMOTE_LOG_START_OFFSET, offset)
            .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
            .execute());
    }

    private record ScenarioResult(double totalPruneMs, DurationStats passStats, DurationStats commitStats) {
    }

    private record DurationStats(int count, double p50Ms, double maxMs) {
        static DurationStats fromNanos(final List<Long> samples) {
            final List<Long> sorted = samples.stream().sorted().toList();
            return new DurationStats(
                sorted.size(),
                nanosToMillis(sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * 0.50) - 1))),
                nanosToMillis(sorted.get(sorted.size() - 1))
            );
        }
    }

    private static double nanosToMillis(final long nanos) {
        return nanos / 1_000_000.0;
    }
}
