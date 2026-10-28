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

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsError;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.LOGS;

@Testcontainers
class PruneDisklessLogsJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String TOPIC = "prune-lock-topic";
    static final Uuid TOPIC_ID = new Uuid(10, 12);
    static final int BROKER_ID = 11;
    static final int BATCH_BYTES = 123;

    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID, 1, TOPIC);

    private final MockTime time = new MockTime();
    private final Consumer<Long> durationCallback = ignored -> {};
    private int fileSequence;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 2)),
            durationCallback
        ).run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void skipsTheLogLockForACompletedPrune() throws Exception {
        initializeEmptySwitchedLog(T0P0, 10L, 100L);
        assertThat(prune(List.of(new PruneDisklessLogsRequest(T0P0, 99L)), 0))
            .singleElement()
            .satisfies(response -> assertSuccess(response, 100L, 0, 0L));

        final List<PruneDisklessLogsResponse> responses = runPruneWhileHoldingLogLockWithoutBlocking(
            new PruneDisklessLogsRequest(T0P0, 99L),
            0
        );

        assertThat(responses).singleElement().satisfies(response ->
            assertSuccess(response, 100L, 0, 0L));
    }

    @Test
    void skipsTheLogLockWhenTheOldestBatchStraddlesTheTieredOffset() throws Exception {
        // seedBatches gives each batch 10 offsets (base 0, last 9 for the first), so a tiered offset
        // strictly between them simulates a multi-record or coalesced batch only partially tiered:
        // `log_start_offset` (0) <= `highest_tiered_offset` (5), yet no batch is actually deletable.
        seedBatches(T0P0, 1);
        setRemoteLogStartOffset(T0P0, 0L);

        final List<PruneDisklessLogsResponse> responses = runPruneWhileHoldingLogLockWithoutBlocking(
            new PruneDisklessLogsRequest(T0P0, 5L),
            0
        );

        assertThat(responses).singleElement().satisfies(response ->
            assertSuccess(response, 0L, 0, 0L));
        assertThat(batchCount(T0P0)).isEqualTo(1L);
    }

    @Test
    void takesTheLogLockToBootstrapTheCrossTierStart() throws Exception {
        seedBatches(T0P0, 1);

        final List<PruneDisklessLogsResponse> responses = runPruneWhileHoldingLogLock(
            new PruneDisklessLogsRequest(T0P0, 0L),
            0
        );

        assertThat(responses).singleElement().satisfies(response ->
            assertSuccess(response, 0L, 0, 0L));
        assertThat(singleLog(T0P0).getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(batchCount(T0P0)).isEqualTo(1L);
    }

    @Test
    void takesTheLogLockToDeleteBatches() throws Exception {
        seedBatches(T0P0, 2);
        setRemoteLogStartOffset(T0P0, 0L);

        final List<PruneDisklessLogsResponse> responses = runPruneWhileHoldingLogLock(
            new PruneDisklessLogsRequest(T0P0, 9L),
            0
        );

        assertThat(responses).singleElement().satisfies(response ->
            assertSuccess(response, 10L, 1, BATCH_BYTES));
        assertThat(singleLog(T0P0).getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(batchCount(T0P0)).isEqualTo(1L);
    }

    @Test
    void takesTheLogLockToBootstrapAnEmptySwitchedLog() throws Exception {
        initializeEmptySwitchedLog(T0P0, 10L, 100L);

        final List<PruneDisklessLogsResponse> responses = runPruneWhileHoldingLogLock(
            new PruneDisklessLogsRequest(T0P0, 99L),
            0
        );

        assertThat(responses).singleElement().satisfies(response ->
            assertSuccess(response, 100L, 0, 0L));
        assertThat(singleLog(T0P0).getLogStartOffset()).isEqualTo(100L);
        assertThat(singleLog(T0P0).getRemoteLogStartOffset()).isEqualTo(10L);
    }

    @Test
    void releasesEachPartitionLockBeforeTakingTheNextOne() throws Exception {
        seedBatches(T0P0, 1);
        seedBatches(T0P1, 1);
        setRemoteLogStartOffset(T0P0, 0L);
        setRemoteLogStartOffset(T0P1, 0L);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection lockConnection = pgContainer.getDataSource().getConnection()) {
            lockConnection.setAutoCommit(false);
            final DSLContext lockContext = DSL.using(lockConnection, SQLDialect.POSTGRES);
            final int holderPid = holdLogLock(lockContext, T0P1);
            final Future<List<PruneDisklessLogsResponse>> pruneResult = executor.submit(() -> prune(
                List.of(
                    new PruneDisklessLogsRequest(T0P0, 9L),
                    new PruneDisklessLogsRequest(T0P1, 9L)
                ),
                0
            ));

            await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(20, TimeUnit.MILLISECONDS)
                .until(() -> pruneResult.isDone() || isBlockedBy(holderPid));
            assertThat(pruneResult.isDone()).isFalse();

            assertThat(tryLockNowait(T0P0)).isTrue();

            lockConnection.commit();
            final List<PruneDisklessLogsResponse> responses = pruneResult.get(30, TimeUnit.SECONDS);
            assertThat(responses).hasSize(2);
            assertSuccess(responses.get(0), 10L, 1, BATCH_BYTES);
            assertSuccess(responses.get(1), 10L, 1, BATCH_BYTES);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void retriesOnlyThePartitionThatHitsADeadlock() throws Exception {
        seedBatches(T0P0, 2);
        seedBatches(T0P1, 2);
        setRemoteLogStartOffset(T0P0, 0L);
        setRemoteLogStartOffset(T0P1, 0L);
        raiseDeadlockOnFirstDelete(T0P1);

        final List<PruneDisklessLogsResponse> responses = prune(
            List.of(
                new PruneDisklessLogsRequest(T0P0, 19L),
                new PruneDisklessLogsRequest(T0P1, 19L)
            ),
            1
        );

        assertThat(responses).hasSize(2);
        assertSuccess(responses.get(0), 10L, 1, BATCH_BYTES);
        assertSuccess(responses.get(1), 10L, 1, BATCH_BYTES);
        assertThat(batchCount(T0P0)).isEqualTo(1L);
        assertThat(batchCount(T0P1)).isEqualTo(1L);
    }

    @Test
    void recordsOneDurationSamplePerPartition() throws Exception {
        seedBatches(T0P0, 1);
        seedBatches(T0P1, 1);
        final AtomicInteger samples = new AtomicInteger();

        new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(
                new PruneDisklessLogsRequest(T0P0, 9L),
                new PruneDisklessLogsRequest(T0P1, 9L),
                new PruneDisklessLogsRequest(new TopicIdPartition(TOPIC_ID, 1000, TOPIC), 9L)
            ),
            0,
            duration -> samples.incrementAndGet()
        ).call();

        assertThat(samples).hasValue(3);
    }

    @Test
    void reportsUnknownPartitionWhenTheLogIsDeletedWhileWaitingForTheLock() throws Exception {
        seedBatches(T0P0, 1);

        final List<PruneDisklessLogsResponse> responses = runPruneWhileHoldingLogLock(
            new PruneDisklessLogsRequest(T0P0, 9L),
            0,
            lockContext -> lockContext.update(LOGS)
                .set(LOGS.DELETED_AT, Instant.EPOCH)
                .where(LOGS.TOPIC_ID.eq(T0P0.topicId()).and(LOGS.PARTITION.eq(T0P0.partition())))
                .execute()
        );

        assertThat(responses).singleElement().satisfies(response ->
            assertThat(response.error()).isEqualTo(PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION));
        assertThat(batchCount(T0P0)).isEqualTo(1L);
    }

    @Test
    void keepsEarlierPartitionsCommittedWhenALaterPartitionFails() throws Exception {
        seedBatches(T0P0, 2);
        seedBatches(T0P1, 2);
        setRemoteLogStartOffset(T0P0, 0L);
        setRemoteLogStartOffset(T0P1, 0L);
        failDeletesOnPartition(T0P1);

        assertThatThrownBy(() -> prune(
            List.of(
                new PruneDisklessLogsRequest(T0P0, 9L),
                new PruneDisklessLogsRequest(T0P1, 9L)
            ),
            0
        )).hasStackTraceContaining("forced prune failure");

        assertThat(batchCount(T0P0)).isEqualTo(1L);
        assertThat(singleLog(T0P0).getLogStartOffset()).isEqualTo(10L);
        assertThat(batchCount(T0P1)).isEqualTo(2L);
        assertThat(singleLog(T0P1).getLogStartOffset()).isEqualTo(0L);
    }

    private List<PruneDisklessLogsResponse> runPruneWhileHoldingLogLock(
        final PruneDisklessLogsRequest request,
        final int maxBatchesPerRequest
    ) throws Exception {
        return runPruneWhileHoldingLogLock(request, maxBatchesPerRequest, lockContext -> { });
    }

    private List<PruneDisklessLogsResponse> runPruneWhileHoldingLogLock(
        final PruneDisklessLogsRequest request,
        final int maxBatchesPerRequest,
        final Consumer<DSLContext> beforeRelease
    ) throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection lockConnection = pgContainer.getDataSource().getConnection()) {
            lockConnection.setAutoCommit(false);
            final DSLContext lockContext = DSL.using(lockConnection, SQLDialect.POSTGRES);
            final int holderPid = holdLogLock(lockContext, request.topicIdPartition());
            final Future<List<PruneDisklessLogsResponse>> pruneResult = executor.submit(
                () -> prune(List.of(request), maxBatchesPerRequest)
            );

            await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(20, TimeUnit.MILLISECONDS)
                .until(() -> pruneResult.isDone() || isBlockedBy(holderPid));
            assertThat(pruneResult.isDone()).isFalse();

            beforeRelease.accept(lockContext);
            lockConnection.commit();
            return pruneResult.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private List<PruneDisklessLogsResponse> runPruneWhileHoldingLogLockWithoutBlocking(
        final PruneDisklessLogsRequest request,
        final int maxBatchesPerRequest
    ) throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection lockConnection = pgContainer.getDataSource().getConnection()) {
            lockConnection.setAutoCommit(false);
            final DSLContext lockContext = DSL.using(lockConnection, SQLDialect.POSTGRES);
            final int holderPid = holdLogLock(lockContext, request.topicIdPartition());
            final Future<List<PruneDisklessLogsResponse>> pruneResult = executor.submit(
                () -> prune(List.of(request), maxBatchesPerRequest)
            );

            await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(20, TimeUnit.MILLISECONDS)
                .until(() -> pruneResult.isDone() || isBlockedBy(holderPid));
            assertThat(pruneResult.isDone()).isTrue();
            assertThat(isBlockedBy(holderPid)).isFalse();

            final List<PruneDisklessLogsResponse> responses = pruneResult.get(30, TimeUnit.SECONDS);
            lockConnection.commit();
            return responses;
        } finally {
            executor.shutdownNow();
        }
    }

    private List<PruneDisklessLogsResponse> prune(
        final List<PruneDisklessLogsRequest> requests,
        final int maxBatchesPerRequest
    ) throws Exception {
        return new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            requests,
            maxBatchesPerRequest,
            durationCallback
        ).call();
    }

    private int holdLogLock(final DSLContext context, final TopicIdPartition partition) {
        context.selectFrom(LOGS)
            .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
            .forUpdate()
            .fetch();
        return context.resultQuery("SELECT pg_backend_pid()").fetchOne(0, Integer.class);
    }

    private boolean isBlockedBy(final int holderPid) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            final Integer blocked = context.resultQuery(
                    "SELECT count(*) FROM pg_stat_activity WHERE " + holderPid + " = ANY(pg_blocking_pids(pid))")
                .fetchOne(0, Integer.class);
            return blocked != null && blocked > 0;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean tryLockNowait(final TopicIdPartition partition) throws SQLException {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement(
                "SELECT 1 FROM logs WHERE topic_id = ? AND partition = ? FOR UPDATE NOWAIT"
            )) {
                statement.setObject(1, new java.util.UUID(
                    partition.topicId().getMostSignificantBits(),
                    partition.topicId().getLeastSignificantBits()
                ));
                statement.setInt(2, partition.partition());
                statement.executeQuery().close();
                connection.commit();
                return true;
            } catch (final SQLException e) {
                connection.rollback();
                if ("55P03".equals(e.getSQLState())) {
                    return false;
                }
                throw e;
            }
        }
    }

    private void initializeEmptySwitchedLog(
        final TopicIdPartition partition,
        final long logStartOffset,
        final long disklessStartOffset
    ) {
        new InitDisklessLogJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new InitDisklessLogRequest(
                partition.topicId(),
                partition.topic(),
                partition.partition(),
                logStartOffset,
                disklessStartOffset,
                List.of()
            )),
            durationCallback
        ).call();
    }

    private void seedBatches(final TopicIdPartition partition, final int batchCount) throws Exception {
        final List<CommitBatchRequest> requests = new ArrayList<>(batchCount);
        for (int batch = 0; batch < batchCount; batch++) {
            final long baseOffset = batch * 10L;
            requests.add(CommitBatchRequest.of(
                batch,
                partition,
                batch * BATCH_BYTES,
                BATCH_BYTES,
                baseOffset,
                baseOffset + 9L,
                time.milliseconds(),
                TimestampType.CREATE_TIME
            ));
        }

        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            "prune-lock-" + partition.partition() + "-" + fileSequence++,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            (long) batchCount * BATCH_BYTES,
            requests,
            false,
            durationCallback
        ).call();
    }

    private void setRemoteLogStartOffset(final TopicIdPartition partition, final long offset) {
        pgContainer.getJooqCtx().transaction(conf -> conf.dsl()
            .update(LOGS)
            .set(LOGS.REMOTE_LOG_START_OFFSET, offset)
            .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
            .execute());
    }

    private LogsRecord singleLog(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            return context.selectFrom(LOGS)
                .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
                .fetchOne();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long batchCount(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            return context.selectCount()
                .from(BATCHES)
                .where(BATCHES.TOPIC_ID.eq(partition.topicId()).and(BATCHES.PARTITION.eq(partition.partition())))
                .fetchOne(0, long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void raiseDeadlockOnFirstDelete(final TopicIdPartition partition) {
        final String topicId = new java.util.UUID(
            partition.topicId().getMostSignificantBits(),
            partition.topicId().getLeastSignificantBits()
        ).toString();
        pgContainer.getJooqCtx().transaction(conf -> {
            final DSLContext context = conf.dsl();
            context.execute("CREATE SEQUENCE prune_test_deadlock_attempts");
            context.execute("""
                CREATE OR REPLACE FUNCTION prune_test_force_deadlock() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF OLD.topic_id::uuid = '%s'::uuid
                        AND OLD.partition = %d
                        AND nextval('prune_test_deadlock_attempts') = 1
                    THEN
                        RAISE EXCEPTION 'forced prune retry' USING ERRCODE = '40P01';
                    END IF;
                    RETURN OLD;
                END;
                $$;
                """.formatted(topicId, partition.partition()));
            context.execute("""
                CREATE TRIGGER prune_test_force_deadlock_trigger
                BEFORE DELETE ON batches
                FOR EACH ROW
                EXECUTE FUNCTION prune_test_force_deadlock()
                """);
        });
    }

    private void failDeletesOnPartition(final TopicIdPartition partition) {
        final String topicId = new java.util.UUID(
            partition.topicId().getMostSignificantBits(),
            partition.topicId().getLeastSignificantBits()
        ).toString();
        pgContainer.getJooqCtx().transaction(conf -> {
            final DSLContext context = conf.dsl();
            context.execute("""
                CREATE OR REPLACE FUNCTION prune_test_fail_deletes() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF OLD.topic_id::uuid = '%s'::uuid AND OLD.partition = %d THEN
                        RAISE EXCEPTION 'forced prune failure';
                    END IF;
                    RETURN OLD;
                END;
                $$;
                """.formatted(topicId, partition.partition()));
            context.execute("""
                CREATE TRIGGER prune_test_fail_deletes_trigger
                BEFORE DELETE ON batches
                FOR EACH ROW
                EXECUTE FUNCTION prune_test_fail_deletes()
                """);
        });
    }

    private static void assertSuccess(
        final PruneDisklessLogsResponse response,
        final long logStartOffset,
        final int batchesDeleted,
        final long bytesDeleted
    ) {
        assertThat(response.error()).isEqualTo(PruneDisklessLogsError.NONE);
        assertThat(response.disklessLogStartOffset()).isEqualTo(logStartOffset);
        assertThat(response.batchesDeleted()).isEqualTo(batchesDeleted);
        assertThat(response.bytesDeleted()).isEqualTo(bytesDeleted);
    }
}
