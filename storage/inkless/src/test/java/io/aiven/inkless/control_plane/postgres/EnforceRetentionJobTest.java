/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import org.jooq.generated.udt.DeleteRecordsResponseV1;
import org.jooq.generated.udt.records.DeleteRecordsRequestV1Record;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.DELETE_RECORDS_V1;
import static org.jooq.generated.Tables.LOGS;

/**
 * The majority of testing is done in AbstractControlPlaneTest. Here we only test some behaviors specific to PG.
 */
@Testcontainers
class EnforceRetentionJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 1_000_000;
    // Three batches seeded into one partition: real offsets [0,9], [10,29], [30,59].
    static final int BATCH_1_SIZE = 123;
    static final int BATCH_2_SIZE = 456;
    static final int BATCH_3_SIZE = 789;
    static final long BATCH_1_RECORDS = 10;
    static final long BATCH_2_RECORDS = 20;
    static final long BATCH_3_RECORDS = 30;
    static final long HIGH_WATERMARK = BATCH_1_RECORDS + BATCH_2_RECORDS + BATCH_3_RECORDS;

    MockTime time = new MockTime();
    Consumer<Long> durationCallback = duration -> {};

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 100),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 100)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationCallback)
            .run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void forMultiplePartitionsInArbitraryOrder() throws Exception {
        final EnforceRetentionJob job = new EnforceRetentionJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(
                new EnforceRetentionRequest(TOPIC_ID_0, 99, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_1, 1, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_1, 0, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_0, 3, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_0, 1000, 1, 1),  // non-existent
                new EnforceRetentionRequest(TOPIC_ID_1, 5, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_1, 5, 1, 1)  // duplicate
            ),
            0,
            durationCallback);
        assertThat(job.call()).containsExactly(
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.unknownTopicOrPartition(),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0)
        );
    }

    /**
     * enforce_retention_v2 computes the retention boundary WITHOUT the log lock and only takes the lock for
     * the short delete. If the log is deleted between that unlocked scan and the locked delete, the locked
     * re-read must observe the deletion and return unknown_topic_or_partition rather than acting on the
     * boundary computed from the now-gone log.
     *
     * <p>Interleaving is forced with a second connection that holds {@code logs FOR UPDATE}: enforce finishes
     * its unlocked scan, blocks at the locked re-read, and only proceeds once the holder deletes the log and
     * commits.
     */
    @Test
    void logDeletedBetweenScanAndDelete() throws Exception {
        final TopicIdPartition partition = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
        seedThreeBatches(partition);

        final List<EnforceRetentionResponse> responses = runEnforceWhileHoldingLock(
            partition,
            ctxB -> {
                ctxB.deleteFrom(BATCHES)
                    .where(BATCHES.TOPIC_ID.eq(partition.topicId()).and(BATCHES.PARTITION.eq(partition.partition())))
                    .execute();
                ctxB.deleteFrom(LOGS)
                    .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
                    .execute();
            });

        assertThat(responses).containsExactly(EnforceRetentionResponse.unknownTopicOrPartition());
    }

    /**
     * If a concurrent delete advances log_start_offset past some of the batches the unlocked scan planned to
     * delete, the locked recount must reflect the post-advance state: only the batches that still exist below
     * the boundary are counted and deleted. Here the boundary is the high watermark (retentionBytes=0), the
     * concurrent delete removes the first batch, so enforce must report 2 batches deleted, not 3.
     */
    @Test
    void logStartAdvancedBetweenScanAndDelete() throws Exception {
        final TopicIdPartition partition = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
        seedThreeBatches(partition);

        final List<EnforceRetentionResponse> responses = runEnforceWhileHoldingLock(
            partition,
            ctxB -> {
                // Advance log_start to the start of the second batch via the real delete path: this removes
                // batch 1 (offsets [0,9]) and keeps byte_size / earliest_batch_timestamp consistent with the
                // remaining rows, unlike a raw DELETE that would leave those summary columns stale.
                ctxB.select(
                        DeleteRecordsResponseV1.TOPIC_ID,
                        DeleteRecordsResponseV1.PARTITION,
                        DeleteRecordsResponseV1.ERROR,
                        DeleteRecordsResponseV1.LOG_START_OFFSET)
                    .from(DELETE_RECORDS_V1.call(
                        TimeUtils.now(time),
                        new DeleteRecordsRequestV1Record[]{
                            new DeleteRecordsRequestV1Record(partition.topicId(), partition.partition(), BATCH_1_RECORDS)
                        }))
                    .fetch();
            });

        assertThat(responses).containsExactly(
            EnforceRetentionResponse.success(2, BATCH_2_SIZE + BATCH_3_SIZE, HIGH_WATERMARK));
        assertThat(logStartOffset(partition)).isEqualTo(HIGH_WATERMARK);
    }

    /**
     * When earliest_batch_timestamp is NULL (not yet populated: pre-existing log or lazy backfill), the
     * Phase-1 short-circuit must NOT fire - it cannot prove "nothing to delete" - so enforce falls back to
     * the full boundary scan and still deletes what the policy requires. Guards against a NULL wrongly being
     * treated as "within retention".
     */
    @Test
    void fallsBackToScanWhenEarliestTimestampUnknown() throws Exception {
        final TopicIdPartition partition = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
        final long seededAt = time.milliseconds();
        seedThreeBatches(partition);
        clearEarliestBatchTimestamp(partition);

        // Move now past the batch timestamps so time retention should delete everything.
        time.setCurrentTimeMs(seededAt + 1000);
        final EnforceRetentionJob job = new EnforceRetentionJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new EnforceRetentionRequest(partition.topicId(), partition.partition(), -1, 0)),
            0,
            durationCallback);

        assertThat(job.call()).containsExactly(
            EnforceRetentionResponse.success(3, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE, HIGH_WATERMARK));
        assertThat(logStartOffset(partition)).isEqualTo(HIGH_WATERMARK);
    }

    /**
     * Seeds one file with three batches into the partition. Real offsets become [0,9], [10,29], [30,59].
     */
    private void seedThreeBatches(final TopicIdPartition partition) {
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            "obj-" + partition.topicId() + "-" + partition.partition(),
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            FILE_SIZE,
            List.of(
                CommitBatchRequest.of(0, partition, 0, BATCH_1_SIZE, 0, BATCH_1_RECORDS - 1, time.milliseconds(), TimestampType.CREATE_TIME),
                CommitBatchRequest.of(1, partition, BATCH_1_SIZE, BATCH_2_SIZE, 0, BATCH_2_RECORDS - 1, time.milliseconds(), TimestampType.CREATE_TIME),
                CommitBatchRequest.of(2, partition, BATCH_1_SIZE + BATCH_2_SIZE, BATCH_3_SIZE, 0, BATCH_3_RECORDS - 1, time.milliseconds(), TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();
    }

    /**
     * Runs a delete-everything enforce (retentionBytes=0) on {@code partition} while a second connection holds
     * {@code logs FOR UPDATE}, so enforce blocks at its locked re-read. Once enforce is blocked, {@code mutation}
     * runs on that held connection and commits, releasing the lock; enforce then proceeds into its locked phase
     * against the mutated state. Returns enforce's responses.
     */
    private List<EnforceRetentionResponse> runEnforceWhileHoldingLock(final TopicIdPartition partition,
                                                                      final Consumer<DSLContext> mutation) throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection lockConn = pgContainer.getDataSource().getConnection()) {
            lockConn.setAutoCommit(false);
            final DSLContext ctxB = DSL.using(lockConn, SQLDialect.POSTGRES);

            // Hold the row lock so enforce cannot enter its locked delete phase.
            ctxB.selectFrom(LOGS)
                .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
                .forUpdate()
                .fetch();
            final int holderPid = ctxB.resultQuery("SELECT pg_backend_pid()").fetchOne(0, Integer.class);

            final Future<List<EnforceRetentionResponse>> enforceResult = executor.submit(() ->
                new EnforceRetentionJob(
                    time,
                    pgContainer.getJooqCtx(),
                    List.of(new EnforceRetentionRequest(partition.topicId(), partition.partition(), 0, -1)),
                    0,
                    durationCallback
                ).call());

            // Wait until enforce has finished its unlocked scan and is actually blocked on the locked
            // re-read (logs FOR UPDATE) held by lockConn, so the mutation below is guaranteed to interleave
            // between the unlocked scan and the locked delete. Deterministic and fast, unlike a fixed sleep.
            await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(20, TimeUnit.MILLISECONDS)
                .until(() -> isBlockedBy(holderPid));

            mutation.accept(ctxB);
            lockConn.commit();

            return enforceResult.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * True once some backend is blocked waiting on a lock held specifically by {@code holderPid} (lockConn)
     * -- i.e. enforce has reached its {@code logs FOR UPDATE} re-read and is queued behind the holder.
     * Scoping to the holder's pid (rather than any lock wait in the database) avoids reacting to unrelated
     * lock activity. Uses pg_blocking_pids so it catches the transactionid wait a row lock actually blocks on.
     */
    private boolean isBlockedBy(final int holderPid) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            final Integer blocked = ctx.resultQuery(
                    "SELECT count(*) FROM pg_stat_activity WHERE " + holderPid + " = ANY(pg_blocking_pids(pid))")
                .fetchOne(0, Integer.class);
            return blocked != null && blocked > 0;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long logStartOffset(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.select(LOGS.LOG_START_OFFSET)
                .from(LOGS)
                .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
                .fetchOne(0, long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearEarliestBatchTimestamp(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.update(LOGS)
                .set(LOGS.EARLIEST_BATCH_TIMESTAMP, (Long) null)
                .where(LOGS.TOPIC_ID.eq(partition.topicId()).and(LOGS.PARTITION.eq(partition.partition())))
                .execute();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
