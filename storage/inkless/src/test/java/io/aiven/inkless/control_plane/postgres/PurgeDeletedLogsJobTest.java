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
import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.udt.PurgeDeletedLogsResponseV1;
import org.jooq.generated.udt.records.PurgeDeletedLogsResponseV1Record;
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
import io.aiven.inkless.control_plane.InitDisklessLogProducerState;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.PurgeDeletedLogsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.jooq.generated.Tables.LOGS;
import static org.jooq.generated.Tables.PURGE_DELETED_LOGS_V1;

@Testcontainers
class PurgeDeletedLogsJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);

    Time time = new MockTime();
    Consumer<Long> durationCallback = duration -> {};

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 1)),
            durationCallback
        ).run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void empty() {
        final PurgeDeletedLogsResponse response =
            new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call();
        assertThat(response).isEqualTo(PurgeDeletedLogsResponse.empty());
    }

    @Test
    void cappedThenUnbounded() {
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 100,
            List.of(CommitBatchRequest.of(0, T0P0, 0, 100, 0, 11, 1000, TimestampType.CREATE_TIME)),
            durationCallback
        ).call();
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 100,
            List.of(CommitBatchRequest.of(0, T0P0, 0, 100, 0, 11, 1000, TimestampType.CREATE_TIME)),
            durationCallback
        ).call();
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 100,
            List.of(CommitBatchRequest.of(0, T0P0, 0, 100, 0, 11, 1000, TimestampType.CREATE_TIME)),
            durationCallback
        ).call();

        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(TOPIC_ID_0), durationCallback).run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .allSatisfy(log -> assertThat(log.getDeletedAt()).isNotNull());
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(3);
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .allSatisfy(file -> assertThat(file.getState()).isEqualTo(FileStateT.uploaded));

        final PurgeDeletedLogsResponse first =
            new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 1, durationCallback).call();
        assertThat(first.batchesDeleted()).isEqualTo(1);
        assertThat(first.moreRemain()).isTrue();
        assertThat(first.capReached()).isTrue();
        assertThat(first.filesMarked()).isEqualTo(1);
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).hasSize(1);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(2);

        final PurgeDeletedLogsResponse rest =
            new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call();
        assertThat(rest.batchesDeleted()).isEqualTo(2);
        assertThat(rest.moreRemain()).isFalse();
        assertThat(rest.capReached()).isFalse();
        assertThat(rest.logsPurged()).isEqualTo(1);
        assertThat(rest.filesMarked()).isEqualTo(2);
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).isEmpty();
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void dropsEmptyDeletedLogWithoutSpendingBudget() {
        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(TOPIC_ID_0), durationCallback).run();

        final PurgeDeletedLogsResponse response =
            new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 1, durationCallback).call();
        assertThat(response.batchesDeleted()).isZero();
        assertThat(response.logsPurged()).isEqualTo(1);
        assertThat(response.filesMarked()).isZero();
        assertThat(response.moreRemain()).isFalse();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void purgeRemovesProducerState() {
        final Uuid keptTopicId = new Uuid(11, 13);
        final String keptTopic = "kept";
        final TopicIdPartition kept = new TopicIdPartition(keptTopicId, 0, keptTopic);
        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(keptTopicId, keptTopic, 1)),
            durationCallback
        ).run();
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "pid-deleted", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 100,
            List.of(CommitBatchRequest.idempotent(0, T0P0, 0, 100, 0, 9, 1000, TimestampType.CREATE_TIME,
                42L, (short) 1, 0, 9)),
            durationCallback
        ).call();
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "pid-kept", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 100,
            List.of(CommitBatchRequest.idempotent(0, kept, 0, 100, 0, 9, 1000, TimestampType.CREATE_TIME,
                99L, (short) 1, 0, 9)),
            durationCallback
        ).call();
        assertThat(DBUtils.getAllProducerState(pgContainer.getDataSource())).hasSize(2);

        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(TOPIC_ID_0), durationCallback).run();
        assertThat(DBUtils.getAllProducerState(pgContainer.getDataSource())).hasSize(2);

        new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call();
        assertThat(DBUtils.getAllProducerState(pgContainer.getDataSource()))
            .singleElement()
            .satisfies(row -> {
                assertThat(row.getTopicId()).isEqualTo(keptTopicId);
                assertThat(row.getProducerId()).isEqualTo(99L);
            });
    }

    @Test
    void purgeRemovesProducerStateFromEmptyLog() {
        new InitDisklessLogJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new InitDisklessLogRequest(TOPIC_ID_0, TOPIC_0, 0, 0, 100,
                List.of(new InitDisklessLogProducerState(42L, (short) 1, 5, 9, 99, 5000)))),
            durationCallback
        ).call();
        assertThat(DBUtils.getAllProducerState(pgContainer.getDataSource()))
            .singleElement()
            .satisfies(row -> assertThat(row.getProducerId()).isEqualTo(42L));
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();

        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(TOPIC_ID_0), durationCallback).run();
        assertThat(DBUtils.getAllProducerState(pgContainer.getDataSource())).isNotEmpty();

        new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call();
        assertThat(DBUtils.getAllProducerState(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void emptyLogDropsAreCapped() {
        final Uuid topicId = new Uuid(30, 30);
        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, "empty-many", 5)),
            durationCallback
        ).run();
        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(topicId), durationCallback).run();

        final PurgeDeletedLogsResponse first =
            new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 2, durationCallback).call();
        assertThat(first.logsPurged()).isEqualTo(2);
        assertThat(first.batchesDeleted()).isZero();
        assertThat(first.moreRemain()).isTrue();
        assertThat(first.capReached()).isTrue();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .filteredOn(log -> topicId.equals(log.getTopicId()))
            .hasSize(3);

        final PurgeDeletedLogsResponse rest =
            new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call();
        assertThat(rest.logsPurged()).isEqualTo(3);
        assertThat(rest.moreRemain()).isFalse();
        assertThat(rest.capReached()).isFalse();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .noneMatch(log -> topicId.equals(log.getTopicId()));
    }

    @Test
    void concurrentPurgeMarksSharedFile() throws Exception {
        final Uuid topicId = new Uuid(20, 21);
        final String topic = "shared";
        final TopicIdPartition p0 = new TopicIdPartition(topicId, 0, topic);
        final TopicIdPartition p1 = new TopicIdPartition(topicId, 1, topic);

        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topic, 2)),
            durationCallback
        ).run();
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "shared-obj", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 200,
            List.of(
                CommitBatchRequest.of(0, p0, 0, 100, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, p1, 100, 100, 0, 11, 1000, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();
        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(topicId), durationCallback).run();

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<PurgeDeletedLogsResponse> first = executor.submit(
                () -> new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call());
            final Future<PurgeDeletedLogsResponse> second = executor.submit(
                () -> new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call());
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .noneMatch(log -> topicId.equals(log.getTopicId()));
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .filteredOn(file -> "shared-obj".equals(file.getObjectKey()))
            .singleElement()
            .satisfies(file -> assertThat(file.getState()).isEqualTo(FileStateT.deleting));
    }

    /**
     * Two brokers can drain different partitions of one WAL file ({@code SKIP LOCKED} on logs).
     * Without locking the file before the emptiness check, both transactions see the other's
     * uncommitted batches, skip {@code mark_file_to_delete_v1}, and leave the object uploaded.
     *
     * <p>A third connection holds partition 1 so the first purge can only drain partition 0.
     * The second purge then blocks on the file lock until the first commits, and is the one
     * that marks the file.
     */
    @Test
    void interleavedPurgeMarksSharedFile() throws Exception {
        final Uuid topicId = new Uuid(20, 22);
        final String topic = "shared-interleaved";
        final TopicIdPartition p0 = new TopicIdPartition(topicId, 0, topic);
        final TopicIdPartition p1 = new TopicIdPartition(topicId, 1, topic);

        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(topicId, topic, 2)),
            durationCallback
        ).run();
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), "shared-interleaved-obj",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, 200,
            List.of(
                CommitBatchRequest.of(0, p0, 0, 100, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, p1, 100, 100, 0, 11, 1000, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();
        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(topicId), durationCallback).run();

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection holdConn = pgContainer.getDataSource().getConnection();
             Connection firstConn = pgContainer.getDataSource().getConnection()) {
            holdConn.setAutoCommit(false);
            firstConn.setAutoCommit(false);
            final DSLContext holdCtx = DSL.using(holdConn, SQLDialect.POSTGRES);
            final DSLContext firstCtx = DSL.using(firstConn, SQLDialect.POSTGRES);

            holdCtx.selectFrom(LOGS)
                .where(LOGS.TOPIC_ID.eq(topicId).and(LOGS.PARTITION.eq(1)))
                .forUpdate()
                .fetch();

            final PurgeDeletedLogsResponse first = purgeOn(firstCtx);
            assertThat(first.batchesDeleted()).isEqualTo(1);
            assertThat(first.logsPurged()).isEqualTo(1);
            assertThat(first.filesMarked()).isZero();
            assertThat(first.moreRemain()).isTrue();

            holdConn.commit();

            final int firstPid = firstCtx.resultQuery("SELECT pg_backend_pid()").fetchOne(0, Integer.class);
            final Future<PurgeDeletedLogsResponse> second = executor.submit(
                () -> new PurgeDeletedLogsJob(time, pgContainer.getJooqCtx(), 0, durationCallback).call());

            await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(20, TimeUnit.MILLISECONDS)
                .until(() -> isBlockedBy(firstPid));

            firstConn.commit();
            final PurgeDeletedLogsResponse secondResponse = second.get(30, TimeUnit.SECONDS);
            assertThat(secondResponse.batchesDeleted()).isEqualTo(1);
            assertThat(secondResponse.logsPurged()).isEqualTo(1);
            assertThat(secondResponse.filesMarked()).isEqualTo(1);
            assertThat(secondResponse.moreRemain()).isFalse();
        } finally {
            executor.shutdownNow();
        }

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .noneMatch(log -> topicId.equals(log.getTopicId()));
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .filteredOn(file -> "shared-interleaved-obj".equals(file.getObjectKey()))
            .singleElement()
            .satisfies(file -> assertThat(file.getState()).isEqualTo(FileStateT.deleting));
    }

    private PurgeDeletedLogsResponse purgeOn(final DSLContext ctx) {
        final List<PurgeDeletedLogsResponseV1Record> rows = ctx.select(
            PurgeDeletedLogsResponseV1.BATCHES_DELETED,
            PurgeDeletedLogsResponseV1.LOGS_PURGED,
            PurgeDeletedLogsResponseV1.FILES_MARKED,
            PurgeDeletedLogsResponseV1.MORE_REMAIN,
            PurgeDeletedLogsResponseV1.CAP_REACHED
        ).from(PURGE_DELETED_LOGS_V1.call(TimeUtils.now(time), 0))
            .fetchInto(PurgeDeletedLogsResponseV1Record.class);
        assertThat(rows).hasSize(1);
        final PurgeDeletedLogsResponseV1Record record = rows.get(0);
        return new PurgeDeletedLogsResponse(
            record.getBatchesDeleted(),
            record.getLogsPurged(),
            record.getFilesMarked(),
            Boolean.TRUE.equals(record.getMoreRemain()),
            Boolean.TRUE.equals(record.getCapReached())
        );
    }

    /**
     * True once some backend is waiting on a lock held by {@code holderPid}.
     * Scoping to that pid avoids reacting to unrelated lock waits.
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
}
