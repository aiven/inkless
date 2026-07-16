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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.LOGS;

/**
 * Verifies maintenance of {@code logs.earliest_batch_timestamp} (the effective timestamp of the batch at
 * log_start) across every write path that changes the oldest batch: commit_file_v1/v2, delete_records_v1,
 * and prune_batches_below_highest_tiered_offset_v1. This is the invariant a future retention short-circuit
 * relies on: whenever the log is non-empty the column is either NULL ("unknown, must scan") or exactly the
 * effective timestamp of the current oldest batch.
 */
@Testcontainers
class LogsEarliestBatchTimestampTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456789;
    static final String TOPIC = "topic0";
    static final Uuid TOPIC_ID = new Uuid(10, 12);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);

    MockTime time = new MockTime(0, 0L, 0L);

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 1)), duration -> {}).run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void freshLogHasNullTimestamp() {
        assertThat(earliestTs(T0P0)).isNull();
    }

    @Test
    void firstCommitIntoEmptyLogSetsTimestampFromOldestBatch() {
        // One batch (5 records, offsets 0..4), CREATE_TIME -> effective ts is the batch max timestamp.
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    @Test
    void logAppendTimeUsesCommitClock() {
        time.setCurrentTimeMs(7777L);
        // LOG_APPEND_TIME -> effective ts is the commit clock (arg_now), not the request's max timestamp.
        commit("obj-lat", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.LOG_APPEND_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(7777L);
    }

    @Test
    void appendIntoNonEmptyLogLeavesTimestampUnchanged() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        // A later, separate commit appends a NEWER batch at the head; the oldest batch is unchanged.
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 9000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    @Test
    void deleteAdvancingLogStartRecomputesFromNewOldestBatch() {
        // Two separate files -> two rows: [0..4] ts 1000, [5..9] ts 2000.
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 2000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);

        // Delete up to offset 5 removes the first row; the second row becomes the oldest.
        delete(T0P0, 5L);
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
    }

    @Test
    void deleteThatDoesNotAdvanceLogStartLeavesTimestampUnchanged() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 2000L, TimestampType.CREATE_TIME));
        delete(T0P0, 5L);
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
        // Deleting to an offset at or below the current log_start is a no-op for the oldest batch.
        delete(T0P0, 5L);
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
    }

    @Test
    void deletingAllRecordsResetsTimestampToNull() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        delete(T0P0, -1L);  // -1 == high watermark, deletes everything
        assertThat(earliestTs(T0P0)).isNull();
    }

    @Test
    void commitBackfillsPreexistingNullOnNonEmptyLog() {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        // Simulate pre-migration state: a non-empty log whose column was never populated.
        setEarliestTsNull(T0P0);
        assertThat(earliestTs(T0P0)).isNull();

        // A subsequent commit must backfill from the actual OLDEST batch (ts 1000), not the new one (ts 9000).
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 9000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    @Test
    void pruneAdvancingLogStartRecomputesFromNewOldestBatch() throws Exception {
        // Two separate files -> two rows: [0..4] ts 1000, [5..9] ts 2000.
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        commit("obj-1", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 2000L, TimestampType.CREATE_TIME));
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);

        // Prune up to offset 4 removes the first row; the second row becomes the oldest.
        prune(T0P0, 4L);
        assertThat(earliestTs(T0P0)).isEqualTo(2000L);
    }

    @Test
    void pruneAllRecordsResetsTimestampToNull() throws Exception {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        prune(T0P0, 4L);  // last_offset 4 <= 4 -> deletes the only row
        assertThat(earliestTs(T0P0)).isNull();
    }

    @Test
    void pruneThatDeletesNothingLeavesTimestampUnchanged() throws Exception {
        commit("obj-0", CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000L, TimestampType.CREATE_TIME));
        prune(T0P0, 3L);  // no batch has last_offset <= 3 -> deletes nothing
        assertThat(earliestTs(T0P0)).isEqualTo(1000L);
    }

    private void commit(final String objectKey, final CommitBatchRequest request) {
        new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID, FILE_SIZE, List.of(request), true, duration -> {}).call();
    }

    private void delete(final TopicIdPartition partition, final long offset) {
        new DeleteRecordsJob(time, pgContainer.getJooqCtx(),
            List.of(new DeleteRecordsRequest(partition, offset)), duration -> {}).call();
    }

    private void prune(final TopicIdPartition partition, final long highestTieredOffset) throws Exception {
        new PruneDisklessLogsJob(time, pgContainer.getJooqCtx(),
            List.of(new PruneDisklessLogsRequest(partition, highestTieredOffset)), duration -> {}).call();
    }

    private Long earliestTs(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.select(LOGS.EARLIEST_BATCH_TIMESTAMP)
                .from(LOGS)
                .where(LOGS.TOPIC_ID.eq(partition.topicId()))
                .and(LOGS.PARTITION.eq(partition.partition()))
                .fetchOne(0, Long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setEarliestTsNull(final TopicIdPartition partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.update(LOGS)
                .setNull(LOGS.EARLIEST_BATCH_TIMESTAMP)
                .where(LOGS.TOPIC_ID.eq(partition.topicId()))
                .and(LOGS.PARTITION.eq(partition.partition()))
                .execute();
            connection.commit();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
