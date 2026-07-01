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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;
import static org.jooq.generated.Tables.LOGS;
import static org.jooq.generated.Tables.PRODUCER_STATE;

/**
 * Verifies that commit_file_v2 (batch coalescing) is response-equivalent to commit_file_v1 while writing
 * fewer rows to the batches table, and that idempotency/EOS state is unaffected.
 */
@Testcontainers
class CommitFileJobCoalescingTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final short MAGIC = RecordBatch.CURRENT_MAGIC_VALUE;
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456789;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    // Fixed, controllable clock starting at 0 so the time-based retention tests can set `now` explicitly
    // (the no-arg MockTime starts at real wall-clock time, which can't be moved backwards).
    MockTime time = new MockTime(0, 0L, 0L);

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, duration -> {})
            .run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    private List<CommitBatchResponse> commit(final String objectKey, final boolean coalesce, final List<CommitBatchRequest> requests) {
        return new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID, FILE_SIZE, requests, coalesce, duration -> {}
        ).call();
    }

    @Test
    void coalescesContiguousSamePartitionRunAndMatchesV1Responses() {
        // 5 contiguous non-idempotent batches on the same partition, each 10 bytes / 5 records.
        final List<CommitBatchRequest> v1Requests = List.of(
            CommitBatchRequest.of(0, T0P1, 0, 10, 0, 4, 100, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P1, 10, 10, 0, 4, 200, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P1, 20, 10, 0, 4, 300, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P1, 30, 10, 0, 4, 400, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P1, 40, 10, 0, 4, 500, TimestampType.CREATE_TIME)
        );
        final List<CommitBatchResponse> v1Responses = commit("v1obj", false, v1Requests);

        // Same requests, committed with coalescing into a different object.
        final List<CommitBatchRequest> v2Requests = List.of(
            CommitBatchRequest.of(0, T0P0, 0, 10, 0, 4, 100, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 10, 10, 0, 4, 200, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 20, 10, 0, 4, 300, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 30, 10, 0, 4, 400, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 40, 10, 0, 4, 500, TimestampType.CREATE_TIME)
        );
        final List<CommitBatchResponse> v2Responses = commit("v2obj", true, v2Requests);

        // Per-request assigned offsets must be identical (0,5,10,15,20) for both versions.
        assertThat(v2Responses.stream().map(CommitBatchResponse::assignedBaseOffset).toList())
            .isEqualTo(v1Responses.stream().map(CommitBatchResponse::assignedBaseOffset).toList())
            .containsExactly(0L, 5L, 10L, 15L, 20L);
        assertThat(v2Responses).allSatisfy(r ->
            assertThat(r.errors()).isEqualTo(Errors.NONE));

        // v1 wrote 5 rows for its partition; v2 wrote exactly 1 coalesced row for its partition.
        final Set<BatchesRecord> allBatches = DBUtils.getAllBatches(pgContainer.getDataSource());
        final List<BatchesRecord> v1PartitionBatches = allBatches.stream()
            .filter(b -> b.getTopicId().equals(TOPIC_ID_0) && b.getPartition() == 1)
            .toList();
        final List<BatchesRecord> v2PartitionBatches = allBatches.stream()
            .filter(b -> b.getTopicId().equals(TOPIC_ID_0) && b.getPartition() == 0)
            .toList();
        assertThat(v1PartitionBatches).hasSize(5);
        assertThat(v2PartitionBatches).hasSize(1);

        // The single coalesced row spans the whole run: base 0..last 24, byte 0..50, max ts 500.
        final BatchesRecord coalesced = v2PartitionBatches.get(0);
        assertThat(coalesced.getBaseOffset()).isEqualTo(0L);
        assertThat(coalesced.getLastOffset()).isEqualTo(24L);
        assertThat(coalesced.getByteOffset()).isEqualTo(0L);
        assertThat(coalesced.getByteSize()).isEqualTo(50L);
        assertThat(coalesced.getBatchMaxTimestamp()).isEqualTo(500L);
        assertThat(coalesced.getMagic()).isEqualTo(MAGIC);

        // Both partitions reached the same high watermark (25) and byte size (50).
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .filteredOn(l -> l.getTopicId().equals(TOPIC_ID_0))
            .allSatisfy(l -> {
                assertThat(l.getHighWatermark()).isEqualTo(25L);
                assertThat(l.getByteSize()).isEqualTo(50L);
            });
    }

    @Test
    void breaksRunOnPartitionChange() {
        // t0p0, t0p0 | t0p1 | t0p0 -> three runs because the partition changes and the last t0p0 is not
        // byte-adjacent to the first run (the t0p1 bytes sit between them).
        final List<CommitBatchResponse> responses = commit("obj", true, List.of(
            CommitBatchRequest.of(0, T0P0, 0, 10, 0, 4, 100, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 10, 10, 0, 4, 100, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P1, 20, 10, 0, 4, 100, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 30, 10, 0, 4, 100, TimestampType.CREATE_TIME)
        ));
        assertThat(responses).allSatisfy(r ->
            assertThat(r.errors()).isEqualTo(Errors.NONE));

        final List<BatchesRecord> batches = DBUtils.getAllBatches(pgContainer.getDataSource()).stream()
            .sorted(Comparator.comparing(BatchesRecord::getByteOffset))
            .toList();
        assertThat(batches).hasSize(3);
        // First t0p0 run: 2 batches coalesced -> base 0..9, bytes 0..20
        assertThat(batches.get(0).getPartition()).isEqualTo(0);
        assertThat(batches.get(0).getBaseOffset()).isEqualTo(0L);
        assertThat(batches.get(0).getLastOffset()).isEqualTo(9L);
        assertThat(batches.get(0).getByteSize()).isEqualTo(20L);
        // t0p1 run: base 0..4, bytes 20..30
        assertThat(batches.get(1).getPartition()).isEqualTo(1);
        assertThat(batches.get(1).getBaseOffset()).isEqualTo(0L);
        // Second t0p0 run: base 10..14, bytes 30..40 (continues t0p0's offset space)
        assertThat(batches.get(2).getPartition()).isEqualTo(0);
        assertThat(batches.get(2).getBaseOffset()).isEqualTo(10L);
        assertThat(batches.get(2).getLastOffset()).isEqualTo(14L);
    }

    @Test
    void coalescesAcrossDifferentProducersAndKeepsPerProducerState() {
        // Three distinct idempotent producers, same partition, byte-contiguous -> ONE batch row, but three
        // producer_state rows (idempotency tracked per physical batch).
        final List<CommitBatchResponse> responses = commit("obj", true, List.of(
            CommitBatchRequest.idempotent(0, T1P0, 0, 10, 0, 4, 111, TimestampType.CREATE_TIME, 100L, (short) 0, 0, 4),
            CommitBatchRequest.idempotent(0, T1P0, 10, 10, 0, 4, 222, TimestampType.CREATE_TIME, 200L, (short) 0, 0, 4),
            CommitBatchRequest.idempotent(0, T1P0, 20, 10, 0, 4, 333, TimestampType.CREATE_TIME, 300L, (short) 0, 0, 4)
        ));
        assertThat(responses.stream().map(CommitBatchResponse::assignedBaseOffset).toList())
            .containsExactly(0L, 5L, 10L);

        final List<BatchesRecord> batches = DBUtils.getAllBatches(pgContainer.getDataSource()).stream()
            .filter(b -> b.getTopicId().equals(TOPIC_ID_1))
            .toList();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).getBaseOffset()).isEqualTo(0L);
        assertThat(batches.get(0).getLastOffset()).isEqualTo(14L);
        assertThat(batches.get(0).getByteSize()).isEqualTo(30L);

        assertThat(producerStateCount(TOPIC_ID_1, 0)).isEqualTo(3L);
    }

    @Test
    void duplicateBatchBreaksRunAndIsNotWritten() {
        // Seed producer 7: seq [0..4],[5..9].
        commit("seed", true, List.of(
            CommitBatchRequest.idempotent(0, T1P0, 0, 10, 0, 4, 100, TimestampType.CREATE_TIME, 7L, (short) 3, 0, 4),
            CommitBatchRequest.idempotent(0, T1P0, 10, 10, 0, 4, 100, TimestampType.CREATE_TIME, 7L, (short) 3, 5, 9)
        ));
        // Replay a duplicate [0..4] followed by a fresh [10..14]; the duplicate's bytes break adjacency.
        final List<CommitBatchResponse> responses = commit("replay", true, List.of(
            CommitBatchRequest.idempotent(0, T1P0, 0, 10, 0, 4, 100, TimestampType.CREATE_TIME, 7L, (short) 3, 0, 4),
            CommitBatchRequest.idempotent(0, T1P0, 10, 10, 0, 4, 100, TimestampType.CREATE_TIME, 7L, (short) 3, 10, 14)
        ));
        // The seed advanced HW to 10. The duplicate returns its original offset (0) and does NOT advance
        // HW, so the fresh batch [10..14] is assigned offset 10.
        assertThat(responses.get(0).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(0).isDuplicate()).isTrue();
        assertThat(responses.get(0).assignedBaseOffset()).isEqualTo(0L);
        assertThat(responses.get(1).errors()).isEqualTo(Errors.NONE);
        assertThat(responses.get(1).assignedBaseOffset()).isEqualTo(10L);

        // The "replay" file must contain exactly one row (the accepted [10..14]); the duplicate is not written.
        final List<BatchesRecord> replayBatches = batchesForObject("replay");
        assertThat(replayBatches).hasSize(1);
        assertThat(replayBatches.get(0).getBaseOffset()).isEqualTo(10L);
        assertThat(replayBatches.get(0).getLastOffset()).isEqualTo(14L);
        // The duplicate's bytes [0,10) are dead; the written row starts at the fresh batch's byte offset.
        assertThat(replayBatches.get(0).getByteOffset()).isEqualTo(10L);
    }

    @Test
    void coalescedRowNotDeletedWhenLogStartAdvancesIntoTheRun() {
        // Documents the retention granularity tradeoff: delete_records / prune operate on whole batches
        // rows by last_offset. A coalesced row spanning [0..14] must NOT be deleted when the log start
        // advances to an offset INSIDE the run (10) — only its last_offset (14) being below the new log
        // start would make it deletable. This guards against a coalesced row being dropped while some of
        // its offsets are still live (silent data loss).
        commit("obj-retention", true, List.of(
            CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 100, 100, 0, 4, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 200, 100, 0, 4, 1000, TimestampType.CREATE_TIME)
        ));
        // One coalesced row [0..14], byte 0..300.
        final List<BatchesRecord> before = batchesForObject("obj-retention");
        assertThat(before).hasSize(1);
        assertThat(before.get(0).getBaseOffset()).isEqualTo(0L);
        assertThat(before.get(0).getLastOffset()).isEqualTo(14L);

        // Advance the log start to offset 10 (mid-run).
        new DeleteRecordsJob(time, pgContainer.getJooqCtx(),
            List.of(new DeleteRecordsRequest(T0P0, 10L)), duration -> {}).call();

        // The coalesced row must survive (last_offset 14 >= new log start 10), still fetchable from base 0,
        // and the log start must have advanced to 10.
        final List<BatchesRecord> after = batchesForObject("obj-retention");
        assertThat(after).hasSize(1);
        assertThat(after.get(0).getBaseOffset()).isEqualTo(0L);
        assertThat(after.get(0).getLastOffset()).isEqualTo(14L);
        assertThat(logStartOffset(TOPIC_ID_0, 0)).isEqualTo(10L);
    }

    @Test
    void coalescedRowDeletedWhenLogStartAdvancesPastTheRun() {
        // The complementary case: once the log start advances past the run's last_offset, the whole
        // coalesced row is correctly removed.
        commit("obj-retention-full", true, List.of(
            CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 100, 100, 0, 4, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 200, 100, 0, 4, 1000, TimestampType.CREATE_TIME)
        ));
        assertThat(batchesForObject("obj-retention-full")).hasSize(1);

        // Advance the log start to 15 (== HW, past last_offset 14).
        new DeleteRecordsJob(time, pgContainer.getJooqCtx(),
            List.of(new DeleteRecordsRequest(T0P0, 15L)), duration -> {}).call();

        assertThat(batchesForObject("obj-retention-full")).isEmpty();
        assertThat(logStartOffset(TOPIC_ID_0, 0)).isEqualTo(15L);
    }

    @Test
    void timeRetentionKeepsOrDeletesCoalescedRowsWholeByTheirMaxTimestamp() throws Exception {
        // Time-based retention (enforce_retention) decides per batches-row using that row's effective
        // timestamp. A coalesced row carries one batch_max_timestamp = GREATEST(sub-batch timestamps), so
        // the whole row is kept or deleted as a unit. Two coalesced rows: an old one (ts=1000) and a newer
        // one (ts=5000), each a separate commit -> one row each.
        commit("obj-old", true, List.of(
            CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 100, 100, 0, 4, 1000, TimestampType.CREATE_TIME)
        ));
        commit("obj-new", true, List.of(
            CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 5000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, 100, 100, 0, 4, 5000, TimestampType.CREATE_TIME)
        ));
        // Two coalesced rows: old [0..9] (ts 1000), new [10..19] (ts 5000).
        assertThat(batchesForObject("obj-old")).hasSize(1);
        assertThat(batchesForObject("obj-new")).hasSize(1);

        // now=6000, retentionMs=2000 -> keep timestamps >= 4000. Old row (1000) is deleted, new row (5000) kept.
        time.setCurrentTimeMs(6000L);
        new EnforceRetentionJob(time, pgContainer.getJooqCtx(),
            List.of(new EnforceRetentionRequest(TOPIC_ID_0, 0, -1, 2000L)), 100, duration -> {}).call();

        // The old coalesced row is gone as a whole; the new one survives intact from its base offset.
        assertThat(batchesForObject("obj-old")).isEmpty();
        final List<BatchesRecord> kept = batchesForObject("obj-new");
        assertThat(kept).hasSize(1);
        assertThat(kept.get(0).getBaseOffset()).isEqualTo(10L);
        assertThat(kept.get(0).getLastOffset()).isEqualTo(19L);
        assertThat(logStartOffset(TOPIC_ID_0, 0)).isEqualTo(10L);
    }

    @Test
    void timeRetentionRetainsCoalescedRowWhenAnySubBatchIsWithinRetention() throws Exception {
        // A single coalesced row whose sub-batches span old and recent timestamps. The row's max timestamp
        // (GREATEST) is recent, so the entire row — including the old-timestamp sub-batch — is retained.
        // This is the safe over-retain property of coalescing: it never drops a row that has any live data.
        commit("obj-mixed", true, List.of(
            CommitBatchRequest.of(0, T0P0, 0, 100, 0, 4, 1000, TimestampType.CREATE_TIME),   // old sub-batch
            CommitBatchRequest.of(0, T0P0, 100, 100, 0, 4, 5000, TimestampType.CREATE_TIME)  // recent sub-batch
        ));
        // One coalesced row [0..9], batch_max_timestamp = GREATEST(1000, 5000) = 5000.
        assertThat(batchesForObject("obj-mixed")).hasSize(1);

        // now=6000, retentionMs=2000 -> keep >= 4000. The row's max ts (5000) qualifies, so it is retained
        // whole even though its first sub-batch (ts 1000) is older than the retention horizon.
        time.setCurrentTimeMs(6000L);
        new EnforceRetentionJob(time, pgContainer.getJooqCtx(),
            List.of(new EnforceRetentionRequest(TOPIC_ID_0, 0, -1, 2000L)), 100, duration -> {}).call();

        assertThat(batchesForObject("obj-mixed")).hasSize(1);
        assertThat(logStartOffset(TOPIC_ID_0, 0)).isEqualTo(0L);
    }

    private long logStartOffset(final Uuid topicId, final int partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.select(LOGS.LOG_START_OFFSET)
                .from(LOGS)
                .where(LOGS.TOPIC_ID.eq(topicId))
                .and(LOGS.PARTITION.eq(partition))
                .fetchOne(0, long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long producerStateCount(final Uuid topicId, final int partition) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            return ctx.selectCount()
                .from(PRODUCER_STATE)
                .where(PRODUCER_STATE.TOPIC_ID.eq(topicId))
                .and(PRODUCER_STATE.PARTITION.eq(partition))
                .fetchOne(0, long.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<BatchesRecord> batchesForObject(final String objectKey) {
        try (Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            final Long fileId = ctx.select(FILES.FILE_ID)
                .from(FILES)
                .where(FILES.OBJECT_KEY.eq(objectKey))
                .fetchOne(FILES.FILE_ID);
            if (fileId == null) {
                return List.of();
            }
            return ctx.selectFrom(BATCHES)
                .where(BATCHES.FILE_ID.eq(fileId))
                .orderBy(BATCHES.BYTE_OFFSET)
                .fetchInto(BatchesRecord.class);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
