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
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.PruneBatchesBelowHighestTieredOffsetErrorV1;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.generated.udt.PruneBatchesBelowHighestTieredOffsetResponseV2;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetRequestV1Record;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetResponseV2Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.jooq.generated.Tables.PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V2;
import static org.jooq.generated.tables.Logs.LOGS;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class PruneBatchesBelowHighestTieredOffsetV2Test {

    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 7;

    static final String TOPIC = "t-prune-tier";
    static final Uuid TOPIC_ID = new Uuid(0xDEADBEEFL, 0xCAFEBABEL);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);

    static final Uuid UNKNOWN_TOPIC_ID = new Uuid(0xBADCAFEL, 0xFEEDFACE);
    static final TopicIdPartition UNKNOWN_T0P0 = new TopicIdPartition(UNKNOWN_TOPIC_ID, 0, "never-created");

    @Mock
    Time time;
    @Mock
    Consumer<Long> durationCallback;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 1)),
            durationCallback
        ).run();
    }

    @Test
    void deletesAtMostTheConfiguredBatchCapAndReportsCounts() throws Exception {
        seedThreeBatches();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(100L, 2);

        assertThat(response.getBatchesDeleted()).isEqualTo(2);
        assertThat(response.getBytesDeleted()).isEqualTo(300L);
        assertThat(response.getLogStartOffset()).isEqualTo(20L);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(1);
    }

    @Test
    void deletesAllEligibleBatchesWhenTheCapDoesNotBindAndReportsCounts() throws Exception {
        seedThreeBatches();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(100L, 10);

        assertThat(response.getBatchesDeleted()).isEqualTo(3);
        assertThat(response.getBytesDeleted()).isEqualTo(600L);
        assertThat(response.getLogStartOffset()).isEqualTo(30L);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void usesTheUnboundedDefaultWhenTheCapArgumentIsOmitted() throws Exception {
        seedThreeBatches();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirectWithoutCap(100L);

        assertThat(response.getBatchesDeleted()).isEqualTo(3);
        assertThat(response.getBytesDeleted()).isEqualTo(600L);
        assertThat(response.getLogStartOffset()).isEqualTo(30L);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void reportsNoDeletionWhenRemoteLogStartIsAlreadySet() throws Exception {
        seedThreeBatches();
        pgContainer.getJooqCtx().transaction(conf -> conf.dsl().update(LOGS)
            .set(LOGS.REMOTE_LOG_START_OFFSET, 0L)
            .where(LOGS.TOPIC_ID.eq(TOPIC_ID).and(LOGS.PARTITION.eq(0)))
            .execute());

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(8L, 1);

        assertThat(response.getBatchesDeleted()).isEqualTo(0);
        assertThat(response.getBytesDeleted()).isEqualTo(0L);
        assertThat(response.getLogStartOffset()).isEqualTo(0L);
        assertThat(singleLog().getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(3);
    }

    @Test
    void bootstrapsRemoteLogStartWhenNoBatchIsFullyTiered() throws Exception {
        seedThreeBatches();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(8L, 1);

        assertThat(response.getBatchesDeleted()).isEqualTo(0);
        assertThat(response.getBytesDeleted()).isEqualTo(0L);
        assertThat(response.getLogStartOffset()).isEqualTo(0L);
        assertThat(singleLog().getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(3);
    }

    @Test
    void emptySwitchedLogAdvancesToTheSealAndBootstrapsRemoteLogStart() {
        new InitDisklessLogJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new InitDisklessLogRequest(TOPIC_ID, TOPIC, 0, 10L, 100L, List.of())),
            durationCallback
        ).call();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(99L, 0);

        assertThat(response.getBatchesDeleted()).isZero();
        assertThat(response.getBytesDeleted()).isZero();
        assertThat(response.getLogStartOffset()).isEqualTo(100L);
        assertThat(singleLog().getLogStartOffset()).isEqualTo(100L);
        assertThat(singleLog().getRemoteLogStartOffset()).isEqualTo(10L);
    }

    @Test
    void softDeletedLogReturnsUnknownTopicOrPartition() {
        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(TOPIC_ID), durationCallback).run();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 100L, 0);

        assertThat(response.getError()).isEqualTo(PruneBatchesBelowHighestTieredOffsetErrorV1.unknown_topic_or_partition);
        assertThat(response.getLogStartOffset()).isNull();
    }

    /**
     * A call with several partitions locks their rows in {@code logs} in {@code (topic_id, partition)}
     * order, so two such calls can't deadlock on each other.
     */
    @Test
    void processesRequestsInTopicAndPartitionOrder() throws Exception {
        final Uuid otherTopicId = new Uuid(0x1L, 0x2L);
        final TopicIdPartition otherP0 = new TopicIdPartition(otherTopicId, 0, "t-prune-order");
        final TopicIdPartition otherP1 = new TopicIdPartition(otherTopicId, 1, "t-prune-order");
        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            pgContainer.getJooqCtx(),
            Set.of(new CreateTopicAndPartitionsRequest(otherTopicId, "t-prune-order", 2)),
            durationCallback
        ).run();
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            "obj-prune-order",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            300,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, 100, 0L, 9L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(1, otherP0, 100, 100, 0L, 9L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(2, otherP1, 200, 100, 0L, 9L, 1000L, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();

        final List<PruneBatchesBelowHighestTieredOffsetResponseV2Record> responses = pruneDirect(
            List.of(
                new PruneBatchesBelowHighestTieredOffsetRequestV1Record(T0P0.topicId(), 0, 9L),
                new PruneBatchesBelowHighestTieredOffsetRequestV1Record(otherTopicId, 1, 9L),
                new PruneBatchesBelowHighestTieredOffsetRequestV1Record(otherTopicId, 0, 9L)
            ),
            0);

        assertThat(responses)
            .extracting(
                PruneBatchesBelowHighestTieredOffsetResponseV2Record::getTopicId,
                PruneBatchesBelowHighestTieredOffsetResponseV2Record::getPartition)
            .containsExactly(tuple(otherTopicId, 0), tuple(otherTopicId, 1), tuple(TOPIC_ID, 0));
        assertThat(responses).allSatisfy(r -> {
            assertThat(r.getError()).isEqualTo(PruneBatchesBelowHighestTieredOffsetErrorV1.none);
            assertThat(r.getBatchesDeleted()).isEqualTo(1);
            assertThat(r.getLogStartOffset()).isEqualTo(10L);
        });
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void doesNotDeleteBatchWhenHighestTieredIsStrictlyBelowBatchLastOffset() throws Exception {
        final String objectKey = "obj-prune-keep";
        final int batchBytes = 500;
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 100L, 1000L, TimestampType.CREATE_TIME)),
            durationCallback
        ).call();

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(1);

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 99L, 0);

        assertThat(response.getBatchesDeleted()).isZero();
        assertThat(response.getBytesDeleted()).isZero();
        assertThat(response.getLogStartOffset()).isZero();
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(1);
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()).iterator().next().getLastOffset()).isEqualTo(100L);
    }

    @Test
    void deletesBatchWhenLastOffsetEqualsHighestTieredAndUpdatesDisklessStartWhenNoBatchesRemain() throws Exception {
        final String objectKey = "obj-prune-all";
        final int batchBytes = 400;
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 10L, 1000L, TimestampType.CREATE_TIME)),
            durationCallback
        ).call();

        assertThat(singleLog().getLogStartOffset()).isEqualTo(0L);

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 10L, 0);

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();

        assertThat(response.getBatchesDeleted()).isEqualTo(1);
        assertThat(response.getBytesDeleted()).isEqualTo(batchBytes);
        assertThat(response.getLogStartOffset()).isEqualTo(11L);

        final LogsRecord after = singleLog();
        assertThat(after.getLogStartOffset()).isEqualTo(11L);
    }

    @Test
    void deletesOnlyFullyTieredBatchesAndSetsDisklessStartToMinBaseOfRemaining() throws Exception {
        final String objectKey = "obj-prune-partial";
        final int b1 = 300;
        final int b2 = 300;
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            b1 + b2,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, b1, 0L, 10L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P0, b1, b2, 11L, 25L, 1000L, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).hasSize(2);

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 10L, 0);

        final Set<BatchesRecord> remaining = DBUtils.getAllBatches(pgContainer.getDataSource());
        assertThat(remaining).hasSize(1);
        assertThat(remaining.iterator().next().getBaseOffset()).isEqualTo(11L);
        assertThat(remaining.iterator().next().getLastOffset()).isEqualTo(25L);

        assertThat(singleLog().getLogStartOffset()).isEqualTo(11L);
        assertThat(response.getBatchesDeleted()).isEqualTo(1);
        assertThat(response.getBytesDeleted()).isEqualTo(b1);
    }

    @Test
    void subtractsDeletedBatchBytesFromLogByteSize() throws Exception {
        final String objectKey = "obj-prune-bytes";
        final int b1 = 300;
        final int b2 = 500;
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            b1 + b2,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, b1, 0L, 10L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P0, b1, b2, 11L, 25L, 1000L, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();
        assertThat(singleLog().getByteSize()).isEqualTo(b1 + b2);

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 10L, 0);

        final LogsRecord after = singleLog();
        assertThat(after.getLogStartOffset()).isEqualTo(11L);
        assertThat(after.getByteSize()).isEqualTo(b2);
        assertThat(response.getBatchesDeleted()).isEqualTo(1);
        assertThat(response.getBytesDeleted()).isEqualTo(b1);
    }

    @Test
    void preservesLogStartWhenDeleteRecordsAdvancedIntoRemainingBatch() throws Exception {
        final String objectKey = "obj-prune-delete-records-partial";
        final int b1 = 300;
        final int b2 = 300;
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            b1 + b2,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, b1, 0L, 10L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P0, b1, b2, 11L, 25L, 1000L, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();
        new DeleteRecordsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new DeleteRecordsRequest(T0P0, 5L)),
            durationCallback
        ).call();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 9L, 0);

        assertThat(response.getBatchesDeleted()).isZero();
        assertThat(response.getBytesDeleted()).isZero();
        assertThat(response.getLogStartOffset()).isEqualTo(5L);
        assertThat(singleLog().getLogStartOffset()).isEqualTo(5L);
    }

    @Test
    void whenNoBatchesRemainDisklessStartIsMinOfHighWatermarkAndGreaterTieredFrontier() throws Exception {
        final String objectKey = "obj-prune-cap-hw";
        final int batchBytes = 400;
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            objectKey,
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            batchBytes,
            List.of(CommitBatchRequest.of(0, T0P0, 0, batchBytes, 0L, 10L, 1000L, TimestampType.CREATE_TIME)),
            durationCallback
        ).call();

        final LogsRecord beforePrune = singleLog();
        final long highWatermark = beforePrune.getHighWatermark();
        assertThat(highWatermark).isEqualTo(11L);

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(T0P0, 100L, 0);

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();

        assertThat(response.getBatchesDeleted()).isEqualTo(1);
        assertThat(response.getBytesDeleted()).isEqualTo(batchBytes);
        assertThat(response.getLogStartOffset()).isEqualTo(highWatermark);

        final LogsRecord after = singleLog();
        assertThat(after.getLogStartOffset()).isEqualTo(highWatermark);
        assertThat(after.getLogStartOffset()).isLessThan(101L);
    }

    @Test
    void whenNoLogsRowReturnsUnknownTopicOrPartitionAndNullOffset() throws Exception {
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()).stream()
            .noneMatch(r -> r.getTopicId().equals(UNKNOWN_TOPIC_ID))).isTrue();

        final PruneBatchesBelowHighestTieredOffsetResponseV2Record response = pruneDirect(UNKNOWN_T0P0, 0L, 0);

        assertThat(response.getTopicId()).isEqualTo(UNKNOWN_TOPIC_ID);
        assertThat(response.getError()).isEqualTo(PruneBatchesBelowHighestTieredOffsetErrorV1.unknown_topic_or_partition);
        assertThat(response.getLogStartOffset()).isNull();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()).stream()
            .noneMatch(r -> r.getTopicId().equals(UNKNOWN_TOPIC_ID))).isTrue();
    }

    private LogsRecord singleLog() {
        return DBUtils.getAllLogs(pgContainer.getDataSource()).stream()
            .filter(r -> r.getTopicId().equals(TOPIC_ID) && r.getPartition() == 0)
            .collect(Collectors.toSet())
            .iterator()
            .next();
    }

    private void seedThreeBatches() throws Exception {
        new CommitFileJob(
            time,
            pgContainer.getJooqCtx(),
            "obj-prune-bounded",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            BROKER_ID,
            600,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, 100, 0L, 9L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(1, T0P0, 100, 200, 10L, 19L, 1000L, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(2, T0P0, 300, 300, 20L, 29L, 1000L, TimestampType.CREATE_TIME)
            ),
            durationCallback
        ).call();
    }

    private PruneBatchesBelowHighestTieredOffsetResponseV2Record pruneDirect(final long highestTieredOffset,
                                                                           final int maxBatchesPerRequest) {
        return pruneDirect(T0P0, highestTieredOffset, maxBatchesPerRequest);
    }

    private PruneBatchesBelowHighestTieredOffsetResponseV2Record pruneDirect(final TopicIdPartition partition,
                                                                           final long highestTieredOffset,
                                                                           final int maxBatchesPerRequest) {
        final List<PruneBatchesBelowHighestTieredOffsetResponseV2Record> responses = pruneDirect(
            List.of(new PruneBatchesBelowHighestTieredOffsetRequestV1Record(
                partition.topicId(), partition.partition(), highestTieredOffset)),
            maxBatchesPerRequest);
        assertThat(responses).hasSize(1);
        return responses.get(0);
    }

    private List<PruneBatchesBelowHighestTieredOffsetResponseV2Record> pruneDirect(
        final List<PruneBatchesBelowHighestTieredOffsetRequestV1Record> requests,
        final int maxBatchesPerRequest
    ) {
        final PruneBatchesBelowHighestTieredOffsetRequestV1Record[] requestArray =
            requests.toArray(PruneBatchesBelowHighestTieredOffsetRequestV1Record[]::new);
        return pgContainer.getJooqCtx().transactionResult(conf -> conf.dsl().select(
                PruneBatchesBelowHighestTieredOffsetResponseV2.TOPIC_ID,
                PruneBatchesBelowHighestTieredOffsetResponseV2.PARTITION,
                PruneBatchesBelowHighestTieredOffsetResponseV2.LOG_START_OFFSET,
                PruneBatchesBelowHighestTieredOffsetResponseV2.ERROR,
                PruneBatchesBelowHighestTieredOffsetResponseV2.BATCHES_DELETED,
                PruneBatchesBelowHighestTieredOffsetResponseV2.BYTES_DELETED
            ).from(PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V2.call(Instant.EPOCH, requestArray, maxBatchesPerRequest))
            .fetchInto(PruneBatchesBelowHighestTieredOffsetResponseV2Record.class));
    }

    private PruneBatchesBelowHighestTieredOffsetResponseV2Record pruneDirectWithoutCap(final long highestTieredOffset) {
        return pgContainer.getJooqCtx().transactionResult(conf -> conf.dsl().fetch(
            """
                SELECT *
                FROM prune_batches_below_highest_tiered_offset_v2(
                    CAST({0} AS TIMESTAMP WITH TIME ZONE),
                    ARRAY[
                        ROW(
                            CAST({1} AS UUID),
                            CAST({2} AS INTEGER),
                            CAST({3} AS BIGINT)
                        )::prune_batches_below_highest_tiered_offset_request_v1
                    ]
                )
                """,
            Instant.EPOCH,
            new java.util.UUID(TOPIC_ID.getMostSignificantBits(), TOPIC_ID.getLeastSignificantBits()).toString(),
            0,
            highestTieredOffset
        ).into(PruneBatchesBelowHighestTieredOffsetResponseV2Record.class).get(0));
    }
}
