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

import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.LogsRecord;
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

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsError;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class PruneBatchesBelowHighestTieredOffsetV1Test {

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

        final List<PruneDisklessLogsResponse> responses = new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new PruneDisklessLogsRequest(T0P0, 99L)),
            durationCallback
        ).call();

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(0L);
            });

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

        final List<PruneDisklessLogsResponse> responses = new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new PruneDisklessLogsRequest(T0P0, 10L)),
            durationCallback
        ).call();

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(11L);
            });

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

        new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new PruneDisklessLogsRequest(T0P0, 10L)),
            durationCallback
        ).call();

        final Set<BatchesRecord> remaining = DBUtils.getAllBatches(pgContainer.getDataSource());
        assertThat(remaining).hasSize(1);
        assertThat(remaining.iterator().next().getBaseOffset()).isEqualTo(11L);
        assertThat(remaining.iterator().next().getLastOffset()).isEqualTo(25L);

        assertThat(singleLog().getLogStartOffset()).isEqualTo(11L);
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

        final List<PruneDisklessLogsResponse> responses = new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new PruneDisklessLogsRequest(T0P0, 100L)),
            durationCallback
        ).call();

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.NONE);
                assertThat(r.disklessLogStartOffset()).isEqualTo(highWatermark);
            });

        final LogsRecord after = singleLog();
        assertThat(after.getLogStartOffset()).isEqualTo(highWatermark);
        assertThat(after.getLogStartOffset()).isLessThan(101L);
    }

    @Test
    void whenNoLogsRowReturnsUnknownTopicOrPartitionAndNullOffset() throws Exception {
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()).stream()
            .noneMatch(r -> r.getTopicId().equals(UNKNOWN_TOPIC_ID))).isTrue();

        final List<PruneDisklessLogsResponse> responses = new PruneDisklessLogsJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(new PruneDisklessLogsRequest(UNKNOWN_T0P0, 0L)),
            durationCallback
        ).call();

        assertThat(responses).singleElement()
            .satisfies(r -> {
                assertThat(r.topicIdPartition().topicId()).isEqualTo(UNKNOWN_TOPIC_ID);
                assertThat(r.error()).isEqualTo(PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION);
                assertThat(r.disklessLogStartOffset()).isNull();
            });

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
}
