/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class DeleteRecordsJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final short FORMAT = ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT.id;
    static final short MAGIC = RecordBatch.CURRENT_MAGIC_VALUE;
    static final int BROKER_ID = 11;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final Uuid TOPIC_ID_2 = new Uuid(5555, 3333);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T2P0 = new TopicIdPartition(TOPIC_ID_2, 0, TOPIC_2);

    @Mock
    Time time;
    @Mock
    Consumer<Long> durationCallback;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_2, TOPIC_2, 1)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationCallback)
            .run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    /**
     * This tests randomizes the order in which requests are sent to the control plane by permuting them deterministically.
     */
    @ParameterizedTest
    @MethodSource("deleteRecordsFromMultipleTopicsParams")
    void deleteRecordsFromMultipleTopics(final List<Integer> order) {
        // Verify that order contains only unique elements.
        assert new HashSet<>(order).size() == order.size();
        
        final String objectKey1 = "obj1";
        final String objectKey2 = "obj2";
        final String objectKey3 = "obj3";

        final Instant filesCommittedAt = TimeUtils.now(time);

        // TOPIC_0, partition 0 - non-empty, partially truncated
        // TOPIC_0, partition 1 - non-empty, fully truncated
        // TOPIC_1 - empty, not truncated
        // TOPIC_2 - non-empty, not truncated

        // TOPIC_0, both partitions.
        final int file1Batch1Size = 1000;
        final int file1Batch2Size = 2000;
        final int file1Size = file1Batch1Size + file1Batch2Size;
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file1Size,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, file1Batch1Size, 0, 11, 1000,  TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P1, file1Batch1Size, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        // TOPIC_0, partition 0 and TOPIC_2, partition 0
        final int file2Batch1Size = 1000;
        final int file2Batch2Size = 2000;
        final int file2Size = file2Batch1Size + file2Batch2Size;
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file2Size,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, file2Batch1Size, 12, 23, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T2P0, file2Batch1Size, file2Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        // TOPIC_0, both partitions and TOPIC_2, partition 0
        final int file3Batch1Size = 1000;
        final int file3Batch2Size = 2000;
        final int file3Batch3Size = 3000;
        final int file3Size = file3Batch1Size + file3Batch2Size + file3Batch3Size;
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey3, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file3Size,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, file3Batch1Size, 24, 35, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P1, file3Batch1Size, file3Batch2Size, 12, 23, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T2P0, file3Batch1Size + file3Batch2Size, file3Batch3Size, 12, 23, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        time.sleep(1000);  // advance time
        final Instant topicsDeletedAt = TimeUtils.now(time);

        final List<DeleteRecordsRequest> requests = reorder(List.of(
            new DeleteRecordsRequest(T0P0, 18),
            new DeleteRecordsRequest(T0P1, 24),
            new DeleteRecordsRequest(T2P0, 0),
            new DeleteRecordsRequest(new TopicIdPartition(Uuid.ONE_UUID, 0, "nonexistent"), 0)
        ), order);
        final List<DeleteRecordsResponse> responses =
            new DeleteRecordsJob(time, pgContainer.getJooqCtx(), requests, duration -> {}).call();

        final List<DeleteRecordsResponse> expectedResponses = reorder(List.of(
            new DeleteRecordsResponse(Errors.NONE, 18),
            new DeleteRecordsResponse(Errors.NONE, 24),
            new DeleteRecordsResponse(Errors.NONE, 0),
            new DeleteRecordsResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1)
        ), order);
        assertThat(responses).containsExactlyElementsOf(expectedResponses);

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 18L, 36L, (long) file2Batch1Size + file3Batch1Size, null, null),
            new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 24L, 24L, 0L, null, null),
            new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 0L, 0L, null, null),
            new LogsRecord(TOPIC_ID_2, 0, TOPIC_2, 0L, 24L, (long) file2Batch2Size + file3Batch3Size, null, null)
        );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new BatchesRecord(3L, MAGIC, TOPIC_ID_0, 0, 12L, 23L, 2L, 0L, (long) file1Batch1Size, TimestampType.CREATE_TIME, 0L, 1000L),
            new BatchesRecord(5L, MAGIC, TOPIC_ID_0, 0, 24L, 35L, 3L, 0L, (long) file2Batch1Size, TimestampType.CREATE_TIME, 0L, 1000L),
            new BatchesRecord(4L, MAGIC, TOPIC_ID_2, 0, 0L, 11L, 2L, (long) file2Batch1Size, (long) file2Batch2Size, TimestampType.CREATE_TIME, 0L, 1000L),
            new BatchesRecord(7L, MAGIC, TOPIC_ID_2, 0, 12L, 23L, 3L, (long) file3Batch3Size, (long) file3Batch1Size + file3Batch2Size, TimestampType.CREATE_TIME, 0L, 1000L)
        );

        // File 1 must be `deleting` because it contained only data from the fully truncated TOPIC_1.
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new FilesRecord(1L, objectKey1, FORMAT, FileReason.PRODUCE, FileStateT.deleting, BROKER_ID, filesCommittedAt, topicsDeletedAt, (long) file1Size),
            new FilesRecord(2L, objectKey2, FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, null, (long) file2Size),  // not a single batch deleted from file 2
            new FilesRecord(3L, objectKey3, FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, null, (long) file3Size)
        );
    }

    private static Stream<Arguments> deleteRecordsFromMultipleTopicsParams() {
        return Stream.of(
            Arguments.of(List.of(0, 1, 2, 3)),
            Arguments.of(List.of(3, 2, 1, 0)),
            Arguments.of(List.of(3, 0, 1, 2))
        );
    }

    private <T> List<T> reorder(final List<T> original, final List<Integer> order) {
        assert order.size() == original.size();
        return order.stream().map(original::get).toList();
    }
}
