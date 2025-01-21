// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.FilesToDeleteRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class DeleteRecordsJobTest extends SharedPostgreSQLTest {
    static final int BROKER_ID = 11;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final Uuid TOPIC_ID_2 = new Uuid(5555, 3333);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    static final TopicIdPartition T2P0 = new TopicIdPartition(TOPIC_ID_2, 0, TOPIC_2);

    @Mock
    Time time;
    @Mock
    Consumer<Long> durationCallback;

    @BeforeEach
    void createTopics() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_2, TOPIC_2, 1)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, durationCallback)
            .run();
    }

    @Test
    void deleteRecordsFromMultipleTopics() {
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
            time, jooqCtx, objectKey1, BROKER_ID, file1Size,
            List.of(
                CommitBatchRequest.of(T0P0, 0, file1Batch1Size, 0, 11, 1000,  TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T0P1, file1Batch1Size, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        // TOPIC_0, partition 0 and TOPIC_2, partition 0
        final int file2Batch1Size = 1000;
        final int file2Batch2Size = 2000;
        final int file2Size = file2Batch1Size + file2Batch2Size;
        new CommitFileJob(
            time, jooqCtx, objectKey2, BROKER_ID, file2Size,
            List.of(
                CommitBatchRequest.of(T0P0, 0, file2Batch1Size, 12, 23, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T2P0, file2Batch1Size, file2Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        // TOPIC_0, both partitions and TOPIC_2, partition 0
        final int file3Batch1Size = 1000;
        final int file3Batch2Size = 2000;
        final int file3Batch3Size = 3000;
        final int file3Size = file3Batch1Size + file3Batch2Size + file3Batch3Size;
        new CommitFileJob(
            time, jooqCtx, objectKey3, BROKER_ID, file3Size,
            List.of(
                CommitBatchRequest.of(T0P0, 0, file3Batch1Size, 24, 35, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T0P1, file3Batch1Size, file3Batch2Size, 12, 23, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T2P0, file3Batch1Size + file3Batch2Size, file3Batch3Size, 12, 23, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        time.sleep(1000);  // advance time
        final Instant topicsDeletedAt = TimeUtils.now(time);
        final Uuid nonexistentTopicId = Uuid.ONE_UUID;
        final List<DeleteRecordsResponse> responses = new DeleteRecordsJob(time, jooqCtx, List.of(
            new DeleteRecordsRequest(T0P0, 18),
            new DeleteRecordsRequest(T0P1, 24),
            new DeleteRecordsRequest(T2P0, 0),
            new DeleteRecordsRequest(new TopicIdPartition(nonexistentTopicId, 0, "nonexistent"), 0)
        )).call();

        assertThat(responses).containsExactly(
            new DeleteRecordsResponse(Errors.NONE, 18),
            new DeleteRecordsResponse(Errors.NONE, 24),
            new DeleteRecordsResponse(Errors.NONE, 0),
            new DeleteRecordsResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 18L, 36L),
            new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 24L, 24L),
            new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 0L),
            new LogsRecord(TOPIC_ID_2, 0, TOPIC_2, 0L, 24L)
        );

        assertThat(DBUtils.getAllBatches(hikariDataSource)).containsExactlyInAnyOrder(
            new BatchesRecord(3L, TOPIC_ID_0, 0, 12L, 23L, 12L, 23L, 2L, 0L, (long) file1Batch1Size, TimestampType.CREATE_TIME, 0L, 1000L),
            new BatchesRecord(5L, TOPIC_ID_0, 0, 24L, 35L, 24L, 35L, 3L, 0L, (long) file2Batch1Size, TimestampType.CREATE_TIME, 0L, 1000L),
            new BatchesRecord(4L, TOPIC_ID_2, 0, 0L, 11L, 0L, 11L, 2L, (long) file2Batch1Size, (long) file2Batch2Size, TimestampType.CREATE_TIME, 0L, 1000L),
            new BatchesRecord(7L, TOPIC_ID_2, 0, 12L, 23L, 12L, 23L, 3L, (long) file3Batch3Size, (long) file3Batch1Size + file3Batch2Size, TimestampType.CREATE_TIME, 0L, 1000L)
        );

        // File 1 must be `deleting` because it contained only data from the fully truncated TOPIC_1.
        assertThat(DBUtils.getAllFiles(hikariDataSource)).containsExactlyInAnyOrder(
            new FilesRecord(1L, objectKey1, FileReason.PRODUCE, FileStateT.deleting, BROKER_ID, filesCommittedAt, (long) file1Size, 0L),
            new FilesRecord(2L, objectKey2, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file2Size, (long) file2Size),  // not a single batch deleted from file 2
            new FilesRecord(3L, objectKey3, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file3Size, (long) file3Size - file3Batch2Size)
        );
        assertThat(DBUtils.getAllFilesToDelete(hikariDataSource)).containsExactlyInAnyOrder(
            new FilesToDeleteRecord(1L, topicsDeletedAt)
        );
    }
}
