// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.FileReasonT;
import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.FilesToDeleteRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteTopicJobTest extends SharedPostgreSQLTest {
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

    Time time = new MockTime();
    Consumer<Long> durationCallback = duration -> {};

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
    void deleteMultipleTopics() {
        final String objectKey1 = "obj1";
        final String objectKey2 = "obj2";
        final String objectKey3 = "obj3";

        final Instant filesCommittedAt = TimeUtils.now(time);

        // TOPIC_0 - non-empty, deleted
        // TOPIC_1 - empty, deleted
        // TOPIC_2 - non-empty, not deleted

        // TOPIC_0, both partitions.
        final int file1Batch1Size = 1000;
        final int file1Batch2Size = 2000;
        final int file1Size = file1Batch1Size + file1Batch2Size;
        new CommitFileJob(
            time, jooqCtx, objectKey1, BROKER_ID, file1Size,
            List.of(
                CommitBatchRequest.of(T0P0, 0, file1Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T0P1, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        // TOPIC_0, partition 0 and TOPIC_2, partition 0
        final int file2Batch1Size = 1000;
        final int file2Batch2Size = 2000;
        final int file2Size = file2Batch1Size + file2Batch2Size;
        new CommitFileJob(
            time, jooqCtx, objectKey2, BROKER_ID, file2Size,
            List.of(
                CommitBatchRequest.of(T0P0, 0, file2Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T2P0, 0, file2Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
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
                CommitBatchRequest.of(T0P0, 0, file1Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T0P1, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(T2P0, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        time.sleep(1000);  // advance time
        final Instant topicsDeletedAt = TimeUtils.now(time);
        final Uuid nonexistentTopicId = Uuid.ONE_UUID;
        new DeleteTopicJob(time, jooqCtx, Set.of(
            TOPIC_ID_0, TOPIC_ID_1, nonexistentTopicId
        ), durationCallback).run();

        // The logs of the deleted topics must be gone, i.e. only TOPIC_2 remains.
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactly(
            new LogsRecord(TOPIC_ID_2, 0, TOPIC_2, 0L, 24L)
        );

        // The batches of the deleted topics must be gone, i.e. only TOPIC_2 remains.
        assertThat(DBUtils.getAllBatches(hikariDataSource)).containsExactlyInAnyOrder(
            new BatchesRecord(TOPIC_ID_2, 0, 0L, 11L, 0L, 11L, 2L, 0L, 2000L, TimestampType.CREATE_TIME, filesCommittedAt.toEpochMilli(), 1000L),
            new BatchesRecord(TOPIC_ID_2, 0, 12L, 23L, 0L, 11L, 3L, 0L, 2000L, TimestampType.CREATE_TIME, filesCommittedAt.toEpochMilli(), 1000L)
        );

        // File 1 must be `deleting` because it contained only data from the deleted TOPIC_1.
        assertThat(DBUtils.getAllFiles(hikariDataSource)).containsExactlyInAnyOrder(
            new FilesRecord(1L, objectKey1, FileReasonT.produce, FileStateT.deleting, BROKER_ID, filesCommittedAt, (long) file1Size, 0L),
            new FilesRecord(2L, objectKey2, FileReasonT.produce, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file2Size, (long) file2Batch2Size),
            new FilesRecord(3L, objectKey3, FileReasonT.produce, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file3Size, (long) file3Batch3Size)
        );
        assertThat(DBUtils.getAllFilesToDelete(hikariDataSource)).containsExactlyInAnyOrder(
            new FilesToDeleteRecord(1L, topicsDeletedAt)
        );
    }
}
