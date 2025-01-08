// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

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
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
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
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests, durationCallback)
            .run();
    }

    @Test
    void deleteMultipleTopics() {
        final String nonExistentTopic = "non-existent";

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
            time, hikariDataSource, objectKey1, BROKER_ID, file1Size,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T0P0, 0, file1Batch1Size, 12, 1000, TimestampType.CREATE_TIME)),
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T0P1, 0, file1Batch2Size, 12, 1000, TimestampType.CREATE_TIME))
            ), durationCallback
        ).call();

        // TOPIC_0, partition 0 and TOPIC_2, partition 0
        final int file2Batch1Size = 1000;
        final int file2Batch2Size = 2000;
        final int file2Size = file2Batch1Size + file2Batch2Size;
        new CommitFileJob(
            time, hikariDataSource, objectKey2, BROKER_ID, file2Size,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T0P0, 0, file2Batch1Size, 12, 1000, TimestampType.CREATE_TIME)),
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T2P0, 0, file2Batch2Size, 12, 1000, TimestampType.CREATE_TIME))
            ), durationCallback
        ).call();

        // TOPIC_0, both partitions and TOPIC_2, partition 0
        final int file3Batch1Size = 1000;
        final int file3Batch2Size = 2000;
        final int file3Batch3Size = 3000;
        final int file3Size = file3Batch1Size + file3Batch2Size + file3Batch3Size;
        new CommitFileJob(
            time, hikariDataSource, objectKey3, BROKER_ID, file3Size,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T0P0, 0, file1Batch1Size, 12, 1000, TimestampType.CREATE_TIME)),
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T0P1, 0, file1Batch2Size, 12, 1000, TimestampType.CREATE_TIME)),
                new CommitFileJob.CommitBatchRequestJson(new CommitBatchRequest(T2P0, 0, file1Batch2Size, 12, 1000, TimestampType.CREATE_TIME))
            ), durationCallback
        ).call();

        time.sleep(1000);  // advance time
        final Instant topicsDeletedAt = TimeUtils.now(time);
        final Uuid nonexistentTopicId = Uuid.ONE_UUID;
        new DeleteTopicJob(time, hikariDataSource, Set.of(
            TOPIC_ID_0, TOPIC_ID_1, nonexistentTopicId
        ), durationCallback).run();

        // The logs of the deleted topics must be gone, i.e. only TOPIC_2 remains.
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactly(
            new DBUtils.Log(TOPIC_ID_2, 0, TOPIC_2, 0, 24)
        );

        // The batches of the deleted topics must be gone, i.e. only TOPIC_2 remains.
        assertThat(DBUtils.getAllBatches(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.Batch(TOPIC_ID_2, 0, 0, 11, 2, 0, 2000, 12),
            new DBUtils.Batch(TOPIC_ID_2, 0, 12, 23, 3, 0, 2000, 12)
        );

        // File 1 must be `deleting` because it contained only data from the deleted TOPIC_1.
        assertThat(DBUtils.getAllFiles(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.File(1, objectKey1, FileReason.PRODUCE, FileState.DELETING, BROKER_ID, filesCommittedAt, file1Size, 0),
            new DBUtils.File(2, objectKey2, FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, filesCommittedAt, file2Size, file2Batch2Size),
            new DBUtils.File(3, objectKey3, FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, filesCommittedAt, file3Size, file3Batch3Size)
        );
        assertThat(DBUtils.getAllFilesToDelete(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.FileToDelete(1, topicsDeletedAt)
        );
    }
}
