// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

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
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, 0);
    static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, 1);
    static final TopicPartition T1P0 = new TopicPartition(TOPIC_1, 0);
    static final TopicPartition T2P0 = new TopicPartition(TOPIC_2, 0);

    @Mock
    Time time;
    @Mock
    Consumer<Long> durationCallback;

    @BeforeEach
    void createTopics() {
        final MetadataView metadataView = mock(MetadataView.class);
        when(metadataView.isInklessTopic(anyString())).thenReturn(true);

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_0).setTopicId(TOPIC_ID_0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_0).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_0).setPartitionId(1));
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID_1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_1).setPartitionId(0));
        delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID_2));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_2).setPartitionId(0));
        new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), durationCallback)
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
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P0, 0, file1Batch1Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_0),
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P1, 0, file1Batch2Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_0)
            ), durationCallback
        ).call();

        // TOPIC_0, partition 0 and TOPIC_2, partition 0
        final int file2Batch1Size = 1000;
        final int file2Batch2Size = 2000;
        final int file2Size = file2Batch1Size + file2Batch2Size;
        new CommitFileJob(
            time, hikariDataSource, objectKey2, BROKER_ID, file2Size,
            List.of(
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P0, 0, file2Batch1Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_0),
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T2P0, 0, file2Batch2Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_2)
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
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P0, 0, file1Batch1Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_0),
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P1, 0, file1Batch2Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_0),
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T2P0, 0, file1Batch2Size, 12, 1000, TimestampType.CREATE_TIME), TOPIC_ID_2)
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
