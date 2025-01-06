// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
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

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
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
class CommitFileJobTest extends SharedPostgreSQLTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456789;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, 0);
    static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, 1);
    static final TopicPartition T1P0 = new TopicPartition(TOPIC_1, 0);

    static final long EXPECTED_FILE_ID_1 = 1;
    static final long EXPECTED_FILE_ID_2 = 2;

    @Mock
    Time time;

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
        new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), duration -> {})
            .run();
    }

    @Test
    void simpleCommit() {
        final String objectKey = "obj1";

        when(time.milliseconds()).thenReturn(123456L);

        final CommitFileJob job = new CommitFileJob(time, hikariDataSource, objectKey, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P1, 0, 100, 15, 1000), TOPIC_ID_0, TimestampType.CREATE_TIME),
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T1P0, 100, 50, 27, 2000), TOPIC_ID_1, TimestampType.LOG_APPEND_TIME)
        ), duration -> {});
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, 123456L, 0),
            new CommitBatchResponse(Errors.NONE, 0, 123456L, 0)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID_0, 0, TOPIC_0, 0, 0),
                new DBUtils.Log(TOPIC_ID_0, 1, TOPIC_0, 0, 15),
                new DBUtils.Log(TOPIC_ID_1, 0, TOPIC_1, 0, 27)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.File(EXPECTED_FILE_ID_1, "obj1", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, Instant.ofEpochMilli(123456L), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Batch(TOPIC_ID_0, 1, 0, 14, EXPECTED_FILE_ID_1, 0, 100, 15),
                new DBUtils.Batch(TOPIC_ID_1, 0, 0, 26, EXPECTED_FILE_ID_1, 100, 50, 27)
            );
    }

    @Test
    void commitMultipleFiles() {
        final String objectKey1 = "obj1";
        final String objectKey2 = "obj2";

        when(time.milliseconds()).thenReturn(1000L);

        final CommitFileJob job1 = new CommitFileJob(time, hikariDataSource, objectKey1, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P1, 0, 100, 15, 1000), TOPIC_ID_0, TimestampType.CREATE_TIME),
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T1P0, 100, 50, 27, 2000), TOPIC_ID_1, TimestampType.LOG_APPEND_TIME)
        ), duration -> {});
        final List<CommitBatchResponse> result1 = job1.call();

        assertThat(result1).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, 1000L, 0),
            new CommitBatchResponse(Errors.NONE, 0, 1000L, 0)
        );

        when(time.milliseconds()).thenReturn(2000L);

        final CommitFileJob job2 = new CommitFileJob(time, hikariDataSource, objectKey2, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P0, 0, 111, 159, 3000), TOPIC_ID_0, TimestampType.CREATE_TIME),
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P1, 111, 222, 245, 4000), TOPIC_ID_0, TimestampType.CREATE_TIME)
        ), duration -> {});
        final List<CommitBatchResponse> result2 = job2.call();

        assertThat(result2).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, 2000L, 0),
            new CommitBatchResponse(Errors.NONE, 15, 2000L, 0)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID_0, 0, TOPIC_0, 0, 159),
                new DBUtils.Log(TOPIC_ID_0, 1, TOPIC_0, 0, 15 + 245),
                new DBUtils.Log(TOPIC_ID_1, 0, TOPIC_1, 0, 27)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.File(EXPECTED_FILE_ID_1, "obj1", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, Instant.ofEpochMilli(1000L), FILE_SIZE, FILE_SIZE),
                new DBUtils.File(EXPECTED_FILE_ID_2, "obj2", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, Instant.ofEpochMilli(2000L), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Batch(TOPIC_ID_0, 1, 0, 14, EXPECTED_FILE_ID_1, 0, 100, 15),
                new DBUtils.Batch(TOPIC_ID_1, 0, 0, 26, EXPECTED_FILE_ID_1, 100, 50, 27),

                new DBUtils.Batch(TOPIC_ID_0, 0, 0, 158, EXPECTED_FILE_ID_2, 0, 111, 159),
                new DBUtils.Batch(TOPIC_ID_0, 1, 15, 15 + 245 - 1, EXPECTED_FILE_ID_2, 111, 222, 245)
            );
    }

    @Test
    void nonExistentPartition() {
        final String objectKey = "obj1";

        when(time.milliseconds()).thenReturn(123456L);

        // Non-existent partition.
        final var t1p1 = new TopicPartition(TOPIC_1, 10);
        final CommitFileJob job = new CommitFileJob(time, hikariDataSource, objectKey, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P1, 0, 100, 15, 1000), TOPIC_ID_0, TimestampType.CREATE_TIME),
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T1P0, 100, 50, 27, 2000), TOPIC_ID_1, TimestampType.LOG_APPEND_TIME),
            new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(t1p1, 150, 1243, 82, 3000), TOPIC_ID_1, TimestampType.LOG_APPEND_TIME)
        ), duration -> {});

        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, 123456L, 0),
            new CommitBatchResponse(Errors.NONE, 0, 123456L, 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID_0, 0, TOPIC_0, 0, 0),
                new DBUtils.Log(TOPIC_ID_0, 1, TOPIC_0, 0, 15),
                new DBUtils.Log(TOPIC_ID_1, 0, TOPIC_1, 0, 27)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.File(EXPECTED_FILE_ID_1, "obj1", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, Instant.ofEpochMilli(123456L), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Batch(TOPIC_ID_0, 1, 0, 14, EXPECTED_FILE_ID_1, 0, 100, 15),
                new DBUtils.Batch(TOPIC_ID_1, 0, 0, 26, EXPECTED_FILE_ID_1, 100, 50, 27)
            );
    }
}
