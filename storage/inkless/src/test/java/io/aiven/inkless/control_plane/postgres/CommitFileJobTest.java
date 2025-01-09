// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

class CommitFileJobTest extends SharedPostgreSQLTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456789;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    static final long EXPECTED_FILE_ID_1 = 1;
    static final long EXPECTED_FILE_ID_2 = 2;

    Time time = new MockTime();

    @BeforeEach
    void createTopics() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests, duration -> {})
            .run();
    }

    @Test
    void simpleCommit() {
        final String objectKey = "obj1";

        final CommitBatchRequest request1 = CommitBatchRequest.of(T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job = new CommitFileJob(time, hikariDataSource, objectKey, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestJson(request1),
            new CommitFileJob.CommitBatchRequestJson(request2)
        ), duration -> {});
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID_0, 0, TOPIC_0, 0, 0),
                new DBUtils.Log(TOPIC_ID_0, 1, TOPIC_0, 0, 15),
                new DBUtils.Log(TOPIC_ID_1, 0, TOPIC_1, 0, 27)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.File(EXPECTED_FILE_ID_1, "obj1", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, TimeUtils.now(time), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Batch(TOPIC_ID_0, 1, 0, 14, EXPECTED_FILE_ID_1, 0, 100, 0, 14),
                new DBUtils.Batch(TOPIC_ID_1, 0, 0, 26, EXPECTED_FILE_ID_1, 100, 50, 0, 26)
            );
    }

    @Test
    void commitMultipleFiles() {
        final String objectKey1 = "obj1";
        final String objectKey2 = "obj2";
        final Instant time1 = TimeUtils.now(time);

        final CommitBatchRequest request1 = CommitBatchRequest.of(T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job1 = new CommitFileJob(time, hikariDataSource, objectKey1, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestJson(request1),
            new CommitFileJob.CommitBatchRequestJson(request2)
        ), duration -> {});
        final List<CommitBatchResponse> result1 = job1.call();

        assertThat(result1).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0)
        );

        time.sleep(1000);  // advance time
        final Instant time2 = TimeUtils.now(time);

        final CommitBatchRequest request3 = CommitBatchRequest.of(T0P0, 0, 111, 0, 158, 3000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request4 = CommitBatchRequest.of(T0P1, 111, 222, 0, 244, 4000, TimestampType.CREATE_TIME);
        final CommitFileJob job2 = new CommitFileJob(time, hikariDataSource, objectKey2, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestJson(request3),
            new CommitFileJob.CommitBatchRequestJson(request4)
        ), duration -> {});
        final List<CommitBatchResponse> result2 = job2.call();

        assertThat(result2).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.NONE, 15, time.milliseconds(), 0)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID_0, 0, TOPIC_0, 0, 159),
                new DBUtils.Log(TOPIC_ID_0, 1, TOPIC_0, 0, 15 + 245),
                new DBUtils.Log(TOPIC_ID_1, 0, TOPIC_1, 0, 27)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.File(EXPECTED_FILE_ID_1, "obj1", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, time1, FILE_SIZE, FILE_SIZE),
                new DBUtils.File(EXPECTED_FILE_ID_2, "obj2", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, time2, FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                // first pair
                new DBUtils.Batch(TOPIC_ID_0, 1, 0, 14, EXPECTED_FILE_ID_1, 0, 100, 0, 14),
                new DBUtils.Batch(TOPIC_ID_1, 0, 0, 26, EXPECTED_FILE_ID_1, 100, 50, 0, 26),
                // second pair
                new DBUtils.Batch(TOPIC_ID_0, 0, 0, 158, EXPECTED_FILE_ID_2, 0, 111, 0, 158),
                new DBUtils.Batch(TOPIC_ID_0, 1, 15, 15 + 245 - 1, EXPECTED_FILE_ID_2, 111, 222, 0, 244)
            );
    }

    @Test
    void nonExistentPartition() {
        final String objectKey = "obj1";

        // Non-existent partition.
        final var t1p1 = new TopicIdPartition(TOPIC_ID_1, 10, TOPIC_1);
        final CommitBatchRequest request1 = CommitBatchRequest.of(T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job = new CommitFileJob(time, hikariDataSource, objectKey, BROKER_ID, FILE_SIZE, List.of(
            new CommitFileJob.CommitBatchRequestJson(request1),
            new CommitFileJob.CommitBatchRequestJson(request2),
            new CommitFileJob.CommitBatchRequestJson(CommitBatchRequest.of(t1p1, 150, 1243, 82, 100, 3000, TimestampType.LOG_APPEND_TIME))
        ), duration -> {});

        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
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
                new DBUtils.File(EXPECTED_FILE_ID_1, "obj1", FileReason.PRODUCE, FileState.UPLOADED, BROKER_ID, TimeUtils.now(time), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new DBUtils.Batch(TOPIC_ID_0, 1, 0, 14, EXPECTED_FILE_ID_1, 0, 100, 0, 14),
                new DBUtils.Batch(TOPIC_ID_1, 0, 0, 26, EXPECTED_FILE_ID_1, 100, 50, 0, 26)
            );
    }
}
