// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.FileReasonT;
import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
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
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, duration -> {})
            .run();
    }

    @Test
    void simpleCommit() {
        final String objectKey = "obj1";

        final CommitBatchRequest request1 = CommitBatchRequest.of(T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job = new CommitFileJob(time, jooqCtx, objectKey, BROKER_ID, FILE_SIZE, List.of(
            request1,
            request2
        ), duration -> {});
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FileReasonT.produce, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, TOPIC_ID_0, 1, 0L, 14L, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L),
                new BatchesRecord(2L, TOPIC_ID_1, 0, 0L, 26L, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, time.milliseconds(), 2000L)
            );
    }

    @Test
    void commitMultipleFiles() {
        final String objectKey1 = "obj1";
        final String objectKey2 = "obj2";
        final Instant time1 = TimeUtils.now(time);

        final long firstFileCommittedAt = time.milliseconds();
        final CommitBatchRequest request1 = CommitBatchRequest.of(T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job1 = new CommitFileJob(time, jooqCtx, objectKey1, BROKER_ID, FILE_SIZE, List.of(
            request1,
            request2
        ), duration -> {});
        final List<CommitBatchResponse> result1 = job1.call();

        assertThat(result1).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, firstFileCommittedAt, 0),
            new CommitBatchResponse(Errors.NONE, 0, firstFileCommittedAt, 0)
        );

        time.sleep(1000);  // advance time
        final Instant time2 = TimeUtils.now(time);

        final long secondFileCommittedAt = time.milliseconds();
        final CommitBatchRequest request3 = CommitBatchRequest.of(T0P0, 0, 111, 0, 158, 3000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request4 = CommitBatchRequest.of(T0P1, 111, 222, 0, 244, 4000, TimestampType.CREATE_TIME);
        final CommitFileJob job2 = new CommitFileJob(time, jooqCtx, objectKey2, BROKER_ID, FILE_SIZE, List.of(
            request3,
            request4
        ), duration -> {});
        final List<CommitBatchResponse> result2 = job2.call();

        assertThat(result2).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, secondFileCommittedAt, 0),
            new CommitBatchResponse(Errors.NONE, 15, secondFileCommittedAt, 0)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 159L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L + 245),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FileReasonT.produce, FileStateT.uploaded, BROKER_ID, time1, FILE_SIZE, FILE_SIZE),
                new FilesRecord(EXPECTED_FILE_ID_2, "obj2", FileReasonT.produce, FileStateT.uploaded, BROKER_ID, time2, FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                // first pair
                new BatchesRecord(1L, TOPIC_ID_0, 1, 0L, 14L, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, firstFileCommittedAt, 1000L),
                new BatchesRecord(2L, TOPIC_ID_1, 0, 0L, 26L, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, firstFileCommittedAt, 2000L),
                // second pair
                new BatchesRecord(3L, TOPIC_ID_0, 0, 0L, 158L, 0L, 158L, EXPECTED_FILE_ID_2, 0L, 111L, TimestampType.CREATE_TIME, secondFileCommittedAt, 3000L),
                new BatchesRecord(4L, TOPIC_ID_0, 1, 15L, 15L + 245 - 1, 0L, 244L, EXPECTED_FILE_ID_2, 111L, 222L, TimestampType.CREATE_TIME, secondFileCommittedAt, 4000L)
            );
    }

    @Test
    void nonExistentPartition() {
        final String objectKey = "obj1";

        // Non-existent partition.
        final var t1p1 = new TopicIdPartition(TOPIC_ID_1, 10, TOPIC_1);
        final CommitBatchRequest request1 = CommitBatchRequest.of(T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job = new CommitFileJob(time, jooqCtx, objectKey, BROKER_ID, FILE_SIZE, List.of(
            request1,
            request2,
            CommitBatchRequest.of(t1p1, 150, 1243, 82, 100, 3000, TimestampType.LOG_APPEND_TIME)
        ), duration -> {});

        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        assertThat(DBUtils.getAllLogs(hikariDataSource))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L)
            );

        assertThat(DBUtils.getAllFiles(hikariDataSource))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FileReasonT.produce, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), FILE_SIZE, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(hikariDataSource))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, TOPIC_ID_0, 1, 0L, 14L, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L),
                new BatchesRecord(2L, TOPIC_ID_1, 0, 0L, 26L, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, time.milliseconds(), 2000L)
            );
    }
}
