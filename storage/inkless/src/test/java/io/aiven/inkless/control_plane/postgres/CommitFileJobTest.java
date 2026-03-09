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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.tables.records.BatchesRecord;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.LogsRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class CommitFileJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final short FORMAT = ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT.id;
    static final short MAGIC = RecordBatch.CURRENT_MAGIC_VALUE;
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
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, duration -> {})
            .run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void simpleCommit() {
        final String objectKey = "obj1";

        final CommitBatchRequest request1 = CommitBatchRequest.of(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(0, T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(
            request1,
            request2
        ), duration -> {});
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request1),
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request2)
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L, 100L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L, 50L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L),
                new BatchesRecord(2L, MAGIC, TOPIC_ID_1, 0, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, time.milliseconds(), 2000L)
            );
    }

    @Test
    void commitMultipleFiles() {
        final String objectKey1 = "obj1";
        final String objectKey2 = "obj2";
        final Instant time1 = TimeUtils.now(time);

        final long firstFileCommittedAt = time.milliseconds();
        final CommitBatchRequest request1 = CommitBatchRequest.of(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(0, T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final CommitFileJob job1 = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(
            request1,
            request2
        ), duration -> {});
        final List<CommitBatchResponse> result1 = job1.call();

        assertThat(result1).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, firstFileCommittedAt, 0, objectKey1, request1),
            CommitBatchResponse.success(0, firstFileCommittedAt, 0, objectKey1, request2)
        );

        time.sleep(1000);  // advance time
        final Instant time2 = TimeUtils.now(time);

        final long secondFileCommittedAt = time.milliseconds();
        final CommitBatchRequest request3 = CommitBatchRequest.of(0, T0P0, 0, 111, 0, 158, 3000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request4 = CommitBatchRequest.of(0, T0P1, 111, 222, 0, 244, 4000, TimestampType.CREATE_TIME);
        final CommitFileJob job2 = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(
            request3,
            request4
        ), duration -> {});
        final List<CommitBatchResponse> result2 = job2.call();

        assertThat(result2).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, secondFileCommittedAt, 0, objectKey2, request3),
            CommitBatchResponse.success(15, secondFileCommittedAt, 0, objectKey2, request4)
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 159L, 111L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L + 245, 322L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L, 50L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, time1, null, FILE_SIZE),
                new FilesRecord(EXPECTED_FILE_ID_2, "obj2", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, time2, null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                // first pair
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, firstFileCommittedAt, 1000L),
                new BatchesRecord(2L, MAGIC, TOPIC_ID_1, 0, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, firstFileCommittedAt, 2000L),
                // second pair
                new BatchesRecord(3L, MAGIC, TOPIC_ID_0, 0, 0L, 158L, EXPECTED_FILE_ID_2, 0L, 111L, TimestampType.CREATE_TIME, secondFileCommittedAt, 3000L),
                new BatchesRecord(4L, MAGIC, TOPIC_ID_0, 1, 15L, 15L + 245 - 1, EXPECTED_FILE_ID_2, 111L, 222L, TimestampType.CREATE_TIME, secondFileCommittedAt, 4000L)
            );
    }

    @Test
    void nonExistentPartition() {
        final String objectKey = "obj1";

        // Non-existent partition.
        final var t1p1 = new TopicIdPartition(TOPIC_ID_1, 10, TOPIC_1);
        final CommitBatchRequest request1 = CommitBatchRequest.of(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME);
        final CommitBatchRequest request2 = CommitBatchRequest.of(0, T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME);
        final int request3BatchSize = 1243;
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(
            request1,
            request2,
            CommitBatchRequest.of(0, t1p1, 150, request3BatchSize, 82, 100, 3000, TimestampType.LOG_APPEND_TIME)
        ), duration -> {});

        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request1),
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request2),
            CommitBatchResponse.unknownTopicOrPartition()
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L, 100L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L, 50L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L),
                new BatchesRecord(2L, MAGIC, TOPIC_ID_1, 0, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, time.milliseconds(), 2000L)
            );
    }

    @Test
    void simpleIdempotentCommit() {
        final String objectKey = "obj1";

        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME, 1L, (short) 3, 0, 14);
        final CommitBatchRequest request2 = CommitBatchRequest.idempotent(0, T1P0, 100, 50, 0, 26, 2000, TimestampType.LOG_APPEND_TIME, 1L, (short) 3, 0, 26);
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1, request2), duration -> {
        });
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request1),
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request2)
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L, 100L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 27L, 50L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L),
                new BatchesRecord(2L, MAGIC, TOPIC_ID_1, 0, 0L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, time.milliseconds(), 2000L)
            );
    }

    @Test
    void inSequenceCommit() {
        final String objectKey = "obj1";

        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME, 1L, (short) 3, 0, 14);
        final CommitBatchRequest request2 = CommitBatchRequest.idempotent(0, T0P1, 100, 50, 15, 26, 2000, TimestampType.LOG_APPEND_TIME, 1L, (short) 3, 15, 26);
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1, request2), duration -> {
        });
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request1),
            CommitBatchResponse.success(15, time.milliseconds(), 0, objectKey, request2)
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 27L, 150L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 0L, 0L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L),
                new BatchesRecord(2L, MAGIC, TOPIC_ID_0, 1, 15L, 26L, EXPECTED_FILE_ID_1, 100L, 50L, TimestampType.LOG_APPEND_TIME, time.milliseconds(), 2000L)
            );
    }

    @ParameterizedTest
    @CsvSource({
        "14, 13", // lower than 15
        "14, 14", // lower than 15
        "14, 16", // larger than 15
        "2147483647, 1" // not zero
    })
        // 15 is the first sequence number for the second batch
    void outOfOrderCommit(int lastBatchSequence, int firstBatchSequence) {
        final String objectKey = "obj1";

        final int request2BatchSize = 50;
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME, 1L, (short) 3, 0, lastBatchSequence);
        final CommitBatchRequest request2 = CommitBatchRequest.idempotent(0, T0P1, 100, request2BatchSize, 0, 26, 2000, TimestampType.LOG_APPEND_TIME, 1L, (short) 3, firstBatchSequence, 26);
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1, request2), duration -> {
        });
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request1),
            CommitBatchResponse.sequenceOutOfOrder(request2)
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L, 100L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 0L, 0L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L)
            );
    }

    @Test
    void outOfOrderCommitNewEpoch() {
        final String objectKey = "obj1";

        final int request1BatchSize = 100;
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, T0P1, 0, request1BatchSize, 0, 14, 1000, TimestampType.CREATE_TIME, 1L, (short) 2, 1, 15);
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1), duration -> {
        });
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.sequenceOutOfOrder(request1)
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 0L, 0L, 0L)
            );

        // The file will be deleted because its only batch is rejected.
        final Instant now = TimeUtils.now(time);
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.deleting, BROKER_ID, now, now, FILE_SIZE)
            );
        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void invalidProducerEpoch() {
        final String objectKey = "obj1";

        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, T0P1, 0, 100, 0, 14, 1000, TimestampType.CREATE_TIME, 1L, (short) 3, 0, 14);
        final int request2BatchSize = 50;
        final CommitBatchRequest request2 = CommitBatchRequest.idempotent(0, T0P1, 100, request2BatchSize, 0, 26, 2000, TimestampType.LOG_APPEND_TIME, 1L, (short) 2, 15, 26);
        final CommitFileJob job = new CommitFileJob(time, pgContainer.getJooqCtx(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1, request2), duration -> {
        });
        final List<CommitBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            CommitBatchResponse.success(0, time.milliseconds(), 0, objectKey, request1),
            CommitBatchResponse.invalidProducerEpoch()
        );

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID_0, 0, TOPIC_0, 0L, 0L, 0L, 0L),
                new LogsRecord(TOPIC_ID_0, 1, TOPIC_0, 0L, 15L, 100L, 0L),
                new LogsRecord(TOPIC_ID_1, 0, TOPIC_1, 0L, 0L, 0L, 0L)
            );

        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new FilesRecord(EXPECTED_FILE_ID_1, "obj1", FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, TimeUtils.now(time), null, FILE_SIZE)
            );

        assertThat(DBUtils.getAllBatches(pgContainer.getDataSource()))
            .containsExactlyInAnyOrder(
                new BatchesRecord(1L, MAGIC, TOPIC_ID_0, 1, 0L, 14L, EXPECTED_FILE_ID_1, 0L, 100L, TimestampType.CREATE_TIME, time.milliseconds(), 1000L)
            );
    }
}
