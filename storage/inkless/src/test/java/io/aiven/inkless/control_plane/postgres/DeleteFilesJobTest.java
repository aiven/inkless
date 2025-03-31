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

import io.aiven.inkless.common.ObjectFormat;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jooq.generated.enums.FileStateT;
import org.jooq.generated.tables.records.FilesRecord;
import org.jooq.generated.tables.records.FilesToDeleteRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DeleteFilesJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final short FORMAT = ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT.id;
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

    @Test void test() {
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
            time, pgContainer.getJooqCtx(), objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file1Size,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, file1Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P1, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        // TOPIC_0, partition 0 and TOPIC_2, partition 0
        final int file2Batch1Size = 1000;
        final int file2Batch2Size = 2000;
        final int file2Size = file2Batch1Size + file2Batch2Size;
        new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file2Size,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, file2Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T2P0, 0, file2Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
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
                CommitBatchRequest.of(0, T0P0, 0, file1Batch1Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T0P1, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, T2P0, 0, file1Batch2Size, 0, 11, 1000, TimestampType.CREATE_TIME)
            ), durationCallback
        ).call();

        time.sleep(1000);  // advance time
        final Instant topicsDeletedAt = TimeUtils.now(time);
        final Uuid nonexistentTopicId = Uuid.ONE_UUID;
        new DeleteTopicJob(time, pgContainer.getJooqCtx(), Set.of(
            TOPIC_ID_0, TOPIC_ID_1, nonexistentTopicId
        ), durationCallback).run();

        // File 1 must be `deleting` because it contained only data from the deleted TOPIC_1.
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new FilesRecord(1L, objectKey1, FORMAT, FileReason.PRODUCE, FileStateT.deleting, BROKER_ID, filesCommittedAt, (long) file1Size, 0L),
            new FilesRecord(2L, objectKey2, FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file2Size, (long) file2Batch2Size),
            new FilesRecord(3L, objectKey3, FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file3Size, (long) file3Batch3Size)
        );
        assertThat(DBUtils.getAllFilesToDelete(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new FilesToDeleteRecord(1L, topicsDeletedAt)
        );

        new DeleteFilesJob(
            time, pgContainer.getJooqCtx(), new DeleteFilesRequest(Set.of(objectKey1)), durationCallback
        ).run();
        assertThat(DBUtils.getAllFilesToDelete(pgContainer.getDataSource())).isEmpty();
        assertThat(DBUtils.getAllFiles(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new FilesRecord(2L, objectKey2, FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file2Size, (long) file2Batch2Size),
            new FilesRecord(3L, objectKey3, FORMAT, FileReason.PRODUCE, FileStateT.uploaded, BROKER_ID, filesCommittedAt, (long) file3Size, (long) file3Batch3Size)
        );

    }
}
