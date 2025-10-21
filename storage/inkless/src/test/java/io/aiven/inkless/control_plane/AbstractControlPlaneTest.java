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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIERED_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.MAX_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractControlPlaneTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456;

    static final String EXISTING_TOPIC_1 = "topic-existing-1";
    static final int EXISTING_TOPIC_1_PARTITIONS = 2;
    static final Uuid EXISTING_TOPIC_1_ID = new Uuid(10, 10);
    static final TopicIdPartition EXISTING_TOPIC_1_ID_PARTITION_0 = new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1);
    static final TopicIdPartition EXISTING_TOPIC_1_ID_PARTITION_1 = new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1);
    static final String EXISTING_TOPIC_2 = "topic-existing-2";
    static final Uuid EXISTING_TOPIC_2_ID = new Uuid(20, 20);
    static final TopicIdPartition EXISTING_TOPIC_2_ID_PARTITION_0 = new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_2);
    static final Uuid NONEXISTENT_TOPIC_ID = Uuid.ONE_UUID;
    static final String NONEXISTENT_TOPIC = "topic-nonexistent";

    protected static final long FILE_MERGE_SIZE_THRESHOLD = 100 * 1024 * 1024;
    protected static final Duration FILE_MERGE_LOCK_PERIOD = Duration.ofHours(1);
    protected static final Map<String, String> BASE_CONFIG = Map.of(
        "file.merge.size.threshold.bytes", Long.toString(FILE_MERGE_SIZE_THRESHOLD),
        "file.merge.lock.period.ms", Long.toString(FILE_MERGE_LOCK_PERIOD.toMillis())
    );

    static final long START_TIME = 10000;
    protected MockTime time = new MockTime(0, START_TIME, 0);

    protected ControlPlane controlPlane;

    protected abstract ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo);
    protected abstract void tearDownControlPlane() throws IOException;

    static void configureControlPlane(ControlPlane controlPlane, Map<String, ?> configs) {
        Map<String, Object> override = new HashMap<>(configs);
        override.put("producer.id.expiration.ms", 60_000);
        controlPlane.configure(override);
    }

    @BeforeEach
    void setupControlPlane(final TestInfo testInfo) {
        final var controlPlaneAndConfigs = createControlPlane(testInfo);
        controlPlane = controlPlaneAndConfigs.controlPlane;
        configureControlPlane(controlPlane, controlPlaneAndConfigs.configs);

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, EXISTING_TOPIC_1_PARTITIONS),
            new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_2_ID, EXISTING_TOPIC_2, 1)
        );
        controlPlane.createTopicAndPartitions(createTopicAndPartitionsRequests);
    }

    @AfterEach
    void tearDown() throws IOException {
        tearDownControlPlane();
    }

    @Test
    void emptyCommit() {
        final List<CommitBatchResponse> commitBatchResponse = controlPlane.commitFile(
            "a", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of()
        );
        assertThat(commitBatchResponse).isEmpty();
        assertThat(controlPlane.getLogInfo(List.of(
            new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0),
            new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 1),
            new GetLogInfoRequest(EXISTING_TOPIC_2_ID, 0)
        ))).containsExactly(
            GetLogInfoResponse.success(0, 0, 0),
            GetLogInfoResponse.success(0, 0, 0),
            GetLogInfoResponse.success(0, 0, 0)
        );
    }

    @Test
    void successfulCommitToExistingPartitions() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        final CommitBatchRequest successfulRequest1 = CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 1, 10, 1000, TimestampType.CREATE_TIME);
        final List<CommitBatchResponse> commitResponse1 = controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID,
            FILE_SIZE,
            List.of(
                successfulRequest1,
                // non-existing partition
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1_PARTITIONS + 1, EXISTING_TOPIC_1), 2, 10, 1, 10, 1000, TimestampType.CREATE_TIME),
                // non-existing topic
                CommitBatchRequest.of(0, new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 3, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse1).containsExactly(
            CommitBatchResponse.success(0, time.milliseconds(), 0, successfulRequest1),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, 10));

        final CommitBatchRequest successfulRequest2 = CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, 1, 10, 1000, TimestampType.CREATE_TIME);
        final List<CommitBatchResponse> commitResponse2 = controlPlane.commitFile(
            objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID,
            FILE_SIZE,
            List.of(
                successfulRequest2,
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1_PARTITIONS + 1, EXISTING_TOPIC_1), 200, 10, 1, 10, 2000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 300, 10, 1, 10, 3000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse2).containsExactly(
            CommitBatchResponse.success(10, time.milliseconds(), 0, successfulRequest2),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 20, 20));

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(
                new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1_PARTITIONS + 1, EXISTING_TOPIC_1) , 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(Uuid.ONE_UUID, 0, NONEXISTENT_TOPIC), 11, Integer.MAX_VALUE)
            ), Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(
                Errors.NONE,
                List.of(new BatchInfo(2L, objectKey2, BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 100, 10, 10, 19, time.milliseconds(), 1000, TimestampType.CREATE_TIME))),
                0, 20),
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1, -1),
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1, -1)
        );
    }

    @Test
    void fullSpectrumFind() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";
        final int numberOfRecordsInBatch1 = 3;
        final int numberOfRecordsInBatch2 = 2;
        controlPlane.commitFile(objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 0, numberOfRecordsInBatch1 - 1, 1000, TimestampType.CREATE_TIME)));
        final int lastOffset = numberOfRecordsInBatch1 + numberOfRecordsInBatch2 - 1;
        controlPlane.commitFile(objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, numberOfRecordsInBatch1, lastOffset, 2000, TimestampType.CREATE_TIME)));

        final long expectedLogStartOffset = 0;
        final long expectedHighWatermark = numberOfRecordsInBatch1 + numberOfRecordsInBatch2;
        final long expectedLogAppendTime = time.milliseconds();

        for (int offset = 0; offset < numberOfRecordsInBatch1; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, offset, Integer.MAX_VALUE)),
                Integer.MAX_VALUE, 0
            );
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(1L, objectKey1, BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, 10, 0, numberOfRecordsInBatch1 - 1, expectedLogAppendTime, 1000, TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, objectKey2, BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 100, 10, numberOfRecordsInBatch1, lastOffset, expectedLogAppendTime, 2000, TimestampType.CREATE_TIME))
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
        for (int offset = numberOfRecordsInBatch1; offset < numberOfRecordsInBatch1 + numberOfRecordsInBatch2; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, offset, Integer.MAX_VALUE)),
                Integer.MAX_VALUE, 0
            );
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(2L, objectKey2, BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 100, 10, numberOfRecordsInBatch1, lastOffset, expectedLogAppendTime, 2000, TimestampType.CREATE_TIME))
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
    }

    @Test
    void findEmptyBatchOnLastOffset() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 10, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(), 0, 10)
        );
    }

    @Test
    void findOffsetOutOfRange() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 11, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 10)
        );
    }

    @Test
    void findNegativeOffset() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, -1, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 10)
        );
    }

    @Test
    void findBeforeCommit() {
        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 11, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 0)
        );
    }

    @Test
    void commitEmptyBatches() {
        final String objectKey = "a";

        assertThatThrownBy(() -> controlPlane.commitFile(objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 10, 19, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 2, 0, 10, 19, 1000, TimestampType.CREATE_TIME)
            )
        ))
            .isInstanceOf(ControlPlaneException.class)
            .hasMessage("Batches with size 0 are not allowed");
    }

    @Test
    void createTopicAndPartitions() {
        final String newTopic1Name = "newTopic1";
        final Uuid newTopic1Id = new Uuid(12345, 67890);
        final String newTopic2Name = "newTopic2";
        final Uuid newTopic2Id = new Uuid(88888, 99999);

        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 1)
        ));
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(newTopic1Id, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 0, 0));

        // Produce some data to be sure it's not affected later.
        final String objectKey = "a1";
        controlPlane.commitFile(objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(newTopic1Id, 0, newTopic1Name), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));

        final List<FindBatchRequest> findBatchRequests = List.of(new FindBatchRequest(new TopicIdPartition(newTopic1Id, 0, newTopic1Name), 0, Integer.MAX_VALUE));
        final List<FindBatchResponse> findBatchResponsesBeforeDelete = controlPlane.findBatches(findBatchRequests, Integer.MAX_VALUE, 0
        );

        // Create new topic and partitions for the existing one.
        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 2),
            new CreateTopicAndPartitionsRequest(newTopic2Id, newTopic2Name, 2)
        ));
        assertThat(controlPlane.getLogInfo(List.of(
            new GetLogInfoRequest(newTopic1Id, 0),
            new GetLogInfoRequest(newTopic1Id, 1),
            new GetLogInfoRequest(newTopic2Id, 0),
            new GetLogInfoRequest(newTopic2Id, 1)
        ))).containsExactly(
            GetLogInfoResponse.success(0, 1, FILE_SIZE),
            GetLogInfoResponse.success(0, 0, 0),
            GetLogInfoResponse.success(0, 0, 0),
            GetLogInfoResponse.success(0, 0, 0)
        );

        final List<FindBatchResponse> findBatchResponsesAfterDelete = controlPlane.findBatches(findBatchRequests, Integer.MAX_VALUE, 0
        );
        assertThat(findBatchResponsesBeforeDelete).isEqualTo(findBatchResponsesAfterDelete);

        // Nothing happens as this is idempotent
        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 2),
            new CreateTopicAndPartitionsRequest(newTopic2Id, newTopic2Name, 2)
        ));
        assertThat(controlPlane.getLogInfo(List.of(
            new GetLogInfoRequest(newTopic1Id, 0),
            new GetLogInfoRequest(newTopic1Id, 1),
            new GetLogInfoRequest(newTopic2Id, 0),
            new GetLogInfoRequest(newTopic2Id, 1)
        ))).containsExactly(
            GetLogInfoResponse.success(0, 1, FILE_SIZE),
            GetLogInfoResponse.success(0, 0, 0),
            GetLogInfoResponse.success(0, 0, 0),
            GetLogInfoResponse.success(0, 0, 0)
        );

        final List<FindBatchResponse> findBatchResponsesAfterDelete2 = controlPlane.findBatches(findBatchRequests, Integer.MAX_VALUE, 0
        );
        assertThat(findBatchResponsesAfterDelete2).isEqualTo(findBatchResponsesAfterDelete);
    }

    @Test
    void deleteTopic() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        controlPlane.commitFile(objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));
        final int file2Partition0Size = (int) FILE_SIZE / 2;
        final int file2Partition1Size = (int) FILE_SIZE - file2Partition0Size;
        controlPlane.commitFile(objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, file2Partition0Size, 0, 0, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_2), 1, file2Partition1Size, 1, 1, 2000, TimestampType.CREATE_TIME)
            ));

        final List<FindBatchRequest> findBatchRequests = List.of(new FindBatchRequest(EXISTING_TOPIC_2_ID_PARTITION_0, 0, Integer.MAX_VALUE));
        final List<FindBatchResponse> findBatchResponsesBeforeDelete = controlPlane.findBatches(findBatchRequests, Integer.MAX_VALUE, 0
        );

        time.sleep(1001);  // advance time
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));

        // objectKey2 is kept alive by the second topic, which isn't deleted
        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );

        final List<FindBatchResponse> findBatchResponsesAfterDelete = controlPlane.findBatches(findBatchRequests, Integer.MAX_VALUE, 0
        );
        assertThat(findBatchResponsesAfterDelete).isEqualTo(findBatchResponsesBeforeDelete);

        // Nothing happens as it's idempotent.
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));
        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );

        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.unknownTopicOrPartition());
    }

    @Test
    void partiallyDeleteBatch() {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, FILE_SIZE));

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 3),
            new DeleteRecordsRequest(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 10)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(3),
            DeleteRecordsResponse.unknownTopicOrPartition()
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, findResponseBeforeDelete.get(0).batches(), 3, 10)
        );
        assertThat(controlPlane.getFilesToDelete()).isEmpty();
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(3, 10, FILE_SIZE));
    }

    @Test
    void fullyDeleteBatch() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";
        final String objectKey3 = "a3";

        controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, FILE_SIZE));

        controlPlane.commitFile(
            objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 2, (int) FILE_SIZE, 1, 10, 2000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 20, FILE_SIZE * 2));

        controlPlane.commitFile(
            objectKey3, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 3, (int) FILE_SIZE, 1, 10, 3000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 30, FILE_SIZE * 3));

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 19),
            new DeleteRecordsRequest(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 10)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(19),
            DeleteRecordsResponse.unknownTopicOrPartition()
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(
                findResponseBeforeDelete.get(0).batches().get(1),
                findResponseBeforeDelete.get(0).batches().get(2)
            ), 19, 30)
        );
        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(19, 30, FILE_SIZE * 2));
    }

    @Test
    void deleteUpToLogStartOffset() {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, FILE_SIZE));

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0),
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(0),
            DeleteRecordsResponse.success(0)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).isEqualTo(findResponseBeforeDelete);

        assertThat(controlPlane.getFilesToDelete()).isEmpty();
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, FILE_SIZE));
    }

    @Test
    void deleteUpToHighWatermark() {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, FILE_SIZE));

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(10)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(), 10, 10)
        );

        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(new FileToDelete(objectKey1, TimeUtils.now(time)));
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(10, 10, 0));
    }

    @ParameterizedTest
    @ValueSource(longs = {-2, 11})
    void deleteOffsetOutOfRange(final long deleteOffset) {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, deleteOffset)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.offsetOutOfRange()
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).isEqualTo(findResponseBeforeDelete);

        assertThat(controlPlane.getFilesToDelete()).isEmpty();
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 10, FILE_SIZE));
    }

    @Test
    void fullyDeleteBatchFileNotAffectedIfThereAreOtherBatches() {
        final String objectKey1 = "a1";

        final int tp0BatchSize = (int) FILE_SIZE / 2;
        final int tp1BatchSize = (int) FILE_SIZE - tp0BatchSize;
        controlPlane.commitFile(
            objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, tp0BatchSize, 1, 10, 1000, TimestampType.CREATE_TIME),
                // This batch will keep the file alive after the other batch is deleted.
                CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_1, 100, tp1BatchSize, 1, 2, 2000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 10)
        ));
        assertThat(deleteRecordsResponses).containsExactly(DeleteRecordsResponse.success(10));

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).isEqualTo(findResponseBeforeDelete);

        assertThat(controlPlane.getFilesToDelete()).isEmpty();
    }

    @Disabled
    @Nested
    class Retention {
        private static final String FILE_NAME = "obj1";
        private static final long BATCH_1_RECORDS = 10;
        private static final long BATCH_2_RECORDS = 20;
        private static final long BATCH_3_RECORDS = 30;
        private static final int BATCH_1_SIZE = 123;
        private static final int BATCH_2_SIZE = 456;
        private static final int BATCH_3_SIZE = 789;
        private static final long BATCH_1_MAX_TIMESTAMP = START_TIME;
        private static final long BATCH_2_MAX_TIMESTAMP = START_TIME + 10;
        private static final long BATCH_3_MAX_TIMESTAMP = START_TIME + 30;

        private static final long EXPECTED_HIGH_WATERMARK = BATCH_1_RECORDS + BATCH_2_RECORDS + BATCH_3_RECORDS;

        @Test
        void nonExistentTopicPartition() {
            final var responses = controlPlane.enforceRetention(
                List.of(new EnforceRetentionRequest(Uuid.ZERO_UUID, 12345, 10, 12)),
                0
            );
            assertThat(responses)
                .map(EnforceRetentionResponse::errors)
                .containsExactly(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }

        @Nested
        class BySize {
            @Test
            void empty() {
                final var responses = controlPlane.enforceRetention(
                    List.of(new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, 100, -1)),
                    0
                );
                assertThat(responses).containsExactly(
                    EnforceRetentionResponse.success(0, 0, 0)
                );
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, 0, 0));
            }

            @Test
            void deleteNothing() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE, -1)
                    ),
                    0
                );

                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(0, 0, 0));
                assertThat(getLogStartOffset()).isEqualTo(0);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, EXPECTED_HIGH_WATERMARK, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void deleteSome(final boolean deleteMidBatch) {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                final int retentionBytes = deleteMidBatch ? BATCH_3_SIZE + 1 : BATCH_3_SIZE;
                final var response = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, retentionBytes, -1)
                    ),
                    0
                );

                final long newLogStartOffset = BATCH_1_RECORDS + BATCH_2_RECORDS;
                assertThat(response)
                    .containsExactly(EnforceRetentionResponse.success(2, BATCH_1_SIZE + BATCH_2_SIZE, newLogStartOffset));
                assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(newLogStartOffset, EXPECTED_HIGH_WATERMARK, BATCH_3_SIZE));
            }

            @Test
            void deleteOneAtTheTime() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, EXPECTED_HIGH_WATERMARK, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE));

                final var responses1 = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, 0, -1)
                    ),
                    1
                );

                assertThat(responses1)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_1_SIZE, BATCH_1_RECORDS));
                assertThat(getLogStartOffset()).isEqualTo(BATCH_1_RECORDS);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(BATCH_1_RECORDS, EXPECTED_HIGH_WATERMARK, BATCH_2_SIZE + BATCH_3_SIZE));
                assertThat(controlPlane.getFilesToDelete()).isEmpty();

                final var responses2 = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, 0, -1)
                    ),
                    1
                );

                assertThat(responses2)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_2_SIZE, BATCH_1_RECORDS + BATCH_2_RECORDS));
                assertThat(getLogStartOffset()).isEqualTo(BATCH_1_RECORDS + BATCH_2_RECORDS);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(BATCH_3_RECORDS, EXPECTED_HIGH_WATERMARK, BATCH_3_SIZE));
                assertThat(controlPlane.getFilesToDelete()).isEmpty();

                final var responses3 = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, 0, -1)
                    ),
                    1
                );

                assertThat(responses3)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_3_SIZE, EXPECTED_HIGH_WATERMARK));
                assertThat(getLogStartOffset()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(EXPECTED_HIGH_WATERMARK, EXPECTED_HIGH_WATERMARK, 0));
                assertThat(controlPlane.getFilesToDelete()).containsExactly(
                    new FileToDelete(FILE_NAME, TimeUtils.now(time))
                );
            }

            @Test
            void deleteAll() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, 0, -1)
                    ),
                    0
                );

                final long newLogStartOffset = EXPECTED_HIGH_WATERMARK;
                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(3, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE, newLogStartOffset));
                assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(newLogStartOffset, EXPECTED_HIGH_WATERMARK, 0));
                assertThat(controlPlane.getFilesToDelete()).containsExactly(
                    new FileToDelete(FILE_NAME, TimeUtils.now(time))
                );
            }
        }

        @Nested
        class ByTime {
            @Test
            void empty() {
                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 123456)
                    ),
                    0
                );
                assertThat(responses).containsExactly(EnforceRetentionResponse.success(
                    0, 0, 0
                ));
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, 0, 0));
            }

            @Test
            void deleteNothing() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP + 1);
                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 1000)
                    ),
                    0
                );

                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(0, 0, 0));
                assertThat(getLogStartOffset()).isEqualTo(0);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, EXPECTED_HIGH_WATERMARK, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE));
            }

            @Test
            void deleteSome() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP);
                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, BATCH_3_MAX_TIMESTAMP - BATCH_2_MAX_TIMESTAMP)
                    ),
                    0
                );

                final long newLogStartOffset = BATCH_1_RECORDS;
                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_1_SIZE, newLogStartOffset));
                assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(newLogStartOffset, EXPECTED_HIGH_WATERMARK, BATCH_2_SIZE + BATCH_3_SIZE));
            }


            @Test
            void deleteOneAtTheTime() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, EXPECTED_HIGH_WATERMARK, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE));

                time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP + 1);
                final var responses1 = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 0)
                    ),
                    1
                );

                assertThat(responses1)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_1_SIZE, BATCH_1_RECORDS));
                assertThat(getLogStartOffset()).isEqualTo(BATCH_1_RECORDS);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(BATCH_1_RECORDS, EXPECTED_HIGH_WATERMARK, BATCH_2_SIZE + BATCH_3_SIZE));
                assertThat(controlPlane.getFilesToDelete()).isEmpty();

                final var responses2 = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 0)
                    ),
                    1
                );

                assertThat(responses2)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_2_SIZE, BATCH_1_RECORDS + BATCH_2_RECORDS));
                assertThat(getLogStartOffset()).isEqualTo(BATCH_1_RECORDS + BATCH_2_RECORDS);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(BATCH_3_RECORDS, EXPECTED_HIGH_WATERMARK, BATCH_3_SIZE));
                assertThat(controlPlane.getFilesToDelete()).isEmpty();

                final var responses3 = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 0)
                    ),
                    1
                );

                assertThat(responses3)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_3_SIZE, EXPECTED_HIGH_WATERMARK));
                assertThat(getLogStartOffset()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(EXPECTED_HIGH_WATERMARK, EXPECTED_HIGH_WATERMARK, 0));
                assertThat(controlPlane.getFilesToDelete()).containsExactly(
                    new FileToDelete(FILE_NAME, TimeUtils.now(time))
                );
            }

            @Test
            void deleteAll() {
                addBatches();
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP + 1);
                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 0)
                    ),
                    0
                );

                final long newLogStartOffset = EXPECTED_HIGH_WATERMARK;
                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(3, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE, newLogStartOffset));
                assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(newLogStartOffset, EXPECTED_HIGH_WATERMARK, 0));
                assertThat(controlPlane.getFilesToDelete()).containsExactly(
                    new FileToDelete(FILE_NAME, TimeUtils.now(time))
                );
            }

            @Test
            void futureTimestamps() {
                controlPlane.commitFile(FILE_NAME, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_1_SIZE, 0, BATCH_1_RECORDS - 1, START_TIME * 10, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(1, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_2_SIZE, 0, BATCH_2_RECORDS - 1, BATCH_2_MAX_TIMESTAMP, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_3_SIZE, 0, BATCH_3_RECORDS - 1, BATCH_3_MAX_TIMESTAMP, TimestampType.CREATE_TIME)
                    ));
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 0)
                    ),
                    0
                );

                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(0, 0, 0));
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(0, EXPECTED_HIGH_WATERMARK, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE));
            }

            /**
             * Test that if we encounter a batch that doesn't breach the policy, we don't look further.
             */
            @Test
            void nonBreachingBatchStopsScanning() {
                // Timestamps relative to now are:
                // -10 -9 -8 -1 -7 -6 -5
                // and we want to delete everything that is older than -4.

                final int batch4Size = 991;
                final int batch5Size = 992;
                final int batch6Size = 993;
                final int batch7Size = 994;
                final long batch4Records = 40;
                final long batch5Records = 50;
                final long batch6Records = 60;
                final long batch7Records = 70;
                final long expectedHighWatermark = BATCH_1_RECORDS + BATCH_2_RECORDS + BATCH_3_RECORDS
                    + batch4Records + batch5Records + batch6Records + batch7Records;

                controlPlane.commitFile(FILE_NAME, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_1_SIZE, 0, BATCH_1_RECORDS - 1, START_TIME - 10, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(1, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_2_SIZE, 0, BATCH_2_RECORDS - 1, START_TIME - 9, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_3_SIZE, 0, BATCH_3_RECORDS - 1, START_TIME - 8, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batch4Size, 0, batch4Records - 1, START_TIME - 1, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batch5Size, 0, batch5Records - 1, START_TIME - 7, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batch6Size, 0, batch6Records - 1, START_TIME - 6, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batch7Size, 0, batch7Records - 1, START_TIME - 5, TimestampType.CREATE_TIME)
                    ));
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(expectedHighWatermark);

                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 4)
                    ),
                    0
                );

                // Only the first 3 batches must be deleted, despite more batches breaching the policy.
                final long newLogStartOffset = BATCH_1_RECORDS + BATCH_2_RECORDS + BATCH_3_RECORDS;
                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(3, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE, newLogStartOffset));
                assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
                assertThat(getHighWatermark()).isEqualTo(expectedHighWatermark);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(newLogStartOffset, expectedHighWatermark, batch4Size + batch5Size + batch6Size + batch7Size));
            }

            /**
             * Test that the retention policy works on batches with {@code LOG_APPEND_TIME} timestamp type.
             */
            @Test
            void testAppendTime() {
                final long batchMaxTimestamp = START_TIME * 100;  // far into the future

                // START_TIME
                controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_1_SIZE, 0, BATCH_1_RECORDS - 1, batchMaxTimestamp, TimestampType.LOG_APPEND_TIME)
                    ));

                // START_TIME + 1
                time.setCurrentTimeMs(START_TIME + 1);
                controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_2_SIZE, 0, BATCH_2_RECORDS - 1, batchMaxTimestamp, TimestampType.LOG_APPEND_TIME)
                    ));

                // START_TIME + 2
                time.setCurrentTimeMs(START_TIME + 2);
                controlPlane.commitFile("obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_3_SIZE, 0, BATCH_3_RECORDS - 1, batchMaxTimestamp, TimestampType.LOG_APPEND_TIME)
                    ));
                assertThat(getLogStartOffset()).isEqualTo(0L);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

                final var responses = controlPlane.enforceRetention(
                    List.of(
                        new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, 1)
                    ), 0
                );

                final long newLogStartOffset = BATCH_1_RECORDS;
                assertThat(responses)
                    .containsExactly(EnforceRetentionResponse.success(1, BATCH_1_SIZE, newLogStartOffset));
                assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
                assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
                assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                    .containsExactly(GetLogInfoResponse.success(newLogStartOffset, EXPECTED_HIGH_WATERMARK, BATCH_2_SIZE + BATCH_3_SIZE));
            }
        }

        /**
         * Test both retention policies together
         * and make sure that a batch is deleted if it breaches at least one.
         */
        @Test
        void bothRetentions() {
            addBatches();
            assertThat(getLogStartOffset()).isEqualTo(0L);
            assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

            // Time retention deletes only batch 1. Size retention deletes batch 1 and 2.
            time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP);
            final var responses = controlPlane.enforceRetention(
                List.of(
                    new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, BATCH_3_SIZE, BATCH_3_MAX_TIMESTAMP - BATCH_2_MAX_TIMESTAMP)
                ),
                0
            );

            final long newLogStartOffset = BATCH_1_RECORDS + BATCH_2_RECORDS;
            assertThat(responses)
                .containsExactly(EnforceRetentionResponse.success(2, BATCH_1_SIZE + BATCH_2_SIZE, newLogStartOffset));
            assertThat(getLogStartOffset()).isEqualTo(newLogStartOffset);
            assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(newLogStartOffset, EXPECTED_HIGH_WATERMARK, BATCH_3_SIZE));
        }

        @Test
        void notEnforced() {
            addBatches();
            assertThat(getLogStartOffset()).isEqualTo(0L);
            assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);

            time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP * 10);
            final var responses = controlPlane.enforceRetention(
                List.of(
                    new EnforceRetentionRequest(EXISTING_TOPIC_1_ID, 0, -1, -1)
                ),
                0
            );

            assertThat(responses)
                .containsExactly(EnforceRetentionResponse.success(0, 0, 0));
            assertThat(getLogStartOffset()).isEqualTo(0L);
            assertThat(getHighWatermark()).isEqualTo(EXPECTED_HIGH_WATERMARK);
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(0, EXPECTED_HIGH_WATERMARK, BATCH_1_SIZE + BATCH_2_SIZE + BATCH_3_SIZE));
        }

        @Test
        void multiplePartitions() {
            // Write the same data to two partitions, apply the same retention policies.

            for (final TopicIdPartition topicIdPartition : List.of(EXISTING_TOPIC_1_ID_PARTITION_0, EXISTING_TOPIC_1_ID_PARTITION_1)) {
                controlPlane.commitFile(topicIdPartition.toString(), ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, topicIdPartition, 1, BATCH_1_SIZE, 0, BATCH_1_RECORDS - 1, BATCH_1_MAX_TIMESTAMP, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(1, topicIdPartition, 1, BATCH_2_SIZE, 0, BATCH_2_RECORDS - 1, BATCH_2_MAX_TIMESTAMP, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(2, topicIdPartition, 1, BATCH_3_SIZE, 0, BATCH_3_RECORDS - 1, BATCH_3_MAX_TIMESTAMP, TimestampType.CREATE_TIME)
                    ));
            }

            assertThat(controlPlane.listOffsets(List.of(
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, EARLIEST_TIMESTAMP),
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_1, EARLIEST_TIMESTAMP)
            ))).map(ListOffsetsResponse::offset).containsExactly(0L, 0L);
            assertThat(controlPlane.listOffsets(List.of(
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, LATEST_TIMESTAMP),
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_1, LATEST_TIMESTAMP)
            ))).map(ListOffsetsResponse::offset).containsExactly(EXPECTED_HIGH_WATERMARK, EXPECTED_HIGH_WATERMARK);

            // Time retention deletes only batch 1. Size retention deletes batch 1 and 2.
            time.setCurrentTimeMs(BATCH_3_MAX_TIMESTAMP);
            final var responses = controlPlane.enforceRetention(
                List.of(
                    new EnforceRetentionRequest(
                        EXISTING_TOPIC_1_ID_PARTITION_0.topicId(), EXISTING_TOPIC_1_ID_PARTITION_0.partition(),
                        BATCH_3_SIZE, BATCH_3_MAX_TIMESTAMP - BATCH_2_MAX_TIMESTAMP),
                    new EnforceRetentionRequest(
                        EXISTING_TOPIC_1_ID_PARTITION_1.topicId(), EXISTING_TOPIC_1_ID_PARTITION_1.partition(),
                        BATCH_3_SIZE, BATCH_3_MAX_TIMESTAMP - BATCH_2_MAX_TIMESTAMP)
                ),
                0
            );

            final long newLogStartOffset = BATCH_1_RECORDS + BATCH_2_RECORDS;
            assertThat(responses)
                .containsExactly(
                    EnforceRetentionResponse.success(2, BATCH_1_SIZE + BATCH_2_SIZE, newLogStartOffset),
                    EnforceRetentionResponse.success(2, BATCH_1_SIZE + BATCH_2_SIZE, newLogStartOffset)
                );

            assertThat(controlPlane.listOffsets(List.of(
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, EARLIEST_TIMESTAMP),
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_1, EARLIEST_TIMESTAMP)
            ))).map(ListOffsetsResponse::offset).containsExactly(newLogStartOffset, newLogStartOffset);
            assertThat(controlPlane.listOffsets(List.of(
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, LATEST_TIMESTAMP),
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_1, LATEST_TIMESTAMP)
            ))).map(ListOffsetsResponse::offset).containsExactly(EXPECTED_HIGH_WATERMARK, EXPECTED_HIGH_WATERMARK);
        }

        private void addBatches() {
            controlPlane.commitFile(FILE_NAME, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                    CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_1_SIZE, 0, BATCH_1_RECORDS - 1, BATCH_1_MAX_TIMESTAMP, TimestampType.CREATE_TIME),
                    CommitBatchRequest.of(1, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_2_SIZE, BATCH_1_RECORDS, BATCH_1_RECORDS + BATCH_2_RECORDS - 1, BATCH_2_MAX_TIMESTAMP, TimestampType.CREATE_TIME),
                    CommitBatchRequest.of(2, EXISTING_TOPIC_1_ID_PARTITION_0, 1, BATCH_3_SIZE, BATCH_1_RECORDS + BATCH_2_RECORDS, BATCH_1_RECORDS + BATCH_2_RECORDS + BATCH_3_RECORDS - 1, BATCH_3_MAX_TIMESTAMP, TimestampType.CREATE_TIME)
                ));
        }

        private long getLogStartOffset() {
            return controlPlane.listOffsets(List.of(
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, EARLIEST_TIMESTAMP)
            )).get(0).offset();
        }

        private long getHighWatermark() {
            return controlPlane.listOffsets(List.of(
                new ListOffsetsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, LATEST_TIMESTAMP)
            )).get(0).offset();
        }
    }

    @Test
    void deleteFiles() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        controlPlane.commitFile(objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));
        final int file2Partition0Size = (int) FILE_SIZE / 2;
        final int file2Partition1Size = (int) FILE_SIZE - file2Partition0Size;
        controlPlane.commitFile(objectKey2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, file2Partition0Size, 0, 0, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_2), 1, file2Partition1Size, 1, 1, 2000, TimestampType.CREATE_TIME)
            ));

        time.sleep(1001);  // advance time
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));

        // objectKey2 is kept alive by the second topic, which isn't deleted
        assertThat(controlPlane.getFilesToDelete()).containsExactly(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );

        // Delete files from Control Plane
        controlPlane.deleteFiles(new DeleteFilesRequest(Set.of(objectKey1)));
        assertThat(controlPlane.getFilesToDelete()).isEmpty();
    }

    @Disabled
    @Test
    void isSafeToDeleteFileFile() {
        assertThat(controlPlane.isSafeToDeleteFile("test")).isTrue();
    }

    @Test
    void isNotSafeToDeleteFile() {
        final String objectKey = "test";
        controlPlane.commitFile(objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                List.of(
                CommitBatchRequest.of(0, new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));
        assertThat(controlPlane.isSafeToDeleteFile(objectKey)).isFalse();
    }

    /**
     * Cases are validated against classic topics.
     */
    @Nested
    class ListOffsets {
        private static final String newTopic1Name = "newTopic1";
        private static final Uuid newTopic1Id = new Uuid(12345, 67890);
        private static final String newTopic2Name = "newTopic2";
        private static final Uuid newTopic2Id = new Uuid(88888, 99999);
        private static final TopicIdPartition tidp1 = new TopicIdPartition(newTopic1Id, 0, newTopic1Name);
        private static final TopicIdPartition tidp2 = new TopicIdPartition(newTopic2Id, 0, newTopic2Name);

        private static final long TP1_BATCH1_RECORDS = 10;
        private static final long TP1_BATCH1_BASE_OFFSET = 0;
        private static final long TP1_BATCH2_RECORDS = 100;
        private static final long TP1_BATCH2_BASE_OFFSET = TP1_BATCH1_RECORDS;

        private static final long TP2_BATCH1_RECORDS = 20;
        private static final long TP2_BATCH1_BASE_OFFSET = 0;
        private static final long TP2_BATCH2_RECORDS = 200;
        private static final long TP2_BATCH2_BASE_OFFSET = TP2_BATCH1_RECORDS;

        private static final long TP1_BATCH1_TIMESTAMP = 1000;
        private static final long TP1_BATCH2_TIMESTAMP = 2000;
        private static final long TP2_BATCH1_TIMESTAMP = 1001;
        private static final long TP2_BATCH2_TIMESTAMP = 2001;

        @Nested
        class Empty {
            @BeforeEach
            void prepare() {
                controlPlane.createTopicAndPartitions(Set.of(
                    new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 1)
                ));
            }

            @Test
            void latestTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, LATEST_TIMESTAMP)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, 0)
                );
            }

            @ParameterizedTest
            @ValueSource(longs = {EARLIEST_TIMESTAMP, EARLIEST_LOCAL_TIMESTAMP})
            void earliestTimestamp(final long timestamp) {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, timestamp)
                ));

                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, 0)
                );
            }

            @Test
            void maxTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, MAX_TIMESTAMP)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1)
                );
            }

            @Test
            void latestTieredTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, LATEST_TIERED_TIMESTAMP)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1)
                );
            }

            @Test
            void realTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, 0),
                    new ListOffsetsRequest(tidp1, 111),
                    new ListOffsetsRequest(tidp1, Long.MAX_VALUE)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1),
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1),
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1)
                );
            }
        }

        @Nested
        class NonEmpty {
            private long batch1CommitTimestamp;
            private long batch2CommitTimestamp;

            @BeforeEach
            void prepare() {
                batch1CommitTimestamp = time.milliseconds();
                controlPlane.createTopicAndPartitions(Set.of(
                    new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 1),
                    new CreateTopicAndPartitionsRequest(newTopic2Id, newTopic2Name, 1)
                ));

                controlPlane.commitFile("a1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, tidp1, 0, 1, TP1_BATCH1_BASE_OFFSET, TP1_BATCH1_BASE_OFFSET + TP1_BATCH1_RECORDS - 1, TP1_BATCH1_TIMESTAMP, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(0, tidp2, 0, 1, TP2_BATCH1_BASE_OFFSET, TP2_BATCH1_BASE_OFFSET + TP2_BATCH1_RECORDS - 1, TP2_BATCH1_TIMESTAMP, TimestampType.LOG_APPEND_TIME)
                    ));

                time.sleep(10);
                batch2CommitTimestamp = time.milliseconds();
                controlPlane.commitFile("a2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
                    List.of(
                        CommitBatchRequest.of(0, tidp1, 0, 1, TP1_BATCH2_BASE_OFFSET, TP1_BATCH2_BASE_OFFSET + TP1_BATCH2_RECORDS - 1, TP1_BATCH2_TIMESTAMP, TimestampType.CREATE_TIME),
                        CommitBatchRequest.of(0, tidp2, 0, 1, TP2_BATCH2_BASE_OFFSET, TP2_BATCH2_BASE_OFFSET + TP2_BATCH2_RECORDS - 1, TP2_BATCH2_TIMESTAMP, TimestampType.LOG_APPEND_TIME)
                    ));
            }

            @Test
            void latestTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, LATEST_TIMESTAMP),
                    new ListOffsetsRequest(tidp2, LATEST_TIMESTAMP)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, TP1_BATCH1_RECORDS + TP1_BATCH2_RECORDS),  // high watermark
                    ListOffsetsResponse.success(tidp2, NO_TIMESTAMP, TP2_BATCH1_RECORDS + TP2_BATCH2_RECORDS)  // high watermark
                );
            }

            @ParameterizedTest
            @ValueSource(longs = {EARLIEST_TIMESTAMP, EARLIEST_LOCAL_TIMESTAMP})
            void earliestTimestamp(final long timestamp) {
                controlPlane.deleteRecords(List.of(new DeleteRecordsRequest(tidp1, 1)));

                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, timestamp),
                    new ListOffsetsRequest(tidp2, timestamp)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, 1),
                    ListOffsetsResponse.success(tidp2, NO_TIMESTAMP, 0)
                );
            }

            @Test
            void maxTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, MAX_TIMESTAMP),
                    new ListOffsetsRequest(tidp2, MAX_TIMESTAMP)
                ));
                // We expect the offsets of the records with the max timestamps.
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, TP1_BATCH2_TIMESTAMP, TP1_BATCH1_RECORDS + TP1_BATCH2_RECORDS - 1),
                    ListOffsetsResponse.success(tidp2, batch2CommitTimestamp, TP2_BATCH1_RECORDS + TP2_BATCH2_RECORDS - 1)
                );
            }

            @Test
            void latestTieredTimestamp() {
                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    new ListOffsetsRequest(tidp1, LATEST_TIERED_TIMESTAMP),
                    new ListOffsetsRequest(tidp2, LATEST_TIERED_TIMESTAMP)
                ));
                assertThat(result).containsExactly(
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1),
                    ListOffsetsResponse.success(tidp2, NO_TIMESTAMP, -1)
                );
            }

            @Test
            void realTimestamp() {
                controlPlane.deleteRecords(List.of(new DeleteRecordsRequest(tidp1, 1)));

                final List<ListOffsetsResponse> result = controlPlane.listOffsets(List.of(
                    // Earliest possible offset.
                    new ListOffsetsRequest(tidp1, 0),
                    new ListOffsetsRequest(tidp2, 0),
                    // Before the first batch.
                    new ListOffsetsRequest(tidp1, 111),
                    new ListOffsetsRequest(tidp2, 111),
                    // Equal to the first batch.
                    new ListOffsetsRequest(tidp1, TP1_BATCH1_TIMESTAMP),
                    new ListOffsetsRequest(tidp2, batch1CommitTimestamp),
                    // After the first batch.
                    new ListOffsetsRequest(tidp1, TP1_BATCH1_TIMESTAMP + 1),
                    new ListOffsetsRequest(tidp2, batch1CommitTimestamp + 1),
                    // Equals to the last batch.
                    new ListOffsetsRequest(tidp1, TP1_BATCH2_TIMESTAMP),
                    new ListOffsetsRequest(tidp2, batch2CommitTimestamp),
                    // After the last batch.
                    new ListOffsetsRequest(tidp1, Long.MAX_VALUE),
                    new ListOffsetsRequest(tidp2, Long.MAX_VALUE)
                ));
                assertThat(result).containsExactly(
                    // Earliest possible offset.
                    ListOffsetsResponse.success(tidp1, TP1_BATCH1_TIMESTAMP, 1),
                    ListOffsetsResponse.success(tidp2, batch1CommitTimestamp, 0),
                    // Before the first batch.
                    ListOffsetsResponse.success(tidp1, TP1_BATCH1_TIMESTAMP, 1),
                    ListOffsetsResponse.success(tidp2, batch1CommitTimestamp, 0),
                    // Equal to the first batch.
                    ListOffsetsResponse.success(tidp1, TP1_BATCH1_TIMESTAMP, 1),
                    ListOffsetsResponse.success(tidp2, batch1CommitTimestamp, 0),
                    // After the first batch.
                    ListOffsetsResponse.success(tidp1, TP1_BATCH2_TIMESTAMP, TP1_BATCH2_BASE_OFFSET),
                    ListOffsetsResponse.success(tidp2, batch2CommitTimestamp, TP2_BATCH2_BASE_OFFSET),
                    // Equals to the last batch.
                    ListOffsetsResponse.success(tidp1, TP1_BATCH2_TIMESTAMP, TP1_BATCH2_BASE_OFFSET),
                    ListOffsetsResponse.success(tidp2, batch2CommitTimestamp, TP2_BATCH2_BASE_OFFSET),
                    // After the last batch.
                    ListOffsetsResponse.success(tidp1, NO_TIMESTAMP, -1),
                    ListOffsetsResponse.success(tidp2, NO_TIMESTAMP, -1)
                );
            }
        }
    }

    @Disabled
    @Nested
    class GetFileMergeWorkItem {
        @Test
        void empty() {
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();
            // Not a mistake, checking again, because the behavior may have changed.
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();
        }

        @Test
        void notEnoughData() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 10;
            for (int i = 0; i < 5; i++) {
                controlPlane.commitFile(String.format("obj%d", i), ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, i, fileSize,
                        List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME))
                );
            }

            assertThat(controlPlane.getFileMergeWorkItem()).isNull();
            // Not a mistake, checking again, because the behavior may have changed.
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();
        }

        @Test
        void enoughData() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles = List.of(
                new FileMergeWorkItem.File(1L, "obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(2L, "obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(2, "obj1", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME)))
                )
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles));

            // Not it should not return the same work item again, because the files are locked.
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();

            time.sleep(FILE_MERGE_LOCK_PERIOD.toMillis());

            // Wait for the lock period to end and try again.
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(2L, TimeUtils.now(time), expectedFiles));
        }

        @Test
        void enoughDataForMultipleWorkItems() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            final long committedAt = time.milliseconds();
            // Commit 3 files, that's enough only for 1 merge work item.
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 3, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 3000, TimestampType.CREATE_TIME)));

            // Get the merge work item.
            final List<FileMergeWorkItem.File> expectedFiles1 = List.of(
                new FileMergeWorkItem.File(1L, "obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(2L, "obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(2, "obj1", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles1));

            // Commit one more file.
            controlPlane.commitFile("obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 4000, TimestampType.CREATE_TIME)));

            // Now it's enough to have one more merge work item.
            final List<FileMergeWorkItem.File> expectedFiles2 = List.of(
                new FileMergeWorkItem.File(3L, "obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(3, "obj2", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 202, 302, committedAt, 3000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(4L, "obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(4, "obj3", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 303, 403, committedAt, 4000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(2L, TimeUtils.now(time), expectedFiles2));
        }

        @Test
        void singleFileIsBiggerThanThreshold() {
            // In practice, it doesn't make sense to have such small threshold, but still we need to verify this works.
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD;
            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 0, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles = List.of(
                new FileMergeWorkItem.File(1L, "obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles));
        }

        @Test
        void mustSkipAlreadyMergedFiles() throws FileMergeWorkItemNotExist {
            final long batchSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            final long committedAt = time.milliseconds();
            // Commit 3 files, that's enough for 1 merge work items.
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, batchSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, batchSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 3, batchSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 3000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles1 = List.of(
                new FileMergeWorkItem.File(1L, "obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, batchSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(2L, "obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, batchSize,
                    List.of(new BatchInfo(2, "obj1", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles1));

            // We intentionally make the batch size here smaller to make it fit into the following merging operation.
            controlPlane.commitFileMergeWorkItem(1L, "obj_merged", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, batchSize,
                List.of(
                    new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                    new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, batchSize, batchSize + batchSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME), List.of(2L))
                )
            );

            // The already merged file must not be included into the merge operation and without it there's not enough data to merge.
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();

            // Commit more files and try merging again.
            controlPlane.commitFile("obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, batchSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 4000, TimestampType.CREATE_TIME)));

            // File 4 is the merged file, batches 4 and 5 are in it.
            final List<FileMergeWorkItem.File> expectedFiles2 = List.of(
                new FileMergeWorkItem.File(3L, "obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, batchSize,
                    List.of(new BatchInfo(3, "obj2", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 202, 302, committedAt, 3000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(5L, "obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, batchSize,
                    List.of(new BatchInfo(6, "obj3", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 303, 403, committedAt, 4000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(2L, TimeUtils.now(time), expectedFiles2));
        }
    }

    @Test
    void commitDuplicateFileNames() {
        final String objectKey = "a";

        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(1, EXISTING_TOPIC_1_ID_PARTITION_0, 1, 10, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9);
        final List<CommitBatchResponse> responses = controlPlane.commitFile(
            objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1));
        assertThat(responses).containsExactly(
            CommitBatchResponse.success(0, time.milliseconds(), 0, request1)
        );

        assertThatThrownBy(() -> controlPlane.commitFile(objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request1)))
            .isInstanceOf(ControlPlaneException.class)
            .hasMessage("Error committing file");
    }

    @Test
    void testCommitDuplicates() {
        final int batchSize = 10;
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(1, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batchSize, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9);

        final List<CommitBatchRequest> requests = List.of(
            request1,
            CommitBatchRequest.idempotent(2, EXISTING_TOPIC_1_ID_PARTITION_0, 2, batchSize, 20, 29, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 10, 19),
            CommitBatchRequest.idempotent(3, EXISTING_TOPIC_1_ID_PARTITION_0, 3, batchSize, 30, 39, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 20, 29),
            CommitBatchRequest.idempotent(4, EXISTING_TOPIC_1_ID_PARTITION_0, 4, batchSize, 40, 49, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 30, 39),
            CommitBatchRequest.idempotent(5, EXISTING_TOPIC_1_ID_PARTITION_0, 5, batchSize, 50, 59, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 40, 49)
        );
        final List<CommitBatchResponse> responses = controlPlane.commitFile(
            "a", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, requests);
        assertThat(responses).containsExactly(
            CommitBatchResponse.success(0, time.milliseconds(), 0, requests.get(0)),
            CommitBatchResponse.success(10, time.milliseconds(), 0, requests.get(1)),
            CommitBatchResponse.success(20, time.milliseconds(), 0, requests.get(2)),
            CommitBatchResponse.success(30, time.milliseconds(), 0, requests.get(3)),
            CommitBatchResponse.success(40, time.milliseconds(), 0, requests.get(4))
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 50, batchSize * 5));

        // Try to produce a duplicate.
        final String duplicateFile1Key = "b";
        final List<CommitBatchResponse> dupResponses = controlPlane.commitFile(duplicateFile1Key, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, batchSize, List.of(request1));
        assertThat(dupResponses).containsExactly(
            CommitBatchResponse.ofDuplicate(0, time.milliseconds(), 0)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
            Integer.MAX_VALUE, 0
        );
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(
                Errors.NONE,
                List.of(
                    new BatchInfo(1L, "a", new BatchMetadata(RecordBatch.CURRENT_MAGIC_VALUE, EXISTING_TOPIC_1_ID_PARTITION_0, 1, 10, 0, 9, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, "a", new BatchMetadata(RecordBatch.CURRENT_MAGIC_VALUE, EXISTING_TOPIC_1_ID_PARTITION_0, 2, 10, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                    new BatchInfo(3L, "a", new BatchMetadata(RecordBatch.CURRENT_MAGIC_VALUE, EXISTING_TOPIC_1_ID_PARTITION_0, 3, 10, 20, 29, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                    new BatchInfo(4L, "a", new BatchMetadata(RecordBatch.CURRENT_MAGIC_VALUE, EXISTING_TOPIC_1_ID_PARTITION_0, 4, 10, 30, 39, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME)),
                    new BatchInfo(5L, "a", new BatchMetadata(RecordBatch.CURRENT_MAGIC_VALUE, EXISTING_TOPIC_1_ID_PARTITION_0, 5, 10, 40, 49, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME))
                ),
                0,
                50
            )
        );
        // The file must be deleted as it doesn't contain alive batches after rejecting its only batch.
        assertThat(controlPlane.getFilesToDelete()).singleElement().extracting(FileToDelete::objectKey).isEqualTo(duplicateFile1Key);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 50, batchSize * 5));

        // Make the control plane to forget the original.
        final List<CommitBatchRequest> requests2 = List.of(
            CommitBatchRequest.idempotent(1, EXISTING_TOPIC_1_ID_PARTITION_0, 1, 10, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 50, 59)
        );
        final List<CommitBatchResponse> responses2 = controlPlane.commitFile(
            "c", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, requests2);
        assertThat(responses2).containsExactly(
            CommitBatchResponse.success(50, time.milliseconds(), 0, requests2.get(0))
        );
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 60, batchSize * 6));

        // Try to produce a duplicate again.
        final String duplicateFile2Key = "d";
        final List<CommitBatchResponse> dupResponses2 = controlPlane.commitFile(
            duplicateFile2Key, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, batchSize, List.of(request1));
        assertThat(dupResponses2).containsExactly(
            CommitBatchResponse.sequenceOutOfOrder(request1)
        );
        // The file must also be deleted.
        assertThat(controlPlane.getFilesToDelete()).map(FileToDelete::objectKey).containsExactly(duplicateFile1Key, duplicateFile2Key);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 60, batchSize * 6));
    }

    @Test
    void testOutOfOrderNewEpoch() {
        final String fileKey = "a";
        final CommitBatchRequest request = CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 1, 10);
        final CommitBatchResponse response = controlPlane.commitFile(fileKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request)).get(0);

        assertThat(response)
            .extracting(CommitBatchResponse::errors, CommitBatchResponse::isDuplicate)
            .containsExactly(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, false);

        // The second file must be deleted as it doesn't contain alive batches after rejecting its only batch.
        assertThat(controlPlane.getFilesToDelete()).singleElement().extracting(FileToDelete::objectKey).isEqualTo(fileKey);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 0, 0));
    }

    @ParameterizedTest
    // 15 is the first sequence number for the second batch
    @CsvSource({
        "14, 13", // lower than 15
        "14, 14", // lower than 15
        "14, 16", // larger than 15
        "2147483647, 1" // not zero
    })
    void testOutOfOrderSequenceSameCall(final int lastSeq, final int nextSeq) {
        final int batchSize = 10;
        final CommitBatchRequest request0 = CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batchSize, 0, 10, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, lastSeq);
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 2, batchSize, 0, 20, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, nextSeq, nextSeq + 10);
        final List<CommitBatchResponse> responses = controlPlane.commitFile("a", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request0, request1));

        assertThat(responses)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.NONE, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 11, batchSize));  // only one batch
    }

    @ParameterizedTest
    // 15 is the first sequence number for the second batch
    @CsvSource({
        "14, 13", // lower than 15
        "14, 14", // lower than 15
        "14, 16", // larger than 15
        "2147483647, 1" // not zero
    })
    void testOutOfOrderSequenceDifferentCalls(final int lastSeq, final int nextSeq) {
        final int batchSize = (int) FILE_SIZE;
        final List<CommitBatchResponse> responses0 = controlPlane.commitFile("a", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(
            CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batchSize, 0, 10, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, lastSeq)
        ));
        assertThat(responses0)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.NONE);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 11, batchSize));

        final var file1Size = 100;
        final String file1Key = "b";
        final List<CommitBatchResponse> responses1 = controlPlane.commitFile(file1Key, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file1Size, List.of(
            CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 2, file1Size, 0, 20, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, nextSeq, nextSeq + 10)
        ));
        assertThat(responses1)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);

        // The second file must be deleted as it doesn't contain alive batches after rejecting its only batch.
        assertThat(controlPlane.getFilesToDelete()).singleElement().extracting(FileToDelete::objectKey).isEqualTo(file1Key);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 11, batchSize));
    }

    @Test
    void testInvalidProducerEpochSameCall() {
        final int batchSize = 10;
        final CommitBatchRequest request0 = CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batchSize, 10, 24, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 14);
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 2, batchSize, 25, 35, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 2, 15, 25);
        final List<CommitBatchResponse> responses = controlPlane.commitFile("a", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(request0, request1));

        assertThat(responses)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.NONE, Errors.INVALID_PRODUCER_EPOCH);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 15, batchSize));  // only one batch
    }

    @Test
    void testInvalidProducerEpochDifferentCalls() {
        final int batchSize = (int) FILE_SIZE;
        final List<CommitBatchResponse> responses0 = controlPlane.commitFile("a", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE, List.of(
            CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 1, batchSize, 10, 24, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 14)
        ));
        assertThat(responses0)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.NONE);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 15, batchSize));

        final var file1Size = 100;
        final String file1Key = "b";
        final List<CommitBatchResponse> responses1 = controlPlane.commitFile(file1Key, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, file1Size, List.of(
            CommitBatchRequest.idempotent(0, EXISTING_TOPIC_1_ID_PARTITION_0, 2, file1Size, 25, 35, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 2, 15, 25)
        ));
        assertThat(responses1)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.INVALID_PRODUCER_EPOCH);

        // The second file must be deleted as it doesn't contain alive batches after rejecting its only batch.
        assertThat(controlPlane.getFilesToDelete()).singleElement().extracting(FileToDelete::objectKey).isEqualTo(file1Key);
        assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
            .containsExactly(GetLogInfoResponse.success(0, 15, batchSize));
    }

    @Disabled
    @Nested
    class CommitFileMergeWorkItem {
        @Test
        void workItemNotExist() {
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(100, "obj", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, 0, List.of()))
                .isInstanceOf(FileMergeWorkItemNotExist.class)
                .extracting("workItemId").isEqualTo(100L);

            // not expecting to delete any files on this scenario
            final var filesToDelete = controlPlane.getFilesToDelete();
            assertThat(filesToDelete).isEmpty();
        }

        @Test
        void batchWithoutParents() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            final var batch = new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 0, 0, 0, TimestampType.CREATE_TIME), List.of());
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(workItemId, "obj", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, 0, List.of(batch)))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessage("Invalid parent batch count 0 in " + batch);

            assertThat(controlPlane.getFilesToDelete()).containsExactly(
                new FileToDelete("obj", TimeUtils.now(time))
            );
        }

        @Test
        void batchWithTooManyParents() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            final var batch = new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 0, 0, 0, TimestampType.CREATE_TIME),
                List.of(1L, 2L));
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(workItemId, "obj", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, 0, List.of(batch)))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessage("Invalid parent batch count 2 in " + batch);

            assertThat(controlPlane.getFilesToDelete()).containsExactly(
                new FileToDelete("obj", TimeUtils.now(time))
            );
        }

        @Test
        void batchIsNotPartOfWorkItem() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 3, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 3000, TimestampType.CREATE_TIME)));

            final FileMergeWorkItem fileMergeWorkItem = controlPlane.getFileMergeWorkItem();
            assertThat(fileMergeWorkItem.files()).hasSize(2);

            final var batch = new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 0, 0, 0, TimestampType.CREATE_TIME),
                List.of(3L));
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(fileMergeWorkItem.workItemId(), "obj", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, 0, List.of(batch)))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessage("Batch 3 is not part of work item in " + batch);

            assertThat(controlPlane.getFilesToDelete()).containsExactly(
                new FileToDelete("obj", TimeUtils.now(time))
            );
        }

        @Test
        void simpleManyPartitionMerge() {
            controlPlane.createTopicAndPartitions(Set.of(
                new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, 2)
            ));

            final long fileSize1 = FILE_MERGE_SIZE_THRESHOLD / 3 + 1;
            final int file1Batch1Size = (int) fileSize1;

            final long fileSize2 = fileSize1;
            final int file2Batch1Size = (int) fileSize2 / 2;
            final int file2Batch2Size = (int) fileSize2 - file2Batch1Size;

            final long fileSize3 = FILE_MERGE_SIZE_THRESHOLD - fileSize1 - fileSize2;
            final int file3Batch1Size = (int) fileSize3;

            final long fileSize4 = 1;

            final int file4Batch1Size = (int) fileSize4;

            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize1,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize2,
                    List.of(
                    CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, file2Batch1Size, 0, 100, 2000, TimestampType.LOG_APPEND_TIME),
                    CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_1, file2Batch1Size, file2Batch2Size, 0, 50, 3000, TimestampType.CREATE_TIME)
                ));
            controlPlane.commitFile("obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 3, fileSize3,
                    List.of(
                    CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_1, 0, file3Batch1Size, 0, 200, 4000, TimestampType.LOG_APPEND_TIME)
                ));
            controlPlane.commitFile("obj_untouched", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 4, fileSize4,
                    List.of(
                    CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, file4Batch1Size, 0, 23, 5000, TimestampType.LOG_APPEND_TIME)
                ));
            final int partition0LogSize = file1Batch1Size + file2Batch1Size + file4Batch1Size;
            final int partition1LogSize = file2Batch2Size + file3Batch1Size;
            assertThat(controlPlane.getLogInfo(List.of(
                new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0),
                new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 1)
            ))).containsExactly(
                GetLogInfoResponse.success(0, 226, partition0LogSize),
                GetLogInfoResponse.success(0, 252, partition1LogSize)
            );

            time.sleep(1);
            final Instant mergedAt = TimeUtils.now(time);

            final FileMergeWorkItem fileMergeWorkItem = controlPlane.getFileMergeWorkItem();
            // obj_untouched must not be here.
            assertThat(fileMergeWorkItem.files()).map(FileMergeWorkItem.File::objectKey).containsExactlyInAnyOrder("obj1", "obj2", "obj3");

            final var workItemId = fileMergeWorkItem.workItemId();
            final List<MergedFileBatch> mergedFileBatches = List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize1, file2Batch1Size, 101, 201, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + file2Batch1Size, file2Batch2Size, 0, 50, committedAt, 3000, TimestampType.CREATE_TIME), List.of(3L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + fileSize2, file3Batch1Size, 50, 250, committedAt, 4000, TimestampType.CREATE_TIME), List.of(4L))
            );
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize1 + fileSize2 + fileSize3, mergedFileBatches);
            // Log sizes didn't change.
            assertThat(controlPlane.getLogInfo(List.of(
                new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0),
                new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 1)
            ))).containsExactly(
                GetLogInfoResponse.success(0, 226, partition0LogSize),
                GetLogInfoResponse.success(0, 252, partition1LogSize)
            );

            final var findBatchResult = controlPlane.findBatches(
                List.of(
                    new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE),
                    new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, Integer.MAX_VALUE)
                ), Integer.MAX_VALUE, 0
            );
            assertThat(findBatchResult).containsExactly(
                FindBatchResponse.success(List.of(
                    new BatchInfo(6L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0L, file1Batch1Size, 0L, 100L, committedAt, 1000L, TimestampType.CREATE_TIME)),
                    new BatchInfo(7L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, file1Batch1Size, file2Batch1Size, 101L, 201L, committedAt, 2000L, TimestampType.LOG_APPEND_TIME)),
                    new BatchInfo(5L, "obj_untouched", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize4, 202L, 225L, committedAt, 5000, TimestampType.LOG_APPEND_TIME))
                ), 0, 226L),
                FindBatchResponse.success(List.of(
                    new BatchInfo(8L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + file2Batch1Size, file2Batch2Size, 0L, 50L, committedAt, 3000L, TimestampType.CREATE_TIME)),
                    new BatchInfo(9L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + fileSize2, file3Batch1Size, 50L, 250L, committedAt, 4000L, TimestampType.CREATE_TIME))
                ), 0, 252L)
            );

            assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
                new FileToDelete("obj1", mergedAt),
                new FileToDelete("obj2", mergedAt),
                new FileToDelete("obj3", mergedAt)
            );

            // An attempt to commit again must be unsuccessful.
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize1 + fileSize2 + fileSize3, mergedFileBatches))
                .isInstanceOf(FileMergeWorkItemNotExist.class);

            // not expecting to delete any files on this scenario
            assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
                new FileToDelete("obj1", mergedAt),
                new FileToDelete("obj2", mergedAt),
                new FileToDelete("obj3", mergedAt)
            );
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void mergeAfterTopicWasDeleted(final boolean deletePhysicallyBeforeMerge) {
            final long fileSize1 = FILE_MERGE_SIZE_THRESHOLD / 3 + 1;
            final int file1Batch1Size = (int) fileSize1;

            final long fileSize2 = fileSize1;
            final int file2Batch1Size = (int) fileSize2 / 2;
            final int file2Batch2Size = (int) fileSize2 - file2Batch1Size;

            final long fileSize3 = FILE_MERGE_SIZE_THRESHOLD - fileSize1 - fileSize2;
            final int file3Batch1Size = (int) fileSize3;

            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize1,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize2,
                    List.of(
                    CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, file2Batch1Size, 0, 100, 2000, TimestampType.LOG_APPEND_TIME),
                    CommitBatchRequest.of(0, EXISTING_TOPIC_2_ID_PARTITION_0, file2Batch1Size, file2Batch2Size, 0, 50, 3000, TimestampType.CREATE_TIME)
                ));
            controlPlane.commitFile("obj3", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 3, fileSize3,
                    List.of(
                    CommitBatchRequest.of(0, EXISTING_TOPIC_2_ID_PARTITION_0, 0, file3Batch1Size, 0, 200, 4000, TimestampType.LOG_APPEND_TIME)
                ));
            final int expectedTopic2Partition0LogSize = file2Batch2Size + file3Batch1Size;
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_2_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(0, 252, expectedTopic2Partition0LogSize));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            // Delete TOPIC_1.
            time.sleep(1);
            final Instant deletedAt = TimeUtils.now(time);
            controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID));
            if (deletePhysicallyBeforeMerge) {
                deleteAllFilesThatAreToBeDeleted();
            }

            // Now after the deletion, commit the merge result.
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize1 + fileSize2 + fileSize3, List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize1, file2Batch1Size, 100, 150, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_2_ID_PARTITION_0, fileSize1 + file2Batch1Size, file2Batch2Size, 0, 50, committedAt, 3000, TimestampType.CREATE_TIME), List.of(3L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_2_ID_PARTITION_0, fileSize1 + fileSize2, file3Batch1Size, 50, 250, committedAt, 4000, TimestampType.CREATE_TIME), List.of(4L))
            ));
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_2_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(0, 252, expectedTopic2Partition0LogSize));

            // Obviously, the deleted topic should not be there, but the other one should be fully retrievable.
            final var findBatchResult = controlPlane.findBatches(
                List.of(
                    new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE),
                    new FindBatchRequest(EXISTING_TOPIC_2_ID_PARTITION_0, 0, Integer.MAX_VALUE)
                ), Integer.MAX_VALUE, 0
            );
            assertThat(findBatchResult).containsExactly(
                FindBatchResponse.unknownTopicOrPartition(),
                FindBatchResponse.success(List.of(
                    new BatchInfo(5L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_2_ID_PARTITION_0, file1Batch1Size + file2Batch1Size, file2Batch2Size, 0L, 50L, committedAt, 3000L, TimestampType.CREATE_TIME)),
                    new BatchInfo(6L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_2_ID_PARTITION_0, fileSize1 + fileSize2, file3Batch1Size, 50L, 250L, committedAt, 4000L, TimestampType.CREATE_TIME))
                ), 0, 252L)
            );

            final List<FileToDelete> expectedFilesToDelete = new ArrayList<>();
            if (!deletePhysicallyBeforeMerge) {
                expectedFilesToDelete.add(new FileToDelete("obj1", deletedAt));
            }
            expectedFilesToDelete.add(new FileToDelete("obj2", deletedAt));
            expectedFilesToDelete.add(new FileToDelete("obj3", deletedAt));
            assertThat(controlPlane.getFilesToDelete()).hasSameElementsAs(expectedFilesToDelete);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void mergeAfterSomeBatchesWereDeletedButNotWholeTopic(final boolean deletePhysicallyBeforeMerge) {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2 + 1;
            final long committedAt = time.milliseconds();
            final int batchSize = (int) fileSize;
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 3, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 200, 2000, TimestampType.LOG_APPEND_TIME)));
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(0, 302, batchSize * 2));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            // Delete some records (covering the 1st batch) from TOPIC_1.
            time.sleep(1);
            final Instant deletedAt = TimeUtils.now(time);
            controlPlane.deleteRecords(List.of(new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 110)));
            if (deletePhysicallyBeforeMerge) {
                deleteAllFilesThatAreToBeDeleted();
            }

            // Now after the deletion, commit the merge result.
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize * 2, List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize, fileSize, 100, 200, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L))
            ));

            final var findBatchResult = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
                Integer.MAX_VALUE, 0
            );
            assertThat(findBatchResult).containsExactly(
                FindBatchResponse.success(List.of(
                    new BatchInfo(3L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize, fileSize, 100L, 200L, committedAt, 2000L, TimestampType.LOG_APPEND_TIME))
                ), 110, 302L)
            );

            final List<FileToDelete> expectedFilesToDelete = new ArrayList<>();
            if (!deletePhysicallyBeforeMerge) {
                expectedFilesToDelete.add(new FileToDelete("obj1", deletedAt));
            }
            expectedFilesToDelete.add(new FileToDelete("obj2", deletedAt));
            assertThat(controlPlane.getFilesToDelete()).hasSameElementsAs(expectedFilesToDelete);
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(110, 302, batchSize));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void mergeAfterAllBatchesWereDeleted(final boolean deletePhysicallyBeforeMerge) {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            final long committedAt = time.milliseconds();
            final int batchSize = (int) fileSize;
            controlPlane.commitFile("obj1", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 2, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            assertThat(controlPlane.getLogInfo(List.of(new GetLogInfoRequest(EXISTING_TOPIC_1_ID, 0))))
                .containsExactly(GetLogInfoResponse.success(0, 202, batchSize * 2));

            time.sleep(1);
            final Instant deletedAt = TimeUtils.now(time);
            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID));
            if (deletePhysicallyBeforeMerge) {
                deleteAllFilesThatAreToBeDeleted();
            }

            // Now after the deletion, commit the merge result.
            time.sleep(1);
            final Instant mergedAt = TimeUtils.now(time);
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize * 2, List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize, fileSize, 100, 200, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L))
            ));

            final var findBatchResult = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)),
                Integer.MAX_VALUE, 0
            );
            assertThat(findBatchResult).containsExactly(FindBatchResponse.unknownTopicOrPartition());

            // Since the new merged file doesn't host any live batch, it should end up in files-to-delete as well.
            final List<FileToDelete> expectedFilesToDelete = new ArrayList<>();
            if (!deletePhysicallyBeforeMerge) {
                expectedFilesToDelete.add(new FileToDelete("obj1", deletedAt));
                expectedFilesToDelete.add(new FileToDelete("obj2", deletedAt));
            }
            expectedFilesToDelete.add(new FileToDelete("obj_merged", mergedAt));
            assertThat(controlPlane.getFilesToDelete()).hasSameElementsAs(expectedFilesToDelete);
        }

        private void deleteAllFilesThatAreToBeDeleted() {
            final Set<String> deletedObjectKeys = controlPlane.getFilesToDelete().stream()
                .map(FileToDelete::objectKey)
                .collect(Collectors.toSet());
            controlPlane.deleteFiles(new DeleteFilesRequest(deletedObjectKeys));
        }
    }

    @Disabled
    @Nested
    class ReleaseFileMergeWorkItem {
        @Test
        void workItemNotExist() {
            assertThatThrownBy(() -> controlPlane.releaseFileMergeWorkItem(100))
                .isInstanceOf(FileMergeWorkItemNotExist.class)
                .extracting("workItemId").isEqualTo(100L);
        }

        @Test
        void workItemWillBeReturnedAfterReleasing() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD + 1;
            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 1, fileSize,
                    List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles = List.of(
                new FileMergeWorkItem.File(1L, "obj0", ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))
                )
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles));

            // Now it should not return the same work item again, because the files are locked.
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();

            // After releasing, the work item must be returnable again.
            controlPlane.releaseFileMergeWorkItem(1L);
            time.sleep(10);
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(2L, TimeUtils.now(time), expectedFiles));
        }
    }

    public record ControlPlaneAndConfigs(ControlPlane controlPlane, Map<String, ?> configs) {
    }
}
