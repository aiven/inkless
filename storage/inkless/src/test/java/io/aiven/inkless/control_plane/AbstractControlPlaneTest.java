// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
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

    protected Time time = new MockTime();

    protected ControlPlane controlPlane;

    protected abstract ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo);

    static void configureControlPlane(ControlPlane controlPlane, Map<String, ?> configs) {
        Map<String, Object> override = new HashMap<>(configs);
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
        controlPlane.close();
    }

    @Test
    void emptyCommit() {
        final List<CommitBatchResponse> commitBatchResponse = controlPlane.commitFile(
            "a", BROKER_ID, FILE_SIZE, List.of()
        );
        assertThat(commitBatchResponse).isEmpty();
    }

    @Test
    void successfulCommitToExistingPartitions() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        final List<CommitBatchResponse> commitResponse1 = controlPlane.commitFile(
            objectKey1, BROKER_ID,
            FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 1, 10, 1000, TimestampType.CREATE_TIME),
                // non-existing partition
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1_PARTITIONS + 1, EXISTING_TOPIC_1), 2, 10, 1, 10, 1000, TimestampType.CREATE_TIME),
                // non-existing topic
                CommitBatchRequest.of(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 3, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse1).containsExactly(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        final List<CommitBatchResponse> commitResponse2 = controlPlane.commitFile(
            objectKey2, BROKER_ID,
            FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, 1, 10, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1_PARTITIONS + 1, EXISTING_TOPIC_1), 200, 10, 1, 10, 2000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 300, 10, 1, 10, 3000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse2).containsExactly(
            new CommitBatchResponse(Errors.NONE, 10, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(
                new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1_PARTITIONS + 1, EXISTING_TOPIC_1) , 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(Uuid.ONE_UUID, 0, NONEXISTENT_TOPIC), 11, Integer.MAX_VALUE)
            ), true, Integer.MAX_VALUE);
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
        controlPlane.commitFile(objectKey1, BROKER_ID, FILE_SIZE,
            List.of(CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 0, numberOfRecordsInBatch1 - 1, 1000, TimestampType.CREATE_TIME)));
        final int lastOffset = numberOfRecordsInBatch1 + numberOfRecordsInBatch2 - 1;
        controlPlane.commitFile(objectKey2, BROKER_ID, FILE_SIZE,
            List.of(CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, numberOfRecordsInBatch1, lastOffset, 2000, TimestampType.CREATE_TIME)));

        final long expectedLogStartOffset = 0;
        final long expectedHighWatermark = numberOfRecordsInBatch1 + numberOfRecordsInBatch2;
        final long expectedLogAppendTime = time.milliseconds();

        for (int offset = 0; offset < numberOfRecordsInBatch1; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(1L, objectKey1, BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, 10, 0, numberOfRecordsInBatch1 - 1, expectedLogAppendTime, 1000, TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, objectKey2, BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 100, 10, numberOfRecordsInBatch1, lastOffset, expectedLogAppendTime, 2000, TimestampType.CREATE_TIME))
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
        for (int offset = numberOfRecordsInBatch1; offset < numberOfRecordsInBatch1 + numberOfRecordsInBatch2; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
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
            objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 10, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(), 0, 10)
        );
    }

    @Test
    void findOffsetOutOfRange() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 11, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 10)
        );
    }

    @Test
    void findNegativeOffset() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, -1, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 10)
        );
    }

    @Test
    void findBeforeCommit() {
        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 11, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 0)
        );
    }

    @Test
    void commitEmptyBatches() {
        final String objectKey = "a";

        assertThatThrownBy(() -> controlPlane.commitFile(objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 10, 19, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 2, 0, 10, 19, 1000, TimestampType.CREATE_TIME)
            )
        ))
            .isInstanceOf(IllegalArgumentException.class)
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

        // Produce some data to be sure it's not affected later.
        final String objectKey = "a1";
        controlPlane.commitFile(objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(newTopic1Id, 0, newTopic1Name), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));

        final List<FindBatchRequest> findBatchRequests = List.of(new FindBatchRequest(new TopicIdPartition(newTopic1Id, 0, newTopic1Name), 0, Integer.MAX_VALUE));
        final List<FindBatchResponse> findBatchResponsesBeforeDelete = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);

        // Create new topic and partitions for the existing one.
        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 2),
            new CreateTopicAndPartitionsRequest(newTopic2Id, newTopic2Name, 2)
        ));

        final List<FindBatchResponse> findBatchResponsesAfterDelete = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);
        assertThat(findBatchResponsesBeforeDelete).isEqualTo(findBatchResponsesAfterDelete);

        // Nothing happens as this is idempotent
        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(newTopic1Id, newTopic1Name, 2),
            new CreateTopicAndPartitionsRequest(newTopic2Id, newTopic2Name, 2)
        ));

        final List<FindBatchResponse> findBatchResponsesAfterDelete2 = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);
        assertThat(findBatchResponsesAfterDelete2).isEqualTo(findBatchResponsesAfterDelete);
    }

    @Test
    void deleteTopic() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        controlPlane.commitFile(objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));
        final int file2Partition0Size = (int) FILE_SIZE / 2;
        final int file2Partition1Size = (int) FILE_SIZE - file2Partition0Size;
        controlPlane.commitFile(objectKey2, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, file2Partition0Size, 0, 0, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_2), 1, file2Partition1Size, 1, 1, 2000, TimestampType.CREATE_TIME)
            ));

        final List<FindBatchRequest> findBatchRequests = List.of(new FindBatchRequest(EXISTING_TOPIC_2_ID_PARTITION_0, 0, Integer.MAX_VALUE));
        final List<FindBatchResponse> findBatchResponsesBeforeDelete = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);

        time.sleep(1001);  // advance time
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));

        // objectKey2 is kept alive by the second topic, which isn't deleted
        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );

        final List<FindBatchResponse> findBatchResponsesAfterDelete = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);
        assertThat(findBatchResponsesAfterDelete).isEqualTo(findBatchResponsesBeforeDelete);

        // Nothing happens as it's idempotent.
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));
        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );
    }

    @Test
    void partiallyDeleteBatch() {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 3),
            new DeleteRecordsRequest(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 10)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(3),
            DeleteRecordsResponse.unknownTopicOrPartition()
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, findResponseBeforeDelete.get(0).batches(), 3, 10)
        );
        assertThat(controlPlane.getFilesToDelete()).isEmpty();
    }

    @Test
    void fullyDeleteBatch() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";
        final String objectKey3 = "a3";

        controlPlane.commitFile(
            objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        controlPlane.commitFile(
            objectKey2, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 2, (int) FILE_SIZE, 1, 10, 2000, TimestampType.CREATE_TIME)
            )
        );
        controlPlane.commitFile(
            objectKey3, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 3, (int) FILE_SIZE, 1, 10, 3000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 19),
            new DeleteRecordsRequest(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 10)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(19),
            DeleteRecordsResponse.unknownTopicOrPartition()
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(
                findResponseBeforeDelete.get(0).batches().get(1),
                findResponseBeforeDelete.get(0).batches().get(2)
            ), 19, 30)
        );
        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );
    }

    @Test
    void deleteUpToLogStartOffset() {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(0)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
        assertThat(findResponse).isEqualTo(findResponseBeforeDelete);

        assertThat(controlPlane.getFilesToDelete()).isEmpty();
    }

    @Test
    void deleteUpToHighWatermark() {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.success(10)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(), 10, 10)
        );

        assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(new FileToDelete(objectKey1, TimeUtils.now(time)));
    }

    @ParameterizedTest
    @ValueSource(longs = {-2, 11})
    void deleteOffsetOutOfRange(final long deleteOffset) {
        final String objectKey1 = "a1";

        controlPlane.commitFile(
            objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, (int) FILE_SIZE, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, deleteOffset)
        ));
        assertThat(deleteRecordsResponses).containsExactly(
            DeleteRecordsResponse.offsetOutOfRange()
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
        assertThat(findResponse).isEqualTo(findResponseBeforeDelete);

        assertThat(controlPlane.getFilesToDelete()).isEmpty();
    }

    @Test
    void fullyDeleteBatchFileNotAffectedIfThereAreOtherBatches() {
        final String objectKey1 = "a1";

        final int tp0BatchSize = (int) FILE_SIZE / 2;
        final int tp1BatchSize = (int) FILE_SIZE - tp0BatchSize;
        controlPlane.commitFile(
            objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_0, 1, tp0BatchSize, 1, 10, 1000, TimestampType.CREATE_TIME),
                // This batch will keep the file alive after the other batch is deleted.
                CommitBatchRequest.of(EXISTING_TOPIC_1_ID_PARTITION_1, 100, tp1BatchSize, 1, 2, 2000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponseBeforeDelete = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);

        final List<DeleteRecordsResponse> deleteRecordsResponses = controlPlane.deleteRecords(List.of(
            new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 10)
        ));
        assertThat(deleteRecordsResponses).containsExactly(DeleteRecordsResponse.success(10));

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
        assertThat(findResponse).isEqualTo(findResponseBeforeDelete);

        assertThat(controlPlane.getFilesToDelete()).isEmpty();
    }

    @Test
    void deleteFiles() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        controlPlane.commitFile(objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, (int) FILE_SIZE, 0, 0, 1000, TimestampType.CREATE_TIME)
            ));
        final int file2Partition0Size = (int) FILE_SIZE / 2;
        final int file2Partition1Size = (int) FILE_SIZE - file2Partition0Size;
        controlPlane.commitFile(objectKey2, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, file2Partition0Size, 0, 0, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_2), 1, file2Partition1Size, 1, 1, 2000, TimestampType.CREATE_TIME)
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
                controlPlane.commitFile(String.format("obj%d", i), i, fileSize,
                    List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME))
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
            controlPlane.commitFile("obj0", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", 2, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles = List.of(
                new FileMergeWorkItem.File(1L, "obj0", fileSize, fileSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(2L, "obj1", fileSize, fileSize,
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
            controlPlane.commitFile("obj0", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", 2, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 3, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 3000, TimestampType.CREATE_TIME)));

            // Get the merge work item.
            final List<FileMergeWorkItem.File> expectedFiles1 = List.of(
                new FileMergeWorkItem.File(1L, "obj0", fileSize, fileSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(2L, "obj1", fileSize, fileSize,
                    List.of(new BatchInfo(2, "obj1", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles1));

            // Commit one more file.
            controlPlane.commitFile("obj3", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 4000, TimestampType.CREATE_TIME)));

            // Now it's enough to have one more merge work item.
            final List<FileMergeWorkItem.File> expectedFiles2 = List.of(
                new FileMergeWorkItem.File(3L, "obj2", fileSize, fileSize,
                    List.of(new BatchInfo(3, "obj2", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 202, 302, committedAt, 3000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(4L, "obj3", fileSize, fileSize,
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
            controlPlane.commitFile("obj0", 0, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles = List.of(
                new FileMergeWorkItem.File(1L, "obj0", fileSize, fileSize,
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
            controlPlane.commitFile("obj0", 1, batchSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", 2, batchSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 3, batchSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 3000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles1 = List.of(
                new FileMergeWorkItem.File(1L, "obj0", batchSize, batchSize,
                    List.of(new BatchInfo(1, "obj0", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(2L, "obj1", batchSize, batchSize,
                    List.of(new BatchInfo(2, "obj1", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(1L, TimeUtils.now(time), expectedFiles1));

            // We intentionally make the batch size here smaller to make it fit into the following merging operation.
            controlPlane.commitFileMergeWorkItem(1L, "obj_merged", 1, batchSize,
                List.of(
                    new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                    new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, batchSize, batchSize + batchSize, 101, 201, committedAt, 2000, TimestampType.CREATE_TIME), List.of(2L))
                )
            );

            // The already merged file must not be included into the merge operation and without it there's not enough data to merge.
            assertThat(controlPlane.getFileMergeWorkItem()).isNull();

            // Commit more files and try merging again.
            controlPlane.commitFile("obj3", 1, batchSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) batchSize, 0, 100, 4000, TimestampType.CREATE_TIME)));

            // File 4 is the merged file, batches 4 and 5 are in it.
            final List<FileMergeWorkItem.File> expectedFiles2= List.of(
                new FileMergeWorkItem.File(3L, "obj2", batchSize, batchSize,
                    List.of(new BatchInfo(3, "obj2", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 202, 302, committedAt, 3000, TimestampType.CREATE_TIME)))),
                new FileMergeWorkItem.File(5L, "obj3", batchSize, batchSize,
                    List.of(new BatchInfo(6, "obj3", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, batchSize, 303, 403, committedAt, 4000, TimestampType.CREATE_TIME))))
            );
            assertThat(controlPlane.getFileMergeWorkItem())
                .isEqualTo(new FileMergeWorkItem(2L, TimeUtils.now(time), expectedFiles2));
        }
    }

    @Nested
    class CommitFileMergeWorkItem {
        @Test
        void workItemNotExist() {
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(100, "obj", 1, 0, List.of()))
                .isInstanceOf(FileMergeWorkItemNotExist.class)
                .extracting("workItemId").isEqualTo(100L);
        }

        @Test
        void batchWithoutParents() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            controlPlane.commitFile("obj0", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", 2, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            final var batch = new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 0, 0, 0, TimestampType.CREATE_TIME), List.of());
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(workItemId, "obj", 1, 0, List.of(batch)))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessage("Invalid parent batch count 0 in " + batch);
        }

        @Test
        void batchWithTooManyParents() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            controlPlane.commitFile("obj0", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", 2, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            final var batch = new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 0, 0, 0, TimestampType.CREATE_TIME),
                List.of(1L, 2L));
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(workItemId, "obj", 1, 0, List.of(batch)))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessage("Invalid parent batch count 2 in " + batch);
        }

        @Test
        void batchIsNotPartOfWorkItem() {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            controlPlane.commitFile("obj0", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj1", 2, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 3, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 3000, TimestampType.CREATE_TIME)));

            final FileMergeWorkItem fileMergeWorkItem = controlPlane.getFileMergeWorkItem();
            assertThat(fileMergeWorkItem.files()).hasSize(2);

            final var batch = new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 0, 0, 0, TimestampType.CREATE_TIME),
                List.of(3L));
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(fileMergeWorkItem.workItemId(), "obj", 1, 0, List.of(batch)))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessage("Batch 3 is not part of work item in: " + batch);
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

            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj1", 1, fileSize1,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 2, fileSize2,
                List.of(
                    new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file2Batch1Size, 0, 100, 2000, TimestampType.LOG_APPEND_TIME),
                    new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, file2Batch1Size, file2Batch2Size, 0, 50, 3000, TimestampType.CREATE_TIME)
                ));
            controlPlane.commitFile("obj3", 3, fileSize3,
                List.of(
                    new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, file3Batch1Size, 0, 200, 4000, TimestampType.LOG_APPEND_TIME)
                ));

            time.sleep(1);
            final Instant mergedAt = TimeUtils.now(time);
            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();
            final List<MergedFileBatch> mergedFileBatches = List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize1, file2Batch1Size, 100, 150, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + file2Batch1Size, file2Batch2Size, 0, 50, committedAt, 3000, TimestampType.CREATE_TIME), List.of(3L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + fileSize2, file3Batch1Size, 50, 250, committedAt, 4000, TimestampType.CREATE_TIME), List.of(4L))
            );
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", 1, fileSize1 + fileSize2 + fileSize3, mergedFileBatches);

            final var findBatchResult = controlPlane.findBatches(
                List.of(
                    new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE),
                    new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_1, 0, Integer.MAX_VALUE)
                ), true, Integer.MAX_VALUE);
            assertThat(findBatchResult).containsExactly(
                FindBatchResponse.success(List.of(
                    new BatchInfo(5L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0L, file1Batch1Size, 0L, 100L, committedAt, 1000L, TimestampType.CREATE_TIME)),
                    new BatchInfo(6L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, file1Batch1Size, file2Batch1Size, 100L, 150L, committedAt, 2000L, TimestampType.LOG_APPEND_TIME))
                ), 0, 202L),
                FindBatchResponse.success(List.of(
                    new BatchInfo(7L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + file2Batch1Size, file2Batch2Size, 0L, 50L, committedAt, 3000L, TimestampType.CREATE_TIME)),
                    new BatchInfo(8L, "obj_merged", BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_1, fileSize1 + fileSize2, file3Batch1Size, 50L, 250L, committedAt, 4000L, TimestampType.CREATE_TIME))
                ), 0, 252L)
            );

            assertThat(controlPlane.getFilesToDelete()).containsExactlyInAnyOrder(
                new FileToDelete("obj1", mergedAt),
                new FileToDelete("obj2", mergedAt),
                new FileToDelete("obj3", mergedAt)
            );

            // An attempt to commit again must be unsuccessful.
            assertThatThrownBy(() -> controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", 1, fileSize1 + fileSize2 + fileSize3, mergedFileBatches))
                .isInstanceOf(FileMergeWorkItemNotExist.class);
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
            controlPlane.commitFile("obj1", 1, fileSize1,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 2, fileSize2,
                List.of(
                    new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file2Batch1Size, 0, 100, 2000, TimestampType.LOG_APPEND_TIME),
                    new CommitBatchRequest(EXISTING_TOPIC_2_ID_PARTITION_0, file2Batch1Size, file2Batch2Size, 0, 50, 3000, TimestampType.CREATE_TIME)
                ));
            controlPlane.commitFile("obj3", 3, fileSize3,
                List.of(
                    new CommitBatchRequest(EXISTING_TOPIC_2_ID_PARTITION_0, 0, file3Batch1Size, 0, 200, 4000, TimestampType.LOG_APPEND_TIME)
                ));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            // Delete TOPIC_1.
            time.sleep(1);
            final Instant deletedAt = TimeUtils.now(time);
            controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID));
            if (deletePhysicallyBeforeMerge) {
                deleteAllFilesThatAreToBeDeleted();
            }

            // Now after the deletion, commit the merge result.
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", 1, fileSize1 + fileSize2 + fileSize3, List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, file1Batch1Size, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize1, file2Batch1Size, 100, 150, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_2_ID_PARTITION_0, fileSize1 + file2Batch1Size, file2Batch2Size, 0, 50, committedAt, 3000, TimestampType.CREATE_TIME), List.of(3L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_2_ID_PARTITION_0, fileSize1 + fileSize2, file3Batch1Size, 50, 250, committedAt, 4000, TimestampType.CREATE_TIME), List.of(4L))
            ));

            // Obviously, the deleted topic should not be there, but the other one should be fully retrievable.
            final var findBatchResult = controlPlane.findBatches(
                List.of(
                    new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE),
                    new FindBatchRequest(EXISTING_TOPIC_2_ID_PARTITION_0, 0, Integer.MAX_VALUE)
                ), true, Integer.MAX_VALUE);
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
            controlPlane.commitFile("obj1", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 3, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 200, 2000, TimestampType.LOG_APPEND_TIME)));

            final var workItemId = controlPlane.getFileMergeWorkItem().workItemId();

            // Delete some records (covering the 1st batch) from TOPIC_1.
            time.sleep(1);
            final Instant deletedAt = TimeUtils.now(time);
            controlPlane.deleteRecords(List.of(new DeleteRecordsRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 110)));
            if (deletePhysicallyBeforeMerge) {
                deleteAllFilesThatAreToBeDeleted();
            }

            // Now after the deletion, commit the merge result.
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", 1, fileSize * 2, List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize, fileSize, 100, 200, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L))
            ));

            final var findBatchResult = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
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
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void mergeAfterAllBatchesWereDeleted(final boolean deletePhysicallyBeforeMerge) {
            final long fileSize = FILE_MERGE_SIZE_THRESHOLD / 2;
            final long committedAt = time.milliseconds();
            controlPlane.commitFile("obj1", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));
            controlPlane.commitFile("obj2", 2, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 2000, TimestampType.CREATE_TIME)));

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
            controlPlane.commitFileMergeWorkItem(workItemId, "obj_merged", 1, fileSize * 2, List.of(
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, 0, fileSize, 0, 100, committedAt, 1000, TimestampType.CREATE_TIME), List.of(1L)),
                new MergedFileBatch(BatchMetadata.of(EXISTING_TOPIC_1_ID_PARTITION_0, fileSize, fileSize, 100, 200, committedAt, 2000, TimestampType.LOG_APPEND_TIME), List.of(2L))
            ));

            final var findBatchResult = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
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
            controlPlane.commitFile("obj0", 1, fileSize,
                List.of(new CommitBatchRequest(EXISTING_TOPIC_1_ID_PARTITION_0, 0, (int) fileSize, 0, 100, 1000, TimestampType.CREATE_TIME)));

            final List<FileMergeWorkItem.File> expectedFiles = List.of(
                new FileMergeWorkItem.File(1L, "obj0", fileSize, fileSize,
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
