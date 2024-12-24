// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.TimeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
// Leniency is fine in this class, otherwise there will be too much tailored mocking on metadataView.
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class AbstractControlPlaneTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456;

    static final String EXISTING_TOPIC_1 = "topic-existing-1";
    static final Uuid EXISTING_TOPIC_1_ID = new Uuid(10, 10);
    static final TopicIdPartition EXISTING_TOPIC_1_ID_PARTITION = new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1);
    static final String EXISTING_TOPIC_2 = "topic-existing-2";
    static final Uuid EXISTING_TOPIC_2_ID = new Uuid(20, 20);
    static final TopicIdPartition EXISTING_TOPIC_2_ID_PARTITION = new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_2);
    static final Uuid NONEXISTENT_TOPIC_ID = Uuid.ONE_UUID;
    static final String NONEXISTENT_TOPIC = "topic-nonexistent";
    static final Uuid NONEXISTING_TOPIC_ID = new Uuid(20, 20);
    static final TopicIdPartition NONEXISTING_TOPIC_ID_PARTITION = new TopicIdPartition(NONEXISTING_TOPIC_ID, 0, NONEXISTENT_TOPIC);

    protected Time time = new MockTime();

    protected ControlPlane controlPlane;

    protected abstract ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo);

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
            new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, 1)
        );
        controlPlane.createTopicAndPartitions(createTopicAndPartitionsRequests);
    }

    @AfterEach
    void tearDown() {
        try {
            controlPlane.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        final CommitBatchRequest successfulRequest1 = CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 1, 10, 1000, TimestampType.CREATE_TIME);
        final List<CommitBatchResponse> commitResponse1 = controlPlane.commitFile(
            objectKey1, BROKER_ID,
            FILE_SIZE,
            List.of(
                successfulRequest1,
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 2, 10, 1, 10, 1000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 3, 10, 1, 10, 1000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse1).containsExactly(
            CommitBatchResponse.success(0, time.milliseconds(), 0, successfulRequest1),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        final CommitBatchRequest successfulRequest2 = CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, 1, 10, 1000, TimestampType.CREATE_TIME);
        final List<CommitBatchResponse> commitResponse2 = controlPlane.commitFile(
            objectKey2, BROKER_ID,
            FILE_SIZE,
            List.of(
                successfulRequest2,
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 200, 10, 1, 10, 2000, TimestampType.CREATE_TIME),
                CommitBatchRequest.of(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 300, 10, 1, 10, 3000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse2).containsExactly(
            CommitBatchResponse.success(10, time.milliseconds(), 0, successfulRequest2),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            CommitBatchResponse.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(
                new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1) , 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(Uuid.ONE_UUID, 0, NONEXISTENT_TOPIC), 11, Integer.MAX_VALUE)
            ), true, Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(
                Errors.NONE,
                List.of(BatchInfo.of(objectKey2, 100, 10, 10, 1, 10, time.milliseconds(), 1000, TimestampType.CREATE_TIME)),
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
        controlPlane.commitFile(objectKey2, BROKER_ID, FILE_SIZE,
            List.of(CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, numberOfRecordsInBatch1, (numberOfRecordsInBatch1 + numberOfRecordsInBatch2 - 1), 2000, TimestampType.CREATE_TIME)));

        final long expectedLogStartOffset = 0;
        final long expectedHighWatermark = numberOfRecordsInBatch1 + numberOfRecordsInBatch2;
        final long expectedLogAppendTime = time.milliseconds();

        for (int offset = 0; offset < numberOfRecordsInBatch1; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    BatchInfo.of(objectKey1, 1, 10, 0, 0, 2, expectedLogAppendTime, 1000, TimestampType.CREATE_TIME),
                    BatchInfo.of(objectKey2, 100, 10, 3, 3, 4, expectedLogAppendTime, 2000, TimestampType.CREATE_TIME)
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
        for (int offset = numberOfRecordsInBatch1; offset < numberOfRecordsInBatch1 + numberOfRecordsInBatch2; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    BatchInfo.of(objectKey2, 100, 10, 3, 3, 4, expectedLogAppendTime, 2000, TimestampType.CREATE_TIME)
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
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 10, 19, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, 10, Integer.MAX_VALUE)),
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
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 10, 19, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, 11, Integer.MAX_VALUE)),
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
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 10, 19, 1000, TimestampType.CREATE_TIME)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, -1, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 10)
        );
    }

    @Test
    void findBeforeCommit() {
        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, 11, Integer.MAX_VALUE)),
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
                CommitBatchRequest.of(new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_1), 1, file2Partition1Size, 1, 1, 2000, TimestampType.CREATE_TIME)
            ));

        final List<FindBatchRequest> findBatchRequests = List.of(new FindBatchRequest(EXISTING_TOPIC_2_ID_PARTITION, 0, Integer.MAX_VALUE));
        final List<FindBatchResponse> findBatchResponsesBeforeDelete = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);

        time.sleep(1001);  // advance time
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));

        // objectKey2 is kept alive by the second topic, which isn't deleted
        assertThat(controlPlane.getFilesToDelete()).containsExactly(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );

        final List<FindBatchResponse> findBatchResponsesAfterDelete = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);
        assertThat(findBatchResponsesAfterDelete).isEqualTo(findBatchResponsesBeforeDelete);

        // Nothing happens as it's idempotent.
        controlPlane.deleteTopics(Set.of(EXISTING_TOPIC_1_ID, Uuid.ONE_UUID));
        assertThat(controlPlane.getFilesToDelete()).containsExactly(
            new FileToDelete(objectKey1, TimeUtils.now(time))
        );
    }

    @Test
    void findEmptyProducerStateOnNonExistingTopicPartition() {
        final AbstractControlPlane cp = (AbstractControlPlane) controlPlane;
        assertThat(cp.findProducerStates(List.of())).isEmpty();

        final int producerId = 1;
        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(NONEXISTING_TOPIC_ID_PARTITION, producerId, (short) -1))))
            .containsExactly(new FindProducerStateResponse(NONEXISTING_TOPIC_ID_PARTITION.topicPartition(), producerId, (short) -1, List.of()));
    }

    @Test
    void findEmptyProducerStateOnNewTopicPartition() {
        final AbstractControlPlane cp = (AbstractControlPlane) controlPlane;
        final String objectKey = "a";
        controlPlane.commitFile(
            objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9)
            )
        );

        // outside the expiration time
        time.sleep(120000);

        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(EXISTING_TOPIC_1_ID_PARTITION, 2L, (short) -1))))
            .containsExactly(new FindProducerStateResponse(EXISTING_TOPIC_1_ID_PARTITION.topicPartition(), 2L, (short) -1, List.of()));
        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(EXISTING_TOPIC_1_ID_PARTITION, 1L, (short) 2))))
            .containsExactly(new FindProducerStateResponse(EXISTING_TOPIC_1_ID_PARTITION.topicPartition(), 1L, (short) 2, List.of()));
        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(EXISTING_TOPIC_1_ID_PARTITION, 1L, (short) 3))))
            .containsExactly(new FindProducerStateResponse(EXISTING_TOPIC_1_ID_PARTITION.topicPartition(), 1L, (short) 3, List.of()));
    }

    @Test
    void findProducerStateOnNewTopicPartition() {
        final AbstractControlPlane cp = (AbstractControlPlane) controlPlane;
        final String objectKey = "a";
        final var commitTime = time.milliseconds();
        controlPlane.commitFile(
            objectKey, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 19, commitTime, TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9)
            )
        );

        // within the expiration time
        time.sleep(30000);

        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(EXISTING_TOPIC_1_ID_PARTITION, 1L, (short) 3))))
            .containsExactly(new FindProducerStateResponse(EXISTING_TOPIC_1_ID_PARTITION.topicPartition(), 1L, (short) 3,
                List.of(new BatchMetadata(9, 19L, 9, commitTime))));
    }

    @Test
    void testCommitToUpdateProducerStateBatchMetadata() {
        final AbstractControlPlane cp = (AbstractControlPlane) controlPlane;
        assertThat(controlPlane.commitFile(
            "a", BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9),
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 2, 10, 20, 29, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 10, 19),
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 3, 10, 30, 39, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 20, 29)
            )
        )).flatExtracting(CommitBatchResponse::isOk).containsExactly(true, true, true);
        assertThat(controlPlane.commitFile(
            "b", BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 4, 10, 40, 49, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 30, 39),
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 5, 10, 50, 59, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 40, 49),
                CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 6, 10, 60, 69, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 50, 59)
            )
        )).flatExtracting(CommitBatchResponse::isOk).containsExactly(true, true, true);

        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(EXISTING_TOPIC_1_ID_PARTITION, 1L, (short) 3))))
            .containsExactly(new FindProducerStateResponse(EXISTING_TOPIC_1_ID_PARTITION.topicPartition(), 1L, (short) 3,
                List.of(
                    new BatchMetadata(19, 29L, 9, time.milliseconds()),
                    new BatchMetadata(29, 39L, 9, time.milliseconds()),
                    new BatchMetadata(39, 49L, 9, time.milliseconds()),
                    new BatchMetadata(49, 59L, 9, time.milliseconds()),
                    new BatchMetadata(59, 69L, 9, time.milliseconds())
                )));
    }

    @Test
    void testCommitDuplicates() {
        final AbstractControlPlane cp = (AbstractControlPlane) controlPlane;
        final CommitBatchRequest request = CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9);
        final CommitBatchResponse response = controlPlane.commitFile("a", BROKER_ID, FILE_SIZE, List.of(request)).get(0);

        assertThat(response.isOk()).isTrue();

        assertThat(cp.findProducerStates(List.of(new FindProducerStateRequest(EXISTING_TOPIC_1_ID_PARTITION, 1L, (short) 3))))
            .containsExactly(new FindProducerStateResponse(EXISTING_TOPIC_1_ID_PARTITION.topicPartition(), 1L, (short) 3,
                List.of(
                    new BatchMetadata(9, 19L, 9, time.milliseconds())
                )));

        final CommitBatchResponse dupResponse = controlPlane.commitFile("b", BROKER_ID, FILE_SIZE, List.of(request)).get(0);
        assertThat(dupResponse.isDuplicate()).isTrue();

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, 0, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(
                Errors.NONE,
                List.of(
                    new BatchInfo("a", 1, 10, 0, 10, 19, time.milliseconds(), time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 9)
                ),
                0,
                10
            )
        );
    }

    @Test
    void testOutOfOrderNewEpoch() {
        final CommitBatchRequest request = CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 19, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 1, 10);
        final CommitBatchResponse response = controlPlane.commitFile("a", BROKER_ID, FILE_SIZE, List.of(request)).get(0);

        assertThat(response)
            .extracting(CommitBatchResponse::isOk, CommitBatchResponse::isDuplicate, CommitBatchResponse::errors)
            .containsExactly(false, false, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
    }


    @ParameterizedTest
    @CsvSource({
        "14, 13", // lower than 15
        "14, 14", // lower than 15
        "14, 16", // larger than 15
    })
        // 15 is the first sequence number for the second batch
    void testOutOfOrderSequence(final int lastSeq, final int nextSeq) {
        final CommitBatchRequest request0 = CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 10 + lastSeq, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, lastSeq);
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 2, 10, nextSeq + 10, nextSeq + 20, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, nextSeq, nextSeq + 10);
        final List<CommitBatchResponse> responses = controlPlane.commitFile("a", BROKER_ID, FILE_SIZE, List.of(request0, request1));

        assertThat(responses)
            .extracting(CommitBatchResponse::isOk)
            .containsExactly(true, false);
        assertThat(responses)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.NONE, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
    }

    @Test
    void testInvalidProducerEpoch() {
        final CommitBatchRequest request0 = CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 1, 10, 10, 24, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 3, 0, 14);
        final CommitBatchRequest request1 = CommitBatchRequest.idempotent(EXISTING_TOPIC_1_ID_PARTITION, 2, 10, 25, 35, time.milliseconds(), TimestampType.CREATE_TIME, 1L, (short) 2, 15, 25);
        final List<CommitBatchResponse> responses = controlPlane.commitFile("a", BROKER_ID, FILE_SIZE, List.of(request0, request1));

        assertThat(responses)
            .extracting(CommitBatchResponse::isOk)
            .containsExactly(true, false);
        assertThat(responses)
            .extracting(CommitBatchResponse::errors)
            .containsExactly(Errors.NONE, Errors.INVALID_PRODUCER_EPOCH);
    }

    public record ControlPlaneAndConfigs(ControlPlane controlPlane, Map<String, ?> configs) {
    }
}
