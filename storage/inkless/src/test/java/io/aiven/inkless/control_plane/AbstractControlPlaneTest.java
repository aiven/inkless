// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
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

    @Mock
    protected Time time;

    protected ControlPlane controlPlane;

    protected abstract ControlPlane createControlPlane(final TestInfo testInfo);

    @BeforeEach
    void setupControlPlane(final TestInfo testInfo) {
        controlPlane = createControlPlane(testInfo);

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, 1)
        );
        controlPlane.createTopicAndPartitions(createTopicAndPartitionsRequests);
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 10, 1000, TimestampType.CREATE_TIME),
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 2, 10, 10, 1000, TimestampType.CREATE_TIME),
                new CommitBatchRequest(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 3, 10, 10, 1000, TimestampType.CREATE_TIME)
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, 10, 1000, TimestampType.CREATE_TIME),
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 200, 10, 10, 2000, TimestampType.CREATE_TIME),
                new CommitBatchRequest(new TopicIdPartition(NONEXISTENT_TOPIC_ID, 0, NONEXISTENT_TOPIC), 300, 10, 10, 3000, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitResponse2).containsExactly(
            new CommitBatchResponse(Errors.NONE, 10, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
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
                List.of(new BatchInfo(objectKey2, 100, 10, 10, 10, TimestampType.CREATE_TIME, time.milliseconds(), 1000)),
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
            List.of(new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, numberOfRecordsInBatch1, 1000, TimestampType.CREATE_TIME)));
        controlPlane.commitFile(objectKey2, BROKER_ID, FILE_SIZE,
            List.of(new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 100, 10, numberOfRecordsInBatch2, 2000, TimestampType.CREATE_TIME)));

        final long expectedLogStartOffset = 0;
        final long expectedHighWatermark = numberOfRecordsInBatch1 + numberOfRecordsInBatch2;
        final long expectedLogAppendTime = time.milliseconds();

        for (int offset = 0; offset < numberOfRecordsInBatch1; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(objectKey1, 1, 10, 0, numberOfRecordsInBatch1, TimestampType.CREATE_TIME, expectedLogAppendTime, 1000),
                    new BatchInfo(objectKey2, 100, 10, numberOfRecordsInBatch1, numberOfRecordsInBatch2, TimestampType.CREATE_TIME, expectedLogAppendTime, 2000)
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
        for (int offset = numberOfRecordsInBatch1; offset < numberOfRecordsInBatch1 + numberOfRecordsInBatch2; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_1_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(objectKey2, 100, 10, numberOfRecordsInBatch1, numberOfRecordsInBatch2, TimestampType.CREATE_TIME, expectedLogAppendTime, 2000)
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 10, 1000, TimestampType.CREATE_TIME)
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 10, 1000, TimestampType.CREATE_TIME)
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 11, 10, 10, 1000, TimestampType.CREATE_TIME)
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, 10, 10, 1000, TimestampType.CREATE_TIME),
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 1, EXISTING_TOPIC_1), 2, 0, 10, 1000, TimestampType.CREATE_TIME)
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
                new CommitBatchRequest(new TopicIdPartition(newTopic1Id, 0, newTopic1Name), 1, (int) FILE_SIZE, 1, 1000, TimestampType.CREATE_TIME)
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
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, (int) FILE_SIZE, 1, 1000, TimestampType.CREATE_TIME)
            ));
        final int file2Partition0Size = (int) FILE_SIZE / 2;
        final int file2Partition1Size = (int) FILE_SIZE - file2Partition0Size;
        controlPlane.commitFile(objectKey2, BROKER_ID, FILE_SIZE,
            List.of(
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_1_ID, 0, EXISTING_TOPIC_1), 1, file2Partition0Size, 1, 1000, TimestampType.CREATE_TIME),
                new CommitBatchRequest(new TopicIdPartition(EXISTING_TOPIC_2_ID, 0, EXISTING_TOPIC_1), 1, file2Partition1Size, 2, 2000, TimestampType.CREATE_TIME)
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
}
