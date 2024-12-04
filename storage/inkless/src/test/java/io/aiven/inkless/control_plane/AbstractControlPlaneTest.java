// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
// Leniency is fine in this class, otherwise there will be too much tailored mocking on metadataView.
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class AbstractControlPlaneTest {
    static final String EXISTING_TOPIC = "topic-existing";
    static final Uuid EXISTING_TOPIC_ID = new Uuid(10, 10);
    static final TopicIdPartition EXISTING_TOPIC_ID_PARTITION = new TopicIdPartition(EXISTING_TOPIC_ID, 0, EXISTING_TOPIC);
    static final String NONEXISTENT_TOPIC = "topic-nonexistent";

    @Mock
    protected Time time;

    protected MetadataView metadataView;

    @BeforeEach
    void setup() {
        metadataView = mock(MetadataView.class);
        when(metadataView.getTopicPartitions(EXISTING_TOPIC))
            .thenReturn(Set.of(new TopicPartition(EXISTING_TOPIC, 0)));
        when(metadataView.getTopicId(EXISTING_TOPIC))
            .thenReturn(EXISTING_TOPIC_ID);
        when(metadataView.getTopicPartitions(NONEXISTENT_TOPIC))
            .thenReturn(Set.of());
        when(metadataView.getTopicId(NONEXISTENT_TOPIC))
            .thenReturn(Uuid.ZERO_UUID);
        when(metadataView.getTopicConfig(EXISTING_TOPIC))
            .thenReturn(new LogConfig(Map.of()));
        when(metadataView.isInklessTopic(anyString()))
            .thenReturn(true);
    }

    protected ControlPlane controlPlane;

    protected abstract ControlPlane createControlPlane(final TestInfo testInfo);

    @BeforeEach
    void setupControlPlane(final TestInfo testInfo) {
        controlPlane = createControlPlane(testInfo);

        verify(metadataView).subscribeToTopicMetadataChanges(eq(controlPlane));

        final var delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(EXISTING_TOPIC).setTopicId(EXISTING_TOPIC_ID));
        delta.replay(new PartitionRecord().setTopicId(EXISTING_TOPIC_ID).setPartitionId(EXISTING_TOPIC_ID_PARTITION.partition()));
        controlPlane.onTopicMetadataChanges(delta.topicsDelta());
    }

    @Test
    void emptyCommit() {
        final List<CommitBatchResponse> commitBatchResponse = controlPlane.commitFile(
            "a",
            List.of()
        );
        assertThat(commitBatchResponse).isEmpty();
    }

    @Test
    void successfulCommitToExistingPartitions() {
        final String objectKey1 = "a1";
        final String objectKey2 = "a2";

        final List<CommitBatchResponse> commitResponse1 = controlPlane.commitFile(
            objectKey1,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 1, 10, 10),
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 1), 2, 10, 10),
                new CommitBatchRequest(new TopicPartition(NONEXISTENT_TOPIC, 0), 3, 10, 10)
            )
        );
        assertThat(commitResponse1).containsExactly(
            new CommitBatchResponse(Errors.NONE, 0, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        final List<CommitBatchResponse> commitResponse2 = controlPlane.commitFile(
            objectKey2,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 100, 10, 10),
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 1), 200, 10, 10),
                new CommitBatchRequest(new TopicPartition(NONEXISTENT_TOPIC, 0), 300, 10, 10)
            )
        );
        assertThat(commitResponse2).containsExactly(
            new CommitBatchResponse(Errors.NONE, 10, time.milliseconds(), 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(
                new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(EXISTING_TOPIC_ID, 1, EXISTING_TOPIC) , 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(Uuid.ONE_UUID, 0, NONEXISTENT_TOPIC), 11, Integer.MAX_VALUE)
            ), true, Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(
                Errors.NONE,
                List.of(new BatchInfo(objectKey2, 100, 10, 10, 10, TimestampType.CREATE_TIME, time.milliseconds())),
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
        controlPlane.commitFile(objectKey1,
            List.of(new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 1, 10, numberOfRecordsInBatch1)));
        controlPlane.commitFile(objectKey2,
            List.of(new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 100, 10, numberOfRecordsInBatch2)));

        final long expectedLogStartOffset = 0;
        final long expectedHighWatermark = numberOfRecordsInBatch1 + numberOfRecordsInBatch2;
        final long expectedLogAppendTime = time.milliseconds();

        for (int offset = 0; offset < numberOfRecordsInBatch1; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(objectKey1, 1, 10, 0, numberOfRecordsInBatch1, TimestampType.CREATE_TIME, expectedLogAppendTime),
                    new BatchInfo(objectKey2, 100, 10, numberOfRecordsInBatch1, numberOfRecordsInBatch2, TimestampType.CREATE_TIME, expectedLogAppendTime)
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
        for (int offset = numberOfRecordsInBatch1; offset < numberOfRecordsInBatch1 + numberOfRecordsInBatch2; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(
                    new BatchInfo(objectKey2, 100, 10, numberOfRecordsInBatch1, numberOfRecordsInBatch2, TimestampType.CREATE_TIME, expectedLogAppendTime)
                ), expectedLogStartOffset, expectedHighWatermark)
            );
        }
    }

    @Test
    void topicDisappear() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 11, 10, 10)
            )
        );

        final List<FindBatchResponse> findResponse1 = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 0, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse1).containsExactly(
            new FindBatchResponse(
                Errors.NONE,
                List.of(new BatchInfo(objectKey, 11, 10, 0, 10, TimestampType.CREATE_TIME, time.milliseconds())),
                0, 10)
        );

        // Make the topic "disappear".
        when(metadataView.getTopicPartitions(EXISTING_TOPIC))
            .thenReturn(Set.of());
        when(metadataView.getTopicId(EXISTING_TOPIC))
            .thenReturn(Uuid.ZERO_UUID);
        final List<FindBatchResponse> findResponse2 = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 1, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse2).containsExactly(
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1, -1)
        );
    }

    @Test
    void findEmptyBatchOnLastOffset() {
        final String objectKey = "a";

        controlPlane.commitFile(
            objectKey,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 11, 10, 10)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 10, Integer.MAX_VALUE)),
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
            objectKey,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 11, 10, 10)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 11, Integer.MAX_VALUE)),
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
            objectKey,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 11, 10, 10)
            )
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, -1, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 10)
        );
    }

    @Test
    void findBeforeCommit() {
        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 11, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 0)
        );
    }

    @Test
    void commitEmptyBatches() {
        final String objectKey = "a";

        assertThatThrownBy(() -> controlPlane.commitFile(objectKey,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 1, 10, 10),
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 1), 2, 0, 10)
            )
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Batches with size 0 are not allowed");
    }
}
