// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import io.aiven.inkless.common.PlainObjectKey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControlPlaneTest {
    static final String EXISTING_TOPIC = "topic-existing";
    static final Uuid EXISTING_TOPIC_ID = new Uuid(10, 10);
    static final TopicIdPartition EXISTING_TOPIC_ID_PARTITION = new TopicIdPartition(EXISTING_TOPIC_ID, 0, EXISTING_TOPIC);
    static final String NONEXISTENT_TOPIC = "topic-nonexistent";

    MetadataView metadataView;

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
    }

    @Test
    void emptyCommit() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final List<CommitBatchResponse> commitBatchRespons = controlPlane.commitFile(
            new PlainObjectKey("a", "a"),
            List.of()
        );
        assertThat(commitBatchRespons).isEmpty();
    }

    @Test
    void successfulCommitToExistingPartitions() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final PlainObjectKey objectKey1 = new PlainObjectKey("a", "a1");
        final List<CommitBatchResponse> commitResponse1 = controlPlane.commitFile(
            objectKey1,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 1, 10, 10),
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 1), 2, 10, 10),
                new CommitBatchRequest(new TopicPartition(NONEXISTENT_TOPIC, 0), 3, 10, 10)
            )
        );
        assertThat(commitResponse1).containsExactly(
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1)
        );

        final PlainObjectKey objectKey2 = new PlainObjectKey("a", "a2");
        final List<CommitBatchResponse> commitResponse2 = controlPlane.commitFile(
            objectKey2,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 100, 10, 10),
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 1), 200, 10, 10),
                new CommitBatchRequest(new TopicPartition(NONEXISTENT_TOPIC, 0), 300, 10, 10)
            )
        );
        assertThat(commitResponse2).containsExactly(
            new CommitBatchResponse(Errors.NONE, 10),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1),
            new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1)
        );

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(
                new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(EXISTING_TOPIC_ID, 1, EXISTING_TOPIC) , 11, Integer.MAX_VALUE),
                new FindBatchRequest(new TopicIdPartition(Uuid.ONE_UUID, 0, NONEXISTENT_TOPIC), 11, Integer.MAX_VALUE)
            ), true, Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.NONE, List.of(new BatchInfo(objectKey2, 100, 10, 10)), 20),
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1),
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1)
        );
    }

    @Test
    void fullSpectrumFind() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final PlainObjectKey objectKey1 = new PlainObjectKey("a", "a1");
        final PlainObjectKey objectKey2 = new PlainObjectKey("a", "a2");
        final int numberOfRecordsInBatch1 = 3;
        final int numberOfRecordsInBatch2 = 2;
        controlPlane.commitFile(objectKey1,
            List.of(new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 1, 10, numberOfRecordsInBatch1)));
        controlPlane.commitFile(objectKey2,
            List.of(new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 100, 10, numberOfRecordsInBatch2)));

        final long expectedHighWatermark = numberOfRecordsInBatch1 + numberOfRecordsInBatch2;

        for (int offset = 0; offset < numberOfRecordsInBatch1; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(new BatchInfo(objectKey1, 1, 10, numberOfRecordsInBatch1)), expectedHighWatermark)
            );
        }
        for (int offset = numberOfRecordsInBatch1; offset < numberOfRecordsInBatch1 + numberOfRecordsInBatch2; offset++) {
            final List<FindBatchResponse> findResponse = controlPlane.findBatches(
                List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, offset, Integer.MAX_VALUE)), true, Integer.MAX_VALUE);
            assertThat(findResponse).containsExactly(
                new FindBatchResponse(Errors.NONE, List.of(new BatchInfo(objectKey2, 100, 10, numberOfRecordsInBatch2)), expectedHighWatermark)
            );
        }
    }

    @Test
    void topicDisappear() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final PlainObjectKey objectKey = new PlainObjectKey("a", "a");
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
            new FindBatchResponse(Errors.NONE, List.of(new BatchInfo(objectKey, 11, 10, 10)), 10)
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
            new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1)
        );
    }

    @Test
    void findOffsetOutOfRange() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final PlainObjectKey objectKey = new PlainObjectKey("a", "a");
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
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 10)
        );
    }

    @Test
    void findNegativeOffset() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final PlainObjectKey objectKey = new PlainObjectKey("a", "a");
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
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 10)
        );
    }

    @Test
    void findBeforeCommit() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 11, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).containsExactly(
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0)
        );
    }
}
