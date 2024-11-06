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
    void notAppendForUnknownTopicOrPartition() {
        final ControlPlane controlPlane = new ControlPlane(metadataView);
        final PlainObjectKey objectKey = new PlainObjectKey("a", "a");
        final List<CommitBatchResponse> commitResponse = controlPlane.commitFile(
            objectKey,
            List.of(
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 0), 0, 10, 10),
                new CommitBatchRequest(new TopicPartition(EXISTING_TOPIC, 1), 0, 10, 10),
                new CommitBatchRequest(new TopicPartition(NONEXISTENT_TOPIC, 0), 0, 10, 10)
            )
        );
        assertThat(commitResponse.stream().map(CommitBatchResponse::errors))
            .containsExactly(Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION);

        final List<FindBatchResponse> findResponse = controlPlane.findBatches(
            List.of(new FindBatchRequest(EXISTING_TOPIC_ID_PARTITION, 0, Integer.MAX_VALUE)),
            true,
            Integer.MAX_VALUE);
        assertThat(findResponse).isEqualTo(List.of(
            new FindBatchResponse(Errors.NONE, List.of(new BatchInfo(objectKey, 0, 10, 10)), 10)
        ));
    }
}
