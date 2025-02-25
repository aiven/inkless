package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

public record CommitBatchRequestContext (int requestId, TopicPartition topicPartition, CommitBatchRequest request) {
    public CommitBatchRequestContext {
        Objects.requireNonNull(topicPartition, "topicPartition cannot be null");
        Objects.requireNonNull(request, "request cannot be null");
    }
}
