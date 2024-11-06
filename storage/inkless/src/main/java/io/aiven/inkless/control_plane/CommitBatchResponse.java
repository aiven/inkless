// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;

public record CommitBatchResponse(Errors errors, long assignedOffset) {
    public static CommitBatchResponse success(final long assignedOffset) {
        return new CommitBatchResponse(Errors.NONE, assignedOffset);
    }
    public static CommitBatchResponse unknownTopicOrPartition() {
        return new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1);
    }
}
