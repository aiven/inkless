// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;

public record CommitBatchResponse(Errors errors, long assignedOffset, long logStartOffset) {
    public static CommitBatchResponse success(final long assignedOffset, final long logStartOffset) {
        return new CommitBatchResponse(Errors.NONE, assignedOffset, logStartOffset);
    }
    public static CommitBatchResponse unknownTopicOrPartition() {
        return new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1);
    }
}
