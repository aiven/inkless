// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

public record CommitBatchResponse(Errors errors, long assignedOffset, long logAppendTime, long logStartOffset) {
    public static CommitBatchResponse success(final long assignedOffset, final long timestamp, final long logStartOffset) {
        return new CommitBatchResponse(Errors.NONE, assignedOffset, timestamp, logStartOffset);
    }
    public static CommitBatchResponse unknownTopicOrPartition() {
        return new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, RecordBatch.NO_TIMESTAMP, -1);
    }
}
