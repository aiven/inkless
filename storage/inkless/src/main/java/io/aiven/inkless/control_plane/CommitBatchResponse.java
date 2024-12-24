// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.storage.internals.log.BatchMetadata;

public record CommitBatchResponse(Errors errors, long assignedOffset, long logAppendTime, long logStartOffset, boolean isDuplicate, CommitBatchRequest request) {
    public static CommitBatchResponse of(Errors errors, long assignedOffset, long logAppendTime, long logStartOffset) {
        return new CommitBatchResponse(errors, assignedOffset, logAppendTime, logStartOffset, false, null);
    }

    public static CommitBatchResponse success(final long assignedOffset, final long timestamp, final long logStartOffset, final CommitBatchRequest request) {
        return new CommitBatchResponse(Errors.NONE, assignedOffset, timestamp, logStartOffset, false, request);
    }

    public static CommitBatchResponse unknownTopicOrPartition() {
        return new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, RecordBatch.NO_TIMESTAMP, -1, false, null);
    }

    public static CommitBatchResponse invalidProducerEpoch() {
        return new CommitBatchResponse(Errors.INVALID_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, -1, false, null);
    }

    public static CommitBatchResponse sequenceOutOfOrder(final CommitBatchRequest request) {
        return new CommitBatchResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1, RecordBatch.NO_TIMESTAMP, -1, false, request);
    }

    public static CommitBatchResponse ofDuplicate(BatchMetadata batchMetadata, long logStartOffset) {
        return new CommitBatchResponse(Errors.NONE, batchMetadata.lastOffset, batchMetadata.timestamp, logStartOffset, true, null);
    }

    public boolean isOk() {
        return errors.equals(Errors.NONE) && !isDuplicate;
    }
}
