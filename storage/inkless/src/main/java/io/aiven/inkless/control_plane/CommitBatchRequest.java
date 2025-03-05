// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

public record CommitBatchRequest(int requestId,
                                 TopicIdPartition topicIdPartition,
                                 int byteOffset,
                                 int size,
                                 long baseOffset,
                                 long lastOffset,
                                 long batchMaxTimestamp,
                                 TimestampType messageTimestampType,
                                 long producerId,
                                 short producerEpoch,
                                 int baseSequence,
                                 int lastSequence) {

    public static CommitBatchRequest of(
        int requestId,
        TopicIdPartition topicIdPartition,
        int byteOffset,
        RecordBatch batch
    ) {
        return new CommitBatchRequest(
            requestId,
            topicIdPartition,
            byteOffset,
            batch.sizeInBytes(),
            batch.baseOffset(), batch.lastOffset(),
            batch.maxTimestamp(), batch.timestampType(),
            batch.producerId(), batch.producerEpoch(),
            batch.baseSequence(), batch.lastSequence()
        );
    }

    // Accessible for testing
    public static CommitBatchRequest of(
        int requestId,
        TopicIdPartition topicIdPartition,
        int byteOffset,
        int size,
        long baseOffset,
        long lastOffset,
        long batchMaxTimestamp,
        TimestampType messageTimestampType
    ) {
        return new CommitBatchRequest(
            requestId, topicIdPartition, byteOffset, size, baseOffset, lastOffset, batchMaxTimestamp, messageTimestampType,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, RecordBatch.NO_SEQUENCE);
    }

    // Visible for testing
    public static CommitBatchRequest idempotent(
        int requestId,
        TopicIdPartition topicIdPartition,
        int byteOffset,
        int size,
        long baseOffset,
        long lastOffset,
        long batchMaxTimestamp,
        TimestampType messageTimestampType,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int lastSequence
    ) {
        return new CommitBatchRequest(requestId, topicIdPartition, byteOffset, size, baseOffset, lastOffset, batchMaxTimestamp, messageTimestampType,
            producerId, producerEpoch, baseSequence, lastSequence);
    }

    public boolean hasProducerId() {
        return producerId > RecordBatch.NO_PRODUCER_ID;
    }

    public int offsetDelta() {
        return (int) (lastOffset - baseOffset);
    }
}
