// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

public record CommitBatchRequest(TopicIdPartition topicIdPartition,
                                 int byteOffset,
                                 int size,
                                 long baseOffset,
                                 long lastOffset,
                                 long batchMaxTimestamp,
                                 TimestampType messageTimestampType) {
    public static CommitBatchRequest of(
        TopicIdPartition topicIdPartition,
        int byteOffset,
        RecordBatch batch
    ) {
        return new CommitBatchRequest(
            topicIdPartition,
            byteOffset,
            batch.sizeInBytes(),
            batch.baseOffset(), batch.lastOffset(),
            batch.maxTimestamp(), batch.timestampType()
        );
    }

    // Accessible for testing
    public static CommitBatchRequest of(
        TopicIdPartition topicIdPartition,
        int byteOffset,
        int size,
        long baseOffset,
        long lastOffset,
        long batchMaxTimestamp,
        TimestampType messageTimestampType
    ) {
        return new CommitBatchRequest(topicIdPartition, byteOffset, size, baseOffset, lastOffset, batchMaxTimestamp, messageTimestampType);
    }

    public int offsetDelta() {
        return (int) (lastOffset - baseOffset);
    }
}
