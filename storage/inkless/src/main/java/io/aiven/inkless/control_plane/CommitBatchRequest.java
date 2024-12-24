// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.storage.internals.log.BatchMetadata;

public record CommitBatchRequest(TopicIdPartition topicIdPartition,
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
    public static CommitBatchRequest of(TopicIdPartition topicIdPartition,
                                        int byteOffset,
                                        int size,
                                        long baseOffset,
                                        long lastOffset,
                                        long batchMaxTimestamp,
                                        TimestampType messageTimestampType) {
        return new CommitBatchRequest(topicIdPartition, byteOffset, size, baseOffset, lastOffset, batchMaxTimestamp, messageTimestampType,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, RecordBatch.NO_SEQUENCE);
    }

    public static CommitBatchRequest idempotent(TopicIdPartition topicIdPartition,
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
        return new CommitBatchRequest(topicIdPartition, byteOffset, size, baseOffset, lastOffset, batchMaxTimestamp, messageTimestampType,
            producerId, producerEpoch, baseSequence, lastSequence);
    }

    public boolean hasProducerId() {
        return producerId > RecordBatch.NO_PRODUCER_ID;
    }

    public int offsetDelta() {
        return (int) (lastOffset - baseOffset);
    }

    public BatchMetadata batchMetadata() {
        return new BatchMetadata(
            lastSequence,
            lastOffset,
            offsetDelta(),
            batchMaxTimestamp
        );
    }
}
