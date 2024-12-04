// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

public record CommitBatchRequest(TopicPartition topicPartition,
                                 int byteOffset,
                                 int size,
                                 long baseOffset,
                                 long lastOffset,
                                 long maxTimestamp,
                                 long producerId,
                                 short producerEpoch,
                                 int lastSequence) {
    public static CommitBatchRequest of(TopicPartition topicPartition,
                                        int byteOffset,
                                        int size,
                                        long baseOffset,
                                        long lastOffset) {
        return new CommitBatchRequest(
            topicPartition,
            byteOffset,
            size,
            baseOffset,
            lastOffset,
            RecordBatch.NO_TIMESTAMP,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            -1);
    }

    public static CommitBatchRequest ofIdempotent(TopicPartition topicPartition,
                                                  int byteOffset,
                                                  int size,
                                                  long baseOffset,
                                                  long lastOffset,
                                                  long maxTimestamp,
                                                  long producerId,
                                                  short producerEpoch,
                                                  int lastSequence) {
        return new CommitBatchRequest(
            topicPartition,
            byteOffset,
            size,
            baseOffset,
            lastOffset,
            maxTimestamp,
            producerId,
            producerEpoch,
            lastSequence);
    }

    public long numberOfRecords() {
        return lastOffset - baseOffset + 1;
    }

    public boolean hasProducerId() {
        return producerId != RecordBatch.NO_PRODUCER_ID;
    }
}
