package io.aiven.inkless.common;

import org.apache.kafka.common.TopicPartition;

public class FindBatchRequest {
    public final TopicPartition topicPartition;
    public final long kafkaOffset;
    public final int maxPartitionFetchBytes;

    public FindBatchRequest(final TopicPartition topicPartition, final long kafkaOffset, final int maxPartitionFetchBytes) {
        this.topicPartition = topicPartition;
        this.kafkaOffset = kafkaOffset;
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }
}
