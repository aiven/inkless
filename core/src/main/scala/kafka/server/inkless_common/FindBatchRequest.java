package kafka.server.inkless_common;

import org.apache.kafka.common.TopicPartition;

public class FindBatchRequest {
    public final TopicPartition topicPartition;
    public final long kafkaOffset;

    public FindBatchRequest(final TopicPartition topicPartition, final long kafkaOffset) {
        this.topicPartition = topicPartition;
        this.kafkaOffset = kafkaOffset;
    }
}
