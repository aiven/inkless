package io.aiven.cps.common;

public class FindBatchRequest {
    public final String topic;
    public final int partition;
    public final long kafkaOffset;

    public FindBatchRequest(final String topic, final int partition, final long kafkaOffset) {
        this.topic = topic;
        this.partition = partition;
        this.kafkaOffset = kafkaOffset;
    }
}
