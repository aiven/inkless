package io.aiven.inkless.common;

import java.util.Objects;

import org.apache.kafka.common.TopicPartition;

public class BatchCoordinates {
    public final TopicPartition topicPartition;
    public final long offset;

    public BatchCoordinates(final TopicPartition topicPartition,
                            final long offset) {
        this.topicPartition = topicPartition;
        this.offset = offset;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BatchCoordinates that = (BatchCoordinates) o;
        return offset == that.offset
            && Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicPartition.hashCode();
        result = prime * result + Long.hashCode(offset);
        return result;
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "#" + offset;
    }
}
