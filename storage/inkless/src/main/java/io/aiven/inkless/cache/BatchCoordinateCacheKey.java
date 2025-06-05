package io.aiven.inkless.cache;

import org.apache.kafka.common.TopicIdPartition;

public record BatchCoordinateCacheKey(TopicIdPartition topicIdPartition, long offset) {
}
