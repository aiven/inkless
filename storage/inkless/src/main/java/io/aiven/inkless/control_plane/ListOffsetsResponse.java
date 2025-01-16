// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;

public record ListOffsetsResponse(
        Errors errors,
        TopicIdPartition topicIdPartition,
        long timestamp,
        long offset
) {

    public static ListOffsetsResponse success(TopicIdPartition topicIdPartition, final long timestamp, final long offset) {
        return new ListOffsetsResponse(Errors.NONE, topicIdPartition, timestamp, offset);
    }
    public static ListOffsetsResponse unknownTopicOrPartition(TopicIdPartition topicIdPartition) {
        return new ListOffsetsResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicIdPartition, -1, -1);
    }
}
