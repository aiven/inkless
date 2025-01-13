// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;

public record DeleteRecordsRequest(TopicIdPartition topicIdPartition,
                                   long offset) {
}
