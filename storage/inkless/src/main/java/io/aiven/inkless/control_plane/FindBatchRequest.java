// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;

public record FindBatchRequest(TopicIdPartition topicIdPartition,
                               long offset,
                               int maxPartitionFetchBytes) {
}
