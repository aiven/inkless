// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;

public record CommitBatchRequest(TopicPartition topicPartition,
                                 int byteOffset,
                                 int size,
                                 long numberOfRecords) {
}
