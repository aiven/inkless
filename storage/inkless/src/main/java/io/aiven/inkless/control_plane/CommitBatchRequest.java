// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

public record CommitBatchRequest(TopicPartition topicPartition,
                                 int byteOffset,
                                 int size,
                                 long numberOfRecords,
                                 long batchMaxTimestamp,
                                 TimestampType messageTimestampType) {
}
