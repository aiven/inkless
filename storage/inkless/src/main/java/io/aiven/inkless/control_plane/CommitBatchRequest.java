// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.TimestampType;

public record CommitBatchRequest(TopicIdPartition topicIdPartition,
                                 int byteOffset,
                                 int size,
                                 long numberOfRecords,
                                 long batchMaxTimestamp,
                                 TimestampType messageTimestampType) {
}
