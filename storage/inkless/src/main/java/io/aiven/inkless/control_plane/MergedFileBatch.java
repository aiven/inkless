// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;

public record MergedFileBatch(TopicIdPartition topicIdPartition,
                              long byteOffset,
                              long size,
                              long baseOffset,
                              long firstOffset,
                              long lastOffset,
                              long batchMaxTimestamp,
                              long logAppendTimestamp,
                              TimestampType messageTimestampType,
                              List<Long> parentBatches) {
}
