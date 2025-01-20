// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.TimestampType;

import java.time.Instant;
import java.util.List;

public record FileMergeWorkItem(long workItemId,
                                Instant createdAt,
                                List<File> files) {

    public record File(long fileId,
                       String objectKey,
                       long size,
                       long usedSize,
                       List<Batch> batches) {
    }

    public record Batch(long batchId,
                        TopicIdPartition topicIdPartition,
                        String objectKey,
                        long byteOffset,
                        long size,
                        long recordOffset,
                        long requestBaseOffset,
                        long requestLastOffset,
                        long logAppendTimestamp,
                        long batchMaxTimestamp,
                        TimestampType timestampType) {
    }
}
