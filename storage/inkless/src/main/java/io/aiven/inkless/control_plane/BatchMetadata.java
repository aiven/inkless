// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.TimestampType;

import io.aiven.inkless.common.ByteRange;

public record BatchMetadata (
    TopicIdPartition topicIdPartition,
    long byteOffset,
    long size,
    long baseOffset,
    long lastOffset,
    long logAppendTimestamp,
    long batchMaxTimestamp,
    TimestampType timestampType
) {
    public BatchMetadata {
        if (lastOffset < baseOffset) {
            throw new IllegalArgumentException("Invalid record offsets, last cannot be less than base: base="
                + baseOffset + ", last=" + lastOffset);
        }
    }

    // Accessible for testing
    public static BatchMetadata of(
        TopicIdPartition topicIdPartition,
        long byteOffset,
        long size,
        long baseOffset,
        long lastOffset,
        long logAppendTimestamp,
        long batchMaxTimestamp,
        TimestampType timestampType
    ) {
        return new BatchMetadata(
            topicIdPartition,
            byteOffset,
            size,
            baseOffset,
            lastOffset,
            logAppendTimestamp,
            batchMaxTimestamp,
            timestampType
        );
    }

    public ByteRange range() {
        return new ByteRange(byteOffset, size);
    }
}
