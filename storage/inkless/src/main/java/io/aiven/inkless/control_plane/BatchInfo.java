// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.record.TimestampType;

import io.aiven.inkless.common.ByteRange;

public record BatchInfo(
    String objectKey,
    long byteOffset,
    long size,
    long recordOffset,
    long numberOfRecords,
    TimestampType timestampType,
    long logAppendTime
) {
    public ByteRange range() {
        return new ByteRange(byteOffset, size);
    }
}
