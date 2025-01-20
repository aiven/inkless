// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.record.TimestampType;

import io.aiven.inkless.common.ByteRange;

public record BatchInfo(
    long batchId,
    String objectKey,
    long byteOffset,
    long size,
    long recordOffset,
    long requestBaseOffset,
    long requestLastOffset,
    long logAppendTimestamp,
    long batchMaxTimestamp,
    TimestampType timestampType
) {
    public BatchInfo {
        if (requestLastOffset < requestBaseOffset) {
            throw new IllegalArgumentException("Invalid request offsets last cannot be less than base: base="
                + requestBaseOffset + ", last=" + requestLastOffset);
        }
    }

    public static BatchInfo of(
        long batchId,
        String objectKey,
        long byteOffset,
        long size,
        long recordOffset,
        long requestBaseOffset,
        long requestLastOffset,
        long logAppendTimestamp,
        long batchMaxTimestamp,
        TimestampType timestampType
    ) {
        return new BatchInfo(
            batchId,
            objectKey,
            byteOffset,
            size,
            recordOffset,
            requestBaseOffset,
            requestLastOffset,
            logAppendTimestamp,
            batchMaxTimestamp,
            timestampType
        );
    }

    public ByteRange range() {
        return new ByteRange(byteOffset, size);
    }

    public static TimestampType timestampTypeFromId(short id) {
        return switch (id) {
            case -1 -> TimestampType.NO_TIMESTAMP_TYPE;
            case 0 -> TimestampType.CREATE_TIME;
            case 1 -> TimestampType.LOG_APPEND_TIME;
            default -> throw new IllegalStateException("Unexpected value: " + id);
        };
    }

    public int offsetDelta() {
        return (int) (requestLastOffset - requestBaseOffset);
    }

    public long lastOffset() {
        return recordOffset + offsetDelta();
    }
}
