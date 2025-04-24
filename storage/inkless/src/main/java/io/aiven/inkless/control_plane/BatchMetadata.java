/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import io.aiven.inkless.common.ByteRange;

public record BatchMetadata (
    byte magic,
    TopicIdPartition topicIdPartition,
    long byteOffset,
    long byteSize,
    long baseOffset,
    long lastOffset,
    long logAppendTimestamp,
    long batchMaxTimestamp,
    TimestampType timestampType,
    long producerId,
    short producerEpoch,
    int baseSequence,
    int lastSequence
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
        long byteSize,
        long baseOffset,
        long lastOffset,
        long logAppendTimestamp,
        long batchMaxTimestamp,
        TimestampType timestampType
    ) {
        return new BatchMetadata(
            RecordBatch.CURRENT_MAGIC_VALUE,
            topicIdPartition,
            byteOffset,
            byteSize,
            baseOffset,
            lastOffset,
            logAppendTimestamp,
            batchMaxTimestamp,
            timestampType,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            RecordBatch.NO_SEQUENCE
        );
    }

    public ByteRange range() {
        return new ByteRange(byteOffset, byteSize);
    }

    public long timestamp() {
        // See how timestamps are assigned in
        // https://github.com/aiven/inkless/blob/e124d3975bdb3a9ec85eee2fba7a1b0a6967d3a6/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L271-L276
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            return logAppendTimestamp;
        } else {
            return batchMaxTimestamp;
        }
    }
}
