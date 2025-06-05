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
import org.apache.kafka.common.record.TimestampType;

public record BatchCoordinate(
    TopicIdPartition topicIdPartition,
    String objectKey,
    long byteOffset,
    long byteSize,
    long baseOffset,
    long lastOffset,
    TimestampType timestampType,
    long logAppendTimestamp,
    byte magic,
    long logStartOffset
) {

    public BatchCoordinate {
        if (lastOffset < baseOffset) {
            throw new IllegalArgumentException(
                String.format(
                    "lastOffset must be greater than or equal to baseOffset, but got lastOffset=%d and baseOffset=%d",
                    lastOffset,
                    baseOffset
                )
            );
        }
        if (byteSize <= 0) {
            throw new IllegalArgumentException(
                String.format("byteSize must be positive, but got %d", byteSize)
            );
        }
        if (byteOffset < 0) {
            throw new IllegalArgumentException(
                String.format("byteOffset must be non-negative, but got %d", byteOffset)
            );
        }
    }

    public BatchInfo batchInfo(long batchId) {
        return new BatchInfo(
            batchId,
            objectKey,
            new BatchMetadata(
                magic,
                topicIdPartition,
                byteOffset,
                byteSize,
                baseOffset,
                lastOffset,
                logAppendTimestamp,
                0,
                timestampType
            )
        );
    }

    @Override
    public String toString() {
        return "BatchCoordinate[" + topicIdPartition.toString() + "-> " + objectKey + ", (" + baseOffset + ", " + lastOffset + ")]";
    }
}