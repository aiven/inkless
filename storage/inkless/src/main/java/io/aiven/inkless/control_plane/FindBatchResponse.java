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

import org.apache.kafka.common.protocol.Errors;

import java.util.List;

public record FindBatchResponse(Errors errors,
                                List<BatchInfo> batches,
                                long logStartOffset,
                                long highWatermark) {
    public static final long UNKNOWN_OFFSET = -1L;

    public static FindBatchResponse success(final List<BatchInfo> batches,
                                            final long logStartOffset,
                                            final long highWatermark) {
        return new FindBatchResponse(Errors.NONE, batches, logStartOffset, highWatermark);
    }

    public static FindBatchResponse offsetOutOfRange(final long logStartOffset, final long highWatermark) {
        return new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, logStartOffset, highWatermark);
    }

    public static FindBatchResponse unknownTopicOrPartition() {
        return new FindBatchResponse(
            Errors.UNKNOWN_TOPIC_OR_PARTITION,
            null,
            UNKNOWN_OFFSET,
            UNKNOWN_OFFSET);
    }

    public static FindBatchResponse unknownServerError() {
        return new FindBatchResponse(
            Errors.UNKNOWN_SERVER_ERROR,
            null,
            UNKNOWN_OFFSET,
            UNKNOWN_OFFSET);
    }

    /**
     * Returns the size in bytes of the batches returned by this response, considering only the offsets
     * from {@code startingOffset} onwards.
     * When a subset of a batch needs to be considered, its fractional size is estimated by assuming
     * that all messages in that batch have the same size.
     */
    public long estimatedByteSize(long startingOffset) {
        if (errors != Errors.NONE) { return -1; }
        if (batches.isEmpty()) { return 0; }

        long firstOffset = batches.get(0).metadata().baseOffset();
        long lastOffset = batches.get(batches.size() - 1).metadata().lastOffset();
        if (startingOffset < firstOffset || startingOffset > lastOffset) { return 0; }

        long size = 0;
        for (var batch : batches) {
            if (batch.metadata().lastOffset() < startingOffset) { continue; }
            if (batch.metadata().baseOffset() >= startingOffset) {
                size += batch.metadata().byteSize();
                continue;
            }
            double percentageWithinBatch = (double)(startingOffset - batch.metadata().baseOffset()) / (double)(batch.metadata().lastOffset() - batch.metadata().baseOffset() + 1);

            double estimatedBatchSize = (1.0 - percentageWithinBatch) * batch.metadata().byteSize();
            size += (long)estimatedBatchSize;
        }
        return size;
    }
}
