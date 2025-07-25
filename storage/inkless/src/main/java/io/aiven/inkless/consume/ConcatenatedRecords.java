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
package io.aiven.inkless.consume;

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.FlattenedIterator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ConcatenatedRecords extends AbstractRecords {

    private final List<MemoryRecords> backingRecords;
    private final int sizeInBytes;

    public ConcatenatedRecords(List<MemoryRecords> backingRecords) {
        this.backingRecords = Objects.requireNonNull(backingRecords, "backing records must be specified");
        int totalSize = 0;
        for (MemoryRecords backingRecord : this.backingRecords) {
            totalSize += backingRecord.sizeInBytes();
        }
        this.sizeInBytes = totalSize;
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        return this::batchIterator;
    }

    @Override
    public AbstractIterator<MutableRecordBatch> batchIterator() {
        return new FlattenedIterator<>(backingRecords.iterator(), MemoryRecords::batchIterator);
    }

    @Override
    public Records slice(int position, int size) {
        // TODO: this could be required to support ShareGroup feature
        throw new UnsupportedOperationException("Slice operation is not supported for ConcatenatedRecords");
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        int recordsStart = 0;
        for (MemoryRecords records : backingRecords) {
            int recordsSize = records.sizeInBytes();
            int recordsEnd = recordsStart + recordsSize;

            if (position >= recordsEnd) {
                recordsStart += recordsSize;
                // This batch was already written
                continue;
            }

            // Position and length where the current batch should be read from to write to the channel
            // The first byte of position is somewhere in this buffer
            int readPosition = position - recordsStart;
            // Length comes from the remaining bytes to write, so use it only when it is smaller than the records size
            // Adjust the record size when reading based on the position to avoid going over buffer limits
            int readLength = Math.min(recordsSize - readPosition, length);
            return records.writeTo(channel, readPosition, readLength);
        }
        return 0;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }
}
