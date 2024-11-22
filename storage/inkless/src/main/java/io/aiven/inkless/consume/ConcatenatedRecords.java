// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.List;

public class ConcatenatedRecords extends AbstractRecords {

    private final List<MemoryRecords> backingRecords;
    private final int sizeInBytes;

    public ConcatenatedRecords(List<MemoryRecords> backingRecords) {
        this.backingRecords = backingRecords;
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
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        throw new UnsupportedOperationException("Unable to downConvert concatenated records");
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
            // The first byte of position is somewhere in this buffer
            int writePosition = position - recordsStart;
            int writeLength = Math.min(recordsSize, length);
            return records.writeTo(channel, writePosition, writeLength);
        }
        return 0;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }
}
