/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ConcatenatedRecordsTest {

    @Mock
    TransferableChannel channel;

    @Test
    void testNullList() {
        assertThrows(NullPointerException.class, () -> new ConcatenatedRecords(null));
    }

    @Test
    void testEmptyList() throws IOException {
        ConcatenatedRecords records = new ConcatenatedRecords(Collections.emptyList());

        assertFalse(records.batches().iterator().hasNext());
        assertFalse(records.batchIterator().hasNext());

        assertEquals(0, records.writeTo(channel, 0, 1));
        assertEquals(0, records.sizeInBytes());
    }

    @Test
    void testListContainingEmptyRecords() throws IOException {
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(MemoryRecords.EMPTY));

        assertFalse(records.batches().iterator().hasNext());
        assertFalse(records.batchIterator().hasNext());

        assertEquals(0, records.writeTo(channel, 0, 1));
        assertEquals(0, records.sizeInBytes());
    }

    @Test
    void testListWithOneBatch() throws IOException {
        MemoryRecords backingRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(backingRecords));

        assertTrue(records.batches().iterator().hasNext());
        assertTrue(records.batchIterator().hasNext());

        setupChannel(1000);
        assertEquals(backingRecords.writeTo(channel, 0, 1), records.writeTo(channel, 0, 1));
        assertEquals(backingRecords.sizeInBytes(), records.sizeInBytes());
    }

    @Test
    void testListWithTwoBatches() throws IOException {
        MemoryRecords backingRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(backingRecords, backingRecords));

        assertTrue(records.batches().iterator().hasNext());
        assertTrue(records.batchIterator().hasNext());

        setupChannel(1000);
        assertEquals(backingRecords.writeTo(channel, 0, 1), records.writeTo(channel, 0, 1));
        assertEquals(2 * backingRecords.sizeInBytes(), records.sizeInBytes());
    }

    @Test
    void testSegmentedWrites() throws IOException {
        MemoryRecords backingRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(backingRecords));

        setupChannel(3); // limit the block size so DefaultRecordsSend has to make multiple iterations.
        int totalBytesSent = 0;
        while (totalBytesSent < records.sizeInBytes()) {
            final int bytesSent = records.writeTo(channel, totalBytesSent, records.sizeInBytes() - totalBytesSent);
            totalBytesSent += bytesSent;
        }
        assertEquals(records.sizeInBytes(), totalBytesSent);
        assertEquals(0, records.writeTo(channel, totalBytesSent, 1));
    }

    private void setupChannel(int maxReadSize) {
        try {
            // Simulate a channel writing at most a fixed number of bytes at a time.
            when(channel.write((ByteBuffer) any()))
                    .thenAnswer(invocation -> Math.min(
                            ((ByteBuffer) invocation.getArgument(0)).remaining(),
                            maxReadSize));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testWriteToWithPositionOutOfRange() throws IOException {
        // Given a single record with 10 bytes
        MemoryRecords records1 = mock(MemoryRecords.class);
        when(records1.sizeInBytes()).thenReturn(10);

        // When reading from position 11, then no bytes should be written
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(records1));
        assertThat(records.writeTo(channel, 11, 1)).isZero();
        verifyNoMoreInteractions(records1);
    }

    @Test
    void testWriteWithPositionOnNextBuffer() throws IOException {
        // Given two records, each with 10 bytes
        MemoryRecords records1 = mock(MemoryRecords.class);
        MemoryRecords records2 = mock(MemoryRecords.class);
        when(records1.sizeInBytes()).thenReturn(10);
        when(records2.sizeInBytes()).thenReturn(10);

        ConcatenatedRecords records = new ConcatenatedRecords(List.of(records1, records2));
        assertThat(records.sizeInBytes()).isEqualTo(20);

        // When reading from position 11, it should skip the first batch and write from the second batch
        records.writeTo(channel, 11, 1);
        verify(records2).writeTo(channel, 1, 1);
        verifyNoMoreInteractions(records1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 9})
    void testWriteToWithPositionWithinFirstBatch(int pos) throws IOException {
        // Given a single record with 10 bytes
        MemoryRecords records1 = mock(MemoryRecords.class);
        final int size = 10;
        when(records1.sizeInBytes()).thenReturn(size);

        ConcatenatedRecords records = new ConcatenatedRecords(List.of(records1));
        records.writeTo(channel, pos, size - pos);
        verify(records1).writeTo(channel, pos, size - pos);
    }

    @Test
    public void testWriteToBoundaryConditions() throws IOException {
        // Given two records
        List<MemoryRecords> records = List.of(
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("record1".getBytes())),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("record2".getBytes()))
        );
        ConcatenatedRecords concatenatedRecords = new ConcatenatedRecords(records);
        TransferableChannel mockChannel = mock(TransferableChannel.class);

        // Test when position is exactly at recordsEnd
        int position = records.get(0).sizeInBytes();
        int length = 10;
        concatenatedRecords.writeTo(mockChannel, position, length);
        verify(mockChannel, times(1)).write(any(ByteBuffer.class));

        // Test when position is just below recordsEnd
        position = records.get(0).sizeInBytes() - 1;
        concatenatedRecords.writeTo(mockChannel, position, length);
        verify(mockChannel, times(2)).write(any(ByteBuffer.class));

        // Test when position is just above recordsEnd
        position = records.get(0).sizeInBytes() + 1;
        concatenatedRecords.writeTo(mockChannel, position, length);
        verify(mockChannel, times(3)).write(any(ByteBuffer.class));
    }
}
