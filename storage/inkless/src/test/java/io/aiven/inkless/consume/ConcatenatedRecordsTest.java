// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ConcatenatedRecordsTest {

    @Mock
    TransferableChannel channel;

    @Test
    public void testNullList() {
        assertThrows(NullPointerException.class, () -> new ConcatenatedRecords(null));
    }

    @Test
    public void testEmptyList() throws IOException {
        ConcatenatedRecords records = new ConcatenatedRecords(Collections.emptyList());

        assertFalse(records.batches().iterator().hasNext());
        assertFalse(records.batchIterator().hasNext());
        assertThrows(UnsupportedOperationException.class, () -> records.downConvert((byte) 1, 0, null));

        assertEquals(0, records.writeTo(channel, 0, 1));
        assertEquals(0, records.sizeInBytes());
    }

    @Test
    public void testListContainingEmptyRecords() throws IOException {
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(MemoryRecords.EMPTY));

        assertFalse(records.batches().iterator().hasNext());
        assertFalse(records.batchIterator().hasNext());
        assertThrows(UnsupportedOperationException.class, () -> records.downConvert((byte) 1, 0, null));

        assertEquals(0, records.writeTo(channel, 0, 1));
        assertEquals(0, records.sizeInBytes());
    }

    @Test
    public void testListWithOneBatch() throws IOException {
        MemoryRecords backingRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(backingRecords));

        assertTrue(records.batches().iterator().hasNext());
        assertTrue(records.batchIterator().hasNext());
        assertThrows(UnsupportedOperationException.class, () -> records.downConvert((byte) 1, 0, null));

        setupChannel(1000);
        assertEquals(backingRecords.writeTo(channel, 0, 1), records.writeTo(channel, 0, 1));
        assertEquals(backingRecords.sizeInBytes(), records.sizeInBytes());
    }

    @Test
    public void testListWithTwoBatches() throws IOException {
        MemoryRecords backingRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(backingRecords, backingRecords));

        assertTrue(records.batches().iterator().hasNext());
        assertTrue(records.batchIterator().hasNext());
        assertThrows(UnsupportedOperationException.class, () -> records.downConvert((byte) 1, 0, null));

        setupChannel(1000);
        assertEquals(backingRecords.writeTo(channel, 0, 1), records.writeTo(channel, 0, 1));
        assertEquals(2 * backingRecords.sizeInBytes(), records.sizeInBytes());
    }

    @Test
    public void testSegmentedWrites() throws IOException {
        MemoryRecords backingRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        ConcatenatedRecords records = new ConcatenatedRecords(List.of(backingRecords));

        setupChannel(3); // limit the block size so DefaultRecordsSend has to make multiple iterations.
        int bytesSent = 0;
        while (bytesSent < records.sizeInBytes()) {
            bytesSent += records.writeTo(channel, bytesSent, records.sizeInBytes() - bytesSent);
        }
        assertEquals(records.sizeInBytes(), bytesSent);
        assertEquals(0, records.writeTo(channel, bytesSent, 1));
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

}
