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
package io.aiven.inkless.produce.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for PooledBuffer interface contract.
 * These tests verify reference counting, InputStream creation, and data copying behavior.
 */
class PooledBufferTest {

    private static final int ONE_MB = 1024 * 1024;

    private BufferPool pool;

    @BeforeEach
    void setUp() {
        pool = new ElasticBufferPool(2);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void buffer_returnsByteBuffer() {
        PooledBuffer buffer = pool.acquire(1024);

        ByteBuffer byteBuffer = buffer.buffer();
        assertEquals(ONE_MB, byteBuffer.capacity());

        buffer.release();
    }

    @Test
    void refCount_startsAtOne() {
        PooledBuffer buffer = pool.acquire(1024);

        assertEquals(1, buffer.refCount());

        buffer.release();
    }

    @Test
    void retain_incrementsRefCount() {
        PooledBuffer buffer = pool.acquire(1024);

        assertEquals(1, buffer.refCount());

        buffer.retain();
        assertEquals(2, buffer.refCount());

        buffer.retain();
        assertEquals(3, buffer.refCount());

        // Release all references
        buffer.release();
        buffer.release();
        buffer.release();
    }

    @Test
    void retain_canBeCalledMultipleTimes() {
        PooledBuffer buffer = pool.acquire(1024);

        for (int i = 0; i < 10; i++) {
            buffer.retain();
        }
        assertEquals(11, buffer.refCount());

        // Release all
        for (int i = 0; i < 11; i++) {
            buffer.release();
        }
    }

    @Test
    void release_decrementsRefCount() {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.retain();
        buffer.retain();

        assertEquals(3, buffer.refCount());

        buffer.release();
        assertEquals(2, buffer.refCount());

        buffer.release();
        assertEquals(1, buffer.refCount());

        buffer.release();
        // After final release, refCount is 0 and buffer is returned to pool
    }

    @Test
    void release_returnsToPoolWhenZero() {
        // Get first buffer
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        assertEquals(1, pool.activeBufferCount());

        // Get second buffer
        PooledBuffer buffer2 = pool.acquire(ONE_MB);
        assertEquals(2, pool.activeBufferCount());

        // Release first buffer - should return to pool
        buffer1.release();
        assertEquals(1, pool.activeBufferCount());

        // Release second buffer
        buffer2.release();
        assertEquals(0, pool.activeBufferCount());
    }

    @Test
    void release_onlyReturnsToPoolAtZero() {
        PooledBuffer buffer = pool.acquire(ONE_MB);
        buffer.retain();
        buffer.retain();

        assertEquals(1, pool.activeBufferCount());

        // First release - still active
        buffer.release();
        assertEquals(1, pool.activeBufferCount());

        // Second release - still active
        buffer.release();
        assertEquals(1, pool.activeBufferCount());

        // Third release - returned to pool
        buffer.release();
        assertEquals(0, pool.activeBufferCount());
    }

    @Test
    void release_throwsOnDoubleRelease() {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.release();

        assertThrows(IllegalStateException.class, buffer::release);
    }

    @Test
    void buffer_throwsAfterFullRelease() {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.release();

        assertThrows(IllegalStateException.class, buffer::buffer);
    }

    @Test
    void retain_throwsAfterFullRelease() {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.release();

        assertThrows(IllegalStateException.class, buffer::retain);
    }

    @Test
    void asInputStream_returnsReadableStream() throws IOException {
        PooledBuffer buffer = pool.acquire(1024);

        // Write some data
        ByteBuffer byteBuffer = buffer.buffer();
        byte[] testData = "Hello, World!".getBytes();
        byteBuffer.put(testData);
        byteBuffer.flip();

        // Read via InputStream
        InputStream stream = buffer.asInputStream();
        byte[] readData = new byte[testData.length];
        int bytesRead = stream.read(readData);

        assertEquals(testData.length, bytesRead);
        assertArrayEquals(testData, readData);

        buffer.release();
    }

    @Test
    void asInputStream_multipleCallsAreIndependent() throws IOException {
        PooledBuffer buffer = pool.acquire(1024);

        // Write some data
        ByteBuffer byteBuffer = buffer.buffer();
        byte[] testData = "Test Data".getBytes();
        byteBuffer.put(testData);
        byteBuffer.flip();

        // Create two independent streams
        InputStream stream1 = buffer.asInputStream();
        InputStream stream2 = buffer.asInputStream();

        // They should be different instances
        assertNotSame(stream1, stream2);

        // Read from first stream
        byte[] data1 = new byte[testData.length];
        stream1.read(data1);

        // Second stream should still be at position 0
        byte[] data2 = new byte[testData.length];
        stream2.read(data2);

        assertArrayEquals(testData, data1);
        assertArrayEquals(testData, data2);

        buffer.release();
    }

    @Test
    void asInputStream_available_returnsRemaining() throws IOException {
        PooledBuffer buffer = pool.acquire(1024);

        ByteBuffer byteBuffer = buffer.buffer();
        byte[] testData = new byte[100];
        byteBuffer.put(testData);
        byteBuffer.flip();

        InputStream stream = buffer.asInputStream();
        assertEquals(100, stream.available());

        // Read some bytes
        stream.read(new byte[30]);
        assertEquals(70, stream.available());

        buffer.release();
    }

    @Test
    void copyTo_copiesCorrectBytes() {
        PooledBuffer buffer = pool.acquire(1024);

        // Write some data at specific positions
        ByteBuffer byteBuffer = buffer.buffer();
        byte[] sourceData = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        byteBuffer.put(sourceData);

        // Copy middle portion
        byte[] dest = new byte[4];
        buffer.copyTo(3, dest, 0, 4);

        assertArrayEquals(new byte[]{3, 4, 5, 6}, dest);

        buffer.release();
    }

    @Test
    void copyTo_copiesWithOffset() {
        PooledBuffer buffer = pool.acquire(1024);

        ByteBuffer byteBuffer = buffer.buffer();
        byte[] sourceData = {10, 20, 30, 40, 50};
        byteBuffer.put(sourceData);

        byte[] dest = new byte[10];
        dest[0] = 99; // Pre-existing data
        dest[1] = 99;

        buffer.copyTo(1, dest, 2, 3);

        assertEquals(99, dest[0]);
        assertEquals(99, dest[1]);
        assertEquals(20, dest[2]);
        assertEquals(30, dest[3]);
        assertEquals(40, dest[4]);

        buffer.release();
    }

    @Test
    void copyTo_throwsAfterRelease() {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.buffer().put(new byte[10]);
        buffer.release();

        assertThrows(IllegalStateException.class, () ->
            buffer.copyTo(0, new byte[10], 0, 10));
    }

    @Test
    void asInputStream_throwsAfterRelease() {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.release();

        assertThrows(IllegalStateException.class, buffer::asInputStream);
    }

    @Test
    void asInputStream_skip_advancesPosition() throws java.io.IOException {
        PooledBuffer buffer = pool.acquire(1024);

        ByteBuffer byteBuffer = buffer.buffer();
        for (int i = 0; i < 100; i++) {
            byteBuffer.put((byte) i);
        }
        byteBuffer.flip();  // Make data readable from position 0

        try (java.io.InputStream is = buffer.asInputStream()) {
            // Skip first 10 bytes
            long skipped = is.skip(10);
            assertEquals(10, skipped);

            // Next read should return byte 10
            assertEquals(10, is.read());

            // Skip to near end
            skipped = is.skip(80);
            assertEquals(80, skipped);

            // Read remaining bytes
            assertEquals(91, is.read());
        }

        buffer.release();
    }

    @Test
    void asInputStream_skip_handlesZeroAndNegative() throws java.io.IOException {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.buffer().put(new byte[50]);
        buffer.buffer().flip();  // Make data readable from position 0

        try (java.io.InputStream is = buffer.asInputStream()) {
            assertEquals(0, is.skip(0));
            assertEquals(0, is.skip(-5));
            // Position should be unchanged
            assertEquals(0, is.read());
        }

        buffer.release();
    }

    @Test
    void asInputStream_skip_limitsToRemaining() throws java.io.IOException {
        PooledBuffer buffer = pool.acquire(1024);
        buffer.buffer().put(new byte[20]);
        buffer.buffer().flip();  // Make data readable from position 0

        try (java.io.InputStream is = buffer.asInputStream()) {
            // Try to skip more than available
            long skipped = is.skip(100);
            assertEquals(20, skipped);

            // Stream should be at EOF
            assertEquals(-1, is.read());
        }

        buffer.release();
    }
}
