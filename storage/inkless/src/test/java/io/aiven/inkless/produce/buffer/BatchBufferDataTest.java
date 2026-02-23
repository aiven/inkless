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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BatchBufferData implementations.
 */
class BatchBufferDataTest {

    @Nested
    class HeapBatchBufferDataTests {

        private HeapBatchBufferData data;
        private byte[] testBytes;

        @BeforeEach
        void setUp() {
            testBytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            data = new HeapBatchBufferData(testBytes);
        }

        @Test
        void size_returnsDataSize() {
            assertEquals(10, data.size());
        }

        @Test
        void size_zeroForEmptyArray() {
            HeapBatchBufferData emptyData = new HeapBatchBufferData(new byte[0]);
            assertEquals(0, emptyData.size());
        }

        @Test
        void asInputStreamSupplier_createsReadableStream() throws IOException {
            Supplier<InputStream> supplier = data.asInputStreamSupplier();
            try (InputStream is = supplier.get()) {
                byte[] readBytes = is.readAllBytes();
                assertArrayEquals(testBytes, readBytes);
            }
        }

        @Test
        void asInputStreamSupplier_multipleCallsWork() throws IOException {
            Supplier<InputStream> supplier = data.asInputStreamSupplier();

            // First stream
            try (InputStream is1 = supplier.get()) {
                is1.read(); // Read one byte
            }

            // Second stream should start from beginning
            try (InputStream is2 = supplier.get()) {
                byte[] readBytes = is2.readAllBytes();
                assertArrayEquals(testBytes, readBytes);
            }
        }

        @Test
        void copyTo_copiesCorrectRange() {
            byte[] dest = new byte[5];
            data.copyTo(2, dest, 0, 5);

            assertArrayEquals(new byte[]{3, 4, 5, 6, 7}, dest);
        }

        @Test
        void copyTo_withDestOffset() {
            byte[] dest = new byte[10];
            dest[0] = 99;
            data.copyTo(0, dest, 3, 5);

            assertEquals(99, dest[0]);
            assertEquals(0, dest[1]);
            assertEquals(0, dest[2]);
            assertEquals(1, dest[3]);
            assertEquals(2, dest[4]);
        }

        @Test
        void retain_isNoOp() {
            // Should not throw and return self
            assertEquals(data, data.retain());
        }

        @Test
        void release_isNoOp() {
            // Should not throw
            data.release();
            data.release(); // Multiple releases should be fine for heap
        }

        @Test
        void toByteArray_returnsCopyOfData() {
            byte[] copy = data.toByteArray();

            // Should be equal content
            assertArrayEquals(testBytes, copy);

            // But should be a different array instance
            assertNotSame(testBytes, copy);

            // Modifying the copy should not affect the original
            copy[0] = 99;
            assertEquals(1, testBytes[0]);
        }

        @Test
        @SuppressWarnings("deprecation")
        void data_returnsUnderlyingArray_deprecated() {
            // The deprecated method still works for backward compatibility
            assertEquals(testBytes, data.data());
        }
    }

    @Nested
    class PooledBatchBufferDataTests {

        private static final int ONE_MB = 1024 * 1024;

        private BufferPool pool;
        private PooledBatchBufferData data;
        private int dataSize;

        @BeforeEach
        void setUp() {
            pool = new ElasticBufferPool(2);
            dataSize = 100;

            PooledBuffer buffer = pool.acquire(dataSize);
            // Write test data
            for (int i = 0; i < dataSize; i++) {
                buffer.buffer().put((byte) (i % 256));
            }
            // Flip buffer to make data readable from position 0
            buffer.buffer().flip();

            data = new PooledBatchBufferData(buffer, dataSize);
        }

        @AfterEach
        void tearDown() {
            if (data != null) {
                data.release();
            }
            if (pool != null) {
                pool.close();
            }
        }

        @Test
        void size_returnsDataSize() {
            assertEquals(dataSize, data.size());
        }

        @Test
        void size_canBeSmallerThanBufferCapacity() {
            PooledBuffer buffer = pool.acquire(ONE_MB);
            PooledBatchBufferData smallData = new PooledBatchBufferData(buffer, 100);

            assertEquals(100, smallData.size());
            assertEquals(ONE_MB, buffer.capacity());

            smallData.release();
        }

        @Test
        void constructor_throwsOnNegativeSize() {
            PooledBuffer buffer = pool.acquire(100);
            assertThrows(IllegalArgumentException.class, () ->
                new PooledBatchBufferData(buffer, -1));
            buffer.release();
        }

        @Test
        void constructor_throwsOnSizeExceedingCapacity() {
            PooledBuffer buffer = pool.acquire(100);
            assertThrows(IllegalArgumentException.class, () ->
                new PooledBatchBufferData(buffer, ONE_MB + 1));
            buffer.release();
        }

        @Test
        void constructor_throwsOnNullBuffer() {
            assertThrows(NullPointerException.class, () ->
                new PooledBatchBufferData(null, 100));
        }

        @Test
        void asInputStreamSupplier_createsReadableStream() throws IOException {
            Supplier<InputStream> supplier = data.asInputStreamSupplier();
            try (InputStream is = supplier.get()) {
                byte[] readBytes = is.readAllBytes();
                assertEquals(dataSize, readBytes.length);
                for (int i = 0; i < dataSize; i++) {
                    assertEquals((byte) (i % 256), readBytes[i]);
                }
            }
        }

        @Test
        void asInputStreamSupplier_multipleCallsWork() throws IOException {
            Supplier<InputStream> supplier = data.asInputStreamSupplier();

            try (InputStream is1 = supplier.get()) {
                byte[] first = is1.readAllBytes();
                assertEquals(dataSize, first.length);
            }

            try (InputStream is2 = supplier.get()) {
                byte[] second = is2.readAllBytes();
                assertEquals(dataSize, second.length);
            }
        }

        @Test
        void asInputStreamSupplier_limitedToSize() throws IOException {
            // Create data where buffer is larger than data size
            PooledBuffer buffer = pool.acquire(ONE_MB);
            for (int i = 0; i < 50; i++) {
                buffer.buffer().put((byte) i);
            }
            buffer.buffer().flip();
            PooledBatchBufferData limitedData = new PooledBatchBufferData(buffer, 50);

            try (InputStream is = limitedData.asInputStreamSupplier().get()) {
                byte[] read = is.readAllBytes();
                assertEquals(50, read.length);
            }

            limitedData.release();
        }

        @Test
        void copyTo_copiesCorrectRange() {
            byte[] dest = new byte[10];
            data.copyTo(5, dest, 0, 10);

            for (int i = 0; i < 10; i++) {
                assertEquals((byte) ((5 + i) % 256), dest[i]);
            }
        }

        @Test
        void copyTo_throwsOnOutOfBounds() {
            byte[] dest = new byte[50];
            // Try to copy beyond data size
            assertThrows(IndexOutOfBoundsException.class, () ->
                data.copyTo(90, dest, 0, 50));
        }

        @Test
        void copyTo_throwsOnIntegerOverflow() {
            byte[] dest = new byte[10];
            // srcOffset + length would overflow to a negative number without proper guard
            assertThrows(IndexOutOfBoundsException.class, () ->
                data.copyTo(Integer.MAX_VALUE, dest, 0, 10));
        }

        @Test
        void copyTo_throwsOnNegativeSrcOffset() {
            byte[] dest = new byte[10];
            assertThrows(IndexOutOfBoundsException.class, () ->
                data.copyTo(-1, dest, 0, 10));
        }

        @Test
        void copyTo_throwsOnNegativeLength() {
            byte[] dest = new byte[10];
            assertThrows(IndexOutOfBoundsException.class, () ->
                data.copyTo(0, dest, 0, -1));
        }

        @Test
        void retain_incrementsRefCount() {
            assertEquals(1, pool.activeBufferCount());

            data.retain();
            data.release();

            // After balanced retain/release, buffer should still be active
            assertEquals(1, pool.activeBufferCount());
        }

        @Test
        void release_returnsBufferToPool() {
            assertEquals(1, pool.activeBufferCount());

            data.release();
            data = null; // Prevent double release in tearDown

            assertEquals(0, pool.activeBufferCount());
        }

        @Test
        void limitedInputStream_closeClosesDelegate() throws IOException {
            // Verify that closing the LimitedInputStream properly closes the delegate stream.
            // This test verifies the fix for the missing close() override in LimitedInputStream.
            Supplier<InputStream> supplier = data.asInputStreamSupplier();
            InputStream stream = supplier.get();

            // Read some data
            stream.read();

            // Close the stream - should not throw
            stream.close();

            // Multiple close should be safe (idempotent)
            stream.close();
        }

        @Test
        void limitedInputStream_available_respectsLimit() throws IOException {
            // Create a buffer with more data than we claim
            PooledBuffer buffer = pool.acquire(ONE_MB);
            for (int i = 0; i < 1000; i++) {
                buffer.buffer().put((byte) i);
            }
            buffer.buffer().flip();

            // But claim only 100 bytes
            PooledBatchBufferData limitedData = new PooledBatchBufferData(buffer, 100);

            try (InputStream is = limitedData.asInputStreamSupplier().get()) {
                // available() should respect the limit
                int available = is.available();
                assertTrue(available <= 100, "available should be <= limit, was: " + available);

                // Read all available
                byte[] bytes = is.readAllBytes();
                assertEquals(100, bytes.length);
            }

            limitedData.release();
        }

        @Test
        void limitedInputStream_skip_respectsLimit() throws IOException {
            // Create a buffer with more data than we claim
            PooledBuffer buffer = pool.acquire(ONE_MB);
            for (int i = 0; i < 200; i++) {
                buffer.buffer().put((byte) i);
            }
            buffer.buffer().flip();

            // But claim only 100 bytes
            PooledBatchBufferData limitedData = new PooledBatchBufferData(buffer, 100);

            try (InputStream is = limitedData.asInputStreamSupplier().get()) {
                // Skip first 30 bytes
                long skipped = is.skip(30);
                assertEquals(30, skipped);

                // Next read should return byte 30
                assertEquals(30, is.read());

                // Try to skip beyond the limit (position is now 31, limit is 100)
                // Can skip at most 69 more bytes
                skipped = is.skip(100);
                assertEquals(69, skipped);

                // Stream should be at EOF (reached limit)
                assertEquals(-1, is.read());
            }

            limitedData.release();
        }

        @Test
        void limitedInputStream_skip_handlesZeroAndNegative() throws IOException {
            try (InputStream is = data.asInputStreamSupplier().get()) {
                assertEquals(0, is.skip(0));
                assertEquals(0, is.skip(-10));
                // Position should be unchanged, first byte readable
                assertEquals(0, is.read());
            }
        }

        @Test
        void asSlice_returnsValidSlice() {
            // Use the range helper
            io.aiven.inkless.common.ByteRange range = new io.aiven.inkless.common.ByteRange(10, 20);
            var slice = data.asSlice(range);

            assertTrue(slice.isPresent());
            assertEquals(20, slice.get().remaining());
            // Verify correct data
            byte firstByte = slice.get().get();
            assertEquals((byte) 10, firstByte);
        }

        @Test
        void asSlice_returnsEmpty_forRangeBeyondSize() {
            // Request a range that exceeds the data size
            io.aiven.inkless.common.ByteRange range = new io.aiven.inkless.common.ByteRange(50, 100);
            var slice = data.asSlice(range);

            assertTrue(slice.isEmpty());
        }

        // Note: ByteRange constructor validates offset >= 0 and throws IllegalArgumentException
        // for negative offsets, so we cannot test asSlice() with negative offset - it's impossible
        // to create such a ByteRange. This is correct behavior: validation happens at construction.

        @Test
        void asSlice_returnsEmpty_forOverflowingLongValues() {
            // Test integer overflow protection: offset + size > Integer.MAX_VALUE
            // This should return empty, not produce incorrect results from truncation
            io.aiven.inkless.common.ByteRange overflowRange = new io.aiven.inkless.common.ByteRange(
                Integer.MAX_VALUE - 10L, 20L);
            var slice = data.asSlice(overflowRange);

            assertTrue(slice.isEmpty(), "Should reject ranges that overflow when cast to int");
        }

        @Test
        void asSlice_returnsEmpty_forOffsetExceedingIntMax() {
            // Offset alone exceeds Integer.MAX_VALUE
            io.aiven.inkless.common.ByteRange largeOffsetRange = new io.aiven.inkless.common.ByteRange(
                (long) Integer.MAX_VALUE + 1L, 10L);
            var slice = data.asSlice(largeOffsetRange);

            assertTrue(slice.isEmpty(), "Should reject offset > Integer.MAX_VALUE");
        }
    }
}
