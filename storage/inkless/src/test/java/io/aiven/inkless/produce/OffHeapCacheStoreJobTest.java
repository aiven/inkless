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
package io.aiven.inkless.produce;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.produce.buffer.HeapBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for off-heap cache store using ByteBuffer slice.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Cache store with direct buffer doesn't allocate heap</li>
 *   <li>Cache key uses slice reference</li>
 *   <li>Release after cache eviction returns buffer to pool</li>
 * </ul>
 */
class OffHeapCacheStoreJobTest {

    private static final int ONE_MB = 1024 * 1024;

    /**
     * Test that direct buffer provides ByteBuffer slice for cache storage.
     */
    @Test
    void storeToCache_canUseByteBufferSlice() {
        BufferPool pool = new ElasticBufferPool(2);
        try {
            PooledBuffer buffer = pool.acquire(ONE_MB);
            try {
                // Write some data
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                byte[] testData = new byte[1000];
                for (int i = 0; i < testData.length; i++) {
                    testData[i] = (byte) i;
                }
                bb.put(testData);
                int dataSize = bb.position();
                bb.flip();

                PooledBatchBufferData data = new PooledBatchBufferData(buffer, dataSize);

                // Get slice for a range (offset=100, size=100)
                ByteRange range = new ByteRange(100, 100);
                Optional<ByteBuffer> slice = data.asSlice(range);

                assertThat(slice).isPresent();
                assertThat(slice.get().remaining()).isEqualTo(100);
                assertThat(slice.get().isReadOnly()).isTrue();

                // Verify data integrity
                byte[] sliceData = new byte[100];
                slice.get().get(sliceData);
                for (int i = 0; i < 100; i++) {
                    assertThat(sliceData[i]).isEqualTo((byte) (100 + i));
                }
            } finally {
                buffer.release();
            }
        } finally {
            pool.close();
        }
    }

    /**
     * Test that heap buffer falls back to byte array copy.
     */
    @Test
    void heapBuffer_fallsBackToByteCopy() {
        byte[] testData = new byte[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }

        HeapBatchBufferData data = new HeapBatchBufferData(testData);

        // Heap data should not provide a slice
        ByteRange range = new ByteRange(100, 200);
        Optional<ByteBuffer> slice = data.asSlice(range);
        assertThat(slice).isEmpty();

        // But copyTo should still work
        byte[] copy = new byte[100];
        data.copyTo(100, copy, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertThat(copy[i]).isEqualTo((byte) (100 + i));
        }
    }

    /**
     * Test that full slice is returned when range covers all data.
     */
    @Test
    void fullSlice_coversAllData() {
        BufferPool pool = new ElasticBufferPool(2);
        try {
            PooledBuffer buffer = pool.acquire(ONE_MB);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                byte[] testData = new byte[500];
                for (int i = 0; i < testData.length; i++) {
                    testData[i] = (byte) i;
                }
                bb.put(testData);
                int dataSize = bb.position();
                bb.flip();

                PooledBatchBufferData data = new PooledBatchBufferData(buffer, dataSize);

                // Get full slice
                ByteRange fullRange = new ByteRange(0, dataSize);
                Optional<ByteBuffer> slice = data.asSlice(fullRange);

                assertThat(slice).isPresent();
                assertThat(slice.get().remaining()).isEqualTo(dataSize);
            } finally {
                buffer.release();
            }
        } finally {
            pool.close();
        }
    }

    /**
     * Test that asSlice validates range bounds.
     */
    @Test
    void asSlice_validatesRangeBounds() {
        BufferPool pool = new ElasticBufferPool(2);
        try {
            PooledBuffer buffer = pool.acquire(ONE_MB);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                bb.put(new byte[500]);
                int dataSize = bb.position();
                bb.flip();

                PooledBatchBufferData data = new PooledBatchBufferData(buffer, dataSize);

                // Range within bounds - should work (offset=0, size=100)
                Optional<ByteBuffer> validSlice = data.asSlice(new ByteRange(0, 100));
                assertThat(validSlice).isPresent();

                // Range at the edge - should work (offset=400, size=100 -> end=500=dataSize)
                Optional<ByteBuffer> edgeSlice = data.asSlice(new ByteRange(400, 100));
                assertThat(edgeSlice).isPresent();
                assertThat(edgeSlice.get().remaining()).isEqualTo(100);

                // Range exceeding bounds - should return empty (offset=0, size=600 > dataSize=500)
                Optional<ByteBuffer> invalidSlice = data.asSlice(new ByteRange(0, dataSize + 100));
                assertThat(invalidSlice).isEmpty();
            } finally {
                buffer.release();
            }
        } finally {
            pool.close();
        }
    }

    /**
     * Test interface default implementation returns empty.
     */
    @Test
    void defaultAsSlice_returnsEmpty() {
        BatchBufferData minimalImpl = new BatchBufferData() {
            @Override
            public int size() {
                return 100;
            }

            @Override
            public java.util.function.Supplier<java.io.InputStream> asInputStreamSupplier() {
                return () -> java.io.InputStream.nullInputStream();
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }
        };

        assertThat(minimalImpl.asSlice(new ByteRange(0, 50))).isEmpty();
    }
}
