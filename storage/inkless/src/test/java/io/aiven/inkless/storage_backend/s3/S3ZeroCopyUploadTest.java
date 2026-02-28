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
package io.aiven.inkless.storage_backend.s3;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;

import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.produce.buffer.HeapBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for zero-copy S3 upload using ByteBuffer.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Pooled ByteBuffer can be used for upload without intermediate copy</li>
 *   <li>Simple byte array data falls back to stream approach</li>
 *   <li>Large buffers use multipart upload</li>
 * </ul>
 */
class S3ZeroCopyUploadTest {

    private static final int ONE_MB = 1024 * 1024;

    /**
     * Test that PooledBatchBufferData returns a ByteBuffer for zero-copy upload.
     */
    @Test
    void uploadFromByteBuffer_noIntermediateCopy() {
        BufferPool pool = new ElasticBufferPool(2);
        try {
            PooledBuffer buffer = pool.acquire(ONE_MB);
            try {
                // Write some data
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                for (int i = 0; i < 1000; i++) {
                    bb.putInt(i);
                }
                int dataSize = bb.position();
                bb.flip();

                // Create PooledBatchBufferData
                PooledBatchBufferData data = new PooledBatchBufferData(buffer, dataSize);

                // Verify asByteBuffer returns the pooled buffer
                Optional<ByteBuffer> byteBuffer = data.asByteBuffer();
                assertThat(byteBuffer).isPresent();
                assertThat(byteBuffer.get().remaining()).isEqualTo(dataSize);
                assertThat(byteBuffer.get().isReadOnly()).isTrue();

                // Verify data integrity
                ByteBuffer view = byteBuffer.get();
                for (int i = 0; i < 1000; i++) {
                    assertThat(view.getInt()).isEqualTo(i);
                }
            } finally {
                buffer.release();
            }
        } finally {
            pool.close();
        }
    }

    /**
     * Test that HeapBatchBufferData returns a read-only ByteBuffer for zero-copy uploads.
     */
    @Test
    void uploadFromHeapByteBuffer_providesZeroCopyBuffer() {
        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        HeapBatchBufferData heapData = new HeapBatchBufferData(data);

        // Heap data now provides a ByteBuffer for zero-copy uploads
        Optional<ByteBuffer> byteBuffer = heapData.asByteBuffer();
        assertThat(byteBuffer).isPresent();
        assertThat(byteBuffer.get().isReadOnly()).isTrue();
        assertThat(byteBuffer.get().remaining()).isEqualTo(1000);
    }

    /**
     * Test that asByteBuffer returns a read-only view.
     */
    @Test
    void byteBuffer_isReadOnly() {
        BufferPool pool = new ElasticBufferPool(2);
        try {
            PooledBuffer buffer = pool.acquire(ONE_MB);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                bb.put((byte) 42);
                int dataSize = bb.position();
                bb.flip();

                PooledBatchBufferData data = new PooledBatchBufferData(buffer, dataSize);
                Optional<ByteBuffer> byteBuffer = data.asByteBuffer();

                assertThat(byteBuffer).isPresent();
                assertThat(byteBuffer.get().isReadOnly()).isTrue();
            } finally {
                buffer.release();
            }
        } finally {
            pool.close();
        }
    }

    /**
     * Test interface conformance - asByteBuffer is optional.
     */
    @Test
    void batchBufferData_asByteBufferDefaultImplementation() {
        // Create a minimal implementation to test default method
        BatchBufferData minimalImpl = new BatchBufferData() {
            @Override
            public int size() {
                return 0;
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

        // Default implementation returns empty
        assertThat(minimalImpl.asByteBuffer()).isEmpty();
    }
}
