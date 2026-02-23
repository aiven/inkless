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
package io.aiven.inkless.storage_backend.common;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.PooledBuffer;

public interface ObjectFetcher extends Closeable {

    /**
     * Use large enough buffer for reading the blob content to byte buffers to reduce required number
     * of allocations. Usually default implementations in input streams use 16 KiB buffers. The expectation
     * of blob sizes from cloud storages are multi-megabyte.
     */
    int READ_BUFFER_1MiB = 1024 * 1024;

    ReadableByteChannel fetch(ObjectKey key, ByteRange range) throws StorageBackendException, IOException;

    /**
     * Reads all data from the channel into a ByteBuffer using fresh allocations.
     *
     * @param readableByteChannel the channel to read from
     * @return ByteBuffer containing all read data
     * @throws IOException if an I/O error occurs
     */
    default ByteBuffer readToByteBuffer(final ReadableByteChannel readableByteChannel) throws IOException {
        return readToByteBuffer(readableByteChannel, null);
    }

    /**
     * Reads all data from the channel into a ByteBuffer.
     *
     * <p>When a buffer pool is provided, intermediate 1MB read buffers are acquired from
     * the pool and released after copying. The returned ByteBuffer is always a fresh
     * heap allocation since callers need array() access.
     *
     * @param readableByteChannel the channel to read from
     * @param bufferPool optional buffer pool for intermediate buffers (may be null)
     * @return ByteBuffer containing all read data
     * @throws IOException if an I/O error occurs
     */
    default ByteBuffer readToByteBuffer(final ReadableByteChannel readableByteChannel,
                                        final BufferPool bufferPool) throws IOException {
        final List<PooledBuffer> pooledBuffers = new ArrayList<>(5);
        final List<ByteBuffer> buffers = new ArrayList<>(5);
        try {
            int readSize;
            int totalSize = 0;
            do {
                final ByteBuffer tempBuffer = acquireReadBuffer(bufferPool, pooledBuffers);
                readSize = readableByteChannel.read(tempBuffer);
                if (readSize > 0) {
                    buffers.add(tempBuffer);
                    tempBuffer.flip();
                    totalSize += readSize;
                }
            } while (readSize >= 0);

            // Allocate final buffer (not pooled - callers need array() access)
            final ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
            buffers.forEach(byteBuffer::put);
            return byteBuffer.flip();
        } finally {
            // Release all pooled buffers
            pooledBuffers.forEach(PooledBuffer::release);
        }
    }

    /**
     * Acquires a 1MB read buffer from the pool if available, otherwise allocates a fresh buffer.
     *
     * <p>ElasticBufferPool.acquire() is non-blocking and returns a HeapPooledBuffer
     * on fallback, so we always get a valid buffer without exceptions.
     */
    private static ByteBuffer acquireReadBuffer(final BufferPool bufferPool,
                                                final List<PooledBuffer> pooledBuffers) {
        if (bufferPool != null) {
            final PooledBuffer pooled = bufferPool.acquire(READ_BUFFER_1MiB);
            pooledBuffers.add(pooled);
            final ByteBuffer buffer = pooled.buffer();
            buffer.clear();
            return buffer;
        }
        return ByteBuffer.allocate(READ_BUFFER_1MiB);
    }
}
