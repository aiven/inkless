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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Supplier;

import io.aiven.inkless.common.ByteRange;

/**
 * Abstraction over the byte data produced by BatchBuffer.close().
 *
 * <p>This interface allows the write path to work with either:
 * <ul>
 *   <li>{@link HeapBatchBufferData}: Fresh byte array allocation</li>
 *   <li>{@link PooledBatchBufferData}: Pooled buffer from {@link ElasticBufferPool}</li>
 * </ul>
 *
 * <p>The abstraction supports reference counting to allow safe sharing between
 * multiple consumers (S3 upload + cache store) without premature deallocation.
 *
 * <p>Usage pattern:
 * <pre>{@code
 * BatchBufferData data = batchBuffer.close();
 * data.retain(); // For second consumer
 *
 * // Consumer 1: S3 upload
 * executor.submit(() -> {
 *     try (InputStream is = data.asInputStreamSupplier().get()) {
 *         s3Client.upload(is, data.size());
 *     } finally {
 *         data.release();
 *     }
 * });
 *
 * // Consumer 2: Cache store
 * executor.submit(() -> {
 *     try {
 *         byte[] cacheData = new byte[data.size()];
 *         data.copyTo(0, cacheData, 0, data.size());
 *         cache.put(key, cacheData);
 *     } finally {
 *         data.release();
 *     }
 * });
 * }</pre>
 */
public interface BatchBufferData {

    /**
     * Returns the size of the data in bytes.
     *
     * @return size in bytes
     */
    int size();

    /**
     * Returns a supplier that creates new InputStreams for reading this data.
     *
     * <p>Each call to the supplier returns a fresh InputStream starting from position 0.
     * This is useful for retry scenarios where the stream needs to be re-read.
     *
     * @return supplier of InputStreams
     */
    Supplier<InputStream> asInputStreamSupplier();

    /**
     * Copies data from this buffer to a destination byte array.
     *
     * @param srcOffset offset in this buffer to start copying from
     * @param dest destination byte array
     * @param destOffset offset in destination array
     * @param length number of bytes to copy
     * @throws IndexOutOfBoundsException if the copy would exceed bounds
     */
    void copyTo(int srcOffset, byte[] dest, int destOffset, int length);

    /**
     * Increments the reference count.
     *
     * <p>Call this before passing the data to an additional consumer that will
     * independently call {@link #release()}.
     *
     * @return this for method chaining
     */
    BatchBufferData retain();

    /**
     * Decrements the reference count and releases resources if it reaches zero.
     *
     * <p>For simple byte array data, this is a no-op. For pooled data, this returns
     * the buffer to the pool when the reference count reaches zero.
     */
    void release();

    /**
     * Returns a read-only view of the underlying ByteBuffer if available.
     *
     * <p>For pooled buffers, returns a read-only view limited to the actual data size.
     * For heap byte array data, returns empty.
     *
     * @return Optional containing a read-only ByteBuffer view, or empty if not available
     */
    default Optional<ByteBuffer> asByteBuffer() {
        return Optional.empty();
    }

    /**
     * Returns a read-only ByteBuffer slice for the specified range if available.
     *
     * <p>For pooled buffers, this returns a read-only view of the specified
     * range, enabling efficient bulk reads. For simple byte array data, this
     * returns empty and callers should fall back to {@link #copyTo(int, byte[], int, int)}.
     *
     * @param range the byte range to slice (relative to the start of data)
     * @return Optional containing a read-only ByteBuffer slice, or empty if not available
     *         or if the range exceeds the data bounds
     */
    default Optional<ByteBuffer> asSlice(ByteRange range) {
        return Optional.empty();
    }
}
