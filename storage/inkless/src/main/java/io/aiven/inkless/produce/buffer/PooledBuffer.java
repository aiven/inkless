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

/**
 * A pooled buffer with reference counting for safe multi-consumer access.
 *
 * <p>Initial reference count is 1. Call {@link #retain()} before passing to additional
 * consumers, and {@link #release()} when done. Buffer returns to pool when refCount reaches 0.
 */
public interface PooledBuffer {

    /**
     * Returns the underlying ByteBuffer for direct access.
     *
     * <p>Callers should not store references to this buffer beyond the lifetime
     * of the PooledBuffer.
     *
     * @return the underlying ByteBuffer
     * @throws IllegalStateException if the buffer has been fully released
     */
    ByteBuffer buffer();

    /**
     * Returns the total capacity of this buffer in bytes.
     *
     * @return buffer capacity
     */
    int capacity();

    /**
     * Increments the reference count.
     *
     * <p>Call this before passing the buffer to an additional consumer that will
     * independently call {@link #release()}.
     *
     * @return this buffer for method chaining
     * @throws IllegalStateException if the buffer has been fully released
     */
    PooledBuffer retain();

    /**
     * Decrements the reference count and returns the buffer to the pool if it reaches zero.
     *
     * <p>After calling release, the caller must not use this buffer anymore. If this
     * is the final release (refCount becomes 0), the buffer is returned to the pool
     * and may be reused by other threads.
     *
     * @throws IllegalStateException if called more times than retain() + 1 (double release)
     */
    void release();

    /**
     * Returns the current reference count.
     *
     * <p>This is primarily useful for debugging and testing.
     *
     * @return current reference count
     */
    int refCount();

    /**
     * Creates a new InputStream that reads from this buffer.
     *
     * <p>Each call returns an independent stream with its own read position, starting
     * from position 0 up to the buffer's limit. The stream does not affect the underlying
     * buffer's position or limit.
     *
     * <p>The returned stream does NOT automatically retain the buffer - callers must
     * ensure the buffer remains valid for the stream's lifetime by managing references
     * appropriately.
     *
     * @return a new InputStream reading from this buffer's content
     * @throws IllegalStateException if the buffer has been fully released
     */
    InputStream asInputStream();

    /**
     * Copies data from this buffer to a destination byte array.
     *
     * @param srcOffset offset in this buffer to start copying from
     * @param dest destination byte array
     * @param destOffset offset in destination array
     * @param length number of bytes to copy
     * @throws IndexOutOfBoundsException if the copy would exceed bounds
     * @throws IllegalStateException if the buffer has been fully released
     */
    void copyTo(int srcOffset, byte[] dest, int destOffset, int length);
}
