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

import java.io.Closeable;

/**
 * A pool of reusable buffers. Manages buffers in size classes (1MB, 2MB, 4MB, etc.)
 * and allocates from the smallest class that fits the requested size.
 * Implementations must be thread-safe.
 *
 * @see ElasticBufferPool
 */
public interface BufferPool extends Closeable {

    /**
     * Acquires a buffer with at least the specified capacity.
     *
     * <p>The returned buffer may have capacity larger than requested due to size class
     * rounding. The buffer's position will be 0 and limit will be set to capacity.
     *
     * @param sizeBytes minimum required buffer capacity in bytes (must be positive)
     * @return a pooled buffer with at least the requested capacity
     * @throws IllegalArgumentException if sizeBytes is not positive or exceeds max size class
     * @throws IllegalStateException if the pool has been closed
     */
    PooledBuffer acquire(int sizeBytes);

    /**
     * Returns the number of buffers currently in use (not yet released).
     *
     * @return count of active buffers across all size classes
     */
    int activeBufferCount();

    /**
     * Returns the total memory allocated by this pool across all size classes.
     *
     * @return total pool size in bytes
     */
    long totalPoolSizeBytes();

    /**
     * Returns the cumulative count of heap fallback events since pool creation.
     *
     * <p>This is a monotonically increasing counter. A high value relative to
     * successful pool acquisitions indicates the pool is frequently exhausted.
     *
     * @return cumulative count of times callers fell back to heap allocation
     */
    default long heapFallbackCount() {
        return 0;
    }

    /**
     * Returns the cumulative total bytes requested in heap fallback events since pool creation.
     *
     * <p>This is a monotonically increasing counter tracking the sum of all requested
     * sizes that resulted in heap allocation instead of pool usage.
     *
     * @return cumulative total bytes that were allocated on heap instead of pool
     */
    default long heapFallbackBytes() {
        return 0;
    }

    /**
     * Closes the pool and releases all underlying buffer memory.
     *
     * <p>After calling close:
     * <ul>
     *   <li>All subsequent {@link #acquire} calls will throw {@link IllegalStateException}</li>
     *   <li>Any outstanding buffers become invalid</li>
     *   <li>Buffer memory is released</li>
     * </ul>
     *
     * <p>This method is idempotent - calling it multiple times has no additional effect.
     */
    @Override
    void close();
}
