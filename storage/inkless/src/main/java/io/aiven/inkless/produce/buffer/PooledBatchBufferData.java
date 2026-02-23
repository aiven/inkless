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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.aiven.inkless.common.ByteRange;

/**
 * BatchBufferData implementation backed by a pooled buffer from {@link ElasticBufferPool}.
 *
 * <p>This implementation delegates to {@link PooledBuffer} for reference counting
 * and buffer management. When the reference count reaches zero, the buffer is
 * returned to the pool for reuse.
 *
 * <p>The actual data size may be less than the buffer capacity since pooled
 * buffers come from fixed size classes.
 */
public final class PooledBatchBufferData implements BatchBufferData {

    private final PooledBuffer buffer;
    private final int size;

    /**
     * Creates a PooledBatchBufferData wrapping the given pooled buffer.
     *
     * @param buffer the pooled buffer containing the data (must not be null)
     * @param size the actual size of the data (may be less than buffer capacity)
     * @throws NullPointerException if buffer is null
     * @throws IllegalArgumentException if size is negative or exceeds buffer capacity
     */
    public PooledBatchBufferData(PooledBuffer buffer, int size) {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        if (size < 0 || size > buffer.capacity()) {
            throw new IllegalArgumentException(
                "Size " + size + " must be between 0 and buffer capacity " + buffer.capacity());
        }
        this.buffer = buffer;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Supplier<InputStream> asInputStreamSupplier() {
        return () -> {
            // Create a limited view of the buffer up to 'size' bytes
            return new LimitedInputStream(buffer.asInputStream(), size);
        };
    }

    @Override
    public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
        // Guard against integer overflow: use long arithmetic for bounds check.
        // srcOffset + length could overflow if both are large positive ints.
        long endOffset = (long) srcOffset + length;
        if (srcOffset < 0 || length < 0 || endOffset > size) {
            throw new IndexOutOfBoundsException(
                "Copy would exceed data bounds: srcOffset=" + srcOffset + ", length=" + length + ", size=" + size);
        }
        buffer.copyTo(srcOffset, dest, destOffset, length);
    }

    @Override
    public BatchBufferData retain() {
        buffer.retain();
        return this;
    }

    @Override
    public void release() {
        buffer.release();
    }

    @Override
    public Optional<ByteBuffer> asByteBuffer() {
        // Get the underlying buffer and create a limited read-only view
        ByteBuffer bb = buffer.buffer();
        ByteBuffer slice = bb.duplicate();
        slice.position(0);
        slice.limit(size);
        return Optional.of(slice.asReadOnlyBuffer());
    }

    @Override
    public Optional<ByteBuffer> asSlice(ByteRange range) {
        // Validate range is within data bounds, guarding against long-to-int overflow.
        // ByteRange uses long for offset/size, but ByteBuffer uses int positions.
        // If values exceed Integer.MAX_VALUE, we must reject to avoid silent truncation.
        long rangeOffset = range.offset();
        long rangeSize = range.size();
        long rangeEnd = rangeOffset + rangeSize;

        if (rangeOffset < 0 || rangeOffset > Integer.MAX_VALUE
            || rangeEnd < 0 || rangeEnd > Integer.MAX_VALUE
            || rangeEnd > size) {
            return Optional.empty();
        }

        // Safe to cast after validation
        int intOffset = (int) rangeOffset;
        int intEnd = (int) rangeEnd;

        // Create a slice for the specified range
        ByteBuffer bb = buffer.buffer();
        ByteBuffer slice = bb.duplicate();
        slice.position(intOffset);
        slice.limit(intEnd);
        return Optional.of(slice.slice().asReadOnlyBuffer());
    }

    /**
     * InputStream that limits reads to a specified size.
     */
    private static class LimitedInputStream extends InputStream {
        private final InputStream delegate;
        private final int limit;
        private int position;

        LimitedInputStream(InputStream delegate, int limit) {
            this.delegate = delegate;
            this.limit = limit;
            this.position = 0;
        }

        @Override
        public int read() throws java.io.IOException {
            if (position >= limit) {
                return -1;
            }
            int result = delegate.read();
            if (result >= 0) {
                position++;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws java.io.IOException {
            if (position >= limit) {
                return -1;
            }
            int maxRead = Math.min(len, limit - position);
            int result = delegate.read(b, off, maxRead);
            if (result > 0) {
                position += result;
            }
            return result;
        }

        @Override
        public int available() throws java.io.IOException {
            return Math.min(delegate.available(), limit - position);
        }

        @Override
        public long skip(long n) throws java.io.IOException {
            if (n <= 0) {
                return 0;
            }
            long maxSkip = Math.min(n, limit - position);
            long skipped = delegate.skip(maxSkip);
            if (skipped > 0) {
                position += (int) skipped;
            }
            return skipped;
        }

        @Override
        public void close() throws java.io.IOException {
            delegate.close();
        }
    }
}
