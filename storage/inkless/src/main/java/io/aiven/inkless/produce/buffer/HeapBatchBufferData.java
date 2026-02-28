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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * BatchBufferData backed by a heap byte array.
 *
 * <p>Used when buffer pooling is disabled. Reference counting is a no-op.
 * {@link #asByteBuffer()} and {@link #asSlice} return empty since there's no
 * underlying ByteBuffer.
 *
 * @see PooledBatchBufferData
 */
public final class HeapBatchBufferData implements BatchBufferData {

    /**
     * An empty HeapBatchBufferData instance.
     */
    public static final HeapBatchBufferData EMPTY = new HeapBatchBufferData(new byte[0]);

    private final byte[] data;

    /**
     * Creates a HeapBatchBufferData wrapping the given byte array.
     *
     * <p>The array is not copied; the caller should not modify it after construction.
     *
     * @param data the byte array to wrap
     */
    public HeapBatchBufferData(byte[] data) {
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public Supplier<InputStream> asInputStreamSupplier() {
        return () -> new ByteArrayInputStream(data);
    }

    @Override
    public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
        System.arraycopy(data, srcOffset, dest, destOffset, length);
    }

    @Override
    public BatchBufferData retain() {
        // No-op for heap data - GC manages lifecycle
        return this;
    }

    @Override
    public void release() {
        // No-op for heap data - GC manages lifecycle
    }

    @Override
    public Optional<ByteBuffer> asByteBuffer() {
        // Return a read-only view to prevent modifications to the underlying array
        return Optional.of(ByteBuffer.wrap(data).asReadOnlyBuffer());
    }

    /**
     * Returns a copy of the underlying byte array.
     *
     * <p>This is the safe way to get a byte array that can be freely modified
     * without affecting other consumers of this data.
     *
     * @return a new byte array containing a copy of the data
     */
    public byte[] toByteArray() {
        return data.clone();
    }

    /**
     * Returns the underlying byte array without copying.
     *
     * <p>This is provided for backward compatibility and performance-critical code paths
     * where the overhead of copying is unacceptable and the caller guarantees immutability.
     *
     * <p><b>Warning:</b> The returned array is the actual backing array, not a copy.
     * Callers MUST NOT modify the returned array as this would corrupt data visible
     * to other consumers (e.g., S3 upload, cache storage). If modifications are needed,
     * use {@link #toByteArray()} instead.
     *
     * @return the underlying byte array (do not modify)
     * @deprecated Use {@link #toByteArray()} for safe access, or {@link #copyTo(int, byte[], int, int)}
     *             for partial reads. This method will be removed in a future release.
     */
    @Deprecated(forRemoval = true)
    public byte[] data() {
        return data;
    }
}
