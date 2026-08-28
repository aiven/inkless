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

public interface ObjectFetcher extends Closeable {

    /**
     * Use a large enough buffer when reading blob content to reduce the number of allocations.
     * Cloud storage blobs are expected to be multiple megabytes,
     * while channels may return much less data per read.
     */
    int READ_BUFFER_1MiB = 1024 * 1024;

    ReadableByteChannel fetch(ObjectKey key, ByteRange range) throws StorageBackendException, IOException;

    /**
     * Reads the channel into a single buffer.
     *
     * <p>Each scratch buffer is filled before another is allocated.
     * Otherwise, a channel that returns small amounts of data per read causes
     * one 1 MiB allocation per read,
     * which creates unnecessary garbage-collection and out-of-memory pressure.
     *
     * <p>A {@code 0}-byte read is not EOF.
     * The loop retries until {@code tempBuffer} is full or the channel returns {@code -1},
     * so the channel must eventually return data or EOF.
     */
    default ByteBuffer readToByteBuffer(final ReadableByteChannel readableByteChannel) throws IOException {
        final ByteBuffer byteBuffer;
        final List<ByteBuffer> buffers = new ArrayList<>(5);
        int readSize;
        int totalSize = 0;
        do {
            final ByteBuffer tempBuffer = ByteBuffer.allocate(READ_BUFFER_1MiB);
            do {
                readSize = readableByteChannel.read(tempBuffer);
            } while (readSize >= 0 && tempBuffer.hasRemaining());
            if (tempBuffer.position() > 0) {
                buffers.add(tempBuffer);
                tempBuffer.flip();
                totalSize += tempBuffer.remaining();
            }
        } while (readSize >= 0);
        byteBuffer = ByteBuffer.allocate(totalSize);
        buffers.forEach(byteBuffer::put);
        return byteBuffer.flip();
    }
}
