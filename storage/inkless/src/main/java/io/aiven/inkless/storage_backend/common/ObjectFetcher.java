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
     * <p>If the channel is a {@link SizedReadableByteChannel}, the destination is that length.
     * A mismatch fails the fetch. Otherwise the channel is drained into 1 MiB scratch buffers
     * that are filled before another is allocated, then copied.
     *
     * <p>A {@code 0}-byte read is not EOF.
     * Both paths retry until the destination is full or the channel returns {@code -1},
     * so a channel must eventually return data or EOF.
     * A channel that returns {@code 0} indefinitely never completes the read.
     */
    default ByteBuffer readToByteBuffer(final ReadableByteChannel readableByteChannel) throws IOException {
        if (readableByteChannel instanceof SizedReadableByteChannel sized) {
            return readExactly(sized, sized.contentLength());
        }
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
        final ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
        buffers.forEach(byteBuffer::put);
        return byteBuffer.flip();
    }

    private static ByteBuffer readExactly(final ReadableByteChannel channel, final int contentLength)
        throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(contentLength);
        int readSize = 0;
        while (buffer.hasRemaining() && readSize >= 0) {
            readSize = channel.read(buffer);
        }
        if (buffer.hasRemaining()) {
            throw new IOException(
                "Channel delivered " + buffer.position() + " of " + contentLength + " bytes");
        }
        // Only a strictly positive read proves over-delivery.
        // A 0 means no data is available yet, which is not evidence either way.
        if (channel.read(ByteBuffer.allocate(1)) > 0) {
            throw new IOException("Channel delivered more than " + contentLength + " bytes");
        }
        return buffer.flip();
    }
}
