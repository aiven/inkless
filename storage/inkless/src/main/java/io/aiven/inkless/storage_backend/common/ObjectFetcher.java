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
     * Use large enough buffer for reading the blob content to byte buffers to reduce required number
     * of allocations. Usually default implementations in input streams use 16 KiB buffers. The expectation
     * of blob sizes from cloud storages are multi-megabyte.
     */
    int READ_BUFFER_1MiB = 1024 * 1024;

    ReadableByteChannel fetch(ObjectKey key, ByteRange range) throws StorageBackendException, IOException;

    default ByteBuffer readToByteBuffer(final ReadableByteChannel readableByteChannel) throws IOException {
        final ByteBuffer byteBuffer;
        final List<ByteBuffer> buffers = new ArrayList<>(5);
        int readSize;
        int totalSize = 0;
        do {
            final ByteBuffer tempBuffer = ByteBuffer.allocate(READ_BUFFER_1MiB);
            readSize = readableByteChannel.read(tempBuffer);
            if (readSize > 0) {
                buffers.add(tempBuffer);
                tempBuffer.flip();
                totalSize += readSize;
            }
        } while (readSize >= 0);
        byteBuffer = ByteBuffer.allocate(totalSize);
        buffers.forEach(byteBuffer::put);
        return byteBuffer.flip();
    }
}
