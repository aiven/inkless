/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.storage_backend.common.fixtures;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * A channel over a byte array that records what it was asked to fill, so a test can assert on the
 * buffers a reader allocated rather than only on the bytes it returned.
 *
 * <p>Caps each read at {@code maxBytesPerRead} to reproduce a short-reading backend, and can be told
 * to return a run of {@code 0}-byte reads that are not EOF.
 */
public final class RecordingReadChannel implements ReadableByteChannel {
    private final byte[] content;
    private final int maxBytesPerRead;

    /** Identity set, so a reader that reuses one buffer is distinguishable from one that allocates. */
    public final Set<ByteBuffer> destinations = Collections.newSetFromMap(new IdentityHashMap<>());

    public int readCalls;
    public int zeroReadsRemaining;

    private int position;
    private boolean open = true;

    public RecordingReadChannel(final byte[] content, final int maxBytesPerRead) {
        this.content = content;
        this.maxBytesPerRead = maxBytesPerRead;
    }

    public RecordingReadChannel(final byte[] content) {
        this(content, Integer.MAX_VALUE);
    }

    @Override
    public int read(final ByteBuffer dst) {
        readCalls++;
        destinations.add(dst);
        if (zeroReadsRemaining > 0) {
            zeroReadsRemaining--;
            return 0;
        }
        if (position == content.length) {
            return -1;
        }
        final int readSize = Math.min(Math.min(maxBytesPerRead, dst.remaining()), content.length - position);
        dst.put(content, position, readSize);
        position += readSize;
        return readSize;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
