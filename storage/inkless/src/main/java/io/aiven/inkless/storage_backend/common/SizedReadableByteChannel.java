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
package io.aiven.inkless.storage_backend.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

/**
 * A channel that knows how many bytes it delivers before EOF.
 *
 * <p>Lets {@link ObjectFetcher#readToByteBuffer} allocate the destination once.
 * Implement this only when the length is exact: a mismatch fails the fetch.
 */
public interface SizedReadableByteChannel extends ReadableByteChannel {

    /**
     * Returns the exact number of bytes this channel delivers before EOF.
     *
     * <p>An {@code int} because the payload lands in a single {@code ByteBuffer} and a
     * {@code byte[]} on the extent, so a fetch cannot exceed {@link Integer#MAX_VALUE} bytes.
     * That is also why backends narrow their own {@code long} sizes to reach this.
     */
    int contentLength();

    /**
     * Returns a channel that reports a length of {@code 0} and reads EOF immediately.
     */
    static SizedReadableByteChannel empty() {
        return of(ByteBuffer.allocate(0));
    }

    /**
     * Returns a channel over the buffer's remaining bytes, reading from it without an intermediate copy.
     * The length is taken when this is called, so later changes to the buffer's position are not reflected.
     */
    static SizedReadableByteChannel of(final ByteBuffer source) {
        Objects.requireNonNull(source, "source cannot be null");
        final int contentLength = source.remaining();
        return new SizedReadableByteChannel() {
            private boolean open = true;

            @Override
            public int contentLength() {
                return contentLength;
            }

            @Override
            public int read(final ByteBuffer dst) throws IOException {
                if (!open) {
                    throw new ClosedChannelException();
                }
                if (!source.hasRemaining()) {
                    return -1;
                }
                final int readSize = Math.min(dst.remaining(), source.remaining());
                if (readSize == 0) {
                    return 0;
                }
                final int sourceLimit = source.limit();
                source.limit(source.position() + readSize);
                dst.put(source);
                source.limit(sourceLimit);
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
        };
    }

    /**
     * Returns a channel that reports {@code contentLength} and delegates everything else.
     */
    static SizedReadableByteChannel of(final ReadableByteChannel delegate, final int contentLength) {
        Objects.requireNonNull(delegate, "delegate cannot be null");
        if (contentLength < 0) {
            throw new IllegalArgumentException("contentLength cannot be negative: " + contentLength);
        }
        return new SizedReadableByteChannel() {
            private boolean open = true;

            @Override
            public int contentLength() {
                return contentLength;
            }

            @Override
            public int read(final ByteBuffer dst) throws IOException {
                if (!open) {
                    throw new ClosedChannelException();
                }
                return delegate.read(dst);
            }

            @Override
            public boolean isOpen() {
                return open && delegate.isOpen();
            }

            @Override
            public void close() throws IOException {
                open = false;
                delegate.close();
            }
        };
    }
}
