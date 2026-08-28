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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectFetcherTest {

    private static final int ONE_MIB = ObjectFetcher.READ_BUFFER_1MiB;

    private static final ObjectFetcher FETCHER = new ObjectFetcher() {
        @Override
        public ReadableByteChannel fetch(final ObjectKey key, final ByteRange range) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    };

    private record ReadCase(String name, int contentSize, int expectedScratchBuffers) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<ReadCase> reads() {
        return Stream.of(
            new ReadCase("empty", 0, 1),
            new ReadCase("1 byte", 1, 1),
            new ReadCase("just under 8 KiB", 8_191, 1),
            new ReadCase("8 KiB", 8_192, 1),
            new ReadCase("just over 8 KiB", 8_193, 1),
            new ReadCase("several 8 KiB reads", 40_000, 1),
            new ReadCase("just under 1 MiB", ONE_MIB - 1, 1),
            new ReadCase("1 MiB", ONE_MIB, 2),
            new ReadCase("just over 1 MiB", ONE_MIB + 12_345, 2),
            new ReadCase("just over 2 MiB", 2 * ONE_MIB + 1, 3)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("reads")
    void fillsEachScratchBufferBeforeAllocatingAnother(final ReadCase readCase) throws IOException {
        final byte[] content = content(readCase.contentSize);
        final CappedReadChannel channel = new CappedReadChannel(content, 8 * 1024);

        final ByteBuffer buffer = FETCHER.readToByteBuffer(channel);

        assertThat(buffer.array()).isEqualTo(content);
        assertThat(channel.scratchBuffers).hasSize(readCase.expectedScratchBuffers);
        assertThat(channel.scratchBuffers).allMatch(bufferSeen -> bufferSeen.capacity() == ONE_MIB);
    }

    @Test
    void continuesAfterZeroByteReadThatIsNotEof() throws IOException {
        final byte[] content = content(4);
        final CappedReadChannel channel = new CappedReadChannel(content, content.length);
        channel.zeroReadsRemaining = 3;

        final ByteBuffer buffer = FETCHER.readToByteBuffer(channel);

        assertThat(buffer.array()).isEqualTo(content);
        assertThat(channel.readCalls).isEqualTo(5);
        assertThat(channel.scratchBuffers).hasSize(1);
    }

    private static byte[] content(final int size) {
        final byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) i;
        }
        return content;
    }

    private static final class CappedReadChannel implements ReadableByteChannel {
        private final byte[] content;
        private final int maxBytesPerRead;
        private final Set<ByteBuffer> scratchBuffers = Collections.newSetFromMap(new IdentityHashMap<>());
        private int position;
        private int readCalls;
        private int zeroReadsRemaining;
        private boolean open = true;

        CappedReadChannel(final byte[] content, final int maxBytesPerRead) {
            this.content = content;
            this.maxBytesPerRead = maxBytesPerRead;
        }

        @Override
        public int read(final ByteBuffer dst) {
            readCalls++;
            scratchBuffers.add(dst);
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
}
