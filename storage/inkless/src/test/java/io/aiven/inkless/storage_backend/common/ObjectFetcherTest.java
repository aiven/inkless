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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.stream.Stream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.RecordingReadChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static byte[] content(final int size) {
        final byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) i;
        }
        return content;
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
        final RecordingReadChannel channel = new RecordingReadChannel(content, 8 * 1024);

        final ByteBuffer buffer = FETCHER.readToByteBuffer(channel);

        assertThat(buffer.array()).isEqualTo(content);
        assertThat(channel.destinations).hasSize(readCase.expectedScratchBuffers);
        assertThat(channel.destinations).allMatch(bufferSeen -> bufferSeen.capacity() == ONE_MIB);
    }

    @Test
    void continuesAfterZeroByteReadThatIsNotEof() throws IOException {
        final byte[] content = content(4);
        final RecordingReadChannel channel = new RecordingReadChannel(content, content.length);
        channel.zeroReadsRemaining = 3;

        final ByteBuffer buffer = FETCHER.readToByteBuffer(channel);

        assertThat(buffer.array()).isEqualTo(content);
        assertThat(channel.readCalls).isEqualTo(5);
        assertThat(channel.destinations).hasSize(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("reads")
    void sizedChannelReadsIntoOneExactAllocation(final ReadCase readCase) throws IOException {
        final byte[] content = content(readCase.contentSize);
        final RecordingReadChannel channel = new RecordingReadChannel(content, 8 * 1024);

        final ByteBuffer buffer = FETCHER.readToByteBuffer(
            SizedReadableByteChannel.of(channel, content.length));

        assertThat(buffer.array()).isEqualTo(content);
        // Destination plus the 1-byte over-delivery probe.
        if (content.length == 0) {
            assertThat(channel.destinations)
                .extracting(ByteBuffer::capacity)
                .containsExactly(1);
        } else {
            assertThat(channel.destinations)
                .extracting(ByteBuffer::capacity)
                .containsExactlyInAnyOrder(content.length, 1);
        }
    }

    @Test
    void sizedChannelContinuesAfterZeroByteReadThatIsNotEof() throws IOException {
        final byte[] content = content(4);
        final RecordingReadChannel channel = new RecordingReadChannel(content, content.length);
        channel.zeroReadsRemaining = 3;

        final ByteBuffer buffer = FETCHER.readToByteBuffer(
            SizedReadableByteChannel.of(channel, content.length));

        assertThat(buffer.array()).isEqualTo(content);
    }

    @Test
    void failsWhenSizedChannelUnderDelivers() {
        try (final RecordingReadChannel channel = new RecordingReadChannel(content(10), 10)) {
            assertThatThrownBy(() -> FETCHER.readToByteBuffer(SizedReadableByteChannel.of(channel, 20)))
                .isInstanceOf(IOException.class)
                .hasMessage("Channel delivered 10 of 20 bytes");
        }
    }

    @Test
    void failsWhenSizedChannelOverDelivers() {
        try (final RecordingReadChannel channel = new RecordingReadChannel(content(20), 20)) {
            assertThatThrownBy(() -> FETCHER.readToByteBuffer(SizedReadableByteChannel.of(channel, 10)))
                .isInstanceOf(IOException.class)
                .hasMessage("Channel delivered more than 10 bytes");
        }
    }

    @Test
    void rejectsNegativeContentLength() {
        try (final RecordingReadChannel channel = new RecordingReadChannel(content(1), 1)) {
            assertThatThrownBy(() -> SizedReadableByteChannel.of(channel, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("contentLength cannot be negative: -1");
        }
    }

    @Test
    void closedChannelsRejectReads() throws IOException {
        final SizedReadableByteChannel overBuffer = SizedReadableByteChannel.of(ByteBuffer.wrap(content(3)));
        final SizedReadableByteChannel overDelegate =
            SizedReadableByteChannel.of(new RecordingReadChannel(content(3), 3), 3);

        for (final SizedReadableByteChannel channel : List.of(overBuffer, overDelegate)) {
            channel.close();
            assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(1)))
                .as("read after close on %s", channel)
                .isInstanceOf(ClosedChannelException.class);
        }
    }

    @Test
    void ofDelegatesIsOpenAndClose() throws IOException {
        final RecordingReadChannel delegate = new RecordingReadChannel(content(3), 3);
        final SizedReadableByteChannel channel = SizedReadableByteChannel.of(delegate, 3);

        assertThat(channel.isOpen()).isTrue();
        channel.close();
        assertThat(delegate.isOpen()).isFalse();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void ofRejectsNullDelegate() {
        assertThatThrownBy(() -> SizedReadableByteChannel.of(null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("delegate cannot be null");
    }

}
