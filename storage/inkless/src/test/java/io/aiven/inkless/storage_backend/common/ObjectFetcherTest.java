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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Duration;
import java.util.Random;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectFetcherTest {

    // Minimal ObjectFetcher implementation for testing default methods
    private static final ObjectFetcher FETCHER = new ObjectFetcher() {
        @Override
        public ReadableByteChannel fetch(ObjectKey key, ByteRange range) {
            throw new UnsupportedOperationException("Not used in these tests");
        }

        @Override
        public void close() {
            // No-op
        }
    };

    @Test
    void readToByteBuffer_smallData_withoutPool() throws IOException {
        byte[] data = "Hello, World!".getBytes();
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        ByteBuffer result = FETCHER.readToByteBuffer(channel);

        assertThat(result.remaining()).isEqualTo(data.length);
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        assertThat(resultBytes).isEqualTo(data);
    }

    @Test
    void readToByteBuffer_smallData_withPool() throws IOException {
        byte[] data = "Hello, World!".getBytes();
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        try (BufferPool pool = new ElasticBufferPool(2)) {
            ByteBuffer result = FETCHER.readToByteBuffer(channel, pool);

            assertThat(result.remaining()).isEqualTo(data.length);
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            assertThat(resultBytes).isEqualTo(data);
        }
    }

    @Test
    void readToByteBuffer_largeData_withoutPool() throws IOException {
        // 2.5 MB of data to span multiple 1MB buffers
        byte[] data = new byte[2 * 1024 * 1024 + 512 * 1024];
        new Random(42).nextBytes(data);
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        ByteBuffer result = FETCHER.readToByteBuffer(channel);

        assertThat(result.remaining()).isEqualTo(data.length);
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        assertThat(resultBytes).isEqualTo(data);
    }

    @Test
    void readToByteBuffer_largeData_withPool() throws IOException {
        // 2.5 MB of data to span multiple 1MB buffers
        byte[] data = new byte[2 * 1024 * 1024 + 512 * 1024];
        new Random(42).nextBytes(data);
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        try (BufferPool pool = new ElasticBufferPool(2)) {
            ByteBuffer result = FETCHER.readToByteBuffer(channel, pool);

            assertThat(result.remaining()).isEqualTo(data.length);
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            assertThat(resultBytes).isEqualTo(data);

            // Verify pool buffers were released (active count should be 0)
            assertThat(pool.activeBufferCount()).isZero();
        }
    }

    @Test
    void readToByteBuffer_emptyData() throws IOException {
        byte[] data = new byte[0];
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        ByteBuffer result = FETCHER.readToByteBuffer(channel);

        assertThat(result.remaining()).isZero();
    }

    @Test
    void readToByteBuffer_emptyData_withPool() throws IOException {
        byte[] data = new byte[0];
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        try (BufferPool pool = new ElasticBufferPool(2)) {
            ByteBuffer result = FETCHER.readToByteBuffer(channel, pool);

            assertThat(result.remaining()).isZero();
            assertThat(pool.activeBufferCount()).isZero();
        }
    }

    @Test
    void readToByteBuffer_exactlyOneMB() throws IOException {
        // Exactly 1MB - boundary case for single buffer
        byte[] data = new byte[ObjectFetcher.READ_BUFFER_1MiB];
        new Random(42).nextBytes(data);
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        try (BufferPool pool = new ElasticBufferPool(2)) {
            ByteBuffer result = FETCHER.readToByteBuffer(channel, pool);

            assertThat(result.remaining()).isEqualTo(data.length);
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            assertThat(resultBytes).isEqualTo(data);
            assertThat(pool.activeBufferCount()).isZero();
        }
    }

    @Test
    void readToByteBuffer_resultHasBackingArray() throws IOException {
        byte[] data = "test data".getBytes();
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        try (BufferPool pool = new ElasticBufferPool(2)) {
            ByteBuffer result = FETCHER.readToByteBuffer(channel, pool);

            // Result should be heap buffer with accessible array
            assertThat(result.hasArray()).isTrue();
            assertThat(result.array()).isNotNull();
        }
    }

    @Test
    void readToByteBuffer_poolExhaustion_fallsBackToFreshAllocation() throws IOException {
        // 3MB of data needs 3 buffers, but pool only has 1
        byte[] data = new byte[3 * 1024 * 1024];
        new Random(42).nextBytes(data);
        ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(data));

        try (BufferPool pool = new ElasticBufferPool(1)) {
            // Should still work by falling back to fresh allocation
            ByteBuffer result = FETCHER.readToByteBuffer(channel, pool);

            assertThat(result.remaining()).isEqualTo(data.length);
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            assertThat(resultBytes).isEqualTo(data);

            // Heap fallback should have been recorded
            assertThat(pool.heapFallbackCount()).isGreaterThan(0);
        }
    }
}
