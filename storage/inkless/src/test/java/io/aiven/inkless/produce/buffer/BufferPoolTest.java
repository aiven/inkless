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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BufferPool interface contract.
 * These tests define the expected behavior that any BufferPool implementation must satisfy.
 */
class BufferPoolTest {

    private static final int ONE_MB = 1024 * 1024;
    private static final int FOUR_MB = 4 * ONE_MB;

    private BufferPool pool;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        // Create a pool with 2 buffers per size class for testing
        pool = new ElasticBufferPool(2);
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void acquire_returnsBufferOfRequestedSize() {
        PooledBuffer buffer = pool.acquire(ONE_MB);

        assertNotNull(buffer);
        assertTrue(buffer.capacity() >= ONE_MB,
            "Buffer capacity should be at least the requested size");
    }

    @Test
    void acquire_returnsBufferWithSufficientCapacityForLargerSizes() {
        PooledBuffer buffer = pool.acquire(FOUR_MB);

        assertNotNull(buffer);
        assertTrue(buffer.capacity() >= FOUR_MB,
            "Buffer capacity should be at least the requested size");
    }

    @Test
    void acquire_fallsBackToHeapWhenPoolExhausted() {
        // Acquire all buffers in the pool for 1MB size class
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        PooledBuffer buffer2 = pool.acquire(ONE_MB);

        // ElasticBufferPool should fall back to heap instead of blocking
        PooledBuffer buffer3 = pool.acquire(ONE_MB);
        assertNotNull(buffer3, "Should get a heap fallback buffer");

        // Cleanup
        buffer1.release();
        buffer2.release();
        buffer3.release();
    }

    @Test
    void acquire_throwsOnNegativeSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            pool.acquire(-1);
        });
    }

    @Test
    void acquire_throwsOnZeroSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            pool.acquire(0);
        });
    }

    @Test
    void release_returnsBufferToPool() {
        // Acquire all buffers
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        PooledBuffer buffer2 = pool.acquire(ONE_MB);

        // Release one
        buffer1.release();

        // Should be able to acquire again without blocking
        PooledBuffer buffer3 = pool.acquire(ONE_MB);
        assertNotNull(buffer3);

        // Cleanup
        buffer2.release();
        buffer3.release();
    }

    @Test
    void close_releasesAllBuffers() {
        // Acquire some buffers
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        PooledBuffer buffer2 = pool.acquire(FOUR_MB);

        // Close the pool
        pool.close();

        // Further operations should fail
        assertThrows(IllegalStateException.class, () -> {
            pool.acquire(ONE_MB);
        });

        // Buffers should be invalidated (implementation specific)
        // We just verify the pool is closed
        pool = null; // Prevent double-close in tearDown
    }

    @Test
    void metrics_tracksActiveBuffers() {
        assertEquals(0, pool.activeBufferCount());

        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        assertEquals(1, pool.activeBufferCount());

        PooledBuffer buffer2 = pool.acquire(ONE_MB);
        assertEquals(2, pool.activeBufferCount());

        buffer1.release();
        assertEquals(1, pool.activeBufferCount());

        buffer2.release();
        assertEquals(0, pool.activeBufferCount());
    }

    @Test
    void metrics_tracksTotalPoolSize() {
        // Pool should report total capacity
        long totalSize = pool.totalPoolSizeBytes();
        // ElasticBufferPool starts empty and grows on demand
        assertTrue(totalSize >= 0, "Pool size should be non-negative");
    }

    @Test
    void selectsSizeClass_roundsUpToNearest() {
        // Request 1.5MB, should get at least 2MB (next size class)
        int requestSize = ONE_MB + ONE_MB / 2;
        PooledBuffer buffer = pool.acquire(requestSize);

        assertTrue(buffer.capacity() >= requestSize,
            "Buffer should have capacity for requested size");

        buffer.release();
    }

    @Test
    void acquire_1MB_from1MBPool() {
        PooledBuffer buffer = pool.acquire(ONE_MB);

        // Should get exactly 1MB or the size class containing 1MB
        assertTrue(buffer.capacity() >= ONE_MB);
        assertTrue(buffer.capacity() <= 2 * ONE_MB,
            "Should use 1MB size class, not larger");

        buffer.release();
    }

    @Test
    void acquire_3MB_from4MBPool() {
        int threeMB = 3 * ONE_MB;
        PooledBuffer buffer = pool.acquire(threeMB);

        // Should get 4MB (next size class up from 3MB)
        assertTrue(buffer.capacity() >= threeMB);
        assertTrue(buffer.capacity() <= 4 * ONE_MB,
            "Should use 4MB size class");

        buffer.release();
    }

    // ==================== Extended Size Class Tests (16MB, 32MB, 64MB) ====================

    /**
     * Test acquiring a 16MB buffer returns a pooled buffer.
     */
    @Test
    void acquire16MB_returnsPooledBuffer() {
        int requestedSize = 16 * ONE_MB;
        PooledBuffer buffer = pool.acquire(requestedSize);

        try {
            assertThat(buffer).isNotNull();
            assertThat(buffer.capacity()).isGreaterThanOrEqualTo(requestedSize);
        } finally {
            buffer.release();
        }
    }

    /**
     * Test acquiring a 32MB buffer returns a pooled buffer.
     */
    @Test
    void acquire32MB_returnsPooledBuffer() {
        int requestedSize = 32 * ONE_MB;
        PooledBuffer buffer = pool.acquire(requestedSize);

        try {
            assertThat(buffer).isNotNull();
            assertThat(buffer.capacity()).isGreaterThanOrEqualTo(requestedSize);
        } finally {
            buffer.release();
        }
    }

    /**
     * Test acquiring a 64MB buffer returns a pooled buffer.
     */
    @Test
    void acquire64MB_returnsPooledBuffer() {
        int requestedSize = 64 * ONE_MB;
        PooledBuffer buffer = pool.acquire(requestedSize);

        try {
            assertThat(buffer).isNotNull();
            assertThat(buffer.capacity()).isGreaterThanOrEqualTo(requestedSize);
        } finally {
            buffer.release();
        }
    }

    /**
     * Test that requesting more than max size class (64MB) throws exception.
     */
    @Test
    void acquireAboveMax_throwsIllegalArgument() {
        int requestedSize = 65 * ONE_MB;

        assertThatThrownBy(() -> pool.acquire(requestedSize))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exceeds maximum size class");
    }

    /**
     * Test mixed size acquisition doesn't cause pool fragmentation.
     */
    @Test
    void mixedSizeAcquisition_noPoolFragmentation() {
        List<PooledBuffer> buffers = new ArrayList<>();

        try {
            // Acquire one buffer from each size class
            int[] sizes = {1 * ONE_MB, 4 * ONE_MB, 8 * ONE_MB, 16 * ONE_MB, 32 * ONE_MB, 64 * ONE_MB};
            for (int size : sizes) {
                PooledBuffer buffer = pool.acquire(size);
                assertThat(buffer.capacity()).isGreaterThanOrEqualTo(size);
                buffers.add(buffer);
            }

            // Release all
            for (PooledBuffer buffer : buffers) {
                buffer.release();
            }
            buffers.clear();

            // Should be able to re-acquire the same sizes
            for (int size : sizes) {
                PooledBuffer buffer = pool.acquire(size);
                assertThat(buffer.capacity()).isGreaterThanOrEqualTo(size);
                buffers.add(buffer);
            }
        } finally {
            for (PooledBuffer buffer : buffers) {
                buffer.release();
            }
        }
    }

    /**
     * Test that pool correctly rounds up to extended size classes.
     */
    @Test
    void extendedSizeRounding() {
        // Request just over 8MB - should get 16MB buffer
        PooledBuffer buffer = pool.acquire(8 * ONE_MB + 1);
        try {
            assertThat(buffer.capacity()).isEqualTo(16 * ONE_MB);
        } finally {
            buffer.release();
        }

        // Request just over 16MB - should get 32MB buffer
        buffer = pool.acquire(16 * ONE_MB + 1);
        try {
            assertThat(buffer.capacity()).isEqualTo(32 * ONE_MB);
        } finally {
            buffer.release();
        }

        // Request just over 32MB - should get 64MB buffer
        buffer = pool.acquire(32 * ONE_MB + 1);
        try {
            assertThat(buffer.capacity()).isEqualTo(64 * ONE_MB);
        } finally {
            buffer.release();
        }
    }

    /**
     * Test buffer can hold data of the requested size.
     */
    @Test
    void bufferCanHoldRequestedData() {
        int requestedSize = 20 * ONE_MB;  // Will get 32MB buffer
        PooledBuffer pooledBuffer = pool.acquire(requestedSize);

        try {
            ByteBuffer buffer = pooledBuffer.buffer();

            // Write data up to requested size
            byte[] data = new byte[requestedSize];
            for (int i = 0; i < requestedSize; i++) {
                data[i] = (byte) (i % 256);
            }
            buffer.put(data);
            buffer.flip();

            // Read it back
            byte[] readBack = new byte[requestedSize];
            buffer.get(readBack);

            assertThat(readBack).isEqualTo(data);
        } finally {
            pooledBuffer.release();
        }
    }
}
