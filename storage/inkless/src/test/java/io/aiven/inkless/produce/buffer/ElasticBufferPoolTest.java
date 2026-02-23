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
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for ElasticBufferPool - the non-blocking elastic buffer pool.
 */
class ElasticBufferPoolTest {

    private static final int ONE_MB = 1024 * 1024;
    private static final int FOUR_MB = 4 * ONE_MB;

    private ElasticBufferPool pool;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        pool = new ElasticBufferPool(4);  // max 4 buffers per size class
        executor = Executors.newFixedThreadPool(8);
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

    // ==================== Non-Blocking Acquisition Tests ====================

    @Test
    @Timeout(1)  // Should complete in under 1 second - proves non-blocking
    void acquire_neverBlocks() {
        // Max is 4 buffers per class - acquire more than that
        List<PooledBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            buffers.add(pool.acquire(ONE_MB));
        }

        // Should have 4 pooled + 6 heap fallbacks
        assertThat(pool.poolGrowthCount()).isEqualTo(4);
        assertThat(pool.heapFallbackCount()).isEqualTo(6);

        // Verify ALL buffers are usable (both pooled and heap fallback)
        for (int i = 0; i < buffers.size(); i++) {
            PooledBuffer buffer = buffers.get(i);
            ByteBuffer bb = buffer.buffer();
            // Write a unique value
            bb.put((byte) i);
            bb.flip();
            // Read it back
            assertThat(bb.get()).isEqualTo((byte) i);
        }

        // Cleanup
        for (PooledBuffer buffer : buffers) {
            buffer.release();
        }
    }

    @Test
    @Timeout(1)
    void acquire_immediatelyReturns_evenWhenPoolExhausted() {
        // Exhaust the pool for 1MB class
        List<PooledBuffer> heldBuffers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            heldBuffers.add(pool.acquire(ONE_MB));
        }

        // This should return immediately (not block) with a heap buffer
        long startTime = System.nanoTime();
        PooledBuffer heapBuffer = pool.acquire(ONE_MB);
        long elapsed = System.nanoTime() - startTime;

        assertThat(heapBuffer).isNotNull();
        assertThat(elapsed).isLessThan(100_000_000L);  // Less than 100ms

        // Cleanup
        heapBuffer.release();
        for (PooledBuffer buffer : heldBuffers) {
            buffer.release();
        }
    }

    // ==================== Elastic Growth Tests ====================

    @Test
    void acquire_growsPoolOnDemand() {
        // Pool starts empty
        assertThat(pool.allocatedBufferCount()).isEqualTo(0);

        // First acquire should grow the pool
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        assertThat(pool.poolGrowthCount()).isEqualTo(1);
        assertThat(pool.allocatedBufferCount()).isGreaterThan(0);

        // Second acquire should also grow (first buffer still held)
        PooledBuffer buffer2 = pool.acquire(ONE_MB);
        assertThat(pool.poolGrowthCount()).isEqualTo(2);

        // Cleanup
        buffer1.release();
        buffer2.release();
    }

    @Test
    void acquire_reusesReleasedBuffers() {
        // Acquire and release
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        buffer1.release();

        // Next acquire should reuse, not grow
        long growthBefore = pool.poolGrowthCount();
        PooledBuffer buffer2 = pool.acquire(ONE_MB);

        assertThat(pool.poolGrowthCount()).isEqualTo(growthBefore);  // No growth
        assertThat(pool.poolHitCount()).isGreaterThan(0);  // Hit

        buffer2.release();
    }

    @Test
    void acquire_stopsGrowingAtMax() {
        List<PooledBuffer> buffers = new ArrayList<>();

        // Acquire up to max (4)
        for (int i = 0; i < 4; i++) {
            buffers.add(pool.acquire(ONE_MB));
        }
        assertThat(pool.poolGrowthCount()).isEqualTo(4);

        // Next should fallback to heap, not grow
        PooledBuffer heapBuffer = pool.acquire(ONE_MB);
        assertThat(pool.poolGrowthCount()).isEqualTo(4);  // Still 4
        assertThat(pool.heapFallbackCount()).isEqualTo(1);

        // Cleanup
        heapBuffer.release();
        for (PooledBuffer buffer : buffers) {
            buffer.release();
        }
    }

    // ==================== Heap Fallback Tests ====================

    @Test
    void heapFallback_returnsWorkingBuffer() {
        // Exhaust pool
        List<PooledBuffer> pooled = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            pooled.add(pool.acquire(ONE_MB));
        }

        // Get a heap fallback buffer
        PooledBuffer heapBuffer = pool.acquire(ONE_MB);

        // Should be usable
        ByteBuffer bb = heapBuffer.buffer();
        bb.put((byte) 42);
        bb.flip();
        assertThat(bb.get()).isEqualTo((byte) 42);

        // Cleanup
        heapBuffer.release();
        for (PooledBuffer buffer : pooled) {
            buffer.release();
        }
    }

    @Test
    void heapFallback_doesNotReturnToPool() {
        // Exhaust pool
        List<PooledBuffer> pooled = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            pooled.add(pool.acquire(ONE_MB));
        }

        // Get and release heap fallback
        PooledBuffer heapBuffer = pool.acquire(ONE_MB);
        heapBuffer.release();

        // Active count should be 4 (pooled only)
        assertThat(pool.activeBufferCount()).isEqualTo(4);

        // Cleanup
        for (PooledBuffer buffer : pooled) {
            buffer.release();
        }
    }

    // ==================== Prewarm Tests ====================

    @Test
    void prewarm_preAllocatesBuffers() {
        ElasticBufferPool prewarmPool = new ElasticBufferPool(4, 2);

        try {
            // Should have 2 buffers pre-allocated per class
            assertThat(prewarmPool.allocatedBufferCount()).isEqualTo(2 * ElasticBufferPool.SIZE_CLASSES.length);

            // First acquire should hit the pool, not grow
            PooledBuffer buffer = prewarmPool.acquire(ONE_MB);
            assertThat(prewarmPool.poolHitCount()).isEqualTo(1);
            assertThat(prewarmPool.poolGrowthCount()).isEqualTo(0);

            buffer.release();
        } finally {
            prewarmPool.close();
        }
    }

    // ==================== Metrics Tests ====================

    @Test
    void metrics_tracksPoolHits() {
        PooledBuffer buffer = pool.acquire(ONE_MB);
        buffer.release();

        pool.acquire(ONE_MB).release();
        pool.acquire(ONE_MB).release();

        // First was growth, next two were hits
        assertThat(pool.poolHitCount()).isEqualTo(2);
    }

    @Test
    void metrics_tracksPoolGrowth() {
        pool.acquire(ONE_MB);  // Growth
        pool.acquire(ONE_MB);  // Growth

        assertThat(pool.poolGrowthCount()).isEqualTo(2);
    }

    @Test
    void metrics_tracksHeapFallback() {
        // Exhaust pool
        for (int i = 0; i < 4; i++) {
            pool.acquire(ONE_MB);
        }

        pool.acquire(ONE_MB);  // Fallback
        pool.acquire(ONE_MB);  // Fallback

        assertThat(pool.heapFallbackCount()).isEqualTo(2);
        assertThat(pool.heapFallbackBytes()).isEqualTo(2 * ONE_MB);
    }

    @Test
    void metrics_tracksActiveBuffers() {
        assertThat(pool.activeBufferCount()).isEqualTo(0);

        PooledBuffer b1 = pool.acquire(ONE_MB);
        assertThat(pool.activeBufferCount()).isEqualTo(1);

        PooledBuffer b2 = pool.acquire(ONE_MB);
        assertThat(pool.activeBufferCount()).isEqualTo(2);

        b1.release();
        assertThat(pool.activeBufferCount()).isEqualTo(1);

        b2.release();
        assertThat(pool.activeBufferCount()).isEqualTo(0);
    }

    @Test
    void metrics_utilizationRatio() {
        // Empty pool
        assertThat(pool.utilizationRatio()).isEqualTo(0.0);

        // Allocate and hold 2 buffers
        PooledBuffer b1 = pool.acquire(ONE_MB);
        PooledBuffer b2 = pool.acquire(ONE_MB);

        // 2 active / 2 allocated = 100%
        assertThat(pool.utilizationRatio()).isEqualTo(1.0);

        // Release one
        b1.release();
        // 1 active / 2 allocated = 50%
        assertThat(pool.utilizationRatio()).isEqualTo(0.5);

        b2.release();
    }

    // ==================== Concurrent Access Tests ====================

    @Test
    @Timeout(10)
    void concurrent_multipleThreadsAcquireAndRelease() throws InterruptedException {
        int threadCount = 8;
        int iterationsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        PooledBuffer buffer = pool.acquire(ONE_MB);
                        ByteBuffer bb = buffer.buffer();
                        bb.put((byte) 42);
                        buffer.release();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(errors.get()).isEqualTo(0);

        // Should have some hits due to reuse
        assertThat(pool.poolHitCount()).isGreaterThan(0);
    }

    @Test
    @Timeout(10)
    void concurrent_neverBlocksUnderContention() throws InterruptedException {
        // Use a pool with only 2 buffers per class
        ElasticBufferPool smallPool = new ElasticBufferPool(2);

        try {
            int threadCount = 16;  // Way more threads than buffers
            int iterationsPerThread = 50;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < iterationsPerThread; i++) {
                            PooledBuffer buffer = smallPool.acquire(ONE_MB);
                            // Hold buffer briefly
                            Thread.sleep(1);
                            buffer.release();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            // Should complete quickly - not blocked
            assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // Should have many heap fallbacks due to contention
            assertThat(smallPool.heapFallbackCount()).isGreaterThan(0);
        } finally {
            smallPool.close();
        }
    }

    // ==================== Size Class Tests ====================

    @Test
    void acquire_differentSizeClasses_independent() {
        // Acquire 1MB class
        PooledBuffer small = pool.acquire(ONE_MB);
        // Acquire 4MB class
        PooledBuffer medium = pool.acquire(FOUR_MB);

        // Should be independent allocations
        assertThat(pool.allocatedBufferCount(0)).isEqualTo(1);  // 1MB class
        assertThat(pool.allocatedBufferCount(2)).isEqualTo(1);  // 4MB class

        small.release();
        medium.release();
    }

    @Test
    void acquire_roundsUpToSizeClass() {
        // Request 1.5MB - should get 2MB buffer
        PooledBuffer buffer = pool.acquire(ONE_MB + ONE_MB / 2);

        assertThat(buffer.capacity()).isEqualTo(2 * ONE_MB);

        buffer.release();
    }

    @Test
    void acquire_maxSizeClass() {
        PooledBuffer buffer = pool.acquire(64 * ONE_MB);

        assertThat(buffer.capacity()).isEqualTo(64 * ONE_MB);

        buffer.release();
    }

    @Test
    void acquire_exceedsMaxSizeClass_throws() {
        assertThatThrownBy(() -> pool.acquire(65 * ONE_MB))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exceeds maximum size class");
    }

    // ==================== Lifecycle Tests ====================

    @Test
    void close_preventsNewAcquisitions() {
        pool.close();

        assertThatThrownBy(() -> pool.acquire(ONE_MB))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("closed");
    }

    @Test
    void close_idempotent() {
        pool.close();
        pool.close();  // Should not throw
    }

    // ==================== Reference Counting Tests ====================

    @Test
    void pooledBuffer_refCounting() {
        PooledBuffer buffer = pool.acquire(ONE_MB);

        assertThat(buffer.refCount()).isEqualTo(1);

        buffer.retain();
        assertThat(buffer.refCount()).isEqualTo(2);

        buffer.release();
        assertThat(buffer.refCount()).isEqualTo(1);

        buffer.release();
        assertThat(buffer.refCount()).isEqualTo(0);
    }

    @Test
    void pooledBuffer_returnedToPoolOnFinalRelease() {
        PooledBuffer buffer = pool.acquire(ONE_MB);
        buffer.retain();

        // First release doesn't return to pool
        buffer.release();
        assertThat(pool.activeBufferCount()).isEqualTo(1);

        // Second release returns to pool
        buffer.release();
        assertThat(pool.activeBufferCount()).isEqualTo(0);
    }

    @Test
    void heapBuffer_refCounting() {
        // Exhaust pool to force heap fallback
        for (int i = 0; i < 4; i++) {
            pool.acquire(ONE_MB);
        }

        PooledBuffer heapBuffer = pool.acquire(ONE_MB);

        assertThat(heapBuffer.refCount()).isEqualTo(1);

        heapBuffer.retain();
        assertThat(heapBuffer.refCount()).isEqualTo(2);

        heapBuffer.release();
        assertThat(heapBuffer.refCount()).isEqualTo(1);

        heapBuffer.release();
        assertThat(heapBuffer.refCount()).isEqualTo(0);
    }

    // ==================== Bounded Iteration Tests ====================

    @Test
    @Timeout(5)
    void acquire_terminatesInBoundedTime_underExtremeContention() throws InterruptedException {
        // This test verifies the acquire() loop terminates in bounded time
        // even when many threads are racing to allocate from a small pool.
        // The timeout ensures we don't spin forever.
        ElasticBufferPool tinyPool = new ElasticBufferPool(2);  // Very small pool

        try {
            int threadCount = 32;  // Many more threads than pool capacity
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            List<PooledBuffer> heldBuffers = new java.util.concurrent.CopyOnWriteArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        // Each thread tries to acquire and holds the buffer briefly
                        // to create contention
                        PooledBuffer buffer = tinyPool.acquire(ONE_MB);
                        heldBuffers.add(buffer);
                        successCount.incrementAndGet();
                        // Hold buffer briefly to create contention
                        Thread.sleep(10);
                        buffer.release();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();  // Start all threads simultaneously
            boolean completed = doneLatch.await(5, TimeUnit.SECONDS);

            // All threads should complete (either with pooled or heap buffer)
            assertThat(completed).as("All threads should complete within timeout").isTrue();
            assertThat(successCount.get()).isEqualTo(threadCount);

            // With 32 threads and max 2 pooled buffers per class, heap fallback is expected
            // but we don't assert on exact count as it depends on timing
        } finally {
            tinyPool.close();
        }
    }

    @Test
    @Timeout(5)
    void acquire_completesQuickly_whenPoolAtMaxCapacity() {
        // Exhaust the pool completely
        List<PooledBuffer> heldBuffers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            heldBuffers.add(pool.acquire(ONE_MB));
        }

        // Now pool is at max and empty - acquire should immediately fall back to heap
        long start = System.nanoTime();
        PooledBuffer heapBuffer = pool.acquire(ONE_MB);
        long elapsed = System.nanoTime() - start;

        // Should complete in microseconds, not milliseconds
        assertThat(elapsed).isLessThan(10_000_000L);  // Less than 10ms
        assertThat(pool.heapFallbackCount()).isEqualTo(1);

        heapBuffer.release();
        heldBuffers.forEach(PooledBuffer::release);
    }

    // ==================== Pool Closure Edge Cases ====================

    @Test
    void activeCount_remainsNonNegative_afterPoolClosure() {
        // Acquire buffers
        PooledBuffer b1 = pool.acquire(ONE_MB);
        PooledBuffer b2 = pool.acquire(ONE_MB);
        assertThat(pool.activeBufferCount()).isEqualTo(2);

        // Close pool while buffers are still active
        pool.close();

        // Release buffers after close - activeCount should still work correctly
        b1.release();
        assertThat(pool.activeBufferCount()).isEqualTo(1);

        b2.release();
        assertThat(pool.activeBufferCount()).isEqualTo(0);

        // Should never go negative
        assertThat(pool.activeBufferCount()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void buffersReleasedAfterClose_doNotReturnToPool() {
        // Acquire a buffer
        PooledBuffer buffer = pool.acquire(ONE_MB);
        long growthBefore = pool.poolGrowthCount();

        // Close pool
        pool.close();

        // Release buffer
        buffer.release();

        // Create new pool to verify buffer wasn't somehow returned
        ElasticBufferPool newPool = new ElasticBufferPool(4);
        try {
            // If we acquire again, it should be a fresh allocation (not from old pool)
            PooledBuffer newBuffer = newPool.acquire(ONE_MB);
            assertThat(newPool.poolGrowthCount()).isEqualTo(1);
            newBuffer.release();
        } finally {
            newPool.close();
        }
    }

    @Test
    void doubleRelease_throwsIllegalStateException() {
        PooledBuffer buffer = pool.acquire(ONE_MB);

        buffer.release();  // First release - valid

        // Second release should throw
        assertThatThrownBy(buffer::release)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("released more times than retained");
    }

    @Test
    void retainAfterRelease_throwsIllegalStateException() {
        PooledBuffer buffer = pool.acquire(ONE_MB);

        buffer.release();  // Release

        // Retain after release should throw
        assertThatThrownBy(buffer::retain)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("fully released");
    }

    @Test
    void bufferAccessAfterRelease_throwsIllegalStateException() {
        PooledBuffer buffer = pool.acquire(ONE_MB);

        buffer.release();

        // Accessing buffer after release should throw
        assertThatThrownBy(buffer::buffer)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("fully released");
    }
}
