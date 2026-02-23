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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A non-blocking buffer pool that grows elastically and falls back to heap on exhaustion.
 *
 * <h2>Architecture</h2>
 * <p>The pool maintains separate {@link ConcurrentLinkedDeque} queues for each size class
 * (1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB). Each queue starts empty and grows on demand up
 * to {@code maxBuffersPerClass}. Buffers use reference counting via {@link PooledBuffer}
 * for safe multi-consumer sharing.
 *
 * <h2>Acquisition Algorithm</h2>
 * <pre>
 * acquire(size):
 *   1. Select size class (round up to nearest power of 2)
 *   2. Try poll() from the class queue (lock-free, non-blocking)
 *   3. If empty AND allocated < max: CAS to increment count, allocate new buffer
 *   4. If CAS fails: another thread grew the pool, retry from step 2
 *   5. If at max capacity: return a heap-allocated fallback buffer
 * </pre>
 *
 * <h2>Design Rationale</h2>
 * <ul>
 *   <li><b>Non-blocking</b>: Uses ConcurrentLinkedDeque.poll() and CAS for growth, never waits</li>
 *   <li><b>Elastic</b>: Starts at 0 buffers, grows to demand, avoids pre-allocation waste</li>
 *   <li><b>Graceful degradation</b>: Returns heap buffer on exhaustion instead of blocking or failing</li>
 *   <li><b>Size classes</b>: Powers of 2 minimize fragmentation (at most 2x overhead)</li>
 *   <li><b>Heap buffers</b>: Faster allocation than direct buffers, works with S3 SDK</li>
 * </ul>
 *
 * <h2>Return Path</h2>
 * <p>When {@link PooledBuffer#release()} decrements refCount to 0, the buffer is cleared
 * and offered back to its size class queue. The pool tracks active buffer count for metrics.
 *
 * <h2>Memory Bounds</h2>
 * <p>Maximum memory = {@code maxBuffersPerClass × sum(SIZE_CLASSES)} = {@code maxBuffersPerClass × 127MB}.
 * Pool starts empty and only allocates when needed.
 */
public class ElasticBufferPool implements BufferPool {

    private static final Logger log = LoggerFactory.getLogger(ElasticBufferPool.class);

    private static final int ONE_MB = 1024 * 1024;

    /**
     * Extra iterations beyond maxBuffersPerClass to allow for CAS retry contention.
     * Each failed CAS means another thread succeeded and grew the pool, so we retry poll().
     * This buffer accounts for pathological contention scenarios without unbounded spinning.
     */
    private static final int CAS_CONTENTION_BUFFER = 4;

    /**
     * Size classes in bytes: 1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB.
     *
     * <p>Powers of 2 are chosen to:
     * <ul>
     *   <li>Minimize internal fragmentation (at most 2x waste for any request size)</li>
     *   <li>Align with common page sizes and memory allocator bucket sizes</li>
     *   <li>Keep the number of classes small for cache-friendly pool management</li>
     * </ul>
     *
     * <p>The 64MB maximum covers typical Kafka batch sizes. Larger requests will throw
     * {@link IllegalArgumentException}.
     */
    static final int[] SIZE_CLASSES = {
        ONE_MB,
        2 * ONE_MB,
        4 * ONE_MB,
        8 * ONE_MB,
        16 * ONE_MB,
        32 * ONE_MB,
        64 * ONE_MB
    };

    private final List<ConcurrentLinkedDeque<ByteBuffer>> pools;
    private final List<AtomicInteger> allocatedCounts;
    private final int maxBuffersPerClass;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger activeCount = new AtomicInteger(0);

    // Track first heap fallback per size class for INFO-level logging to avoid log spam
    private final AtomicBoolean[] firstHeapFallbackLogged;
    // Track first iteration limit warning per size class to avoid log spam
    private final AtomicBoolean[] iterationLimitWarningLogged;

    // Metrics - use LongAdder for hot-path counters to avoid contention
    // LongAdder uses striped cells to reduce cache-line bouncing under high concurrency
    private final LongAdder poolHitCount = new LongAdder();
    private final LongAdder poolGrowthCount = new LongAdder();
    private final AtomicLong heapFallbackCount = new AtomicLong(0);
    private final AtomicLong heapFallbackBytes = new AtomicLong(0);

    /**
     * Creates a new ElasticBufferPool.
     *
     * @param maxBuffersPerClass maximum buffers to allocate per size class
     */
    public ElasticBufferPool(int maxBuffersPerClass) {
        if (maxBuffersPerClass <= 0) {
            throw new IllegalArgumentException("maxBuffersPerClass must be positive");
        }

        this.maxBuffersPerClass = maxBuffersPerClass;
        this.pools = new ArrayList<>(SIZE_CLASSES.length);
        this.allocatedCounts = new ArrayList<>(SIZE_CLASSES.length);
        this.firstHeapFallbackLogged = new AtomicBoolean[SIZE_CLASSES.length];
        this.iterationLimitWarningLogged = new AtomicBoolean[SIZE_CLASSES.length];

        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            pools.add(new ConcurrentLinkedDeque<>());
            allocatedCounts.add(new AtomicInteger(0));
            firstHeapFallbackLogged[i] = new AtomicBoolean(false);
            iterationLimitWarningLogged[i] = new AtomicBoolean(false);
        }

        log.info("ElasticBufferPool created with maxBuffersPerClass={}, sizeClasses={}",
            maxBuffersPerClass, SIZE_CLASSES.length);
    }

    /**
     * Creates a new ElasticBufferPool with pre-warmed buffers.
     *
     * <p>Pre-warming buffers eliminates large allocation latency spikes during
     * initial load. The ByteBuffer allocations (1-64MB) are the expensive part;
     * wrapper objects are small and efficiently allocated by the JVM.
     *
     * @param maxBuffersPerClass maximum buffers to allocate per size class
     * @param prewarmCount number of buffers to pre-allocate per class (0 for lazy)
     */
    public ElasticBufferPool(int maxBuffersPerClass, int prewarmCount) {
        this(maxBuffersPerClass);

        if (prewarmCount > 0) {
            int toPrewarm = Math.min(prewarmCount, maxBuffersPerClass);
            for (int i = 0; i < SIZE_CLASSES.length; i++) {
                for (int j = 0; j < toPrewarm; j++) {
                    ByteBuffer buffer = allocateBuffer(i);
                    pools.get(i).offer(buffer);
                    allocatedCounts.get(i).incrementAndGet();
                }
            }
            log.info("Pre-warmed {} buffers per size class", toPrewarm);
        }
    }

    /**
     * Allocates a new buffer for the given size class.
     *
     * <p>This method centralizes buffer allocation to ensure consistency between
     * the acquire() path and the pre-warm path. Currently uses heap buffers
     * ({@code ByteBuffer.allocate()}). If direct buffers are needed in the future,
     * this is the single point of change.
     *
     * @param sizeClassIndex index into SIZE_CLASSES array
     * @return a new ByteBuffer with the size class capacity
     */
    private static ByteBuffer allocateBuffer(int sizeClassIndex) {
        return ByteBuffer.allocate(SIZE_CLASSES[sizeClassIndex]);
    }

    @Override
    public PooledBuffer acquire(int sizeBytes) {
        if (sizeBytes <= 0) {
            throw new IllegalArgumentException("sizeBytes must be positive");
        }
        if (closed.get()) {
            throw new IllegalStateException("BufferPool is closed");
        }

        int sizeClassIndex = selectSizeClass(sizeBytes);
        ConcurrentLinkedDeque<ByteBuffer> pool = pools.get(sizeClassIndex);
        AtomicInteger allocated = allocatedCounts.get(sizeClassIndex);

        // Explicit iteration limit to prevent unbounded spinning under pathological contention.
        // In normal operation, we exit via return (pool hit or growth) or break (at max capacity).
        // The limit is set generously above maxBuffersPerClass since that's the theoretical max
        // iterations needed. In practice, we rarely need more than maxBuffersPerClass iterations.
        // If we hit this limit, something unexpected happened.
        final int maxIterations = maxBuffersPerClass + CAS_CONTENTION_BUFFER;

        ByteBuffer buffer = null;
        boolean isPoolHit = false;
        boolean isGrowth = false;

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            // Step 1: Try to get from pool (non-blocking)
            // Note: buffer is already cleared in returnBuffer() when returned to pool
            buffer = pool.poll();
            if (buffer != null) {
                isPoolHit = true;
                break;
            }

            // Step 2: Pool empty - check if we can grow
            int currentAllocated = allocated.get();
            if (currentAllocated >= maxBuffersPerClass) {
                // At max capacity - final poll to catch late releases, then fall through to heap
                buffer = pool.poll();
                if (buffer != null) {
                    isPoolHit = true;
                }
                break;  // Exit loop - either got buffer or truly at max capacity
            }

            // Step 3: Try to allocate a new buffer
            if (allocated.compareAndSet(currentAllocated, currentAllocated + 1)) {
                buffer = allocateBuffer(sizeClassIndex);
                isGrowth = true;
                break;
            }
            // CAS failed - another thread grew the pool, loop back to try poll again
        }

        // Increment counters once after loop
        if (buffer != null) {
            activeCount.incrementAndGet();
            if (isPoolHit) {
                poolHitCount.increment();
            } else if (isGrowth) {
                poolGrowthCount.increment();
                log.debug("Pool growth: allocated new {}MB buffer (class {}, total {})",
                    SIZE_CLASSES[sizeClassIndex] / ONE_MB, sizeClassIndex, allocated.get());
            }
            return new ElasticPooledBuffer(buffer, sizeClassIndex, this);
        }

        // Fallback to heap: either at max capacity (normal) or hit iteration limit (unexpected).
        // Log at warn level only if we hit the iteration limit, which indicates unexpected contention.
        // Rate-limit to first occurrence per size class to avoid log spam under pathological contention.
        if (allocated.get() < maxBuffersPerClass) {
            if (iterationLimitWarningLogged[sizeClassIndex].compareAndSet(false, true)) {
                log.warn("Buffer pool acquisition hit iteration limit ({}) for size class {} "
                    + "(allocated={}, max={}). Falling back to heap allocation. "
                    + "This warning will not repeat for this size class.",
                    maxIterations, sizeClassIndex, allocated.get(), maxBuffersPerClass);
            }
        }
        heapFallbackCount.incrementAndGet();
        heapFallbackBytes.addAndGet(sizeBytes);

        // Log first fallback per size class at INFO for operator visibility during rollout.
        // Subsequent fallbacks log at DEBUG to avoid log spam in production.
        if (firstHeapFallbackLogged[sizeClassIndex].compareAndSet(false, true)) {
            log.info("Heap fallback: pool at max capacity for size class {} ({}MB). "
                + "Consider increasing produce.buffer.pool.size.per.class if this happens frequently. "
                + "Subsequent fallbacks for this size class will log at DEBUG level.",
                sizeClassIndex, SIZE_CLASSES[sizeClassIndex] / ONE_MB);
        } else {
            log.debug("Heap fallback: pool at max capacity for size class {} ({}MB), using heap",
                sizeClassIndex, SIZE_CLASSES[sizeClassIndex] / ONE_MB);
        }
        return new HeapPooledBuffer(sizeBytes);
    }

    @Override
    public int activeBufferCount() {
        return activeCount.get();
    }

    @Override
    public long totalPoolSizeBytes() {
        long total = 0;
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            total += (long) allocatedCounts.get(i).get() * SIZE_CLASSES[i];
        }
        return total;
    }

    @Override
    public long heapFallbackCount() {
        return heapFallbackCount.get();
    }

    @Override
    public long heapFallbackBytes() {
        return heapFallbackBytes.get();
    }

    /**
     * Returns the count of pool hits (buffer reuse).
     * Uses LongAdder for efficient counting under high concurrency.
     */
    public long poolHitCount() {
        return poolHitCount.sum();
    }

    /**
     * Returns the count of pool growth events (new buffer allocation).
     * Uses LongAdder for efficient counting under high concurrency.
     */
    public long poolGrowthCount() {
        return poolGrowthCount.sum();
    }

    /**
     * Returns the total number of buffers currently allocated across all size classes.
     */
    public int allocatedBufferCount() {
        int total = 0;
        for (AtomicInteger count : allocatedCounts) {
            total += count.get();
        }
        return total;
    }

    /**
     * Returns the number of allocated buffers for a specific size class.
     *
     * @param sizeClassIndex index into SIZE_CLASSES array
     * @return allocated buffer count for that class
     */
    public int allocatedBufferCount(int sizeClassIndex) {
        if (sizeClassIndex < 0 || sizeClassIndex >= SIZE_CLASSES.length) {
            throw new IllegalArgumentException("Invalid size class index: " + sizeClassIndex);
        }
        return allocatedCounts.get(sizeClassIndex).get();
    }

    /**
     * Returns the pool utilization ratio (active / allocated).
     * Returns 0.0 if no buffers are allocated.
     */
    public double utilizationRatio() {
        int allocated = allocatedBufferCount();
        if (allocated == 0) {
            return 0.0;
        }
        return (double) activeCount.get() / allocated;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (ConcurrentLinkedDeque<ByteBuffer> pool : pools) {
                pool.clear();
            }
            log.info("ElasticBufferPool closed. Final stats: poolHits={}, growthEvents={}, heapFallbacks={}",
                poolHitCount.sum(), poolGrowthCount.sum(), heapFallbackCount.get());
        }
    }

    /**
     * Returns buffer to the pool.
     *
     * <p>The buffer is cleared here (position=0, limit=capacity) before being added
     * back to the pool. This is the single point where buffers are reset, avoiding
     * redundant clear() calls in acquire().
     *
     * <p>Note: activeCount is decremented regardless of pool closure state because
     * buffers acquired before close() should still be tracked as they're released.
     * The count represents buffers currently in use, which goes to 0 when all
     * outstanding buffers are released - this is correct even after close().
     */
    void returnBuffer(ByteBuffer buffer, int sizeClassIndex) {
        // Decrement first to maintain accurate count even if pool is closed
        int newCount = activeCount.decrementAndGet();
        if (newCount < 0) {
            // This should never happen if refCount tracking is correct, but log if it does
            log.error("activeCount went negative ({}), indicating a reference counting bug", newCount);
        }
        if (!closed.get()) {
            buffer.clear();
            pools.get(sizeClassIndex).offer(buffer);
        }
    }

    /**
     * Selects the appropriate size class for the requested size.
     */
    private int selectSizeClass(int sizeBytes) {
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            if (sizeBytes <= SIZE_CLASSES[i]) {
                return i;
            }
        }
        throw new IllegalArgumentException(
            "Requested size " + sizeBytes + " exceeds maximum size class " + SIZE_CLASSES[SIZE_CLASSES.length - 1]);
    }

    /**
     * Pooled buffer backed by ElasticBufferPool.
     */
    private static class ElasticPooledBuffer implements PooledBuffer {
        private final ByteBuffer buffer;
        private final int sizeClassIndex;
        private final ElasticBufferPool pool;
        private final AtomicInteger refCount = new AtomicInteger(1);

        ElasticPooledBuffer(ByteBuffer buffer, int sizeClassIndex, ElasticBufferPool pool) {
            this.buffer = buffer;
            this.sizeClassIndex = sizeClassIndex;
            this.pool = pool;
        }

        @Override
        public ByteBuffer buffer() {
            checkNotReleased();
            return buffer;
        }

        @Override
        public int capacity() {
            return buffer.capacity();
        }

        @Override
        public PooledBuffer retain() {
            // Use getAndUpdate to atomically increment only if count > 0.
            // This avoids the race condition window that getAndIncrement+rollback has,
            // where concurrent retain() calls could see an artificially elevated refCount.
            int prev = refCount.getAndUpdate(c -> c > 0 ? c + 1 : c);
            if (prev <= 0) {
                throw new IllegalStateException("Buffer has been fully released");
            }
            return this;
        }

        @Override
        public void release() {
            int count = refCount.decrementAndGet();
            if (count < 0) {
                throw new IllegalStateException("Buffer released more times than retained");
            }
            if (count == 0) {
                pool.returnBuffer(buffer, sizeClassIndex);
            }
        }

        @Override
        public int refCount() {
            return refCount.get();
        }

        @Override
        public InputStream asInputStream() {
            checkNotReleased();
            return new ByteBufferInputStream(buffer.duplicate());
        }

        @Override
        public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
            checkNotReleased();
            ByteBuffer dup = buffer.duplicate();
            dup.position(srcOffset);
            dup.get(dest, destOffset, length);
        }

        private void checkNotReleased() {
            if (refCount.get() <= 0) {
                throw new IllegalStateException("Buffer has been fully released");
            }
        }
    }

    /**
     * Heap-allocated buffer for fallback when pool is exhausted.
     * This buffer is NOT returned to the pool - it's garbage collected.
     */
    private static class HeapPooledBuffer implements PooledBuffer {
        private final ByteBuffer buffer;
        private final AtomicInteger refCount = new AtomicInteger(1);

        HeapPooledBuffer(int sizeBytes) {
            this.buffer = ByteBuffer.allocate(sizeBytes);
        }

        @Override
        public ByteBuffer buffer() {
            checkNotReleased();
            return buffer;
        }

        @Override
        public int capacity() {
            return buffer.capacity();
        }

        @Override
        public PooledBuffer retain() {
            // Use getAndUpdate to atomically increment only if count > 0.
            // This avoids the race condition window that getAndIncrement+rollback has,
            // where concurrent retain() calls could see an artificially elevated refCount.
            int prev = refCount.getAndUpdate(c -> c > 0 ? c + 1 : c);
            if (prev <= 0) {
                throw new IllegalStateException("Buffer has been fully released");
            }
            return this;
        }

        @Override
        public void release() {
            int count = refCount.decrementAndGet();
            if (count < 0) {
                throw new IllegalStateException("Buffer released more times than retained");
            }
            // No return to pool - buffer will be garbage collected
        }

        @Override
        public int refCount() {
            return refCount.get();
        }

        @Override
        public InputStream asInputStream() {
            checkNotReleased();
            return new ByteBufferInputStream(buffer.duplicate());
        }

        @Override
        public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
            checkNotReleased();
            ByteBuffer dup = buffer.duplicate();
            dup.position(srcOffset);
            dup.get(dest, destOffset, length);
        }

        private void checkNotReleased() {
            if (refCount.get() <= 0) {
                throw new IllegalStateException("Buffer has been fully released");
            }
        }
    }

    /**
     * InputStream implementation that reads from a ByteBuffer.
     */
    private static class ByteBufferInputStream extends InputStream {
        private final ByteBuffer buffer;

        ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            int toRead = Math.min(len, buffer.remaining());
            buffer.get(b, off, toRead);
            return toRead;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }

        @Override
        public long skip(long n) {
            if (n <= 0) {
                return 0;
            }
            int toSkip = (int) Math.min(n, buffer.remaining());
            buffer.position(buffer.position() + toSkip);
            return toSkip;
        }
    }
}
