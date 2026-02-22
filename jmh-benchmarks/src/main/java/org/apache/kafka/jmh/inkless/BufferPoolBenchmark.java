/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh.inkless;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.produce.buffer.PooledBuffer;

/**
 * Benchmark for ElasticBufferPool acquire/release comparing heap allocation vs pooled buffers.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code useBufferPool}: false = heap allocation, true = pooled buffers</li>
 *   <li>{@code bufferSizeBytes}: requested buffer size</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgs = {
    "-Xms4G",
    "-Xmx4G",
    "-XX:+UseG1GC",
    "-XX:+AlwaysPreTouch"
})
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
public class BufferPoolBenchmark {

    private static final int ONE_MB = 1024 * 1024;
    private static final Random RANDOM = new Random(42);

    /**
     * Buffer size in bytes to allocate. 4MB is the standard Inkless batch size.
     */
    @Param({"4194304"})  // 4MB
    private int bufferSizeBytes;

    /**
     * Whether to use buffer pool.
     * false = fresh allocation each time (baseline)
     * true = pooled buffer reuse (optimization)
     */
    @Param({"false", "true"})
    private boolean useBufferPool;

    private BufferPool bufferPool;
    private byte[] preAllocatedData;

    @Setup(Level.Trial)
    public void setupTrial() {
        // Initialize buffer pool only if enabled
        // ElasticBufferPool: never blocks, grows on demand, graceful heap fallback
        // Max 16 buffers per size class - pool grows lazily as needed
        if (useBufferPool) {
            bufferPool = new ElasticBufferPool(16);
        } else {
            bufferPool = null;
        }

        // Pre-allocate data for write tests (avoid measuring data generation)
        preAllocatedData = new byte[Math.min(bufferSizeBytes, 1024)];
        RANDOM.nextBytes(preAllocatedData);
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        if (bufferPool != null) {
            bufferPool.close();
            bufferPool = null;
        }
    }

    /**
     * Benchmark: Single-threaded buffer allocation.
     *
     * <p>Measures allocation cost:
     * - useBufferPool=false: fresh byte[] allocation (baseline)
     * - useBufferPool=true: pooled ByteBuffer acquisition (optimized)
     */
    @Benchmark
    public void bufferAllocation(Blackhole blackhole) {
        if (useBufferPool) {
            PooledBuffer buffer = bufferPool.acquire(bufferSizeBytes);
            try {
                blackhole.consume(buffer.capacity());
            } finally {
                buffer.release();
            }
        } else {
            byte[] heapBuffer = new byte[bufferSizeBytes];
            blackhole.consume(heapBuffer.length);
        }
    }

    /**
     * Benchmark: Concurrent buffer allocation with 8 threads.
     *
     * <p>Tests pool contention vs heap allocation contention under load.
     */
    @Benchmark
    @Threads(8)
    public void bufferAllocationConcurrent(Blackhole blackhole) {
        bufferAllocation(blackhole);
    }

    /**
     * Benchmark: Concurrent - 8 concurrent allocation threads.
     */
    @Benchmark
    @Threads(8)
    public void concurrent8Threads(Blackhole blackhole) {
        bufferAllocation(blackhole);
    }

    /**
     * Benchmark: Concurrent - 4 concurrent allocation threads.
     */
    @Benchmark
    @Threads(4)
    public void concurrent4Threads(Blackhole blackhole) {
        bufferAllocation(blackhole);
    }

    /**
     * Benchmark: Buffer allocation with data write.
     *
     * <p>Measures allocation + write cost to simulate real usage pattern.
     */
    @Benchmark
    public void bufferAllocationWithWrite(Blackhole blackhole) {
        if (useBufferPool) {
            PooledBuffer buffer = bufferPool.acquire(bufferSizeBytes);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                // Write data pattern across buffer
                for (int offset = 0; offset < bufferSizeBytes; offset += preAllocatedData.length) {
                    int length = Math.min(preAllocatedData.length, bufferSizeBytes - offset);
                    bb.put(preAllocatedData, 0, length);
                }
                blackhole.consume(bb.position());
            } finally {
                buffer.release();
            }
        } else {
            byte[] heapBuffer = new byte[bufferSizeBytes];
            // Write data pattern across buffer
            for (int offset = 0; offset < bufferSizeBytes; offset += preAllocatedData.length) {
                int length = Math.min(preAllocatedData.length, bufferSizeBytes - offset);
                System.arraycopy(preAllocatedData, 0, heapBuffer, offset, length);
            }
            blackhole.consume(heapBuffer);
        }
    }

    /**
     * Benchmark: Concurrent buffer allocation with data write (8 threads).
     *
     * <p>Tests realistic concurrent write workload.
     */
    @Benchmark
    @Threads(8)
    public void bufferAllocationWithWriteConcurrent(Blackhole blackhole) {
        bufferAllocationWithWrite(blackhole);
    }

    /**
     * Benchmark: Buffer allocation with read-back.
     *
     * <p>Measures full lifecycle: allocate, write, read, release.
     */
    @Benchmark
    public void bufferAllocationWithReadWrite(Blackhole blackhole) {
        if (useBufferPool) {
            PooledBuffer buffer = bufferPool.acquire(bufferSizeBytes);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();

                // Write
                for (int offset = 0; offset < bufferSizeBytes; offset += preAllocatedData.length) {
                    int length = Math.min(preAllocatedData.length, bufferSizeBytes - offset);
                    bb.put(preAllocatedData, 0, length);
                }

                // Read back
                bb.flip();
                int checksum = 0;
                while (bb.hasRemaining()) {
                    checksum += bb.get();
                }
                blackhole.consume(checksum);
            } finally {
                buffer.release();
            }
        } else {
            byte[] heapBuffer = new byte[bufferSizeBytes];

            // Write
            for (int offset = 0; offset < bufferSizeBytes; offset += preAllocatedData.length) {
                int length = Math.min(preAllocatedData.length, bufferSizeBytes - offset);
                System.arraycopy(preAllocatedData, 0, heapBuffer, offset, length);
            }

            // Read back
            int checksum = 0;
            for (byte b : heapBuffer) {
                checksum += b;
            }
            blackhole.consume(checksum);
        }
    }

    /**
     * Benchmark: Pool metrics overhead.
     *
     * <p>Measures the cost of tracking active buffer count.
     * Only runs when useBufferPool=true.
     */
    @Benchmark
    public void poolMetricsOverhead(Blackhole blackhole) {
        if (useBufferPool) {
            blackhole.consume(bufferPool.activeBufferCount());
            blackhole.consume(bufferPool.totalPoolSizeBytes());
        } else {
            // Equivalent no-op for fair comparison
            blackhole.consume(0);
            blackhole.consume(0L);
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
