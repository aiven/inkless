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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.produce.buffer.PooledBuffer;

/**
 * Benchmark comparing S3 upload data preparation paths.
 *
 * <p><b>What this measures:</b> CPU time to prepare data for S3 SDK upload. Compares:
 * <ul>
 *   <li>InputStream path: byte[] copy → ByteArrayInputStream → SDK reads in 8KB chunks</li>
 *   <li>ByteBuffer path: pooled buffer → SDK reads directly (fewer intermediate copies)</li>
 * </ul>
 *
 * <p><b>What this does NOT measure:</b> Actual S3 network I/O, which dominates real upload
 * time (~400ms). The data preparation overhead is small relative to network latency.
 *
 * <p><b>Why it matters:</b> Under high throughput, reducing intermediate copies decreases
 * memory bandwidth pressure and GC allocation rate, even if per-operation time is similar.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code useDirectBuffer}: false = InputStream with byte[] copy, true = pooled ByteBuffer</li>
 *   <li>{@code bufferSizeBytes}: data size per upload</li>
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
public class S3UploadCopyBenchmark {

    private static final Random RANDOM = new Random(42);
    private static final int READ_BUFFER_SIZE = 8192;

    /**
     * Buffer size in bytes. 4MB is the standard Inkless batch size.
     */
    @Param({"4194304"})  // 4MB
    private int bufferSizeBytes;

    /**
     * Whether to use ByteBuffer upload path.
     * false = InputStream path with fresh byte[] copy (baseline)
     * true = ByteBuffer path with pooled buffer (optimization)
     */
    @Param({"false", "true"})
    private boolean useDirectBuffer;

    private byte[] heapData;
    private BufferPool bufferPool;

    @Setup(Level.Trial)
    public void setupTrial() {
        // Pre-allocate heap data for source
        heapData = new byte[bufferSizeBytes];
        RANDOM.nextBytes(heapData);

        // Initialize buffer pool only if using direct buffer path
        // ElasticBufferPool: never blocks, grows on demand, graceful heap fallback
        // Max 16 buffers per size class - pool grows lazily as needed
        if (useDirectBuffer) {
            bufferPool = new ElasticBufferPool(16);
        } else {
            bufferPool = null;
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        if (bufferPool != null) {
            bufferPool.close();
            bufferPool = null;
        }
    }

    /**
     * Benchmark: Simulates S3 upload preparation and data transfer.
     *
     * <p>When useDirectBuffer=false (baseline):
     * <ul>
     *   <li>Creates ByteArrayInputStream from heap data</li>
     *   <li>Simulates SDK reading stream into intermediate buffer</li>
     *   <li>Multiple intermediate allocations occur</li>
     * </ul>
     *
     * <p>When useDirectBuffer=true (optimization):
     * <ul>
     *   <li>Acquires buffer from pool</li>
     *   <li>Data written directly to buffer</li>
     *   <li>SDK can use buffer directly (zero-copy for network I/O)</li>
     * </ul>
     */
    @Benchmark
    public void uploadPreparation(Blackhole blackhole) {
        if (useDirectBuffer) {
            // Optimized path: use pooled direct ByteBuffer
            PooledBuffer buffer = bufferPool.acquire(bufferSizeBytes);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                bb.put(heapData, 0, bufferSizeBytes);
                bb.flip();

                // Create read-only view (simulates RequestBody.fromByteBuffer())
                ByteBuffer readOnlyView = bb.asReadOnlyBuffer();

                // Simulate SDK using the buffer - no intermediate copy needed
                blackhole.consume(readOnlyView.remaining());
                blackhole.consume(readOnlyView.get(0));
            } finally {
                buffer.release();
            }
        } else {
            // Baseline path: heap array + InputStream
            byte[] copy = new byte[bufferSizeBytes];
            System.arraycopy(heapData, 0, copy, 0, bufferSizeBytes);

            // Create InputStream (simulates current S3Storage.upload)
            InputStream inputStream = new ByteArrayInputStream(copy);

            // Simulate SDK reading the stream (allocates intermediate buffer)
            byte[] readBuffer = new byte[READ_BUFFER_SIZE];
            int totalRead = 0;
            try {
                int read;
                while ((read = inputStream.read(readBuffer)) != -1) {
                    totalRead += read;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            blackhole.consume(totalRead);
        }
    }

    /**
     * Benchmark: Concurrent upload preparation with 8 threads.
     *
     * <p>Tests pool contention vs heap allocation contention under load.
     */
    @Benchmark
    @Threads(8)
    public void uploadPreparationConcurrent(Blackhole blackhole) {
        uploadPreparation(blackhole);
    }

    /**
     * Benchmark: Concurrent - 8 concurrent upload threads.
     */
    @Benchmark
    @Threads(8)
    public void concurrent8Threads(Blackhole blackhole) {
        uploadPreparation(blackhole);
    }

    /**
     * Benchmark: Concurrent - 4 concurrent upload threads.
     */
    @Benchmark
    @Threads(4)
    public void concurrent4Threads(Blackhole blackhole) {
        uploadPreparation(blackhole);
    }

    /**
     * Benchmark: Isolates data copy overhead.
     */
    @Benchmark
    public void dataCopyOverhead(Blackhole blackhole) {
        if (useDirectBuffer) {
            // Single copy into pooled direct buffer
            PooledBuffer buffer = bufferPool.acquire(bufferSizeBytes);
            try {
                ByteBuffer bb = buffer.buffer();
                bb.clear();
                bb.put(heapData, 0, bufferSizeBytes);
                bb.flip();
                blackhole.consume(bb.remaining());
            } finally {
                buffer.release();
            }
        } else {
            // Two copies (typical InputStream path overhead)
            byte[] copy1 = new byte[bufferSizeBytes];
            System.arraycopy(heapData, 0, copy1, 0, bufferSizeBytes);

            byte[] copy2 = new byte[bufferSizeBytes];
            System.arraycopy(copy1, 0, copy2, 0, bufferSizeBytes);

            blackhole.consume(copy2);
        }
    }

    /**
     * Benchmark: Isolates allocation cost only.
     */
    @Benchmark
    public void isolatedAllocation(Blackhole blackhole) {
        if (useDirectBuffer) {
            PooledBuffer buffer = bufferPool.acquire(bufferSizeBytes);
            try {
                // Touch buffer to ensure usable
                buffer.buffer().putInt(0, 42);
                blackhole.consume(buffer.capacity());
            } finally {
                buffer.release();
            }
        } else {
            byte[] data = new byte[bufferSizeBytes];
            // Touch array pages to ensure allocation
            for (int i = 0; i < data.length; i += 4096) {
                data[i] = (byte) i;
            }
            blackhole.consume(data);
        }
    }

    /**
     * Benchmark: Concurrent allocation with 8 threads.
     */
    @Benchmark
    @Threads(8)
    public void isolatedAllocationConcurrent(Blackhole blackhole) {
        isolatedAllocation(blackhole);
    }

    /**
     * Benchmark: Concurrent allocation with 8 threads.
     */
    @Benchmark
    @Threads(8)
    public void concurrentAllocation8Threads(Blackhole blackhole) {
        isolatedAllocation(blackhole);
    }

    /**
     * Benchmark: Concurrent allocation with 4 threads.
     */
    @Benchmark
    @Threads(4)
    public void concurrentAllocation4Threads(Blackhole blackhole) {
        isolatedAllocation(blackhole);
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
