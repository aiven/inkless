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

import org.openjdk.jmh.annotations.AuxCounters;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

/**
 * Benchmark for fetch path (ObjectFetcher.readToByteBuffer) comparing heap vs pooled intermediate buffers.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code useBufferPool}: false = heap allocation, true = pooled 1MB intermediate buffers</li>
 *   <li>{@code dataSizeBytes}: simulated fetch data size</li>
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
public class FetchPathBufferPoolBenchmark {

    private static final Random RANDOM = new Random(42);

    /**
     * Data size in bytes to fetch. 4MB is the standard Inkless batch size.
     */
    @Param({"4194304"})  // 4MB
    private int dataSizeBytes;

    /**
     * Whether to use buffer pool for intermediate read buffers.
     * false = fresh 1MB allocation per read (baseline)
     * true = pooled 1MB buffers reused across reads (optimization)
     */
    @Param({"false", "true"})
    private boolean useBufferPool;

    private byte[] sourceData;
    private BufferPool bufferPool;
    private ObjectFetcher objectFetcher;

    /**
     * Auxiliary counters for pool metrics.
     *
     * <p>These counters track pool behavior during the benchmark and are reported
     * alongside throughput metrics. Useful for understanding pool hit rates and
     * fallback frequency.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    public static class PoolMetrics {
        public long poolHits;
        public long poolGrowth;
        public long heapFallbacks;

        @Setup(Level.Iteration)
        public void reset() {
            poolHits = 0;
            poolGrowth = 0;
            heapFallbacks = 0;
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        // Pre-allocate source data
        sourceData = new byte[dataSizeBytes];
        RANDOM.nextBytes(sourceData);

        // ElasticBufferPool: never blocks, grows on demand, graceful heap fallback
        // Max 16 buffers per size class - pool grows lazily as needed
        if (useBufferPool) {
            bufferPool = new ElasticBufferPool(16);
        } else {
            bufferPool = null;
        }

        // Create a minimal ObjectFetcher implementation for benchmarking
        objectFetcher = new BenchmarkObjectFetcher();
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        if (bufferPool != null) {
            bufferPool.close();
            bufferPool = null;
        }
        objectFetcher = null;
        sourceData = null;
    }

    /**
     * Benchmark: Single-threaded fetch operation.
     *
     * <p>Tests ObjectFetcher.readToByteBuffer which pools intermediate 1MB read buffers
     * when bufferPool is provided. The final result buffer is always a fresh heap allocation.
     */
    @Benchmark
    public void singleFetch(Blackhole blackhole) throws IOException {
        ReadableByteChannel channel = createChannel();
        ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
        blackhole.consume(result.remaining());
    }

    /**
     * Benchmark: Single-threaded FileFetchJob simulation.
     *
     * <p>Complete FileFetchJob.doWork() path including FileExtent creation.
     */
    @Benchmark
    public void fileFetchJobSimulation(Blackhole blackhole) throws IOException {
        ReadableByteChannel channel = createChannel();
        ByteBuffer byteBuffer = objectFetcher.readToByteBuffer(channel, bufferPool);
        blackhole.consume(byteBuffer.limit());
        blackhole.consume(byteBuffer.array());
    }

    /**
     * Benchmark: Concurrent - 4 concurrent fetch threads.
     */
    @Benchmark
    @Threads(4)
    public void concurrent4Threads(Blackhole blackhole) throws IOException {
        ReadableByteChannel channel = createChannel();
        ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
        blackhole.consume(result.remaining());
    }

    /**
     * Benchmark: Concurrent - 8 concurrent fetch threads.
     */
    @Benchmark
    @Threads(8)
    public void concurrent8Threads(Blackhole blackhole) throws IOException {
        ReadableByteChannel channel = createChannel();
        ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
        blackhole.consume(result.remaining());
    }

    /**
     * Benchmark: Concurrent - Batch of 8 fetches per invocation.
     */
    @Benchmark
    @Threads(4)
    public void concurrentBatchFetch(Blackhole blackhole) throws IOException {
        // Process 8 fetches per invocation (simulates multi-partition fetch)
        for (int i = 0; i < 8; i++) {
            ReadableByteChannel channel = createChannel();
            ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
            blackhole.consume(result.remaining());
        }
    }

    /**
     * Benchmark: Sustained load for GC pressure measurement.
     *
     * <p>Runs multiple fetch operations per invocation to generate sustained allocation
     * pressure. Run with GC profiling to see the benefit of pooling intermediate buffers.
     *
     * <p><b>Run with GC profiling:</b>
     * <pre>{@code
     * ./gradlew :jmh-benchmarks:jmh -Pargs="-prof gc FetchPathBufferPoolBenchmark.sustainedLoad"
     * }</pre>
     *
     * <p><b>Expected results with -prof gc:</b>
     * <ul>
     *   <li>{@code gc.alloc.rate.norm}: Lower with pool (intermediate buffers reused)</li>
     *   <li>{@code gc.count}: Fewer GC cycles with pool</li>
     * </ul>
     */
    @Benchmark
    @Threads(4)
    public void sustainedLoad(Blackhole blackhole) throws IOException {
        // Run 10 fetches per invocation to generate sustained allocation pressure
        for (int i = 0; i < 10; i++) {
            ReadableByteChannel channel = createChannel();
            ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
            blackhole.consume(result.remaining());
        }
    }

    /**
     * Benchmark: High concurrency sustained load.
     *
     * <p>8 threads each doing multiple fetches per invocation. This simulates
     * production-like load where multiple partitions are being fetched concurrently.
     */
    @Benchmark
    @Threads(8)
    public void sustainedLoadHighConcurrency(Blackhole blackhole) throws IOException {
        for (int i = 0; i < 5; i++) {
            ReadableByteChannel channel = createChannel();
            ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
            blackhole.consume(result.remaining());
        }
    }

    /**
     * Benchmark: Fetch with pool metrics tracking.
     *
     * <p>Reports pool hit/growth/fallback counts as auxiliary metrics alongside throughput.
     * Use this to understand pool behavior under load.
     *
     * <p>Metrics reported:
     * <ul>
     *   <li>{@code poolHits}: Buffers successfully acquired from pool</li>
     *   <li>{@code poolGrowth}: New buffers allocated to grow pool</li>
     *   <li>{@code heapFallbacks}: Times pool was exhausted, fell back to heap</li>
     * </ul>
     */
    @Benchmark
    @Threads(4)
    public void fetchWithPoolMetrics(Blackhole blackhole, PoolMetrics metrics) throws IOException {
        ReadableByteChannel channel = createChannel();
        ByteBuffer result = objectFetcher.readToByteBuffer(channel, bufferPool);
        blackhole.consume(result.remaining());

        // Capture pool metrics at end of operation
        if (bufferPool instanceof ElasticBufferPool ebp) {
            metrics.poolHits = ebp.poolHitCount();
            metrics.poolGrowth = ebp.poolGrowthCount();
            metrics.heapFallbacks = ebp.heapFallbackCount();
        }
    }

    private ReadableByteChannel createChannel() {
        return Channels.newChannel(new ByteArrayInputStream(sourceData));
    }

    /**
     * Minimal ObjectFetcher implementation for benchmarking.
     */
    private class BenchmarkObjectFetcher implements ObjectFetcher {
        @Override
        public ReadableByteChannel fetch(ObjectKey key, ByteRange range) {
            return createChannel();
        }

        @Override
        public void close() {
            // No-op
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
