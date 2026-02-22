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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.aiven.inkless.produce.BatchBufferBenchmarkFacade;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;

/**
 * Benchmark for Writer throughput comparing global lock vs striped lock, with and without buffer pool.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code useBufferPool}: false = heap allocation, true = pooled buffers</li>
 *   <li>{@code bufferSizeBytes}: buffer size before rotation</li>
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
public class WriterThroughputBenchmark {

    private static final Uuid TOPIC_ID = new Uuid(1000, 1000);
    private static final String TOPIC_NAME = "benchmark-topic";
    private static final int NUM_PARTITIONS = 32;

    private final Random random = new Random(42);
    private final Time time = Time.SYSTEM;

    /**
     * Buffer size in bytes before rotation.
     * 4MB is the standard Inkless batch size.
     */
    @Param({"4194304"})  // 4MB
    private int bufferSizeBytes;

    /**
     * Whether to use off-heap buffer pool.
     * false = heap allocation (baseline)
     * true = DirectByteBuffer pool (optimization)
     */
    @Param({"false", "true"})
    private boolean useBufferPool;

    private List<TopicIdPartition> partitions;
    private List<MemoryRecords> preCreatedRecords;
    private BufferPool bufferPool;
    private AtomicInteger partitionCounter;

    // Simulates the global Writer lock for baseline measurement
    private Lock globalLock;

    @Setup(Level.Trial)
    public void setupTrial() {
        // Create partitions for parallel access
        partitions = new ArrayList<>(NUM_PARTITIONS);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitions.add(new TopicIdPartition(TOPIC_ID, i, TOPIC_NAME));
        }

        // Pre-create records to avoid allocation during benchmark
        // Each batch is ~100KB (10KB records × 10) to exceed 64KB pool threshold
        int recordSize = 10000;  // 10KB records
        int recordsPerBatch = 10;
        int batchSize = estimateBatchSize(recordSize, recordsPerBatch);  // ~100KB per batch
        int totalBatches = (bufferSizeBytes / batchSize) * 4;  // 4x for multiple iterations

        preCreatedRecords = new ArrayList<>(totalBatches);
        for (int i = 0; i < totalBatches; i++) {
            preCreatedRecords.add(createMemoryRecords(recordSize, recordsPerBatch));
        }

        // Initialize buffer pool only if enabled
        // ElasticBufferPool: never blocks, grows on demand, graceful heap fallback
        // Max 16 buffers per size class - pool grows lazily as needed
        if (useBufferPool) {
            bufferPool = new ElasticBufferPool(16);
        } else {
            bufferPool = null;
        }

        partitionCounter = new AtomicInteger(0);
        globalLock = new ReentrantLock();
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        preCreatedRecords.clear();
        partitions.clear();
        if (bufferPool != null) {
            bufferPool.close();
            bufferPool = null;
        }
    }

    /**
     * Benchmark: Single-threaded writer operation.
     */
    @Benchmark
    public void writerSingleThreaded(Blackhole blackhole) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
        for (var batch : records.batches()) {
            buffer.addBatch(partition, batch, partitionIndex);
        }

        // Always close to exercise the buffer pool allocation path
        BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
        blackhole.consume(result.data());
        blackhole.consume(result.commitBatchRequestCount());
        result.data().release();
    }

    /**
     * Benchmark: Writer with global lock (8 threads).
     */
    @Benchmark
    @Threads(8)
    public void writerWithGlobalLock(Blackhole blackhole) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        // Simulate global lock contention (current Writer behavior)
        globalLock.lock();
        try {
            BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, partitionIndex);
            }

            // Always close to exercise the buffer pool allocation path
            BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
            blackhole.consume(result.data());
            blackhole.consume(result.commitBatchRequestCount());
            result.data().release();
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Benchmark: Writer with per-partition striped lock (8 threads).
     */
    @Benchmark
    @Threads(8)
    public void writerWithStripedLock(Blackhole blackhole, StripedLockState state) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        // Simulate striped lock (partition-based)
        // Use bitwise AND with MAX_VALUE to ensure non-negative (Math.abs can return negative for MIN_VALUE)
        int stripeIndex = (partition.hashCode() & Integer.MAX_VALUE) % state.stripeLocks.length;
        Lock stripeLock = state.stripeLocks[stripeIndex];

        stripeLock.lock();
        try {
            BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, partitionIndex);
            }

            // Always close to exercise the buffer pool allocation path
            BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
            blackhole.consume(result.data());
            blackhole.consume(result.commitBatchRequestCount());
            result.data().release();
        } finally {
            stripeLock.unlock();
        }
    }

    /**
     * Benchmark: File rotation latency (fill buffer to threshold, then close).
     */
    @Benchmark
    public void fileRotationLatency(Blackhole blackhole) {
        BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);

        int partitionIndex = 0;
        int recordIndex = 0;
        int addedBytes = 0;

        // Fill buffer to rotation threshold
        while (addedBytes < bufferSizeBytes && recordIndex < preCreatedRecords.size()) {
            TopicIdPartition partition = partitions.get(partitionIndex % partitions.size());
            MemoryRecords records = preCreatedRecords.get(recordIndex);

            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, recordIndex);
                addedBytes += batch.sizeInBytes();
            }

            partitionIndex++;
            recordIndex++;
        }

        // Measure rotation
        BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
        blackhole.consume(result.data());
        blackhole.consume(result.commitBatchRequestCount());
        result.data().release();
    }

    /**
     * Benchmark: Concurrent file rotation with 8 threads.
     */
    @Benchmark
    @Threads(8)
    public void fileRotationLatencyConcurrent(Blackhole blackhole) {
        fileRotationLatency(blackhole);
    }

    /**
     * Benchmark: Concurrent - 8 threads with global lock.
     */
    @Benchmark
    @Threads(8)
    public void concurrent8ThreadsGlobalLock(Blackhole blackhole) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        globalLock.lock();
        try {
            BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, partitionIndex);
            }

            // Always close to exercise the buffer pool allocation path
            BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
            blackhole.consume(result.data());
            blackhole.consume(result.commitBatchRequestCount());
            result.data().release();
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Benchmark: Concurrent - 4 threads with global lock.
     */
    @Benchmark
    @Threads(4)
    public void concurrent4ThreadsGlobalLock(Blackhole blackhole) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        globalLock.lock();
        try {
            BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, partitionIndex);
            }

            // Always close to exercise the buffer pool allocation path
            BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
            blackhole.consume(result.data());
            blackhole.consume(result.commitBatchRequestCount());
            result.data().release();
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Benchmark: Concurrent - 8 threads with striped lock.
     */
    @Benchmark
    @Threads(8)
    public void concurrent8ThreadsStripedLock(Blackhole blackhole, StripedLockState state) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        int stripeIndex = (partition.hashCode() & Integer.MAX_VALUE) % state.stripeLocks.length;
        Lock stripeLock = state.stripeLocks[stripeIndex];

        stripeLock.lock();
        try {
            BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, partitionIndex);
            }

            // Always close to exercise the buffer pool allocation path
            BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
            blackhole.consume(result.data());
            blackhole.consume(result.commitBatchRequestCount());
            result.data().release();
        } finally {
            stripeLock.unlock();
        }
    }

    /**
     * Benchmark: Concurrent - 4 threads with striped lock.
     */
    @Benchmark
    @Threads(4)
    public void concurrent4ThreadsStripedLock(Blackhole blackhole, StripedLockState state) {
        int partitionIndex = partitionCounter.getAndIncrement() % partitions.size();
        TopicIdPartition partition = partitions.get(partitionIndex);
        int recordIndex = partitionIndex % preCreatedRecords.size();
        MemoryRecords records = preCreatedRecords.get(recordIndex);

        int stripeIndex = (partition.hashCode() & Integer.MAX_VALUE) % state.stripeLocks.length;
        Lock stripeLock = state.stripeLocks[stripeIndex];

        stripeLock.lock();
        try {
            BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);
            for (var batch : records.batches()) {
                buffer.addBatch(partition, batch, partitionIndex);
            }

            // Always close to exercise the buffer pool allocation path
            BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
            blackhole.consume(result.data());
            blackhole.consume(result.commitBatchRequestCount());
            result.data().release();
        } finally {
            stripeLock.unlock();
        }
    }

    /**
     * State for striped lock benchmark.
     */
    @State(Scope.Benchmark)
    public static class StripedLockState {
        private static final int DEFAULT_STRIPE_COUNT = 16;

        Lock[] stripeLocks;

        @Setup(Level.Trial)
        public void setup() {
            stripeLocks = new Lock[DEFAULT_STRIPE_COUNT];
            for (int i = 0; i < DEFAULT_STRIPE_COUNT; i++) {
                stripeLocks[i] = new ReentrantLock();
            }
        }
    }

    private MemoryRecords createMemoryRecords(int recordSize, int recordCount) {
        SimpleRecord[] records = new SimpleRecord[recordCount];
        byte[] payload = new byte[recordSize];
        random.nextBytes(payload);

        for (int i = 0; i < recordCount; i++) {
            records[i] = new SimpleRecord(time.milliseconds(), null, payload);
        }

        return MemoryRecords.withRecords(
            RecordBatch.CURRENT_MAGIC_VALUE,
            0,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            records
        );
    }

    private int estimateBatchSize(int recordSize, int recordCount) {
        int recordOverhead = 20;
        int batchOverhead = 61;
        return (recordSize + recordOverhead) * recordCount + batchOverhead;
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
