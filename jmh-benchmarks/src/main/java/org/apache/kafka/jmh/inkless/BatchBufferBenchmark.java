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
import org.apache.kafka.common.record.MutableRecordBatch;
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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.produce.BatchBufferBenchmarkFacade;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.produce.buffer.PooledBuffer;

/**
 * Benchmark for BatchBuffer.close() comparing heap allocation vs pooled buffers.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code useBufferPool}: false = heap allocation, true = pooled buffers</li>
 *   <li>{@code targetBufferSizeBytes}: buffer size before close()</li>
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
public class BatchBufferBenchmark {

    private static final Uuid TOPIC_ID = new Uuid(1000, 1000);
    private static final String TOPIC_NAME = "benchmark-topic";
    private static final int NUM_PARTITIONS = 10;

    private final Random random = new Random(42);
    private final Time time = Time.SYSTEM;

    /**
     * Target total buffer size in bytes. 4MB is the standard Inkless batch size.
     */
    @Param({"4194304"})  // 4MB
    private int targetBufferSizeBytes;

    /**
     * Record size in bytes. 10KB records to exceed 64KB pool threshold.
     */
    @Param({"10000"})  // 10KB records
    private int recordSizeBytes;

    /**
     * Whether to use buffer pool.
     * false = fresh allocation each time (baseline)
     * true = pooled buffer reuse (optimization)
     */
    @Param({"false", "true"})
    private boolean useBufferPool;

    private List<TopicIdPartition> partitions;
    private List<MutableRecordBatch> preCreatedBatches;
    private BufferPool bufferPool;

    @Setup(Level.Trial)
    public void setupTrial() {
        // Create partition list
        partitions = new ArrayList<>(NUM_PARTITIONS);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitions.add(new TopicIdPartition(TOPIC_ID, i, TOPIC_NAME));
        }

        // Pre-create batches to avoid allocation during benchmark
        int recordsPerBatch = 10;
        int bytesPerBatch = estimateBatchSize(recordSizeBytes, recordsPerBatch);
        int totalBatches = (targetBufferSizeBytes / bytesPerBatch) + 1;

        preCreatedBatches = new ArrayList<>(totalBatches);
        for (int i = 0; i < totalBatches; i++) {
            preCreatedBatches.add(createBatch(recordSizeBytes, recordsPerBatch));
        }

        // Initialize buffer pool only if enabled
        // ElasticBufferPool: never blocks, grows on demand, graceful heap fallback
        // Max 16 buffers per size class - pool grows lazily as needed
        if (useBufferPool) {
            bufferPool = new ElasticBufferPool(16);
        } else {
            bufferPool = null;
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        preCreatedBatches.clear();
        partitions.clear();
        if (bufferPool != null) {
            bufferPool.close();
            bufferPool = null;
        }
    }

    /**
     * Benchmark: Single-threaded BatchBuffer.close() with target buffer size.
     *
     * <p>Measures the allocation and serialization cost of closing a buffer.
     * When useBufferPool=false: allocates fresh byte[] each time (baseline).
     * When useBufferPool=true: uses pooled ByteBuffer (optimized, reduces GC).
     */
    @Benchmark
    public void batchBufferClose(Blackhole blackhole) {
        BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);

        int batchIndex = 0;
        int addedBytes = 0;

        // Add batches round-robin across partitions until we reach target size
        while (addedBytes < targetBufferSizeBytes && batchIndex < preCreatedBatches.size()) {
            TopicIdPartition partition = partitions.get(batchIndex % NUM_PARTITIONS);
            MutableRecordBatch batch = preCreatedBatches.get(batchIndex);

            buffer.addBatch(partition, batch, batchIndex / NUM_PARTITIONS);
            addedBytes += batch.sizeInBytes();
            batchIndex++;
        }

        BatchBufferBenchmarkFacade.CloseResult result = buffer.close();

        // Consume results to prevent dead code elimination
        blackhole.consume(result.data());
        blackhole.consume(result.commitBatchRequestCount());

        // Release pooled buffer back to pool
        result.data().release();
    }

    /**
     * Benchmark: Concurrent BatchBuffer.close() with 8 threads.
     *
     * <p>Simulates multiple producers writing simultaneously.
     * This tests pool contention and concurrent allocation patterns.
     * The useBufferPool param controls whether fresh allocation or pooled buffers are used.
     */
    @Benchmark
    @Threads(8)
    public void batchBufferCloseConcurrent(Blackhole blackhole) {
        BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);

        int batchIndex = 0;
        int addedBytes = 0;

        while (addedBytes < targetBufferSizeBytes && batchIndex < preCreatedBatches.size()) {
            TopicIdPartition partition = partitions.get(batchIndex % NUM_PARTITIONS);
            MutableRecordBatch batch = preCreatedBatches.get(batchIndex);

            buffer.addBatch(partition, batch, batchIndex / NUM_PARTITIONS);
            addedBytes += batch.sizeInBytes();
            batchIndex++;
        }

        BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
        blackhole.consume(result.data());
        blackhole.consume(result.commitBatchRequestCount());
        result.data().release();
    }

    /**
     * Benchmark: Concurrent - 8 concurrent producer threads.
     */
    @Benchmark
    @Threads(8)
    public void concurrent8Threads(Blackhole blackhole) {
        BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);

        int batchIndex = 0;
        int addedBytes = 0;

        while (addedBytes < targetBufferSizeBytes && batchIndex < preCreatedBatches.size()) {
            TopicIdPartition partition = partitions.get(batchIndex % NUM_PARTITIONS);
            MutableRecordBatch batch = preCreatedBatches.get(batchIndex);

            buffer.addBatch(partition, batch, batchIndex / NUM_PARTITIONS);
            addedBytes += batch.sizeInBytes();
            batchIndex++;
        }

        BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
        blackhole.consume(result.data());
        blackhole.consume(result.commitBatchRequestCount());
        result.data().release();
    }

    /**
     * Benchmark: Concurrent - 4 concurrent producer threads.
     */
    @Benchmark
    @Threads(4)
    public void concurrent4Threads(Blackhole blackhole) {
        BatchBufferBenchmarkFacade buffer = new BatchBufferBenchmarkFacade(bufferPool);

        int batchIndex = 0;
        int addedBytes = 0;

        while (addedBytes < targetBufferSizeBytes && batchIndex < preCreatedBatches.size()) {
            TopicIdPartition partition = partitions.get(batchIndex % NUM_PARTITIONS);
            MutableRecordBatch batch = preCreatedBatches.get(batchIndex);

            buffer.addBatch(partition, batch, batchIndex / NUM_PARTITIONS);
            addedBytes += batch.sizeInBytes();
            batchIndex++;
        }

        BatchBufferBenchmarkFacade.CloseResult result = buffer.close();
        blackhole.consume(result.data());
        blackhole.consume(result.commitBatchRequestCount());
        result.data().release();
    }

    /**
     * Benchmark: Isolated allocation cost comparison (no batch serialization).
     */
    @Benchmark
    public void isolatedAllocation(Blackhole blackhole) {
        if (useBufferPool) {
            // Pooled allocation path
            PooledBuffer pooledBuffer = bufferPool.acquire(targetBufferSizeBytes);
            try {
                // Touch the buffer to ensure it's usable
                pooledBuffer.buffer().putInt(0, 42);
                blackhole.consume(pooledBuffer.capacity());
            } finally {
                pooledBuffer.release();
            }
        } else {
            // Fresh allocation path (baseline)
            byte[] data = new byte[targetBufferSizeBytes];
            // Touch the array to ensure it's actually allocated
            for (int i = 0; i < data.length; i += 4096) {
                data[i] = (byte) i;
            }
            blackhole.consume(data);
        }
    }

    /**
     * Benchmark: Concurrent isolated allocation with 8 threads.
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

    private MutableRecordBatch createBatch(int recordSize, int recordCount) {
        SimpleRecord[] records = new SimpleRecord[recordCount];
        byte[] payload = new byte[recordSize];
        random.nextBytes(payload);

        for (int i = 0; i < recordCount; i++) {
            records[i] = new SimpleRecord(time.milliseconds(), null, payload);
        }

        MemoryRecords memoryRecords = MemoryRecords.withRecords(
            RecordBatch.CURRENT_MAGIC_VALUE,
            0,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            records
        );

        Iterator<MutableRecordBatch> iterator = memoryRecords.batches().iterator();
        return iterator.hasNext() ? iterator.next() : null;
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
