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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullBatchCoordinateCache;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.storage_backend.in_memory.InMemoryStorage;

/**
 * Facade for end-to-end produce benchmarking.
 *
 * <p>This class exposes the package-private Writer class for JMH benchmarks,
 * providing a complete produce path with:
 * <ul>
 *   <li>In-memory storage backend (no S3 dependency)</li>
 *   <li>In-memory control plane</li>
 *   <li>Configurable buffer pool (heap vs off-heap)</li>
 * </ul>
 *
 * <p>Use this facade to measure true end-to-end produce latency and throughput.
 */
public class EndToEndProduceBenchmarkFacade implements Closeable {

    private final Writer writer;
    private final InMemoryStorage storage;
    private final InMemoryControlPlane controlPlane;
    private final Map<String, LogConfig> topicConfigs;
    private final AtomicLong recordsProduced = new AtomicLong(0);
    private final AtomicLong bytesProduced = new AtomicLong(0);

    /**
     * Creates a new benchmark facade with the specified configuration.
     *
     * @param time             Time source for timestamps
     * @param brokerId         Broker ID for this writer
     * @param maxBufferSize    Maximum buffer size before rotation (bytes)
     * @param commitInterval   Interval for time-based commits
     * @param bufferPool       Buffer pool for off-heap allocation (null for heap)
     * @param topics           Topics to create (topic name -> partition count)
     */
    public EndToEndProduceBenchmarkFacade(
        Time time,
        int brokerId,
        int maxBufferSize,
        Duration commitInterval,
        BufferPool bufferPool,
        Map<String, TopicSetup> topics
    ) {
        // Initialize storage backend (in-memory)
        this.storage = new InMemoryStorage(new Metrics());
        this.storage.configure(Map.of());

        // Initialize control plane (in-memory)
        this.controlPlane = new InMemoryControlPlane(time);
        this.controlPlane.configure(Map.of());

        // Create topics and partitions
        Set<CreateTopicAndPartitionsRequest> createRequests = topics.entrySet().stream()
            .map(e -> new CreateTopicAndPartitionsRequest(
                e.getValue().topicId(),
                e.getKey(),
                e.getValue().partitionCount()))
            .collect(java.util.stream.Collectors.toSet());
        this.controlPlane.createTopicAndPartitions(createRequests);

        // Create topic configs
        this.topicConfigs = new HashMap<>();
        for (Map.Entry<String, TopicSetup> entry : topics.entrySet()) {
            topicConfigs.put(entry.getKey(), createLogConfig());
        }

        // Initialize caches
        KeyAlignmentStrategy keyAlignmentStrategy = new FixedBlockAlignment(Integer.MAX_VALUE);
        NullCache objectCache = new NullCache();
        NullBatchCoordinateCache batchCoordinateCache = new NullBatchCoordinateCache();

        // Create writer
        this.writer = new Writer(
            time,
            brokerId,
            ObjectKey.creator("benchmark/", false),
            storage,
            keyAlignmentStrategy,
            objectCache,
            batchCoordinateCache,
            controlPlane,
            commitInterval,
            maxBufferSize,
            3,  // maxFileUploadAttempts
            Duration.ofMillis(100),  // fileUploadRetryBackoff
            4,  // fileUploaderThreadPoolSize
            new BrokerTopicStats(),
            bufferPool,
            0  // bufferPoolMinSizeBytes (0 = always use pool when available)
        );
    }

    /**
     * Writes records to the specified partitions and waits for completion.
     *
     * @param records  Map of partition to records
     * @param timeout  Maximum time to wait for completion
     * @return Map of partition to response
     * @throws InterruptedException if interrupted while waiting
     * @throws ExecutionException   if the write failed
     * @throws TimeoutException     if the write did not complete in time
     */
    public Map<TopicIdPartition, PartitionResponse> write(
        Map<TopicIdPartition, MemoryRecords> records,
        Duration timeout
    ) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
            writer.write(records, topicConfigs, RequestLocal.noCaching());

        Map<TopicIdPartition, PartitionResponse> result =
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        // Track metrics
        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : records.entrySet()) {
            MemoryRecords memRecords = entry.getValue();
            bytesProduced.addAndGet(memRecords.sizeInBytes());
            for (var batch : memRecords.batches()) {
                recordsProduced.addAndGet(batch.countOrNull() != null ? batch.countOrNull() : 0);
            }
        }

        return result;
    }

    /**
     * Writes records asynchronously without waiting for completion.
     *
     * @param records Map of partition to records
     * @return Future that completes when the write is acknowledged
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> writeAsync(
        Map<TopicIdPartition, MemoryRecords> records
    ) {
        // Track metrics
        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : records.entrySet()) {
            MemoryRecords memRecords = entry.getValue();
            bytesProduced.addAndGet(memRecords.sizeInBytes());
            for (var batch : memRecords.batches()) {
                recordsProduced.addAndGet(batch.countOrNull() != null ? batch.countOrNull() : 0);
            }
        }

        return writer.write(records, topicConfigs, RequestLocal.noCaching());
    }

    /**
     * Returns the total number of records produced.
     */
    public long getRecordsProduced() {
        return recordsProduced.get();
    }

    /**
     * Returns the total number of bytes produced.
     */
    public long getBytesProduced() {
        return bytesProduced.get();
    }

    /**
     * Resets the metrics counters.
     */
    public void resetMetrics() {
        recordsProduced.set(0);
        bytesProduced.set(0);
    }

    @Override
    public void close() throws IOException {
        writer.close();
        storage.close();
        controlPlane.close();
    }

    private static LogConfig createLogConfig() {
        Properties props = new Properties();
        return new LogConfig(props);
    }

    /**
     * Configuration for a topic to be created.
     */
    public record TopicSetup(Uuid topicId, int partitionCount) {
        public static TopicSetup of(Uuid topicId, int partitionCount) {
            return new TopicSetup(topicId, partitionCount);
        }
    }
}
