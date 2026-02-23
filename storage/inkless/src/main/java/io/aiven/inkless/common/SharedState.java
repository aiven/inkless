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
package io.aiven.inkless.common;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.CaffeineBatchCoordinateCache;
import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullBatchCoordinateCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.BufferPoolMetrics;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.storage_backend.common.StorageBackend;

public final class SharedState implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(SharedState.class);
    public static final String STORAGE_METRIC_CONTEXT = "io.aiven.inkless.storage";

    // Buffer pool size classes: 1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB
    private static final int[] BUFFER_POOL_SIZE_CLASSES = {1, 2, 4, 8, 16, 32, 64};
    private static final int BUFFER_POOL_HIGH_COUNT_THRESHOLD = 8;

    private final Time time;
    private final int brokerId;
    private final InklessConfig config;
    private final MetadataView metadata;
    private final ControlPlane controlPlane;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
    private final BatchCoordinateCache batchCoordinateCache;
    private final BrokerTopicStats brokerTopicStats;
    private final Supplier<LogConfig> defaultTopicConfigs;
    private final Metrics storageMetrics;
    private final BufferPool bufferPool;  // null when disabled
    private final BufferPoolMetrics bufferPoolMetrics;  // null when pool disabled
    private final int bufferPoolMinSizeBytes;

    public SharedState(
        final Time time,
        final int brokerId,
        final InklessConfig config,
        final MetadataView metadata,
        final ControlPlane controlPlane,
        final ObjectKeyCreator objectKeyCreator,
        final KeyAlignmentStrategy keyAlignmentStrategy,
        final ObjectCache cache,
        final BatchCoordinateCache batchCoordinateCache,
        final BrokerTopicStats brokerTopicStats,
        final Supplier<LogConfig> defaultTopicConfigs,
        final BufferPool bufferPool,
        final BufferPoolMetrics bufferPoolMetrics,
        final int bufferPoolMinSizeBytes
    ) {
        this.time = time;
        this.brokerId = brokerId;
        this.config = config;
        this.metadata = metadata;
        this.controlPlane = controlPlane;
        this.objectKeyCreator = objectKeyCreator;
        this.keyAlignmentStrategy = keyAlignmentStrategy;
        this.cache = cache;
        this.batchCoordinateCache = batchCoordinateCache;
        this.brokerTopicStats = brokerTopicStats;
        this.defaultTopicConfigs = defaultTopicConfigs;
        this.bufferPool = bufferPool;
        this.bufferPoolMetrics = bufferPoolMetrics;
        this.bufferPoolMinSizeBytes = bufferPoolMinSizeBytes;

        final MetricsReporter reporter = new JmxReporter();
        this.storageMetrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(STORAGE_METRIC_CONTEXT)
        );
    }

    /**
     * Result of buffer pool initialization containing the pool, metrics, and min size threshold.
     */
    private record BufferPoolInitResult(
        ElasticBufferPool pool,
        BufferPoolMetrics metrics,
        int minSizeBytes
    ) {
        static final BufferPoolInitResult DISABLED = new BufferPoolInitResult(null, null, 0);
    }

    /**
     * Creates and initializes the buffer pool based on configuration.
     *
     * @param config the Inkless configuration
     * @return the initialization result containing pool, metrics, and min size threshold
     */
    private static BufferPoolInitResult initializeBufferPool(InklessConfig config) {
        if (!config.isProduceBufferPoolEnabled()) {
            return BufferPoolInitResult.DISABLED;
        }

        int buffersPerClass = config.produceBufferPoolSizePerClass();
        int minPoolSizeBytes = config.produceBufferPoolMinSizeBytes();

        // Calculate total pool memory for logging
        long totalPoolSizeMb = 0;
        for (int sizeClassMb : BUFFER_POOL_SIZE_CLASSES) {
            totalPoolSizeMb += (long) sizeClassMb * buffersPerClass;
        }

        // Warn if buffer count is very high
        if (buffersPerClass > BUFFER_POOL_HIGH_COUNT_THRESHOLD) {
            log.warn("Buffer pool configured with {} buffers per size class. "
                + "This allocates {}MB of heap memory. Consider reducing if memory constrained.",
                buffersPerClass, totalPoolSizeMb);
        }

        // Determine prewarm count: -1 means all, 0 means lazy, positive is exact count
        int configuredPrewarm = config.produceBufferPoolPrewarmCount();
        int prewarmCount;
        if (configuredPrewarm == -1) {
            prewarmCount = buffersPerClass;  // Pre-warm all
        } else {
            prewarmCount = Math.min(configuredPrewarm, buffersPerClass);  // Clamp to max
        }

        log.info("Initializing elastic buffer pool: {} buffers per size class, {}MB max memory, minSize={}KB, prewarm={}.",
            buffersPerClass, totalPoolSizeMb, minPoolSizeBytes / 1024, prewarmCount);

        ElasticBufferPool pool = new ElasticBufferPool(buffersPerClass, prewarmCount);
        BufferPoolMetrics metrics = new BufferPoolMetrics(pool);
        return new BufferPoolInitResult(pool, metrics, minPoolSizeBytes);
    }

    public static SharedState initialize(
        Time time,
        int brokerId,
        InklessConfig config,
        MetadataView metadata,
        ControlPlane controlPlane,
        BrokerTopicStats brokerTopicStats,
        Supplier<LogConfig> defaultTopicConfigs
    ) {
        Duration maxTtl = config.fileCleanerRetentionPeriod().dividedBy(2);
        if (config.isBatchCoordinateCacheEnabled() && config.batchCoordinateCacheTtl().toMillis() > maxTtl.toMillis()) {
            throw new IllegalArgumentException(
                "Value of consume.batch.coordinate.cache.ttl.ms exceeds file.cleaner.retention.period.ms / 2"
            );
        }

        BufferPoolInitResult bufferPoolResult = initializeBufferPool(config);

        return new SharedState(
            time,
            brokerId,
            config,
            metadata,
            controlPlane,
            ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked()),
            new FixedBlockAlignment(config.fetchCacheBlockBytes()),
            new CaffeineCache(
                config.cacheMaxCount(),
                config.cacheExpirationLifespanSec(),
                config.cacheExpirationMaxIdleSec()
            ),
            config.isBatchCoordinateCacheEnabled() ? new CaffeineBatchCoordinateCache(config.batchCoordinateCacheTtl()) : new NullBatchCoordinateCache(),
            brokerTopicStats,
            defaultTopicConfigs,
            bufferPoolResult.pool(),
            bufferPoolResult.metrics(),
            bufferPoolResult.minSizeBytes()
        );
    }

    @Override
    public void close() throws IOException {
        try {
            cache.close();
            controlPlane.close();
            storageMetrics.close();
            if (bufferPoolMetrics != null) {
                bufferPoolMetrics.close();
            }
            if (bufferPool != null) {
                bufferPool.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Time time() {
        return time;
    }

    public int brokerId() {
        return brokerId;
    }

    public InklessConfig config() {
        return config;
    }

    public MetadataView metadata() {
        return metadata;
    }

    public ControlPlane controlPlane() {
        return controlPlane;
    }

    public ObjectKeyCreator objectKeyCreator() {
        return objectKeyCreator;
    }

    public KeyAlignmentStrategy keyAlignmentStrategy() {
        return keyAlignmentStrategy;
    }

    public ObjectCache cache() {
        return cache;
    }

    public boolean isBatchCoordinateCacheEnabled() {
        return config.isBatchCoordinateCacheEnabled();
    }
    
    public BatchCoordinateCache batchCoordinateCache() {
        return batchCoordinateCache;
    }

    public BrokerTopicStats brokerTopicStats() {
        return brokerTopicStats;
    }

    public Supplier<LogConfig> defaultTopicConfigs() {
        return defaultTopicConfigs;
    }

    public StorageBackend buildStorage() {
        return config.storage(storageMetrics);
    }

    /**
     * Returns the buffer pool for produce buffers, or null if disabled.
     *
     * @return the buffer pool, or null if {@code produce.buffer.pool.enabled} is false
     */
    public BufferPool bufferPool() {
        return bufferPool;
    }

    /**
     * Returns the minimum buffer size in bytes to use the pool.
     * Smaller buffers use heap allocation directly.
     *
     * @return min pool size threshold in bytes, or 0 if pool is disabled
     */
    public int bufferPoolMinSizeBytes() {
        return bufferPoolMinSizeBytes;
    }
}
