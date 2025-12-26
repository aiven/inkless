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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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
import io.aiven.inkless.storage_backend.common.StorageBackend;

public final class SharedState implements Closeable {
    public static final String STORAGE_METRIC_CONTEXT = "io.aiven.inkless.storage";

    private final Time time;
    private final int brokerId;
    private final InklessConfig config;
    private final MetadataView metadata;
    private final ControlPlane controlPlane;
    private final StorageBackend fetchStorage;
    // Separate storage client for lagging consumers to:
    // 1. Isolate connection pool usage (lagging consumers shouldn't exhaust connections for hot path)
    // 2. Allow independent tuning of timeouts/retries for cold storage access patterns
    //    (This requires some refactoring on how the storage client is built/configured)
    private final Optional<StorageBackend> maybeLaggingFetchStorage;
    private final StorageBackend produceStorage;
    // backgroundStorage is shared by FileCleaner and FileMerger executors which run concurrently.
    // Kafka storage backends are required to be thread-safe (they share the same Metrics instance).
    // However, these tasks perform high-latency object storage calls and retries. A dedicated backend
    // instance guarantees they don't contend with hot-path fetch/produce clients and prevents threading/double-close issues.
    private final StorageBackend backgroundStorage;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
    private final BatchCoordinateCache batchCoordinateCache;
    private final BrokerTopicStats brokerTopicStats;
    private final Supplier<LogConfig> defaultTopicConfigs;
    private final Metrics storageMetrics;

    private SharedState(
        final Time time,
        final int brokerId,
        final InklessConfig config,
        final MetadataView metadata,
        final ControlPlane controlPlane,
        final StorageBackend fetchStorage,
        final Optional<StorageBackend> maybeLaggingFetchStorage,
        final StorageBackend produceStorage,
        final StorageBackend backgroundStorage,
        final Metrics storageMetrics,
        final ObjectKeyCreator objectKeyCreator,
        final KeyAlignmentStrategy keyAlignmentStrategy,
        final ObjectCache cache,
        final BatchCoordinateCache batchCoordinateCache,
        final BrokerTopicStats brokerTopicStats,
        final Supplier<LogConfig> defaultTopicConfigs
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
        this.fetchStorage = fetchStorage;
        this.maybeLaggingFetchStorage = maybeLaggingFetchStorage;
        this.produceStorage = produceStorage;
        this.backgroundStorage = backgroundStorage;
        this.storageMetrics = storageMetrics;
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

        CaffeineCache objectCache = null;
        BatchCoordinateCache batchCoordinateCache = null;
        StorageBackend fetchStorage = null;
        StorageBackend laggingFetchStorage = null;
        StorageBackend produceStorage = null;
        StorageBackend backgroundStorage = null;
        Metrics storageMetrics = null;
        try {
            objectCache = new CaffeineCache(
                config.cacheMaxCount(),
                config.cacheExpirationLifespanSec(),
                config.cacheExpirationMaxIdleSec()
            );
            batchCoordinateCache = config.isBatchCoordinateCacheEnabled()
                ? new CaffeineBatchCoordinateCache(config.batchCoordinateCacheTtl())
                : new NullBatchCoordinateCache();

            final MetricsReporter reporter = new JmxReporter();
            storageMetrics = new Metrics(
                new MetricConfig(), List.of(reporter), Time.SYSTEM,
                new KafkaMetricsContext(STORAGE_METRIC_CONTEXT)
            );
            fetchStorage = config.storage(storageMetrics);
            // If thread pool size is 0, disabling lagging consumer support, don't create a separate client
            //
            // NOTE: The client for lagging consumers is created only when this SharedState
            // is constructed. If fetchLaggingConsumerThreadPoolSize() is 0 at this time, no separate
            // client is created and lagging consumer support is effectively disabled for the lifetime
            // of this instance, even if the configuration is later reloaded with a non-zero value.
            // Enabling lagging consumer support therefore requires a broker restart (or reconstruction
            // of the SharedState) so that a new storage client can be created.
            laggingFetchStorage = config.fetchLaggingConsumerThreadPoolSize() > 0 ? config.storage(storageMetrics) : null;
            produceStorage = config.storage(storageMetrics);
            backgroundStorage = config.storage(storageMetrics);
            final var objectKeyCreator = ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked());
            final var keyAlignmentStrategy = new FixedBlockAlignment(config.fetchCacheBlockBytes());
            return new SharedState(
                time,
                brokerId,
                config,
                metadata,
                controlPlane,
                fetchStorage,
                Optional.ofNullable(laggingFetchStorage),
                produceStorage,
                backgroundStorage,
                storageMetrics,
                objectKeyCreator,
                keyAlignmentStrategy,
                objectCache,
                batchCoordinateCache,
                brokerTopicStats,
                defaultTopicConfigs
            );
        } catch (Exception e) {
            // Closing storage backends
            Utils.closeQuietly(backgroundStorage, "backgroundStorage");
            Utils.closeQuietly(produceStorage, "produceStorage");
            Utils.closeQuietly(fetchStorage, "fetchStorage");
            Utils.closeQuietly(laggingFetchStorage, "laggingFetchStorage");
            // Closing storage metrics
            Utils.closeQuietly(storageMetrics, "storageMetrics");
            // Closing caches
            Utils.closeQuietly(batchCoordinateCache, "batchCoordinateCache");
            Utils.closeQuietly(objectCache, "objectCache");

            throw new RuntimeException("Failed to initialize SharedState", e);
        }
    }

    @Override
    public void close() throws IOException {
        // Closing storage backends
        Utils.closeQuietly(backgroundStorage, "backgroundStorage");
        Utils.closeQuietly(produceStorage, "produceStorage");
        maybeLaggingFetchStorage.ifPresent(s -> Utils.closeQuietly(s, "laggingFetchStorage"));
        Utils.closeQuietly(fetchStorage, "fetchStorage");
        // Closing storage metrics
        Utils.closeQuietly(storageMetrics, "storageMetrics");
        // Closing caches
        Utils.closeQuietly(cache, "objectCache");
        Utils.closeQuietly(batchCoordinateCache, "batchCoordinateCache");
        // Closing control plane
        Utils.closeQuietly(controlPlane, "controlPlane");
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

    public StorageBackend fetchStorage() {
        return fetchStorage;
    }

    /**
     * Optional access to the lagging fetch storage backend.
     *
     * <p>When {@code fetch.lagging.consumer.thread.pool.size == 0}, the lagging consumer
     * path is explicitly disabled and this storage backend is not created.</p>
     */
    public Optional<StorageBackend> maybeLaggingFetchStorage() {
        return maybeLaggingFetchStorage;
    }

    public StorageBackend produceStorage() {
        return produceStorage;
    }

    public StorageBackend backgroundStorage() {
        return backgroundStorage;
    }
}
