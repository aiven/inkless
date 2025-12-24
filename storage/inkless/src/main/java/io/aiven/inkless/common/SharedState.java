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
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.CaffeineBatchCoordinateCache;
import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullBatchCoordinateCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.metrics.ThreadPoolMonitor;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

public final class SharedState implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SharedState.class);

    public static final String STORAGE_METRIC_CONTEXT = "io.aiven.inkless.storage";

    private final Time time;
    private final int brokerId;
    private final InklessConfig config;
    private final MetadataView metadata;
    private final ControlPlane controlPlane;
    private final StorageBackend fetchStorage;
    private final StorageBackend produceStorage;
    private final StorageBackend backgroundStorage;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
    private final BatchCoordinateCache batchCoordinateCache;
    private final BrokerTopicStats brokerTopicStats;
    private final Supplier<LogConfig> defaultTopicConfigs;
    private final Metrics storageMetrics;
    private final ExecutorService fetchMetadataExecutor;
    private final ExecutorService fetchDataExecutor;
    private final ScheduledExecutorService produceCommitTickScheduler;
    private final ExecutorService produceUploadExecutor;
    private final ExecutorService produceCommitExecutor;

    private final ThreadPoolMonitor fetchMetadataThreadPoolMonitor;
    private final ThreadPoolMonitor fetchDataThreadPoolMonitor;
    private final ThreadPoolMonitor produceUploaderThreadPoolMonitor;

    SharedState(
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
        final ExecutorService fetchMetadataExecutor,
        final ExecutorService fetchDataExecutor,
        final ScheduledExecutorService produceCommitTickScheduler,
        final ExecutorService produceUploadExecutor,
        final ExecutorService produceCommitExecutor,
        final ThreadPoolMonitor fetchMetadataThreadPoolMonitor,
        final ThreadPoolMonitor fetchDataThreadPoolMonitor,
        final ThreadPoolMonitor produceUploaderThreadPoolMonitor
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
        this.fetchMetadataExecutor = fetchMetadataExecutor;
        this.fetchDataExecutor = fetchDataExecutor;
        this.produceCommitTickScheduler = produceCommitTickScheduler;
        this.produceUploadExecutor = produceUploadExecutor;
        this.produceCommitExecutor = produceCommitExecutor;
        this.fetchMetadataThreadPoolMonitor = fetchMetadataThreadPoolMonitor;
        this.fetchDataThreadPoolMonitor = fetchDataThreadPoolMonitor;
        this.produceUploaderThreadPoolMonitor = produceUploaderThreadPoolMonitor;

        final MetricsReporter reporter = new JmxReporter();
        this.storageMetrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(STORAGE_METRIC_CONTEXT)
        );
        this.fetchStorage = config.storage(storageMetrics);
        this.produceStorage = config.storage(storageMetrics);
        this.backgroundStorage = config.storage(storageMetrics);
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

        // Track created executors to clean up on failure
        ExecutorService fetchMetadataExecutor = null;
        ExecutorService fetchDataExecutor = null;
        ScheduledExecutorService commitTickScheduler = null;
        ExecutorService produceUploadExecutor = null;
        ExecutorService produceCommitExecutor = null;
        ThreadPoolMonitor fetchMetadataThreadPoolMonitor = null;
        ThreadPoolMonitor fetchDataThreadPoolMonitor = null;
        ThreadPoolMonitor produceUploaderThreadPoolMonitor = null;

        CaffeineCache objectCache = null;
        BatchCoordinateCache batchMetadataCache = null;

        try {
            // Create dedicated thread pools for fetch operations (metadata and data)
            fetchMetadataExecutor = Executors.newFixedThreadPool(
                config.fetchMetadataThreadPoolSize(),
                new InklessThreadFactory("inkless-fetch-metadata-", false)
            );

            fetchDataExecutor = Executors.newFixedThreadPool(
                config.fetchDataThreadPoolSize(),
                new InklessThreadFactory("inkless-fetch-data-", false)
            );

            // Create scheduled executor for commit interval timer (triggers commits in Writer)
            commitTickScheduler = Executors.newScheduledThreadPool(
                1,
                new InklessThreadFactory("inkless-produce-commit-ticker-", true)
            );

            // Create thread pools for file operations (upload and commit to control plane)
            produceUploadExecutor = Executors.newFixedThreadPool(
                config.produceUploadThreadPoolSize(),
                new InklessThreadFactory("inkless-produce-uploader-", false)
            );

            // Single thread executor for sequential commits (preserves ordering)
            // Note: We use newFixedThreadPool(1) instead of newSingleThreadExecutor() to allow
            // validation at construction time in FileCommitter constructor.
            // Both create single-threaded executors, but newSingleThreadExecutor() returns a
            // DelegatedExecutorService wrapper that would require fragile reflection-based introspection to validate.
            // newFixedThreadPool(1) returns a ThreadPoolExecutor directly, enabling simple instanceof checks.
            // Tradeoff: We lose the immutability wrapper that prevents pool size reconfiguration, but
            // this is acceptable because:
            // (1) we control executor creation and lifecycle,
            // (2) the executor is not exposed to external code, and
            // (3) validation prevents the real bug (multithreaded executor causing ordering violations)
            // compared to a theoretical misconfiguration that would be immediately obvious.
            produceCommitExecutor = Executors.newFixedThreadPool(
                1,
                new InklessThreadFactory("inkless-produce-committer-", false)
            );

            // Create thread pool monitors - must be created before SharedState constructor
            // so they're in scope for exception handling if construction fails
            fetchMetadataThreadPoolMonitor = new ThreadPoolMonitor("inkless-fetch-metadata", fetchMetadataExecutor);
            fetchDataThreadPoolMonitor = new ThreadPoolMonitor("inkless-fetch-data", fetchDataExecutor);
            produceUploaderThreadPoolMonitor = new ThreadPoolMonitor("inkless-produce-uploader", produceUploadExecutor);

            objectCache = new CaffeineCache(
                config.cacheMaxCount(),
                config.cacheExpirationLifespanSec(),
                config.cacheExpirationMaxIdleSec()
            );
            batchMetadataCache = config.isBatchCoordinateCacheEnabled()
                ? new CaffeineBatchCoordinateCache(config.batchCoordinateCacheTtl())
                : new NullBatchCoordinateCache();

            return new SharedState(
                time,
                brokerId,
                config,
                metadata,
                controlPlane,
                ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked()),
                new FixedBlockAlignment(config.fetchCacheBlockBytes()),
                objectCache,
                batchMetadataCache,
                brokerTopicStats,
                defaultTopicConfigs,
                fetchMetadataExecutor,
                fetchDataExecutor,
                commitTickScheduler,
                produceUploadExecutor,
                produceCommitExecutor,
                fetchMetadataThreadPoolMonitor,
                fetchDataThreadPoolMonitor,
                produceUploaderThreadPoolMonitor
            );
        } catch (Exception e) {
            // Clean up any executors and monitors that were created before the failure
            LOGGER.error("Failed to initialize SharedState, shutting down any created executors", e);
            cleanupExecutorsAndMonitors(
                fetchMetadataExecutor,
                fetchDataExecutor,
                commitTickScheduler,
                produceUploadExecutor,
                produceCommitExecutor,
                fetchMetadataThreadPoolMonitor,
                fetchDataThreadPoolMonitor,
                produceUploaderThreadPoolMonitor
            );
            // Also close caches that may have been created before the failure
            Utils.closeQuietly(objectCache, "objectCache", LOGGER);
            Utils.closeQuietly(batchMetadataCache, "batchMetadataCache", LOGGER);
            throw new RuntimeException("Failed to initialize SharedState", e);
        }
    }

    /**
     * Cleanup helper for shutting down executors and closing monitors.
     * Safe to call with null references - all utility methods handle null gracefully.
     */
    private static void cleanupExecutorsAndMonitors(
        ExecutorService fetchMetadataExecutor,
        ExecutorService fetchDataExecutor,
        ScheduledExecutorService commitTickScheduler,
        ExecutorService produceUploadExecutor,
        ExecutorService produceCommitExecutor,
        ThreadPoolMonitor fetchMetadataThreadPoolMonitor,
        ThreadPoolMonitor fetchDataThreadPoolMonitor,
        ThreadPoolMonitor produceUploaderThreadPoolMonitor
    ) {
        // Shutdown executors - write path first, then read path
        ThreadUtils.shutdownExecutorServiceQuietly(commitTickScheduler, 5, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(produceUploadExecutor, 5, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(produceCommitExecutor, 5, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(fetchMetadataExecutor, 5, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(fetchDataExecutor, 5, TimeUnit.SECONDS);

        // Close monitors
        Utils.closeQuietly(fetchMetadataThreadPoolMonitor, "fetchMetadataThreadPoolMonitor", LOGGER);
        Utils.closeQuietly(fetchDataThreadPoolMonitor, "fetchDataThreadPoolMonitor", LOGGER);
        Utils.closeQuietly(produceUploaderThreadPoolMonitor, "produceUploaderThreadPoolMonitor", LOGGER);
    }

    @Override
    public void close() throws IOException {
        // Shutdown executors and close monitors
        cleanupExecutorsAndMonitors(
            fetchMetadataExecutor,
            fetchDataExecutor,
            produceCommitTickScheduler,
            produceUploadExecutor,
            produceCommitExecutor,
            fetchMetadataThreadPoolMonitor,
            fetchDataThreadPoolMonitor,
            produceUploaderThreadPoolMonitor
        );

        // Close remaining resources
        Utils.closeQuietly(cache, "cache", LOGGER);
        Utils.closeQuietly(batchCoordinateCache, "batchCoordinateCache", LOGGER);
        Utils.closeQuietly(controlPlane, "controlPlane", LOGGER);
        Utils.closeQuietly(fetchStorage, "fetchStorage", LOGGER);
        Utils.closeQuietly(produceStorage, "produceStorage", LOGGER);
        Utils.closeQuietly(backgroundStorage, "backgroundStorage", LOGGER);
        Utils.closeQuietly(storageMetrics, "storageMetrics", LOGGER);
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

    public StorageBackend produceStorage() {
        return produceStorage;
    }

    public StorageBackend fetchStorage() {
        return fetchStorage;
    }

    public StorageBackend backgroundStorage() {
        return backgroundStorage;
    }

    public ExecutorService fetchMetadataExecutor() {
        return fetchMetadataExecutor;
    }

    public ExecutorService fetchDataExecutor() {
        return fetchDataExecutor;
    }

    public ScheduledExecutorService produceCommitTickScheduler() {
        return produceCommitTickScheduler;
    }

    public ExecutorService produceUploadExecutor() {
        return produceUploadExecutor;
    }

    public ExecutorService produceCommitExecutor() {
        return produceCommitExecutor;
    }
}
