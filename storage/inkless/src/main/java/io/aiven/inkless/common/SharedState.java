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
import io.aiven.inkless.cache.TieredObjectCache;
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
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
    private final BatchCoordinateCache batchCoordinateCache;
    private final BrokerTopicStats brokerTopicStats;
    private final Supplier<LogConfig> defaultTopicConfigs;
    private final Metrics storageMetrics;

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

        final MetricsReporter reporter = new JmxReporter();
        this.storageMetrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(STORAGE_METRIC_CONTEXT)
        );
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

        final ObjectCache objectCache = createObjectCache(config, time);

        return new SharedState(
            time,
            brokerId,
            config,
            metadata,
            controlPlane,
            ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked()),
            new FixedBlockAlignment(config.fetchCacheBlockBytes()),
            objectCache,
            config.isBatchCoordinateCacheEnabled() ? new CaffeineBatchCoordinateCache(config.batchCoordinateCacheTtl()) : new NullBatchCoordinateCache(),
            brokerTopicStats,
            defaultTopicConfigs
        );
    }

    private static ObjectCache createObjectCache(final InklessConfig config, final Time time) {
        final CaffeineCache hotCache = new CaffeineCache(
            config.cacheMaxCount(),
            config.cacheExpirationLifespanSec(),
            config.cacheExpirationMaxIdleSec()
        );

        if (!config.isLaggingCacheEnabled()) {
            return hotCache;
        }

        final CaffeineCache laggingCache = new CaffeineCache(
            config.laggingCacheMaxCount(),
            config.laggingCacheTtlSec(),
            -1  // No idle expiration for lagging cache
        );

        final long hotCacheTtlMs = config.cacheExpirationLifespanSec() * 1000L;
        final long rateLimitBytesPerSec = config.laggingCacheRateLimitBytesPerSec();

        return new TieredObjectCache(
            hotCache,
            laggingCache,
            hotCacheTtlMs,
            time,
            rateLimitBytesPerSec,
            TieredObjectCache.TieredCacheMetrics.NOOP
        );
    }

    @Override
    public void close() throws IOException {
        try {
            cache.close();
            controlPlane.close();
            storageMetrics.close();
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
}
