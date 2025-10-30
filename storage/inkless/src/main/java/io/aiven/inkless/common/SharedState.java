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
import java.util.List;
import java.util.function.Supplier;

import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
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
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
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
            brokerTopicStats,
            defaultTopicConfigs
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
