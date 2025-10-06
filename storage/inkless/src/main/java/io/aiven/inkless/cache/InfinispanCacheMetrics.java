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
package io.aiven.inkless.cache;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import io.aiven.inkless.common.metrics.MeasurableValue;
import io.aiven.inkless.common.metrics.SensorProvider;

import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.APPROX_CACHE_ENTRIES;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.APPROX_CACHE_ENTRIES_IN_MEMORY;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.APPROX_CACHE_ENTRIES_UNIQUE;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.AVG_READ_TIME;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_DATA_MEMORY_USED;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_EVICTIONS;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_HITS;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_MAX_IN_MEMORY_ENTRIES;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_MISSES;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_OFF_HEAP_MEMORY_USED;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_REMOVE_HITS;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_REMOVE_MISSES;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.CACHE_SIZE;
import static io.aiven.inkless.cache.InfinispanCacheMetricsRegistry.METRIC_CONTEXT;

public class InfinispanCacheMetrics implements Closeable {
    private final Metrics metrics;

    private final Sensor cacheSizeSensor;
    private final Sensor cacheMaxInMemoryEntriesSensor;
    private final Sensor approxCacheEntriesSensor;
    private final Sensor approxCacheEntriesInMemorySensor;
    private final Sensor approxCacheEntriesUniqueSensor;
    private final Sensor cacheHitsSensor;
    private final Sensor cacheMissesSensor;
    private final Sensor avgReadTimeSensor;
    private final Sensor cacheDataMemoryUsedSensor;
    private final Sensor cacheOffHeapMemoryUsedSensor;
    private final Sensor cacheEvictionsSensor;
    private final Sensor cacheRemoveHitsSensor;
    private final Sensor cacheRemoveMissesSensor;

    public InfinispanCacheMetrics(final InfinispanCache cache) {
        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );

        final var metricsRegistry = new InfinispanCacheMetricsRegistry();
        cacheSizeSensor = registerSensor(metrics, metricsRegistry.cacheSizeMetricName, CACHE_SIZE, cache::size);
        cacheMaxInMemoryEntriesSensor = registerSensor(metrics, metricsRegistry.cacheMaxInMemoryEntries, CACHE_MAX_IN_MEMORY_ENTRIES, cache::maxCacheSize);
        approxCacheEntriesSensor = registerSensor(metrics, metricsRegistry.approxCacheEntriesMetricName, APPROX_CACHE_ENTRIES, () -> cache.metrics().getApproximateEntries());
        approxCacheEntriesInMemorySensor = registerSensor(metrics, metricsRegistry.approxCacheEntriesInMemoryMetricName, APPROX_CACHE_ENTRIES_IN_MEMORY, () -> cache.metrics().getApproximateEntriesInMemory());
        approxCacheEntriesUniqueSensor = registerSensor(metrics, metricsRegistry.approxCacheEntriesUniqueMetricName, APPROX_CACHE_ENTRIES_UNIQUE, () -> cache.metrics().getApproximateEntriesUnique());
        cacheHitsSensor = registerSensor(metrics, metricsRegistry.cacheHitsMetricName, CACHE_HITS, () -> cache.metrics().getHits());
        cacheMissesSensor = registerSensor(metrics, metricsRegistry.cacheMissesMetricName, CACHE_MISSES, () -> cache.metrics().getMisses());
        avgReadTimeSensor = registerSensor(metrics, metricsRegistry.avgReadTimeMetricName, AVG_READ_TIME, () -> cache.metrics().getAverageReadTime());
        cacheDataMemoryUsedSensor = registerSensor(metrics, metricsRegistry.cacheDataMemoryUsedMetricName, CACHE_DATA_MEMORY_USED, () -> cache.metrics().getDataMemoryUsed());
        cacheOffHeapMemoryUsedSensor = registerSensor(metrics, metricsRegistry.cacheOffHeapMemoryUsedMetricName, CACHE_OFF_HEAP_MEMORY_USED, () -> cache.metrics().getOffHeapMemoryUsed());
        cacheEvictionsSensor = registerSensor(metrics, metricsRegistry.cacheEvictionsMetricName, CACHE_EVICTIONS, () -> cache.metrics().getEvictions());
        cacheRemoveHitsSensor = registerSensor(metrics, metricsRegistry.cacheRemoveHitsMetricName, CACHE_REMOVE_HITS, () -> cache.metrics().getRemoveHits());
        cacheRemoveMissesSensor = registerSensor(metrics, metricsRegistry.cacheRemoveMissesMetricName, CACHE_REMOVE_MISSES, () -> cache.metrics().getRemoveMisses());
    }

    static Sensor registerSensor(final Metrics metrics, final MetricNameTemplate metricName, final String sensorName, final Supplier<Long> supplier) {
        return new SensorProvider(metrics, sensorName)
            .with(metricName, new MeasurableValue<>(supplier))
            .get();
    }

    @Override
    public String toString() {
        return "InfinispanCacheMetrics{" +
            "metrics=" + metrics +
            ", cacheSizeSensor=" + cacheSizeSensor +
            ", cacheMaxInMemoryEntriesSensor=" + cacheMaxInMemoryEntriesSensor +
            ", approxCacheEntriesSensor=" + approxCacheEntriesSensor +
            ", approxCacheEntriesInMemorySensor=" + approxCacheEntriesInMemorySensor +
            ", approxCacheEntriesUniqueSensor=" + approxCacheEntriesUniqueSensor +
            ", cacheHitsSensor=" + cacheHitsSensor +
            ", cacheMissesSensor=" + cacheMissesSensor +
            ", avgReadTimeSensor=" + avgReadTimeSensor +
            ", cacheDataMemoryUsedSensor=" + cacheDataMemoryUsedSensor +
            ", cacheOffHeapMemoryUsedSensor=" + cacheOffHeapMemoryUsedSensor +
            ", cacheEvictionsSensor=" + cacheEvictionsSensor +
            ", cacheRemoveHitsSensor=" + cacheRemoveHitsSensor +
            ", cacheRemoveMissesSensor=" + cacheRemoveMissesSensor +
            '}';
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }
}
