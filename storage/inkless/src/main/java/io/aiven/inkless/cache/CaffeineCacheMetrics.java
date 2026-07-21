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

import com.github.benmanes.caffeine.cache.Cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import io.aiven.inkless.common.metrics.MeasurableValue;
import io.aiven.inkless.common.metrics.SensorProvider;

public final class CaffeineCacheMetrics implements Closeable {

    private final Metrics metrics;

    private final Sensor cacheSizeSensor;
    private final Sensor cacheHitCountSensor;
    private final Sensor cacheHitRateSensor;
    private final Sensor cacheMissCountSensor;
    private final Sensor cacheMissRateSensor;
    private final Sensor cacheAvgLoadPenaltySensor;
    private final Sensor cacheEvictionsSensor;

    public CaffeineCacheMetrics(final Cache<?, ?> cache) {
        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
                new MetricConfig(), List.of(reporter), Time.SYSTEM,
                new KafkaMetricsContext(CaffeineCacheMetricsRegistry.METRIC_CONTEXT)
        );

        final CaffeineCacheMetricsRegistry metricsRegistry = new CaffeineCacheMetricsRegistry();
        cacheSizeSensor = registerLongSensor(metrics, metricsRegistry.cacheSizeMetricName, CaffeineCacheMetricsRegistry.CACHE_SIZE, cache::estimatedSize);
        cacheHitCountSensor = registerLongSensor(metrics, metricsRegistry.cacheHitCountMetricName, CaffeineCacheMetricsRegistry.CACHE_HIT_COUNT, () -> cache.stats().hitCount());
        cacheHitRateSensor = registerDoubleSensor(metrics, metricsRegistry.cacheHitRateMetricName, CaffeineCacheMetricsRegistry.CACHE_HIT_RATE, () -> cache.stats().hitRate());
        cacheMissCountSensor = registerLongSensor(metrics, metricsRegistry.cacheMissCountMetricName, CaffeineCacheMetricsRegistry.CACHE_MISS_COUNT, () -> cache.stats().missCount());
        cacheMissRateSensor = registerDoubleSensor(metrics, metricsRegistry.cacheMissRateMetricName, CaffeineCacheMetricsRegistry.CACHE_MISS_RATE, () -> cache.stats().missRate());
        cacheAvgLoadPenaltySensor = registerDoubleSensor(metrics, metricsRegistry.avgReadTimeMetricName, CaffeineCacheMetricsRegistry.CACHE_AVG_LOAD_PENALTY_NANOSECONDS, () -> cache.stats().averageLoadPenalty());
        cacheEvictionsSensor = registerLongSensor(metrics, metricsRegistry.cacheEvictionsMetricName, CaffeineCacheMetricsRegistry.CACHE_EVICTION_COUNT, () -> cache.stats().evictionCount());
    }

    static Sensor registerDoubleSensor(final Metrics metrics, final MetricNameTemplate metricName, final String sensorName, final Supplier<Double> supplier) {
        return new SensorProvider(metrics, sensorName)
                .with(metricName, new MeasurableValue<>(supplier))
                .get();
    }

    static Sensor registerLongSensor(final Metrics metrics, final MetricNameTemplate metricName, final String sensorName, final Supplier<Long> supplier) {
        return new SensorProvider(metrics, sensorName)
                .with(metricName, new MeasurableValue<>(supplier))
                .get();
    }

    @Override
    public String toString() {
        return "CaffeineCacheMetrics{" +
                "metrics=" + metrics +
                ", cacheSizeSensor=" + cacheSizeSensor +
                ", cacheHitCountSensor=" + cacheHitCountSensor +
                ", cacheHitRateSensor=" + cacheHitRateSensor +
                ", cacheMissCountSensor=" + cacheMissCountSensor +
                ", cacheMissRateSensor=" + cacheMissRateSensor +
                ", cacheAvgLoadPenaltySensor=" + cacheAvgLoadPenaltySensor +
                ", cacheEvictionsSensor=" + cacheEvictionsSensor +
                '}';
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }
}
