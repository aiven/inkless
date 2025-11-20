package io.aiven.inkless.cache;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import com.github.benmanes.caffeine.cache.Cache;

import java.util.function.Supplier;

import io.aiven.inkless.common.metrics.MeasurableValue;
import io.aiven.inkless.common.metrics.SensorProvider;

public final class CaffeineCacheMetrics {
    private final Sensor cacheSizeSensor;
    private final Sensor cacheHitCountSensor;
    private final Sensor cacheHitRateSensor;
    private final Sensor cacheMissCountSensor;
    private final Sensor cacheMissRateSensor;
    private final Sensor cacheAvgLoadPenaltySensor;
    private final Sensor cacheEvictionsSensor;

    public CaffeineCacheMetrics(final Metrics metrics, final Cache<?, ?> cache) {
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
                "cacheSizeSensor=" + cacheSizeSensor +
                ", cacheHitCountSensor=" + cacheHitCountSensor +
                ", cacheHitRateSensor=" + cacheHitRateSensor +
                ", cacheMissCountSensor=" + cacheMissCountSensor +
                ", cacheMissRateSensor=" + cacheMissRateSensor +
                ", cacheAvgLoadPenaltySensor=" + cacheAvgLoadPenaltySensor +
                ", cacheEvictionsSensor=" + cacheEvictionsSensor +
                '}';
    }
}
