package io.aiven.inkless.cache;

import io.aiven.inkless.common.metrics.SensorProvider;
import io.aiven.inkless.common.metrics.MeasurableValue;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.function.Supplier;

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

public class InfinispanCacheMetrics {
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

    public InfinispanCacheMetrics(final InfinispanCache cache, final long maxCacheSize) {
        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );

        final var metricsRegistry = new InfinispanCacheMetricsRegistry();
        cacheSizeSensor = registerSensor(metricsRegistry.cacheSizeMetricName, CACHE_SIZE, cache::size);
        cacheMaxInMemoryEntriesSensor = registerSensor(metricsRegistry.cacheMaxInMemoryEntries, CACHE_MAX_IN_MEMORY_ENTRIES, () -> maxCacheSize);
        approxCacheEntriesSensor = registerSensor(metricsRegistry.approxCacheEntriesMetricName, APPROX_CACHE_ENTRIES, () -> cache.metrics().getApproximateEntries());
        approxCacheEntriesInMemorySensor = registerSensor(metricsRegistry.approxCacheEntriesInMemoryMetricName, APPROX_CACHE_ENTRIES_IN_MEMORY, () -> cache.metrics().getApproximateEntriesInMemory());
        approxCacheEntriesUniqueSensor = registerSensor(metricsRegistry.approxCacheEntriesUniqueMetricName, APPROX_CACHE_ENTRIES_UNIQUE, () -> cache.metrics().getApproximateEntriesUnique());
        cacheHitsSensor = registerSensor(metricsRegistry.cacheHitsMetricName, CACHE_HITS, () -> cache.metrics().getHits());
        cacheMissesSensor = registerSensor(metricsRegistry.cacheMissesMetricName, CACHE_MISSES, () -> cache.metrics().getMisses());
        avgReadTimeSensor = registerSensor(metricsRegistry.avgReadTimeMetricName, AVG_READ_TIME, () -> cache.metrics().getAverageReadTime());
        cacheDataMemoryUsedSensor = registerSensor(metricsRegistry.cacheDataMemoryUsedMetricName, CACHE_DATA_MEMORY_USED, () -> cache.metrics().getDataMemoryUsed());
        cacheOffHeapMemoryUsedSensor = registerSensor(metricsRegistry.cacheOffHeapMemoryUsedMetricName, CACHE_OFF_HEAP_MEMORY_USED, () -> cache.metrics().getOffHeapMemoryUsed());
        cacheEvictionsSensor = registerSensor(metricsRegistry.cacheEvictionsMetricName, CACHE_EVICTIONS, () -> cache.metrics().getEvictions());
        cacheRemoveHitsSensor = registerSensor(metricsRegistry.cacheRemoveHitsMetricName, CACHE_REMOVE_HITS, () -> cache.metrics().getRemoveHits());
        cacheRemoveMissesSensor = registerSensor(metricsRegistry.cacheRemoveMissesMetricName, CACHE_REMOVE_MISSES, () -> cache.metrics().getRemoveMisses());
    }


    Sensor registerSensor(final MetricNameTemplate metricName, final String sensorName, final Supplier<Long> supplier) {
        return new SensorProvider(metrics, sensorName)
            .with(metricName, new MeasurableValue(supplier))
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
}
