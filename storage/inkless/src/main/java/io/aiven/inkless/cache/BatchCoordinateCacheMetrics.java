package io.aiven.inkless.cache;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;


public final class BatchCoordinateCacheMetrics implements Closeable {
    private static final String GROUP = BatchCoordinateCache.class.getSimpleName();

    static final String CACHE_HITS = "CacheHits";
    private static final String CACHE_HITS_DOC = "Total number of batch coordinate cache hits";
    static final String CACHE_HITS_WITHOUT_DATA = "CacheHitsWithoutData";
    private static final String CACHE_HITS_WITHOUT_DATA_DOC = "Total number of cache hits where coordinate was found but data was not available";
    static final String CACHE_MISSES = "CacheMisses";
    private static final String CACHE_MISSES_DOC = "Total number of batch coordinate cache misses";
    static final String CACHE_INVALIDATIONS = "CacheInvalidations";
    private static final String CACHE_INVALIDATIONS_DOC = "Total number of cache entry invalidations";
    static final String CACHE_EVICTIONS = "CacheEvictions";
    private static final String CACHE_EVICTIONS_DOC = "Total number of cache entry evictions";
    static final String CACHE_SIZE = "CacheSize";
    private static final String CACHE_SIZE_DOC = "Current number of entries in the batch coordinate cache";

    /**
     * This method returns a list of all the metric name templates for the BatchCoordinateCacheMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(CACHE_HITS, GROUP, CACHE_HITS_DOC),
            new MetricNameTemplate(CACHE_HITS_WITHOUT_DATA, GROUP, CACHE_HITS_WITHOUT_DATA_DOC),
            new MetricNameTemplate(CACHE_MISSES, GROUP, CACHE_MISSES_DOC),
            new MetricNameTemplate(CACHE_INVALIDATIONS, GROUP, CACHE_INVALIDATIONS_DOC),
            new MetricNameTemplate(CACHE_EVICTIONS, GROUP, CACHE_EVICTIONS_DOC),
            new MetricNameTemplate(CACHE_SIZE, GROUP, CACHE_SIZE_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        BatchCoordinateCache.class.getPackageName(), BatchCoordinateCache.class.getSimpleName());
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheHitsWithoutData = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();
    private final LongAdder cacheInvalidations = new LongAdder();
    private final LongAdder cacheEvictions = new LongAdder();
    private final LongAdder cacheSize = new LongAdder();

    public BatchCoordinateCacheMetrics() {
        metricsGroup.newGauge(CACHE_HITS, cacheHits::intValue);
        metricsGroup.newGauge(CACHE_HITS_WITHOUT_DATA, cacheHitsWithoutData::intValue);
        metricsGroup.newGauge(CACHE_MISSES, cacheMisses::intValue);
        metricsGroup.newGauge(CACHE_INVALIDATIONS, cacheInvalidations::intValue);
        metricsGroup.newGauge(CACHE_EVICTIONS, cacheEvictions::intValue);
        metricsGroup.newGauge(CACHE_SIZE, cacheSize::intValue);
    }

    public void recordCacheHit() {
        cacheHits.increment();
    }

    public void recordCacheHitWithoutData() {
        cacheHitsWithoutData.increment();
    }


    public void recordCacheMiss() {
        cacheMisses.increment();
    }

    public void recordCacheInvalidation() {
        cacheInvalidations.increment();
    }

    public void recordCacheEviction() {
        cacheEvictions.increment();
    }

    public void incrementCacheSize() {
        cacheSize.increment();
    }

    public void decreaseCacheSize(int removedEntries) {
        cacheSize.add(-removedEntries);
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CACHE_HITS);
        metricsGroup.removeMetric(CACHE_HITS_WITHOUT_DATA);
        metricsGroup.removeMetric(CACHE_MISSES);
        metricsGroup.removeMetric(CACHE_INVALIDATIONS);
        metricsGroup.removeMetric(CACHE_EVICTIONS);
        metricsGroup.removeMetric(CACHE_SIZE);
    }
}
