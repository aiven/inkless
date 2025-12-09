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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.concurrent.atomic.LongAdder;


public final class BatchCoordinateCacheMetrics implements Closeable {
    static final String CACHE_HITS = "CacheHits";
    static final String CACHE_HITS_WITHOUT_DATA = "CacheHitsWithoutData";
    static final String CACHE_MISSES = "CacheMisses";
    static final String CACHE_INVALIDATIONS = "CacheInvalidations";
    static final String CACHE_EVICTIONS = "CacheEvictions";
    static final String CACHE_SIZE = "CacheSize";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(BatchCoordinateCache.class);
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
