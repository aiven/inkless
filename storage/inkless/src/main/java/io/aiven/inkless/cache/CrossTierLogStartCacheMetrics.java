/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * JMX metrics for the read-through {@link CrossTierLogStartCache}. A hit is served entirely from the
 * cache without a control-plane {@code listOffsets} call; a miss falls through to the control plane
 * and (on success) populates the cache.
 */
public final class CrossTierLogStartCacheMetrics implements Closeable {
    private static final String GROUP = CrossTierLogStartCache.class.getSimpleName();

    static final String CACHE_HITS = "CacheHits";
    private static final String CACHE_HITS_DOC =
        "Total number of cross-tier log start cache hits served without a control-plane call";
    static final String CACHE_MISSES = "CacheMisses";
    private static final String CACHE_MISSES_DOC =
        "Total number of cross-tier log start cache misses that fell through to the control plane";
    static final String CACHE_SIZE = "CacheSize";
    private static final String CACHE_SIZE_DOC =
        "Current number of partitions cached for the cross-tier log start offset";

    /**
     * This method returns a list of all the metric name templates for the CrossTierLogStartCacheMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(CACHE_HITS, GROUP, CACHE_HITS_DOC),
            new MetricNameTemplate(CACHE_MISSES, GROUP, CACHE_MISSES_DOC),
            new MetricNameTemplate(CACHE_SIZE, GROUP, CACHE_SIZE_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        CrossTierLogStartCache.class.getPackageName(), CrossTierLogStartCache.class.getSimpleName());
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();

    public CrossTierLogStartCacheMetrics(final Supplier<Long> sizeSupplier) {
        metricsGroup.newGauge(CACHE_HITS, cacheHits::intValue);
        metricsGroup.newGauge(CACHE_MISSES, cacheMisses::intValue);
        metricsGroup.newGauge(CACHE_SIZE, () -> sizeSupplier.get().intValue());
    }

    public void recordCacheHit() {
        cacheHits.increment();
    }

    public void recordCacheMiss() {
        cacheMisses.increment();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CACHE_HITS);
        metricsGroup.removeMetric(CACHE_MISSES);
        metricsGroup.removeMetric(CACHE_SIZE);
    }
}
