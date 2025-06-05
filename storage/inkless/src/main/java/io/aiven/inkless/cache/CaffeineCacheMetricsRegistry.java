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

public class CaffeineCacheMetricsRegistry {
    public static final String METRIC_CONTEXT = "io.aiven.inkless.cache.caffeine";
//    public static final String METRIC_GROUP = "wal-segment-cache";

    public static final String CACHE_SIZE = "cache-size";
    public static final String CACHE_HIT_RATE = "cache-hits-rate";
    public static final String CACHE_HIT_COUNT = "cache-hit-count";
    public static final String CACHE_MISS_RATE = "cache-miss-rate";
    public static final String CACHE_MISS_COUNT = "cache-miss-count";
    public static final String CACHE_AVG_LOAD_PENALTY_NANOSECONDS = "avg-load-penalty-ns";
    public static final String CACHE_EVICTION_COUNT = "cache-evictions-count";

    public final MetricNameTemplate cacheSizeMetricName;
    public final MetricNameTemplate cacheHitRateMetricName;
    public final MetricNameTemplate cacheHitCountMetricName;
    public final MetricNameTemplate cacheMissRateMetricName;
    public final MetricNameTemplate cacheMissCountMetricName;
    public final MetricNameTemplate avgReadTimeMetricName;
    public final MetricNameTemplate cacheEvictionsMetricName;

    public CaffeineCacheMetricsRegistry(String metricGroup) {
        cacheSizeMetricName = new MetricNameTemplate(
                CACHE_SIZE,
                metricGroup,
                "Current size of the cache"
        );
        cacheHitRateMetricName = new MetricNameTemplate(
                CACHE_HIT_RATE,
                metricGroup,
                "Cache hit rate"
        );

        cacheHitCountMetricName = new MetricNameTemplate(
                CACHE_HIT_COUNT,
                metricGroup,
                "Number of cache hits"
        );
        cacheMissRateMetricName = new MetricNameTemplate(
                CACHE_MISS_RATE,
                metricGroup,
                "Cache miss rate"
        );

        cacheMissCountMetricName = new MetricNameTemplate(
                CACHE_MISS_COUNT,
                metricGroup,
                "Number of cache misses"
        );
        avgReadTimeMetricName = new MetricNameTemplate(
                CACHE_AVG_LOAD_PENALTY_NANOSECONDS,
                metricGroup,
                "Average cache load penalty in nanoseconds"
        );
        cacheEvictionsMetricName = new MetricNameTemplate(
                CACHE_EVICTION_COUNT,
                metricGroup,
                "Number of evictions from the cache"
        );
    }
}