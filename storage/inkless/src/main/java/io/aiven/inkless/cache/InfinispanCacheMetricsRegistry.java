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

public class InfinispanCacheMetricsRegistry {
    public static final String METRIC_CONTEXT = "io.aiven.inkless.cache.infinispan";
    public static final String METRIC_GROUP = "wal-segment-cache";

    public static final String CACHE_SIZE = "cache-size";
    public static final String CACHE_MAX_IN_MEMORY_ENTRIES = "cache-max-in-memory-entries";
    public static final String APPROX_CACHE_ENTRIES = "approx-cache-entries";
    public static final String APPROX_CACHE_ENTRIES_IN_MEMORY = "approx-cache-entries-in-memory";
    public static final String APPROX_CACHE_ENTRIES_UNIQUE = "approx-cache-entries-unique";
    public static final String CACHE_HITS = "cache-hits";
    public static final String CACHE_MISSES = "cache-misses";
    public static final String AVG_READ_TIME = "avg-read-time";
    public static final String CACHE_DATA_MEMORY_USED = "cache-data-memory-used";
    public static final String CACHE_OFF_HEAP_MEMORY_USED = "cache-off-heap-memory-used";
    public static final String CACHE_EVICTIONS = "cache-evictions";
    public static final String CACHE_REMOVE_HITS = "cache-remove-hits";
    public static final String CACHE_REMOVE_MISSES = "cache-remove-misses";

    public MetricNameTemplate cacheSizeMetricName;
    public MetricNameTemplate cacheMaxInMemoryEntries;
    public MetricNameTemplate approxCacheEntriesMetricName;
    public MetricNameTemplate approxCacheEntriesInMemoryMetricName;
    public MetricNameTemplate approxCacheEntriesUniqueMetricName;
    public MetricNameTemplate cacheHitsMetricName;
    public MetricNameTemplate cacheMissesMetricName;
    public MetricNameTemplate avgReadTimeMetricName;
    public MetricNameTemplate cacheDataMemoryUsedMetricName;
    public MetricNameTemplate cacheOffHeapMemoryUsedMetricName;
    public MetricNameTemplate cacheEvictionsMetricName;
    public MetricNameTemplate cacheRemoveHitsMetricName;
    public MetricNameTemplate cacheRemoveMissesMetricName;

    public InfinispanCacheMetricsRegistry() {
        cacheSizeMetricName = new MetricNameTemplate(
            CACHE_SIZE,
            METRIC_GROUP,
            "Current size of the cache"
        );
        cacheMaxInMemoryEntries = new MetricNameTemplate(
            CACHE_MAX_IN_MEMORY_ENTRIES,
            METRIC_GROUP,
            "Maximum number of entries that can be stored in memory"
        );
        approxCacheEntriesMetricName = new MetricNameTemplate(
            APPROX_CACHE_ENTRIES,
            METRIC_GROUP,
            "Approximate number of entries in the cache"
        );
        approxCacheEntriesInMemoryMetricName = new MetricNameTemplate(
            APPROX_CACHE_ENTRIES_IN_MEMORY,
            METRIC_GROUP,
            "Approximate number of entries stored in memory"
        );
        approxCacheEntriesUniqueMetricName = new MetricNameTemplate(
            APPROX_CACHE_ENTRIES_UNIQUE,
            METRIC_GROUP,
            "Approximate number of unique entries in the cache"
        );
        cacheHitsMetricName = new MetricNameTemplate(
            CACHE_HITS,
            METRIC_GROUP,
            "Number of cache hits"
        );
        cacheMissesMetricName = new MetricNameTemplate(
            CACHE_MISSES,
            METRIC_GROUP,
            "Number of cache misses"
        );
        avgReadTimeMetricName = new MetricNameTemplate(
            AVG_READ_TIME,
            METRIC_GROUP,
            "Average time taken to read from the cache"
        );
        cacheDataMemoryUsedMetricName = new MetricNameTemplate(
            CACHE_DATA_MEMORY_USED,
            METRIC_GROUP,
            "Amount of memory used for data storage in the cache"
        );
        cacheOffHeapMemoryUsedMetricName = new MetricNameTemplate(
            CACHE_OFF_HEAP_MEMORY_USED,
            METRIC_GROUP,
            "Amount of off-heap memory used by the cache"
        );
        cacheEvictionsMetricName = new MetricNameTemplate(
            CACHE_EVICTIONS,
            METRIC_GROUP,
            "Number of evictions from the cache"
        );
        cacheRemoveHitsMetricName = new MetricNameTemplate(
            CACHE_REMOVE_HITS,
            METRIC_GROUP,
            "Number of successful remove operations from the cache"
        );
        cacheRemoveMissesMetricName = new MetricNameTemplate(
            CACHE_REMOVE_MISSES,
            METRIC_GROUP,
            "Number of unsuccessful remove operations from the cache"
        );
    }
}
