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

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.awaitility.Awaitility.await;

class CaffeineCacheMetricsTest {

    @Test
    void testCacheHitMissMetric() {
        // Given
        final Metrics metrics = new Metrics();
        final Cache<String, String> cache = Caffeine.newBuilder()
            .recordStats()
            .build();
        new CaffeineCacheMetrics(metrics, cache);
        cache.put("key1", "value1");

        // When there is a cache hit
        var value = cache.getIfPresent("key1"); // hit
        assertThat(value).isEqualTo("value1");

        // Then
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_HIT_COUNT).metricValue())
            .asInstanceOf(DOUBLE)
            .isOne();
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_HIT_RATE).metricValue())
            .asInstanceOf(DOUBLE)
            .isOne();
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_MISS_RATE).metricValue())
            .asInstanceOf(DOUBLE)
            .isZero();

        // When there is a cache miss
        value = cache.getIfPresent("key2"); // miss
        assertThat(value).isNull();

        // Then
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_MISS_COUNT).metricValue())
            .asInstanceOf(DOUBLE)
            .isOne();
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_HIT_RATE).metricValue())
            .asInstanceOf(DOUBLE)
            .isEqualTo(0.5); // 1 hit, 1 miss
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_MISS_RATE).metricValue())
            .asInstanceOf(DOUBLE)
            .isEqualTo(0.5); // 1 hit, 1 miss

        // overall load is zero
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_AVG_LOAD_PENALTY_NANOSECONDS).metricValue())
            .asInstanceOf(DOUBLE)
            .isZero();
    }

    @Test
    void testCacheSizeAndEviction() {
        // Given
        final Metrics metrics = new Metrics();
        final Cache<String, String> cache = Caffeine.newBuilder()
            .maximumSize(1)
            .recordStats()
            .build();
        new CaffeineCacheMetrics(metrics, cache);

        // Initially, cache size should be zero
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_SIZE).metricValue())
            .asInstanceOf(DOUBLE)
            .isZero();

        // When adding entries to the cache
        cache.put("key1", "value1");
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_SIZE).metricValue())
            .asInstanceOf(DOUBLE)
            .isOne();

        // Adding another entry should evict the first one due to max size of 1
        cache.put("key2", "value2"); // This should evict key1
        await().atMost(Duration.ofSeconds(10)).until(() ->
            // size should eventually come back to 1
            (double) getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_SIZE).metricValue() == 1.0
        );
        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_EVICTION_COUNT).metricValue())
            .asInstanceOf(DOUBLE)
            .isOne();
    }

    @Test
    void testCacheLoadTime() {
        final Metrics metrics = new Metrics();
        final Cache<String, String> cache = Caffeine.newBuilder()
            .recordStats()
            .build();
        new CaffeineCacheMetrics(metrics, cache);

        // Simulate cache loads with some computation time
        cache.get("key1", key -> {
            try {
                Thread.sleep(10); // Simulate load time
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "value1";
        });

        assertThat(getMetric(metrics, CaffeineCacheMetricsRegistry.CACHE_AVG_LOAD_PENALTY_NANOSECONDS).metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    private static KafkaMetric getMetric(Metrics metrics, String metricName) {
        return metrics.metric(metrics.metricName(metricName, CaffeineCacheMetricsRegistry.METRIC_GROUP));
    }
}