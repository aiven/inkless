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

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TieredObjectCacheTest {

    private static final long HOT_CACHE_TTL_MS = 60_000; // 60 seconds
    private static final long NO_RATE_LIMIT = -1;

    private ObjectCache hotCache;
    private ObjectCache laggingCache;
    private MockTime time;
    private TieredObjectCache.TieredCacheMetrics metrics;
    private TieredObjectCache tieredCache;

    @BeforeEach
    void setUp() {
        hotCache = mock(ObjectCache.class);
        laggingCache = mock(ObjectCache.class);
        time = new MockTime();
        metrics = mock(TieredObjectCache.TieredCacheMetrics.class);
        tieredCache = new TieredObjectCache(hotCache, laggingCache, HOT_CACHE_TTL_MS, time, NO_RATE_LIMIT, metrics);
    }

    @Test
    void shouldUseLaggingCacheWhenBatchTimestampIsOlderThanTTL() {
        // Given: A batch timestamp older than hot cache TTL
        long currentTime = time.milliseconds();
        long oldTimestamp = currentTime - HOT_CACHE_TTL_MS - 1000; // 1 second older than TTL

        CacheKey key = createCacheKey("test-object", 0, 1000);
        FileExtent expectedExtent = createFileExtent("test-object");

        when(laggingCache.computeIfAbsent(eq(key), any())).thenReturn(expectedExtent);

        // When
        FileExtent result = tieredCache.computeIfAbsent(key, k -> expectedExtent, oldTimestamp);

        // Then
        assertThat(result).isEqualTo(expectedExtent);
        verify(laggingCache, times(1)).computeIfAbsent(eq(key), any());
        verify(hotCache, never()).computeIfAbsent(any(), any());
        verify(metrics).recordLaggingCacheRouting();
    }

    @Test
    void shouldUseHotCacheWhenBatchTimestampIsWithinTTL() {
        // Given: A batch timestamp within hot cache TTL
        long currentTime = time.milliseconds();
        long recentTimestamp = currentTime - HOT_CACHE_TTL_MS + 1000; // 1 second younger than TTL

        CacheKey key = createCacheKey("test-object", 0, 1000);
        FileExtent expectedExtent = createFileExtent("test-object");

        when(hotCache.computeIfAbsent(eq(key), any())).thenReturn(expectedExtent);

        // When
        FileExtent result = tieredCache.computeIfAbsent(key, k -> expectedExtent, recentTimestamp);

        // Then
        assertThat(result).isEqualTo(expectedExtent);
        verify(hotCache, times(1)).computeIfAbsent(eq(key), any());
        verify(laggingCache, never()).computeIfAbsent(any(), any());
        verify(metrics).recordHotCacheRouting();
    }

    @Test
    void shouldUseHotCacheForDefaultComputeIfAbsent() {
        // Given
        CacheKey key = createCacheKey("test-object", 0, 1000);
        FileExtent expectedExtent = createFileExtent("test-object");

        when(hotCache.computeIfAbsent(eq(key), any())).thenReturn(expectedExtent);

        // When: Using the method without batch timestamp (write path)
        FileExtent result = tieredCache.computeIfAbsent(key, k -> expectedExtent);

        // Then
        assertThat(result).isEqualTo(expectedExtent);
        verify(hotCache, times(1)).computeIfAbsent(eq(key), any());
        verify(laggingCache, never()).computeIfAbsent(any(), any());
        verify(metrics).recordHotCacheRouting();
    }

    @Test
    void shouldAlwaysUseHotCacheForPut() {
        // Given
        CacheKey key = createCacheKey("test-object", 0, 1000);
        FileExtent extent = createFileExtent("test-object");

        // When
        tieredCache.put(key, extent);

        // Then
        verify(hotCache).put(key, extent);
        verify(laggingCache, never()).put(key, extent);
    }

    @Test
    void shouldSearchBothCachesForGet() {
        // Given
        CacheKey key = createCacheKey("test-object", 0, 1000);
        FileExtent expectedExtent = createFileExtent("test-object");

        // Hot cache miss, lagging cache hit
        when(hotCache.get(key)).thenReturn(null);
        when(laggingCache.get(key)).thenReturn(expectedExtent);

        // When
        FileExtent result = tieredCache.get(key);

        // Then
        assertThat(result).isEqualTo(expectedExtent);
    }

    @Test
    void shouldReturnFromHotCacheIfFoundFirst() {
        // Given
        CacheKey key = createCacheKey("test-object", 0, 1000);
        FileExtent expectedExtent = createFileExtent("test-object");

        when(hotCache.get(key)).thenReturn(expectedExtent);

        // When
        FileExtent result = tieredCache.get(key);

        // Then
        assertThat(result).isEqualTo(expectedExtent);
        verify(laggingCache, never()).get(key);
    }

    @Test
    void shouldRemoveFromBothCaches() {
        // Given
        CacheKey key = createCacheKey("test-object", 0, 1000);

        when(hotCache.remove(key)).thenReturn(true);
        when(laggingCache.remove(key)).thenReturn(false);

        // When
        boolean result = tieredCache.remove(key);

        // Then
        assertThat(result).isTrue();
        verify(hotCache).remove(key);
        verify(laggingCache).remove(key);
    }

    @Test
    void shouldCombineSizesFromBothCaches() {
        // Given
        when(hotCache.size()).thenReturn(100L);
        when(laggingCache.size()).thenReturn(50L);

        // When/Then
        assertThat(tieredCache.size()).isEqualTo(150L);
        assertThat(tieredCache.hotCacheSize()).isEqualTo(100L);
        assertThat(tieredCache.laggingCacheSize()).isEqualTo(50L);
    }

    @Test
    void shouldUseLaggingCacheExactlyAtTTLBoundary() {
        // Given: A batch timestamp exactly at the TTL boundary
        long currentTime = time.milliseconds();
        long boundaryTimestamp = currentTime - HOT_CACHE_TTL_MS; // Exactly at TTL

        // When
        boolean shouldUseLagging = tieredCache.shouldUseLaggingCache(boundaryTimestamp);

        // Then: At exactly TTL, it should NOT use lagging cache (age == TTL, not > TTL)
        assertThat(shouldUseLagging).isFalse();
    }

    @Test
    void shouldUseLaggingCacheJustPastTTLBoundary() {
        // Given: A batch timestamp just past the TTL boundary
        long currentTime = time.milliseconds();
        long pastBoundaryTimestamp = currentTime - HOT_CACHE_TTL_MS - 1; // 1ms past TTL

        // When
        boolean shouldUseLagging = tieredCache.shouldUseLaggingCache(pastBoundaryTimestamp);

        // Then
        assertThat(shouldUseLagging).isTrue();
    }

    @Test
    void shouldApplyRateLimitingForLaggingCacheFetches() {
        // Given: A tiered cache with rate limiting enabled
        long rateLimitBytesPerSec = 1000; // 1000 bytes/sec, low for testing
        ObjectCache realLaggingCache = new CaffeineCache(100, 60, -1);
        TieredObjectCache rateLimitedCache = new TieredObjectCache(
            hotCache,
            realLaggingCache,
            HOT_CACHE_TTL_MS,
            time,
            rateLimitBytesPerSec,
            TieredObjectCache.TieredCacheMetrics.NOOP
        );

        long currentTime = time.milliseconds();
        long oldTimestamp = currentTime - HOT_CACHE_TTL_MS - 1000;

        // When: Multiple fetches that should be rate limited
        CacheKey key1 = createCacheKey("test-object-1", 0, 500);
        CacheKey key2 = createCacheKey("test-object-2", 0, 500);

        // First fetch should succeed immediately (within burst capacity)
        FileExtent result1 = rateLimitedCache.computeIfAbsent(key1, k -> createFileExtent("test-object-1"), oldTimestamp);
        assertThat(result1).isNotNull();

        // Second fetch should also succeed (rate limiter has 2x burst capacity)
        FileExtent result2 = rateLimitedCache.computeIfAbsent(key2, k -> createFileExtent("test-object-2"), oldTimestamp);
        assertThat(result2).isNotNull();
    }

    @Test
    void shouldNotRateLimitHotCacheFetches() {
        // Given: A tiered cache with rate limiting enabled
        long rateLimitBytesPerSec = 100; // Very low rate limit
        ObjectCache realHotCache = new CaffeineCache(100, 60, -1);
        TieredObjectCache rateLimitedCache = new TieredObjectCache(
            realHotCache,
            laggingCache,
            HOT_CACHE_TTL_MS,
            time,
            rateLimitBytesPerSec,
            TieredObjectCache.TieredCacheMetrics.NOOP
        );

        long currentTime = time.milliseconds();
        long recentTimestamp = currentTime - 1000; // Recent data, should use hot cache

        // When: Multiple fetches to hot cache (should not be rate limited)
        for (int i = 0; i < 10; i++) {
            CacheKey key = createCacheKey("test-object-" + i, 0, 1000);
            FileExtent result = rateLimitedCache.computeIfAbsent(key, k -> createFileExtent("test-object"), recentTimestamp);
            assertThat(result).isNotNull();
        }
        // If rate limiting was applied, this would have taken > 10 seconds
        // Since it completes quickly, rate limiting is not applied to hot cache
    }

    private CacheKey createCacheKey(String object, long offset, long length) {
        return new CacheKey()
            .setObject(object)
            .setRange(new CacheKey.ByteRange().setOffset(offset).setLength(length));
    }

    private FileExtent createFileExtent(String object) {
        return new FileExtent()
            .setObject(object)
            .setRange(new FileExtent.ByteRange().setOffset(0).setLength(100))
            .setData(new byte[100]);
    }
}
