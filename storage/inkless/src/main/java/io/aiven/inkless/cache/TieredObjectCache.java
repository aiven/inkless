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

import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BlockingBucket;
import io.github.bucket4j.Bucket;

/**
 * A tiered cache implementation that routes requests to either a hot cache or a lagging cache
 * based on the age of the data being requested.
 *
 * <p>The hot cache is used for recent data (within the hot cache TTL), while the lagging cache
 * is used for historical data. This separation prevents lagging consumers from evicting hot data
 * that is being actively used by tail consumers and the write path.</p>
 *
 * <p>The classification is based on the batch timestamp: if the data is older than the
 * hot cache TTL, it cannot possibly be in the hot cache, so we route to the lagging cache.</p>
 *
 * <p>Rate limiting is applied to lagging cache fetches to protect remote storage from
 * being overwhelmed by lagging consumers.</p>
 */
public class TieredObjectCache implements ObjectCache {

    private final ObjectCache hotCache;
    private final ObjectCache laggingCache;
    private final long hotCacheTtlMs;
    private final Time time;
    private final TieredCacheMetrics metrics;
    private final BlockingBucket rateLimiter;

    /**
     * Creates a tiered cache without rate limiting.
     */
    public TieredObjectCache(
        final ObjectCache hotCache,
        final ObjectCache laggingCache,
        final long hotCacheTtlMs,
        final Time time
    ) {
        this(hotCache, laggingCache, hotCacheTtlMs, time, -1, TieredCacheMetrics.NOOP);
    }

    /**
     * Creates a tiered cache with optional rate limiting for lagging cache fetches.
     *
     * @param hotCache the cache for recent data
     * @param laggingCache the cache for historical data
     * @param hotCacheTtlMs the TTL threshold for routing (data older than this goes to lagging cache)
     * @param time time source
     * @param rateLimitBytesPerSec rate limit in bytes per second for lagging cache fetches, or -1 to disable
     * @param metrics metrics callback
     */
    public TieredObjectCache(
        final ObjectCache hotCache,
        final ObjectCache laggingCache,
        final long hotCacheTtlMs,
        final Time time,
        final long rateLimitBytesPerSec,
        final TieredCacheMetrics metrics
    ) {
        this.hotCache = hotCache;
        this.laggingCache = laggingCache;
        this.hotCacheTtlMs = hotCacheTtlMs;
        this.time = time;
        this.metrics = metrics;
        this.rateLimiter = rateLimitBytesPerSec > 0 ? createRateLimiter(rateLimitBytesPerSec) : null;
    }

    /**
     * Creates a rate limiter for lagging cache fetches.
     */
    private static BlockingBucket createRateLimiter(final long bytesPerSecond) {
        // Uses Bucket4j token bucket algorithm (https://github.com/bucket4j/bucket4j)
        //
        // Capacity: 2x per-second rate allows short bursts (e.g., 50 MiB/s -> 100 MiB burst)
        // Refill: "Greedy" adds tokens continuously for smooth rate limiting
        // Blocking: consume() blocks until tokens available, creating backpressure
        //
        // Example with 50 MiB/s and 16 MiB blocks: ~3 fetches/sec sustained
        final Bandwidth bandwidth = Bandwidth.builder()
            .capacity(bytesPerSecond * 2)
            .refillGreedy(bytesPerSecond, Duration.ofSeconds(1))
            .build();

        return Bucket.builder()
            .addLimit(bandwidth)
            .build()
            .asBlocking();
    }

    /**
     * Computes if absent from the appropriate cache tier based on batch timestamp.
     *
     * <p>For lagging cache fetches, rate limiting is applied before invoking the mapping function
     * to protect remote storage from being overwhelmed.</p>
     *
     * @param key the cache key
     * @param mappingFunction the function to compute the value if absent
     * @param batchTimestamp the timestamp of the batch (from BatchMetadata.timestamp())
     * @return the cached or computed file extent
     */
    @Override
    public FileExtent computeIfAbsent(
        final CacheKey key,
        final Function<CacheKey, FileExtent> mappingFunction,
        final long batchTimestamp
    ) {
        final boolean useLaggingCache = shouldUseLaggingCache(batchTimestamp);
        if (useLaggingCache) {
            metrics.recordLaggingCacheRouting();
            return laggingCache.computeIfAbsent(key, rateLimitedMappingFunction(mappingFunction, key));
        } else {
            metrics.recordHotCacheRouting();
            return hotCache.computeIfAbsent(key, mappingFunction);
        }
    }

    /**
     * Wraps the mapping function with rate limiting for lagging cache fetches.
     * Rate limiting is applied based on the expected fetch size (from the cache key's byte range).
     */
    private Function<CacheKey, FileExtent> rateLimitedMappingFunction(
        final Function<CacheKey, FileExtent> mappingFunction,
        final CacheKey key
    ) {
        if (rateLimiter == null) {
            return mappingFunction;
        }
        return cacheKey -> {
            // Rate limit based on the aligned block size (from cache key)
            // This is a conservative estimate; actual fetch may be smaller
            final long bytesToFetch = key.range().length();
            if (bytesToFetch > 0) {
                try {
                    rateLimiter.consume(bytesToFetch);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Rate limit wait interrupted", e);
                }
            }
            return mappingFunction.apply(cacheKey);
        };
    }

    /**
     * Default computeIfAbsent routes to hot cache.
     * This is used by the write path which always uses the hot cache.
     */
    @Override
    public FileExtent computeIfAbsent(final CacheKey key, final Function<CacheKey, FileExtent> mappingFunction) {
        metrics.recordHotCacheRouting();
        return hotCache.computeIfAbsent(key, mappingFunction);
    }

    /**
     * Determines if the lagging cache should be used based on batch age.
     * If data is older than hot cache TTL, it cannot be in hot cache.
     */
    boolean shouldUseLaggingCache(final long batchTimestamp) {
        final long batchAge = time.milliseconds() - batchTimestamp;
        return batchAge > hotCacheTtlMs;
    }

    @Override
    public FileExtent get(final CacheKey key) {
        // Try hot cache first, then lagging cache
        FileExtent result = hotCache.get(key);
        if (result == null) {
            result = laggingCache.get(key);
        }
        return result;
    }

    /**
     * Put always goes to hot cache.
     * This is used by the write path (CacheStoreJob) to cache produced data.
     */
    @Override
    public void put(final CacheKey key, final FileExtent value) {
        hotCache.put(key, value);
    }

    @Override
    public boolean remove(final CacheKey key) {
        boolean removedFromHot = hotCache.remove(key);
        boolean removedFromLagging = laggingCache.remove(key);
        return removedFromHot || removedFromLagging;
    }

    @Override
    public long size() {
        return hotCache.size() + laggingCache.size();
    }

    public long hotCacheSize() {
        return hotCache.size();
    }

    public long laggingCacheSize() {
        return laggingCache.size();
    }

    @Override
    public void close() throws IOException {
        try {
            hotCache.close();
        } finally {
            laggingCache.close();
        }
    }

    /**
     * Metrics interface for tiered cache operations.
     */
    public interface TieredCacheMetrics {
        TieredCacheMetrics NOOP = new TieredCacheMetrics() {
            @Override
            public void recordHotCacheRouting() {}
            @Override
            public void recordLaggingCacheRouting() {}
        };

        void recordHotCacheRouting();
        void recordLaggingCacheRouting();
    }
}
