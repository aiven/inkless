package io.aiven.inkless.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public final class CaffeineCache implements ObjectCache {

    private final Cache<CacheKey, FileExtent> cache;

    public CaffeineCache(
        final long maxCacheSize,
        final long lifespanSeconds,
        final int maxIdleSeconds) {
        cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofSeconds(lifespanSeconds))
                .expireAfterAccess(Duration.ofSeconds(maxIdleSeconds != -1 ? maxIdleSeconds: 180))
                .recordStats()
                .build();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean supportsComputeIfAbsent() {
        return true;
    }

    @Override
    public FileExtent computeIfAbsent(final CacheKey key, final Function<CacheKey, FileExtent> mappingFunction) {
        return cache.asMap().computeIfAbsent(key, mappingFunction);
    }

    @Override
    public FileExtent get(final CacheKey key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void put(final CacheKey key, final FileExtent value) {
        cache.asMap().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final CacheKey key) {
        if (cache.getIfPresent(key) != null) {
            cache.invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public long size() {
        return cache.estimatedSize();
    }
}
