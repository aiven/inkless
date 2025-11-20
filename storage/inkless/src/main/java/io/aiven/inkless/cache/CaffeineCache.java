package io.aiven.inkless.cache;

import org.apache.kafka.common.metrics.Metrics;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public final class CaffeineCache implements ObjectCache {

    /**
     * Asynchronous Caffeine cache.
     * The cache mutations use CompletableFuture<FileExtent> to guard for competing
     * loads from object storage.
     */
    private final AsyncCache<CacheKey, FileExtent> cache;

    public CaffeineCache(
        final long maxCacheSize,
        final long lifespanSeconds,
        final int maxIdleSeconds,
        final Metrics storageMetrics
    ) {
        final Caffeine<Object, Object> builder = Caffeine.newBuilder();
        // size and weight limits
        builder.maximumSize(maxCacheSize);
        // expiration policies
        builder.expireAfterWrite(Duration.ofSeconds(lifespanSeconds));
        builder.expireAfterAccess(Duration.ofSeconds(maxIdleSeconds != -1 ? maxIdleSeconds : 180));
        // enable metrics
        builder.recordStats();
        cache = builder.buildAsync();
        new CaffeineCacheMetrics(storageMetrics, cache.synchronous());
    }

    @Override
    public void close() throws IOException {
        cache.synchronous().cleanUp();
    }

    @Override
    public FileExtent computeIfAbsent(final CacheKey key, final Function<CacheKey, FileExtent> mappingFunction) {
        final CompletableFuture<FileExtent> future = new CompletableFuture<>();
        final CompletableFuture<FileExtent> existingFuture = cache.asMap().computeIfAbsent(key, (cacheKey) -> future);
        // If existing future is not the same object as created in this function
        // there was a pending cache load and this call is required to join the existing future
        // and discard the created one.
        if (future != existingFuture) {
            return existingFuture.join();
        }
        final FileExtent fileExtent = mappingFunction.apply(key);
        future.complete(fileExtent);
        return fileExtent;
    }

    @Override
    public FileExtent get(final CacheKey key) {
        final CompletableFuture<FileExtent> future = cache.getIfPresent(key);
        if (future != null) {
            return future.join();
        }
        return null;
    }

    @Override
    public void put(final CacheKey key, final FileExtent value) {
        cache.synchronous().put(key, value);
    }

    @Override
    public boolean remove(final CacheKey key) {
        if (cache.getIfPresent(key) != null) {
            cache.synchronous().invalidate(key);
            return true;
        }
        return false;
    }

    @Override
    public long size() {
        return cache.synchronous().estimatedSize();
    }

    public CacheStats stats() {
        return cache.synchronous().stats();
    }
}
