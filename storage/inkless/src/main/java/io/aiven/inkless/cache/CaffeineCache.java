package io.aiven.inkless.cache;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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

    private final CaffeineCacheMetrics metrics;

    public CaffeineCache(
        final long maxCacheSize,
        final long lifespanSeconds,
        final int maxIdleSeconds
    ) {
        cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofSeconds(lifespanSeconds))
                .expireAfterAccess(Duration.ofSeconds(maxIdleSeconds != -1 ? maxIdleSeconds: 180))
                .recordStats()
                .buildAsync();
        metrics = new CaffeineCacheMetrics(cache.synchronous());
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }

    @Override
    public CompletableFuture<FileExtent> computeIfAbsent(
        final CacheKey key,
        final Function<CacheKey, FileExtent> load,
        final Executor loadExecutor
    ) {
        // Caffeine's AsyncCache.get() provides atomic cache population per key.
        // When multiple threads concurrently request the same uncached key, the mapping function
        // is invoked only once, and all waiting threads receive the same CompletableFuture.
        // This guarantees that the load function is called exactly once per key, preventing duplicate
        // fetch operations from object storage.
        return cache.get(key, (k, defaultExecutor) -> {
            // Use the provided executor instead of Caffeine's default executor.
            // This allows us to control which thread pool handles the fetch and blocks there,
            // while Caffeine's internal threads remain unblocked, so cache operations can continue to be served.
            return CompletableFuture.supplyAsync(() -> load.apply(k), loadExecutor)
                .whenComplete((result, throwable) -> {
                    // Evict the entry if the future completed exceptionally.
                    // While Caffeine has built-in failed future cleanup, it happens asynchronously.
                    // Explicit invalidation ensures immediate removal for faster retry on subsequent requests.
                    if (throwable != null) {
                        cache.synchronous().invalidate(key);
                    }
                });
        });
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
