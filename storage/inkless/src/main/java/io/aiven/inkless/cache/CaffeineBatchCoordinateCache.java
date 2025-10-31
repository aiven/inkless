package io.aiven.inkless.cache;

import org.apache.kafka.common.TopicIdPartition;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class CaffeineBatchCoordinateCache implements BatchCoordinateCache {

    static class UncheckedStaleCacheEntryException extends RuntimeException {
        public UncheckedStaleCacheEntryException(StaleCacheEntryException cause) {
            super(cause);
        }

        @Override
        public synchronized StaleCacheEntryException getCause() {
            return (StaleCacheEntryException) super.getCause();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineBatchCoordinateCache.class);

    private final Cache<TopicIdPartition, LogFragment> cache;
    private final Duration ttl;
    private final Clock clock;

    private final BatchCoordinateCacheMetrics metrics;

    public CaffeineBatchCoordinateCache(Duration ttl, Clock clock, BatchCoordinateCacheMetrics metrics) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be a positive duration.");
        }
        this.ttl = ttl;
        if (clock == null) {
            throw new IllegalArgumentException("Clock must not be null.");
        }
        this.clock = clock;

        this.cache = Caffeine.newBuilder().build();
        this.metrics = metrics;
    }

    public CaffeineBatchCoordinateCache(Duration ttl, Clock clock) {
        this(ttl, clock, new BatchCoordinateCacheMetrics());
    }

    public CaffeineBatchCoordinateCache(Duration ttl) {
        this(ttl, Clock.systemUTC());
    }


    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        final LogFragment logFragment = cache.getIfPresent(topicIdPartition);
        if (logFragment == null) {
            metrics.recordCacheMiss();
            return null;
        }
        metrics.recordCacheHit();
        return logFragment.subFragment(offset);
    }

    private void putInternal(CacheBatchCoordinate value) throws StaleCacheEntryException {
        try {
            cache.asMap().compute(value.topicIdPartition(), (key, existingLogFragment) -> {

                if (existingLogFragment == null) {
                    existingLogFragment = new LogFragment(key, value.logStartOffset(), this.ttl, this.clock);
                }

                try {
                    existingLogFragment.addBatch(value);
                    metrics.incrementCacheSize();

                    evictExpiredEntries(existingLogFragment);

                    return existingLogFragment;
                } catch (StaleCacheEntryException e) {
                    throw new UncheckedStaleCacheEntryException(e);
                }
            });

        } catch (UncheckedStaleCacheEntryException e) {
            throw e.getCause();
        }
    }

    private void evictExpiredEntries(LogFragment logFragment) {
        int removedInFragment = logFragment.evictExpired();

        if (removedInFragment > 0) {
            LOGGER.debug("Evicted {} expired batches", removedInFragment);
            metrics.recordCacheEviction();
            metrics.decreaseCacheSize(removedInFragment);
        }
    }

    @Override
    public void put(CacheBatchCoordinate cacheBatchCoordinate) throws IllegalStateException {
        TopicIdPartition topicIdPartition = cacheBatchCoordinate.topicIdPartition();
        try {
            putInternal(cacheBatchCoordinate);
        } catch (StaleCacheEntryException e) {
            LOGGER.debug("[{}] Stale cache entry found, invalidating cache", topicIdPartition, e);
            invalidatePartition(topicIdPartition);
            try {
                putInternal(cacheBatchCoordinate);
            } catch (StaleCacheEntryException secondException) {
                LOGGER.error("[{}] Failed to add batch coordinate after invalidation", topicIdPartition, secondException);
                invalidatePartition(topicIdPartition);
                throw new RuntimeException("Failed to put item in cache after retry", secondException);
            }
        } catch (IllegalStateException e) {
            LOGGER.debug("[{}] Failed to add batch coordinate, invalidating cache", topicIdPartition, e);
            invalidatePartition(topicIdPartition);
            throw e;
        }
    }

    /**
     * Removes the entry from the cache for a topic partition
     */
    @Override
    public int invalidatePartition(TopicIdPartition topicIdPartition) {
        final AtomicInteger sizeOfRemoved = new AtomicInteger(0);

        cache.asMap().computeIfPresent(topicIdPartition, (key, fragment) -> {
            int size = fragment.size();
            if (size > 0) {
                metrics.decreaseCacheSize(size);
                metrics.recordCacheInvalidation();
                sizeOfRemoved.set(size);
            }

            return null;
        });

        return sizeOfRemoved.get();
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
        cache.cleanUp();
        metrics.close();
    }
}
