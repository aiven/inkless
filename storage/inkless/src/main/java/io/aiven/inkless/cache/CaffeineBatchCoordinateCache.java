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

/**
 * A thread-safe cache that manages {@link LogFragment} instances for
 * multiple Kafka diskless topic partitions.
 * It uses a time-to-live (TTL) parameter as a cleanup policy.
 * This cache automatically handles stale and expired entries.
 */
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


    /**
     * Retrieves a view of the log's tail for a specific partition, starting from
     * the requested offset.
     *
     * <p>This operation will result in a <b>cache miss</b> (returning {@code null})
     * in several scenarios:
     * <ul>
     * <li>If the entire partition is not currently tracked by the cache.</li>
     * <li>If the requested {@code offset} is older than the data held in the
     * fragment (i.e., it has already been evicted).</li>
     * <li>If the data at that offset is found but has expired at the moment of
     * the request.</li>
     * </ul></p>
     *
     * @param topicIdPartition The specific topic partition to read from.
     * @param offset           The desired starting offset for the read.
     * @return A new {@code LogFragment} view containing the requested data, or
     * {@code null} if the data is not available (a cache miss).
     */
    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        final LogFragment logFragment = cache.getIfPresent(topicIdPartition);
        if (logFragment == null) {
            metrics.recordCacheMiss();
            return null;
        }
        metrics.recordCacheHit();
        final LogFragment subFragment = logFragment.subFragment(offset);
        if (subFragment == null || subFragment.isEmpty()) {
            metrics.recordCacheMiss();
        } else {
            metrics.recordCacheHit();
        }

        return subFragment;
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

    /**
     * Appends a new batch coordinate to the cache, adding it to the correct log
     * fragment for its partition.
     *
     * <p>This method handles stale cache entries: when a batch is added,
     * it is passed to the appropriate {@link LogFragment}.
     * If that fragment detects that the batch would create an integrity problem
     * (such as a data gap, an overlap, or if the batch indicates the log start offset was
     * increased), the entire cache entry for the requested TopicPartition will be invalidated
     * and the batch insertion will be retried, creating a new log fragment.</p>
     *
     * <p>After a successful batch addition, this method also opportunistically
     * triggers an eviction check on the fragment to purge any expired batches.</p>
     *
     * @param cacheBatchCoordinate The batch coordinate to add.
     * @throws IllegalStateException If the new batch is rejected for being invalid (lower log start offset
     * than the one present in the cache, or lower base offset than the high watermark present in the cache)
     */
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
     * Removes the entry from the cache for a topic partition.
     * @param topicIdPartition The specific topic partition to remove from the cache.
     * @return the number of batches removed from the cache.
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
