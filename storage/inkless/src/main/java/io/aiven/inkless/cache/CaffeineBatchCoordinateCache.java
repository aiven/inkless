package io.aiven.inkless.cache;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * A thread-safe cache that manages {@link LogFragment} instances for
 * multiple Kafka diskless topic partitions.
 * It uses a time-to-live (TTL) parameter as a cleanup policy.
 * This cache automatically handles stale and expired entries.
 */
public class CaffeineBatchCoordinateCache implements BatchCoordinateCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineBatchCoordinateCache.class);

    private final Cache<TopicIdPartition, LogFragment> cache;
    private final Duration ttl;
    private final Time time;

    private final BatchCoordinateCacheMetrics metrics;

    public CaffeineBatchCoordinateCache(Duration ttl, Time time, BatchCoordinateCacheMetrics metrics) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be a positive duration.");
        }
        this.ttl = ttl;
        if (time == null) {
            throw new IllegalArgumentException("Time must not be null.");
        }
        this.time = time;

        this.cache = Caffeine.newBuilder().build();
        this.metrics = metrics;
    }

    public CaffeineBatchCoordinateCache(Duration ttl, Time time) {
        this(ttl, time, new BatchCoordinateCacheMetrics());
    }

    public CaffeineBatchCoordinateCache(Duration ttl) {
        this(ttl, Time.SYSTEM);
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
        final LogFragment subFragment = logFragment.subFragment(offset);
        if (subFragment == null) {
            metrics.recordCacheMiss();
        } else if (subFragment.isEmpty()) {
            metrics.recordCacheHitWithoutData();
        } else {
            metrics.recordCacheHit();
        }

        return subFragment;
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
    public void put(TopicIdPartition topicIdPartition, CacheBatchCoordinate cacheBatchCoordinate) {
        cache.asMap().compute(topicIdPartition, (key, existingLogFragment) -> {
            LogFragment currentFragment = existingLogFragment;

            if (currentFragment == null) {
                currentFragment = new LogFragment(cacheBatchCoordinate.logStartOffset(), this.ttl, this.time);
            }

            try {
                currentFragment.addBatch(cacheBatchCoordinate);
                metrics.incrementCacheSize();
                evictExpiredEntries(currentFragment);
                return currentFragment;
            } catch (StaleLogFragmentException staleException) {
                LOGGER.debug("[{}] Stale log fragment detected, invalidating and retrying.", key, staleException);
                // Record invalidation metrics for the fragment we are about to discard
                if (existingLogFragment != null && !existingLogFragment.isEmpty()) {
                    metrics.decreaseCacheSize(existingLogFragment.size());
                    metrics.recordCacheInvalidation();
                }
                // Create a new fragment for the retry
                LogFragment newFragment = new LogFragment(cacheBatchCoordinate.logStartOffset(), this.ttl, this.time);
                try {
                    newFragment.addBatch(cacheBatchCoordinate);
                    metrics.incrementCacheSize();
                    return newFragment;
                } catch (StaleLogFragmentException | IllegalArgumentException secondAttemptException) {
                    LOGGER.error(
                        "[{}] Failed to add batch coordinate after invalidation. " +
                            "This batch will be dropped. First exception: {}, Second exception: {}",
                        key, staleException.getMessage(), secondAttemptException
                    );
                    return null;
                }

            } catch (IllegalArgumentException illegalArgumentException) {
                LOGGER.warn("[{}] Failed to add batch coordinate due to invalid state, invalidating cache", key, illegalArgumentException);
                // Record invalidation metrics for the fragment we are about to discard
                if (existingLogFragment != null && !existingLogFragment.isEmpty()) {
                    metrics.decreaseCacheSize(existingLogFragment.size());
                    metrics.recordCacheInvalidation();
                }
                return null;
            }
        });
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
        cache.cleanUp();
        metrics.close();
    }
}
