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
import java.util.concurrent.atomic.AtomicLong;

import io.aiven.inkless.control_plane.BatchCoordinate;

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
    private final AtomicLong totalSize;
    private final AtomicLong totalPartitionInvalidations;
    private final Duration ttl;
    private final Clock clock;

    private final CaffeineBatchCoordinateCacheMetrics metrics;

    public CaffeineBatchCoordinateCache(Duration ttl, Clock clock) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be a positive duration.");
        }
        this.ttl = ttl;
        if (clock == null) {
            throw new IllegalArgumentException("Clock must not be null.");
        }
        this.clock = clock;
        this.totalSize = new AtomicLong(0);
        this.totalPartitionInvalidations = new AtomicLong(0);

        this.cache = Caffeine.newBuilder().build();
        this.metrics = new CaffeineBatchCoordinateCacheMetrics(totalSize::get, totalPartitionInvalidations::get, cache);
    }

    public CaffeineBatchCoordinateCache(Duration ttl) {
        this(ttl, Clock.systemUTC());
    }


    @Override
    public LogFragment get(TopicIdPartition topicIdPartition) {
        return cache.getIfPresent(topicIdPartition);
    }

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset, long logStartOffset, long highWaterMark) {
        final LogFragment logFragment = cache.getIfPresent(topicIdPartition);
        if (logFragment == null) {
            return null;
        }

        if (logStartOffset > logFragment.logStartOffset()) {
            LOGGER.debug("[{}] LogFragment logStartOffset ({}) is less than the provided logStartOffset ({}), invalidating entry", topicIdPartition, logStartOffset, logFragment.logStartOffset());
            invalidatePartition(topicIdPartition);
            return null;
        }
        if (highWaterMark > logFragment.highWaterMark()) {
            LOGGER.debug("[{}] LogFragment highWaterMark ({}) is less than the provided highWaterMark ({}), invalidating entry", topicIdPartition, highWaterMark, logFragment.highWaterMark());
            invalidatePartition(topicIdPartition);
            return null;
        }

        return logFragment.subFragment(offset);
    }

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        final LogFragment logFragment = cache.getIfPresent(topicIdPartition);
        if (logFragment == null) {
            return null;
        }
        return logFragment.subFragment(offset);
    }

    private void putInternal(BatchCoordinate value) throws StaleCacheEntryException {
        try {
            cache.asMap().compute(value.topicIdPartition(), (key, existingLogFragment) -> {

                if (existingLogFragment == null) {
                    existingLogFragment = new LogFragment(key, value.logStartOffset(), this.ttl, this.clock);
                }

                try {
                    existingLogFragment.addBatch(value);
                    totalSize.incrementAndGet();

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
            totalSize.addAndGet(-removedInFragment);
            LOGGER.debug("Evicted {} expired batches", removedInFragment);
        }
    }

    @Override
    public void put(BatchCoordinate value) {
        TopicIdPartition topicIdPartition = value.topicIdPartition();
        try {
            putInternal(value);
        } catch (StaleCacheEntryException e) {
            LOGGER.debug("[{}] Failed to add batch coordinate, invalidating cache", topicIdPartition, e);
            invalidatePartition(topicIdPartition);
            try {
                putInternal(value);
            } catch (StaleCacheEntryException secondException) {
                LOGGER.error("[{}] Failed to add batch coordinate after invalidation", topicIdPartition, secondException);
                throw new RuntimeException("Failed to put item in cache after retry", secondException);
            }
        }
    }

    /**
     * Invalidate all batches for this partition.
     */
    @Override
    public int invalidatePartition(TopicIdPartition topicIdPartition) {
        final AtomicInteger sizeOfRemoved = new AtomicInteger(0);

        cache.asMap().computeIfPresent(topicIdPartition, (key, fragment) -> {
            int size = fragment.size();
            if (size > 0) {
                fragment.clear();
                totalSize.addAndGet(-size);
                totalPartitionInvalidations.addAndGet(1);
                sizeOfRemoved.set(size);
            }

            return fragment;
        });

        return sizeOfRemoved.get();
    }

    @Override
    public long size() {
        return totalSize.get();
    }

    public long totalPartitionInvalidations() {
        return totalPartitionInvalidations.get();
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
        cache.cleanUp();
        metrics.close();
        totalSize.set(0);
    }
}
