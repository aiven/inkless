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

import org.apache.kafka.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.aiven.inkless.control_plane.BatchCoordinate;

public class MemoryBatchCoordinateCache implements BatchCoordinateCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryBatchCoordinateCache.class);

    private final Map<TopicIdPartition, LogFragment> store;
    private final AtomicLong totalSize;
    private final Duration ttl;
    private final Clock clock;

    public MemoryBatchCoordinateCache(Duration ttl, Clock clock) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be a positive duration.");
        }
        this.ttl = ttl;
        if (clock == null) {
            throw new IllegalArgumentException("Clock must not be null.");
        }
        this.clock = clock;
        this.store = new ConcurrentHashMap<>();
        this.totalSize = new AtomicLong(0);
    }

    public MemoryBatchCoordinateCache(Duration ttl) {
        this(ttl, Clock.systemUTC());
    }

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition) {
        return store.get(topicIdPartition);
    }

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        final LogFragment logFragment = store.get(topicIdPartition);
        if (logFragment == null) {
            return null;
        }
        return logFragment.subFragment(offset);
    }

    private void putInternal(BatchCoordinate value) throws StaleCacheEntryException {
        LogFragment logFragment = store.computeIfAbsent(value.topicIdPartition(),
            key -> new LogFragment(key, value.logStartOffset(), this.ttl, this.clock));

        logFragment.addBatch(value);

        totalSize.incrementAndGet();
        evictExpiredEntries(logFragment);
    }

    @Override
    public void put(BatchCoordinate value) {
        TopicIdPartition topicIdPartition = value.topicIdPartition();
        try {
            putInternal(value);
        } catch (StaleCacheEntryException e) {
            LOGGER.debug("Failed to add batch coordinate, invalidating cache for partition {}", topicIdPartition, e);
            remove(topicIdPartition);
            try {
                putInternal(value);
            } catch (StaleCacheEntryException secondException) {
                LOGGER.error("Failed to add batch coordinate for {}", topicIdPartition, secondException);
            }
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
    public int remove(TopicIdPartition topicIdPartition) {
        final LogFragment removedFragment = store.remove(topicIdPartition);

        if (removedFragment != null) {
            final int sizeOfRemoved = removedFragment.size();
            totalSize.addAndGet(-sizeOfRemoved);
            return sizeOfRemoved;
        }

        return 0;
    }

    @Override
    public long size() {
        return totalSize.get();
    }

    @Override
    public void close() throws IOException {
        store.clear();
        totalSize.set(0);
    }
}
