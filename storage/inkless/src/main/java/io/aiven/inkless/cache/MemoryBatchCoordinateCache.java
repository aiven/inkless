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

import java.util.Collections;
import org.apache.kafka.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.aiven.inkless.control_plane.BatchCoordinate;

public class MemoryBatchCoordinateCache implements BatchCoordinateCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryBatchCoordinateCache.class);

    private final Map<TopicIdPartition, LogFragment> store;
    private final int capacity;
    private final AtomicLong totalSize;
    private final Map<TopicIdPartition, Boolean> lruTracker;

    public MemoryBatchCoordinateCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive.");
        }
        this.capacity = capacity;
        this.store = new ConcurrentHashMap<>();
        this.totalSize = new AtomicLong(0);
        this.lruTracker = Collections.synchronizedMap(new LinkedHashMap<>(capacity, 0.75f, true));
    }

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        LogFragment logFragment = store.get(topicIdPartition);
        if (logFragment == null) {
            return null;
        }

        // Update the LRU tracker
        lruTracker.get(topicIdPartition);

        return logFragment.subFragment(offset);
    }

    @Override
    public void put(TopicIdPartition topicIdPartition, BatchCoordinate value) {
        LogFragment list = store.computeIfAbsent(topicIdPartition, k -> new LogFragment(k, value.logStartOffset()));

        try {
            list.addBatch(value);
        } catch (NonContiguousLogFragmentException e) {
            LOGGER.error("Failed to add batch coordinate to list, invalidating cache for this partition", e);
            remove(topicIdPartition);
            return;
        }

        totalSize.incrementAndGet();

        synchronized (lruTracker) {
            lruTracker.put(topicIdPartition, true);
            evictOldestItems();
        }
    }

    // This method must be called from within a block synchronized on `lruTracker`.
    private void evictOldestItems() {
        while (totalSize.get() > capacity && !lruTracker.isEmpty()) {
            TopicIdPartition lruKey = lruTracker.keySet().iterator().next();
            LogFragment lruList = store.get(lruKey);

            if (lruList != null) {
                if (lruList.evictOldestBatch()) {
                    totalSize.decrementAndGet();
                }
                if (lruList.isEmpty()) {
                    store.remove(lruKey);
                    lruTracker.remove(lruKey);
                }
            } else {
                lruTracker.remove(lruKey);
            }
        }
    }

    @Override
    public boolean remove(TopicIdPartition topicIdPartition) {
        LogFragment removedFragment;

        synchronized (lruTracker) {
            if (!store.containsKey(topicIdPartition)) {
                return false;
            }
            lruTracker.remove(topicIdPartition);
            removedFragment = store.remove(topicIdPartition);
        }

        if (removedFragment != null) {
            totalSize.addAndGet(-removedFragment.size());
        }

        return removedFragment != null;
    }

    @Override
    public long size() {
        return totalSize.get();
    }

    @Override
    public void close() throws IOException {
        synchronized (lruTracker) {
            store.clear();
            lruTracker.clear();
            totalSize.set(0);
        }
    }
}
