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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import io.aiven.inkless.control_plane.BatchCoordinate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryBatchCoordinateCacheTest {

    private static final TopicIdPartition PARTITION_0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
    private static final TopicIdPartition PARTITION_1 = new TopicIdPartition(Uuid.randomUuid(), 1, "topic");
    private static final TopicIdPartition PARTITION_2 = new TopicIdPartition(Uuid.randomUuid(), 2, "topic");

    private MemoryBatchCoordinateCache cache;

    @BeforeEach
    void setUp() {
        cache = new MemoryBatchCoordinateCache(10);
    }

    private BatchCoordinate createBatch(TopicIdPartition topicIdPartition, long baseOffset, int recordCount) {
        return createBatch(topicIdPartition, baseOffset, recordCount, 0L);
    }

    private BatchCoordinate createBatch(TopicIdPartition topicIdPartition, long baseOffset, int recordCount, long logStartOffset) {
        return new BatchCoordinate(
            topicIdPartition,
            "test-object-key",
            100L,
            1024L,
            baseOffset,
            baseOffset + recordCount - 1,
            TimestampType.CREATE_TIME,
            System.currentTimeMillis(),
            (byte) 2,
            logStartOffset
        );
    }

    @Test
    void constructWithInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new MemoryBatchCoordinateCache(0));
        assertThrows(IllegalArgumentException.class, () -> new MemoryBatchCoordinateCache(-1));
    }

    @Test
    void putAndGetSingleItem() {
        BatchCoordinate batch = createBatch(PARTITION_0, 0, 10);
        cache.put(PARTITION_0, batch);

        assertEquals(1, cache.size());
        LogFragment result = cache.get(PARTITION_0, 5);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(0, result.firstOffset());
        assertEquals(10, result.highWaterMark());
    }

    @Test
    void getNonExistentPartition() {
        assertNull(cache.get(PARTITION_0, 0));
    }

    @Test
    void getFromDifferentOffsets() {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 10));  // [0-9]
        cache.put(PARTITION_0, createBatch(PARTITION_0, 10, 10)); // [10-19]
        cache.put(PARTITION_0, createBatch(PARTITION_0, 20, 10)); // [20-29]

        LogFragment fromStart = cache.get(PARTITION_0, 0);
        assertNotNull(fromStart);
        assertEquals(3, fromStart.size());
        assertEquals(0, fromStart.firstOffset());
        assertEquals(0, fromStart.logStartOffset());
        assertEquals(30, fromStart.highWaterMark());

        LogFragment fromMiddle = cache.get(PARTITION_0, 15);
        assertNotNull(fromMiddle);
        assertEquals(2, fromMiddle.size());
        assertEquals(10, fromMiddle.firstOffset());
        assertEquals(0, fromMiddle.logStartOffset());
        assertEquals(30, fromMiddle.highWaterMark());

        LogFragment fromLast = cache.get(PARTITION_0, 25);
        assertNotNull(fromLast);
        assertEquals(1, fromLast.size());
        assertEquals(20, fromLast.firstOffset());
        assertEquals(0, fromLast.logStartOffset());
        assertEquals(30, fromLast.highWaterMark());
    }

    @Test
    void getFromDifferentOffsetsAndFirstOffsetGreaterThanZero() {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 10, 10)); // [10-19]
        cache.put(PARTITION_0, createBatch(PARTITION_0, 20, 10)); // [20-29]

        assertNull(cache.get(PARTITION_0, 0));

        LogFragment fromFirst = cache.get(PARTITION_0, 15);
        assertNotNull(fromFirst);
        assertEquals(2, fromFirst.size());
        assertEquals(10, fromFirst.firstOffset());
        assertEquals(0, fromFirst.logStartOffset());
        assertEquals(30, fromFirst.highWaterMark());

        LogFragment fromLast = cache.get(PARTITION_0, 25);
        assertNotNull(fromLast);
        assertEquals(1, fromLast.size());
        assertEquals(20, fromLast.firstOffset());
        assertEquals(0, fromLast.logStartOffset());
        assertEquals(30, fromLast.highWaterMark());
    }

    @Test
    void getFromOffsetBeforeThanFirstOffset() {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 10, 19));
        assertNull(cache.get(PARTITION_0, 5));
    }

    @Test
    void getEmptyLogFragmentForOffsetGreaterThanHighWaterMark() {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 9));
        LogFragment result = cache.get(PARTITION_0, 100);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testPutNonContiguousBatch() {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 10));
        assertEquals(1, cache.size());
        assertNotNull(cache.get(PARTITION_0, 0));

        // Expected baseOffset is 11
        cache.put(PARTITION_0, createBatch(PARTITION_0, 20, 10));

        assertEquals(0, cache.size(), "Cache size should be 0 after invalidation");
        assertNull(cache.get(PARTITION_0, 0), "Fragment should be removed after non-contiguous put");
    }

    @Test
    void keyEviction() {
        cache = new MemoryBatchCoordinateCache(2);

        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 10));
        cache.put(PARTITION_1, createBatch(PARTITION_1, 0, 10));
        assertEquals(2, cache.size());

        // Access PARTITION_0 to make it the most recently used, now PARTITION_1 is the LRU
        assertNotNull(cache.get(PARTITION_0, 0));

        // Add a new batch, triggering eviction
        cache.put(PARTITION_2, createBatch(PARTITION_2, 0, 10));

        assertEquals(2, cache.size());
        assertNotNull(cache.get(PARTITION_0, 0));
        assertNotNull(cache.get(PARTITION_2, 0));
        // Entire LRU partition should have been evicted
        assertNull(cache.get(PARTITION_1, 0));
    }

    @Test
    void batchEviction() {
        cache = new MemoryBatchCoordinateCache(3);

        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 10));
        cache.put(PARTITION_1, createBatch(PARTITION_1, 0, 10));
        cache.put(PARTITION_1, createBatch(PARTITION_1, 10, 10));
        assertEquals(3, cache.size());

        // Access PARTITION_0 to make it the most recently used, now PARTITION_1 is the LRU
        assertNotNull(cache.get(PARTITION_0, 0));

        // Add a new batch, triggering eviction
        cache.put(PARTITION_2, createBatch(PARTITION_2, 0, 9));

        assertEquals(3, cache.size());
        assertNotNull(cache.get(PARTITION_0, 0));
        assertNotNull(cache.get(PARTITION_2, 0));
        // First batch from the LRU partition was evicted
        assertNull(cache.get(PARTITION_1, 0));
        // The other batch remains intact
        assertNotNull(cache.get(PARTITION_1, 10));
    }

    @Test
    void evictionWithinSinglePartition() {
        cache = new MemoryBatchCoordinateCache(3);

        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 10));
        cache.put(PARTITION_0, createBatch(PARTITION_0, 10, 10));
        cache.put(PARTITION_0, createBatch(PARTITION_0, 20, 10));
        assertEquals(3, cache.size());

        // Add another batch to the same partition, triggering eviction
        cache.put(PARTITION_0, createBatch(PARTITION_0, 30, 10));
        assertEquals(3, cache.size());

        // First batch was evicted
        assertNull(cache.get(PARTITION_0, 0));

        // Other batches remain intact
        LogFragment validFragment = cache.get(PARTITION_0, 10);
        assertNotNull(validFragment);
        assertEquals(3, validFragment.size());
        assertEquals(10L, validFragment.firstOffset());
    }

    @Test
    void testRemove() {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 10));
        cache.put(PARTITION_0, createBatch(PARTITION_0, 10, 10));
        cache.put(PARTITION_1, createBatch(PARTITION_1, 0, 10));
        assertEquals(3, cache.size());

        assertTrue(cache.remove(PARTITION_0));

        assertEquals(1, cache.size());
        assertNull(cache.get(PARTITION_0, 0));

        assertNotNull(cache.get(PARTITION_1, 0));
    }

    @Test
    void removeNonExistentPartition() {
        assertFalse(cache.remove(PARTITION_0));
        assertEquals(0, cache.size());
    }

    @Test
    void testClose() throws IOException {
        cache.put(PARTITION_0, createBatch(PARTITION_0, 0, 9));
        cache.put(PARTITION_1, createBatch(PARTITION_1, 0, 9));
        assertEquals(2, cache.size());

        cache.close();

        assertEquals(0, cache.size());
        assertNull(cache.get(PARTITION_0, 0));
        assertNull(cache.get(PARTITION_1, 0));
    }
}