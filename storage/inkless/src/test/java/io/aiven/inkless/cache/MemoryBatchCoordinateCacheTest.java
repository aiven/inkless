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
import java.time.Clock;
import java.time.Duration;

import io.aiven.inkless.control_plane.BatchCoordinate;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        cache = new MemoryBatchCoordinateCache(Duration.ofSeconds(10), Clock.systemUTC());
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
    void invalidConstructor() {
        assertThrows(IllegalArgumentException.class, () -> new MemoryBatchCoordinateCache(null));
        assertThrows(IllegalArgumentException.class, () -> new MemoryBatchCoordinateCache(Duration.ofSeconds(0)));
        assertThrows(IllegalArgumentException.class, () -> new MemoryBatchCoordinateCache(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> new MemoryBatchCoordinateCache(Duration.ofSeconds(1), null));
    }

    @Test
    void putAndGet() {
        BatchCoordinate batch = createBatch(PARTITION_0, 0, 10, 0);
        cache.put(batch);

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
        cache.put(createBatch(PARTITION_0, 0, 10, 0));  // [0-9]
        cache.put(createBatch(PARTITION_0, 10, 10, 0)); // [10-19]
        cache.put(createBatch(PARTITION_0, 20, 10, 0)); // [20-29]

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
    void putStartingAfterOffsetZeroAndGetFromDifferentOffsets() {
        cache.put(createBatch(PARTITION_0, 10, 10, 0)); // [10-19]
        cache.put(createBatch(PARTITION_0, 20, 10, 0)); // [20-29]

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
    void getFromOffsetBeforeThanFirstOffsetReturnsNull() {
        cache.put(createBatch(PARTITION_0, 10, 19, 0));
        assertNull(cache.get(PARTITION_0, 5));
    }

    @Test
    void getForOffsetGreaterThanHighWaterMarkReturnsEmptyLogFragment() {
        cache.put(createBatch(PARTITION_0, 0, 9, 0));
        LogFragment result = cache.get(PARTITION_0, 100);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testPutNonContiguousBatchInvalidatesKey() {
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        assertEquals(1, cache.size());
        assertNotNull(cache.get(PARTITION_0, 0));

        // Expected baseOffset is 11
        cache.put(createBatch(PARTITION_0, 20, 10, 0));

        assertEquals(0, cache.size(), "Cache size should be 0 after invalidation");
        assertNull(cache.get(PARTITION_0, 0), "Key should be removed after non-contiguous put");
    }


    @Test
    void getDoesNotReturnExpiredBatches() {
        ManualClock clock = new ManualClock();
        cache = new MemoryBatchCoordinateCache(Duration.ofSeconds(30), clock);

        // t=0: put first batch
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        assertNotNull(cache.get(PARTITION_0, 0));
        assertEquals(1, cache.size());

        // t=20: put second batch, the first one is not expired yet
        clock.advanceBy(Duration.ofSeconds(20));
        cache.put(createBatch(PARTITION_0, 10, 10, 0));
        assertNotNull(cache.get(PARTITION_0, 0));
        assertEquals(2, cache.size());

        // t=40: first batch is now expired
        clock.advanceBy(Duration.ofSeconds(20));
        // size is still 2 if no operation is performed
        assertEquals(2, cache.size());

        var logFragment = cache.get(PARTITION_0, 0);
        assertNotNull(logFragment);
        // first batch is not returned
        assertEquals(10, logFragment.firstOffset());
        // size is still 2 because get does not clean expired batches
        assertEquals(2, cache.size());

        // putting a new batch will remove the expired one
        cache.put(createBatch(PARTITION_0, 20, 10, 0));
        assertEquals(2, cache.size());
    }

    @Test
    void keyEvictionDueToExpiration() {
        ManualClock clock = new ManualClock();
        cache = new MemoryBatchCoordinateCache(Duration.ofSeconds(30), clock);

        // t=0: put PARTITION_0
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        assertEquals(1, cache.size());

        // t=10: put PARTITION_1
        clock.advanceBy(Duration.ofSeconds(10));
        cache.put(createBatch(PARTITION_1, 0, 10, 0));
        assertEquals(2, cache.size());

        // t=40: add a new batch, triggering eviction of PARTITION_0
        clock.advanceBy(Duration.ofSeconds(30));
        cache.put(createBatch(PARTITION_2, 0, 10, 0));

        assertEquals(2, cache.size());
        assertNull(cache.get(PARTITION_0, 0));
        assertNotNull(cache.get(PARTITION_1, 0));
        assertNotNull(cache.get(PARTITION_2, 0));
    }

    @Test
    void multipleKeyEvictionDueToExpiration() {
        ManualClock clock = new ManualClock();
        cache = new MemoryBatchCoordinateCache(Duration.ofSeconds(30), clock);

        // t=0: put PARTITION_0 and PARTITION_1
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        cache.put(createBatch(PARTITION_1, 0, 10, 0));
        assertEquals(2, cache.size());

        // t=10: put PARTITION_1
        clock.advanceBy(Duration.ofSeconds(10));

        // t=40: add a new batch, triggering eviction of both PARTITION_0 and PARTITION_1
        clock.advanceBy(Duration.ofSeconds(30));
        cache.put( createBatch(PARTITION_2, 0, 10, 0));

        assertEquals(1, cache.size());
        assertNull(cache.get(PARTITION_0, 0));
        assertNull(cache.get(PARTITION_1, 0));
        assertNotNull(cache.get(PARTITION_2, 0));
    }

    @Test
    void removeRemovesAllBatches() {
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        cache.put(createBatch(PARTITION_0, 10, 10, 0));
        cache.put(createBatch(PARTITION_1, 0, 10, 0));
        assertEquals(3, cache.size());

        assertEquals(2, cache.remove(PARTITION_0));

        assertEquals(1, cache.size());
        assertNull(cache.get(PARTITION_0, 0));

        assertNotNull(cache.get(PARTITION_1, 0));
    }

    @Test
    void removeNonExistentPartition() {
        assertEquals(0, cache.remove(PARTITION_0));
        assertEquals(0, cache.size());
    }

    @Test
    void testClose() throws IOException {
        cache.put(createBatch(PARTITION_0, 0, 9, 0));
        cache.put(createBatch(PARTITION_1, 0, 9, 0));
        assertEquals(2, cache.size());

        cache.close();

        assertEquals(0, cache.size());
        assertNull(cache.get(PARTITION_0, 0));
        assertNull(cache.get(PARTITION_1, 0));
    }

    @Test
    void testMultipleCaches() {
        // Simulate a scenario where a single producer produces to the same partition by calling 2 different brokers.

        // Cache of the first broker
        var cache1 = new MemoryBatchCoordinateCache(Duration.ofSeconds(30), Clock.systemUTC());
        // Cache of the second broker
        var cache2 = new MemoryBatchCoordinateCache(Duration.ofSeconds(30), Clock.systemUTC());

        // Producer creates 5 batches
        var batch1 = createBatch(PARTITION_0, 0, 10, 0);
        var batch2 = createBatch(PARTITION_0, 10, 10, 0);
        var batch3 = createBatch(PARTITION_0, 20, 10, 0);
        var batch4 = createBatch(PARTITION_0, 40, 10, 0);
        var batch5 = createBatch(PARTITION_0, 50, 10, 0);

        // Produce first batch to broker 1
        cache1.put(batch1);
        // Produce second batch to broker 2
        cache2.put(batch2);
        // Produce third batch to broker 1.
        // Cache notices that a batch is missing and invalidates the entry for this partition
        cache1.put(batch3);
        // Produce fourth batch to broker 2
        // Cache notices that a batch is missing and invalidates the entry for this partition
        cache2.put(batch4);
        // Produce fifth batch to broker 2, appending to the fourth batch because they're contiguous
        cache2.put(batch5);

        assertEquals(1, cache1.size());
        var logFragmentFromCache1 = cache1.get(PARTITION_0);
        assertEquals(20, logFragmentFromCache1.firstOffset());
        assertEquals(30, logFragmentFromCache1.highWaterMark());

        assertEquals(2, cache2.size());
        var logFragmentFromCache2 = cache2.get(PARTITION_0);
        assertEquals(40, logFragmentFromCache2.firstOffset());
        assertEquals(60, logFragmentFromCache2.highWaterMark());

        var batch6 = createBatch(PARTITION_0, 50, 10, 0);
    }

}