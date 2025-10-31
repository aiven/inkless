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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class CaffeineBatchCoordinateCacheTest {

    private static final TopicIdPartition PARTITION_0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
    private static final TopicIdPartition PARTITION_1 = new TopicIdPartition(Uuid.randomUuid(), 1, "topic");
    private static final TopicIdPartition PARTITION_2 = new TopicIdPartition(Uuid.randomUuid(), 2, "topic");

    static CaffeineBatchCoordinateCache cache;

    @Mock
    BatchCoordinateCacheMetrics metricsMock;

    @BeforeEach
    void setUp() {
        cache = new CaffeineBatchCoordinateCache(Duration.ofSeconds(10), Clock.systemUTC(), metricsMock);
    }

    @AfterAll
    static void cleanUp() throws IOException {
        cache.close();
    }


    private CacheBatchCoordinate createBatch(TopicIdPartition topicIdPartition, long baseOffset, int recordCount, long logStartOffset) {
        return new CacheBatchCoordinate(
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
        assertThrows(IllegalArgumentException.class, () -> new CaffeineBatchCoordinateCache(null));
        assertThrows(IllegalArgumentException.class, () -> new CaffeineBatchCoordinateCache(Duration.ofSeconds(0)));
        assertThrows(IllegalArgumentException.class, () -> new CaffeineBatchCoordinateCache(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> new CaffeineBatchCoordinateCache(Duration.ofSeconds(1), null));
    }

    @Test
    void putAndGet() {
        CacheBatchCoordinate batch = createBatch(PARTITION_0, 0, 10, 0);
        cache.put(batch);

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
    void testPutNonContiguousBatchInvalidatesOlderBatches() {
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        assertNotNull(cache.get(PARTITION_0, 0));

        // Expected baseOffset is 10: key is invalidated and a new entry is created
        cache.put(createBatch(PARTITION_0, 20, 10, 0));

        assertNull(cache.get(PARTITION_0, 0), "First batch should be removed after non-contiguous put");
        assertNotNull(cache.get(PARTITION_0, 20));
    }

    @Test
    void testPutWithLogStartOffsetIncreasedInvalidatesEntry() {
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        assertNotNull(cache.get(PARTITION_0, 0));

        cache.put(createBatch(PARTITION_0, 20, 10, 5));
        var logFragment = cache.get(PARTITION_0, 20);
        assertEquals(5, logFragment.logStartOffset());
        assertEquals(30, logFragment.highWaterMark());
    }


    @Test
    void getDoesNotReturnExpiredBatches() {
        ManualClock clock = new ManualClock();
        cache = new CaffeineBatchCoordinateCache(Duration.ofSeconds(30), clock, metricsMock);

        // t=0: put first batch
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        assertNotNull(cache.get(PARTITION_0, 0));

        // t=20: put second batch, the first one is not expired yet
        clock.advanceBy(Duration.ofSeconds(20));
        cache.put(createBatch(PARTITION_0, 10, 10, 0));
        assertEquals(2, cache.get(PARTITION_0, 0).size());

        // t=40: first batch is now expired
        clock.advanceBy(Duration.ofSeconds(20));
        assertNull(cache.get(PARTITION_0, 0));
        assertEquals(1, cache.get(PARTITION_0, 10).size());
        // eviction is not triggered by the get
        verify(metricsMock, never()).recordCacheEviction();

        // putting a new batch will remove the expired one
        cache.put(createBatch(PARTITION_0, 20, 10, 0));
        verify(metricsMock).recordCacheEviction();
    }

    @Test
    void notPossibleToInsertBatchesWithLowerLogStartOffset() {
        cache.put(createBatch(PARTITION_0, 50, 100, 50)); // [50-99]
        assertThrows(IllegalStateException.class, () -> cache.put(createBatch(PARTITION_0, 100, 110, 10)));
        assertNull(cache.get(PARTITION_0, 50));
    }

    @Test
    void notPossibleToInsertOffsetsLowerThanHighWater() {
        cache.put(createBatch(PARTITION_0, 0, 100, 0)); // [0-99]
        assertThrows(IllegalStateException.class, () -> cache.put(createBatch(PARTITION_0, 50, 60, 0))); // [50-59]
        assertNull(cache.get(PARTITION_0, 0));
    }

    @Test
    void removeRemovesAllBatches() {
        cache.put(createBatch(PARTITION_0, 0, 10, 0));
        cache.put(createBatch(PARTITION_0, 10, 10, 0));
        cache.put(createBatch(PARTITION_1, 0, 10, 0));
        assertNotNull(cache.get(PARTITION_0, 0));
        assertNotNull(cache.get(PARTITION_1, 0));

        assertEquals(2, cache.invalidatePartition(PARTITION_0));
        assertNull(cache.get(PARTITION_0, 0));
        assertNotNull(cache.get(PARTITION_1, 0));
    }

    @Test
    void removeNonExistentPartition() {
        assertEquals(0, cache.invalidatePartition(PARTITION_0));
    }

    @Test
    void testClose() throws IOException {
        cache.put(createBatch(PARTITION_0, 0, 9, 0));
        cache.put(createBatch(PARTITION_1, 0, 9, 0));

        cache.close();

        assertNull(cache.get(PARTITION_0, 0));
        assertNull(cache.get(PARTITION_1, 0));
    }

    @Test
    void testMultipleCaches() throws IOException {
        // Simulate a scenario where a single producer produces to the same partition by calling 2 different brokers.

        // Cache of the first broker
        var cache1 = new CaffeineBatchCoordinateCache(Duration.ofSeconds(30), Clock.systemUTC());
        // Cache of the second broker
        var cache2 = new CaffeineBatchCoordinateCache(Duration.ofSeconds(30), Clock.systemUTC());

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

        assertNull(cache1.get(PARTITION_0, 0));
        var logFragmentFromCache1 = cache1.get(PARTITION_0, 20);
        assertEquals(20, logFragmentFromCache1.firstOffset());
        assertEquals(30, logFragmentFromCache1.highWaterMark());

        assertNull(cache2.get(PARTITION_0, 0));
        var logFragmentFromCache2 = cache2.get(PARTITION_0, 40);
        assertEquals(40, logFragmentFromCache2.firstOffset());
        assertEquals(60, logFragmentFromCache2.highWaterMark());

        cache1.close();
        cache2.close();
    }

}