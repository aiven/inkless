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

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;

import io.aiven.inkless.control_plane.BatchCoordinate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogFragmentTest {

    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
    private static final Clock clock = Clock.systemUTC();

    private BatchCoordinate createBatch(long baseOffset, int recordCount) {
        return createBatch(baseOffset, recordCount, 0L);
    }

    private BatchCoordinate createBatch(long baseOffset, int recordCount, long logStartOffset) {
        return new BatchCoordinate(
            TOPIC_ID_PARTITION,
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
    void addBatchWithMismatchedTopicIdPartitionThrowsException() {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        TopicIdPartition otherPartition = new TopicIdPartition(Uuid.randomUuid(), 1, "other-topic");
        BatchCoordinate mismatchedBatch = new BatchCoordinate(otherPartition, "key", 0, 100, 0, 1, TimestampType.NO_TIMESTAMP_TYPE, 0, (byte) 2, 0);

        assertThrows(IllegalArgumentException.class, () -> logFragment.addBatch(mismatchedBatch));
    }

    @Test
    void logFragmentWithLSOEqualsZero() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.highWaterMark());
        assertEquals(TOPIC_ID_PARTITION, logFragment.topicIdPartition());

        // Add new batch: increase HW
        BatchCoordinate batch1 = createBatch(0, 10); // [0-9]
        long newHighWaterMark = logFragment.addBatch(batch1);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(10L, newHighWaterMark);
        assertEquals(10L, logFragment.highWaterMark());

        // New batch must be contiguous
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(11, 5)));
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(8, 5)));

        // Cannot add a new batch that has LSO != current LSO
        assertThrows(LogTruncationException.class, () -> logFragment.addBatch(createBatch(10, 5, 6)));

        // Add new batch: increase HW
        BatchCoordinate batch2 = createBatch(10, 10); // [10-19]
        newHighWaterMark = logFragment.addBatch(batch2);

        assertFalse(logFragment.isEmpty());
        assertEquals(2, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(20L, newHighWaterMark);
        assertEquals(20L, logFragment.highWaterMark());
    }

    @Test
    void logFragmentWithLSOGreaterThanZero() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 50, Duration.ofSeconds(1), clock);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(50, logFragment.logStartOffset());
        assertEquals(50, logFragment.highWaterMark());
        assertEquals(TOPIC_ID_PARTITION, logFragment.topicIdPartition());

        // Cannot add new batch that has a base offset < HW
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(10, 5, 50)));
        // Cannot add a new batch that has LSO != current LSO
        assertThrows(IllegalStateException.class, () -> logFragment.addBatch(createBatch(50, 5, 40)));
        assertThrows(LogTruncationException.class, () -> logFragment.addBatch(createBatch(50, 5, 60)));

        // Add new batch: increase HW
        BatchCoordinate batch1 = createBatch(50, 10, 50); // [50-59]
        long newHighWaterMark = logFragment.addBatch(batch1);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(50L, logFragment.logStartOffset());
        assertEquals(60L, newHighWaterMark);
        assertEquals(60L, logFragment.highWaterMark());

        // New batch must be contiguous
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(30, 5, 50)));
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(70, 5, 50)));

        // Add new batch: increase HW
        BatchCoordinate batch2 = createBatch(60, 10, 50); // [60-69]
        newHighWaterMark = logFragment.addBatch(batch2);

        assertFalse(logFragment.isEmpty());
        assertEquals(2, logFragment.size());
        assertEquals(50L, logFragment.logStartOffset());
        assertEquals(70L, newHighWaterMark);
        assertEquals(70L, logFragment.highWaterMark());
    }

    @Test
    void logFragmentWithLSOEqualsZeroJumpToMiddle() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.highWaterMark());
        assertEquals(TOPIC_ID_PARTITION, logFragment.topicIdPartition());

        // If LogFragment is empty it's possible to add a new batch with base offset > HW
        BatchCoordinate batch = createBatch(100, 10); // [100-109]
        long newHighWaterMark = logFragment.addBatch(batch);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(110L, newHighWaterMark);
        assertEquals(110L, logFragment.highWaterMark());
    }

    @Test
    void logFragmentWithLSOGreaterThanZeroJumpToMiddle() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 50, Duration.ofSeconds(1), clock);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(50, logFragment.logStartOffset());
        assertEquals(50, logFragment.highWaterMark());
        assertEquals(TOPIC_ID_PARTITION, logFragment.topicIdPartition());

        // Cannot add new batch with base offset < HW
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(30, 5, 50)));

        // If LogFragment is empty it's possible to add a new batch with starting offset > HW
        BatchCoordinate batch = createBatch(100, 10, 50); // [100-109]
        long newHighWaterMark = logFragment.addBatch(batch);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(50L, logFragment.logStartOffset());
        assertEquals(110L, newHighWaterMark);
        assertEquals(110L, logFragment.highWaterMark());
    }

    @Test
    void truncateLogRemovesBatchesEntirelyBeforeNewStartOffset() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        logFragment.addBatch(createBatch(0, 10));   // [0-9]
        logFragment.addBatch(createBatch(10, 10));  // [10-19]
        logFragment.addBatch(createBatch(20, 10));  // [20-29]

        assertEquals(3, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.batches().getFirst().baseOffset());

        // Remove first two batches
        logFragment.truncateLog(20);

        assertEquals(1, logFragment.size());
        assertEquals(20, logFragment.logStartOffset());
        assertEquals(20, logFragment.firstOffset());
    }

    @Test
    void truncateLogWhenOffsetIsInMiddleOfBatchDoesNotRemoveBatch() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        logFragment.addBatch(createBatch(0, 10));  // [0-9]
        logFragment.addBatch(createBatch(10, 10)); // [10-19]

        logFragment.truncateLog(5);

        assertEquals(2, logFragment.size());
        assertEquals(5, logFragment.logStartOffset());
        assertEquals(0, logFragment.firstOffset());
    }

    @Test
    void truncateLogToHigherThanAllOffsetsClearsBatches() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        logFragment.addBatch(createBatch(0, 10));  // [0-9]
        logFragment.addBatch(createBatch(10, 10)); // [10-19]

        logFragment.truncateLog(20);

        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(20, logFragment.logStartOffset());
    }

    @Test
    void truncateLogOverHighWatermarkThrowsException() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        logFragment.addBatch(createBatch(0, 10));  // [0-9]
        logFragment.addBatch(createBatch(10, 10)); // [10-19]

        assertThrows(IllegalArgumentException.class, () -> logFragment.truncateLog(50));

        assertEquals(2, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.firstOffset());
    }

    @Test
    void subFragmentOnEmptyLogReturnsNull() {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        assertNull(logFragment.subFragment(0));
    }

    @Test
    void subFragmentWhenStartOffsetIsInMiddleOfBatchReturnsCorrectSublist() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        BatchCoordinate batch1 = createBatch(0, 10);
        BatchCoordinate batch2 = createBatch(10, 10);
        BatchCoordinate batch3 = createBatch(20, 10);
        logFragment.addBatch(batch1);
        logFragment.addBatch(batch2);
        logFragment.addBatch(batch3);

        LogFragment subFragment = logFragment.subFragment(15);

        assertEquals(3, logFragment.size()); // Original fragment size should remain unchanged

        assertNotNull(subFragment);
        assertEquals(2, subFragment.size());
        assertEquals(batch2, subFragment.batches().get(0));
        assertEquals(batch3, subFragment.batches().get(1));
        assertEquals(0, subFragment.logStartOffset()); // Should inherit from original
        assertEquals(30, subFragment.highWaterMark()); // Should inherit from original
        assertEquals(TOPIC_ID_PARTITION, subFragment.topicIdPartition()); // Should inherit
    }

    @Test
    void subFragmentWhenStartOffsetIsTooHighReturnsEmptyFragment() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        // HWM = 10
        logFragment.addBatch(createBatch(0, 10));

        LogFragment subFragment = logFragment.subFragment(11);

        assertNotNull(subFragment);
        assertTrue(subFragment.isEmpty());
        assertEquals(0, subFragment.logStartOffset());
        assertEquals(10, subFragment.highWaterMark());
        assertEquals(TOPIC_ID_PARTITION, subFragment.topicIdPartition());
    }

    @Test
    void subFragmentWhenStartOffsetIsNotFoundBeforeFirstBatchReturnsNull() throws StaleCacheEntryException {
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        logFragment.addBatch(createBatch(0, 5)); // [0-4]
        logFragment.addBatch(createBatch(5, 5)); // [5-9]

        // This truncateLog will make logStartOffset = 5, but the first batch is still [0-4] in the list
        // and its lastOffset is 4, which is < 5, so it's removed.
        // The current logic of truncateLog will remove batch [0-4], leaving batch [5-9] as first.
        logFragment.truncateLog(5);
        assertEquals(1, logFragment.size()); // Only batch [5-9] remains
        assertEquals(5, logFragment.logStartOffset());
        assertEquals(5, logFragment.firstOffset()); // First offset is now 5

        LogFragment subFragment = logFragment.subFragment(3); // Looking for offset 3, which is before current firstOffset (5)

        assertNull(subFragment);
    }

    @Test
    void evictOldestBatchOnNonEmptyFragmentRemovesFirstBatch() throws StaleCacheEntryException {
        var clock = new ManualClock();
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(30), clock);

        BatchCoordinate batch1 = createBatch(0, 10);
        logFragment.addBatch(batch1);

        clock.advanceBy(Duration.ofSeconds(40));
        BatchCoordinate batch2 = createBatch(10, 10);
        logFragment.addBatch(batch2);

        long originalHwm = logFragment.highWaterMark();
        long originalLogStartOffset = logFragment.logStartOffset(); // This is 0, as not changed by addBatch

        int evicted = logFragment.evictExpired();

        assertEquals(1, evicted);
        assertEquals(1, logFragment.size());
        assertEquals(batch2, logFragment.batches().getFirst());
        assertEquals(originalHwm, logFragment.highWaterMark());
        assertEquals(originalLogStartOffset, logFragment.logStartOffset()); // logStartOffset does not change
    }

    @Test
    void evictOldestBatchOnEmptyFragmentReturnsFalse() {
        var clock = new ManualClock();
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(30), clock);
        int evicted = logFragment.evictExpired();

        assertEquals(0, evicted);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.highWaterMark());
    }

    @Test
    void subFragmentWhenStartOffsetIsLessThanFirstBatchOffsetReturnsNull() throws StaleCacheEntryException {
        var clock = new ManualClock();
        LogFragment logFragment = new LogFragment(TOPIC_ID_PARTITION, 0, Duration.ofSeconds(1), clock);
        logFragment.addBatch(createBatch(0, 5)); // [0-4]
        clock.advanceBy(Duration.ofSeconds(5));
        logFragment.addBatch(createBatch(5, 5)); // [5-9]

        logFragment.evictExpired(); // Evicts [0-4], leaving [5-9]
        assertEquals(1, logFragment.size());
        assertEquals(5, logFragment.firstOffset());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(10, logFragment.highWaterMark());

        assertNull(logFragment.subFragment(3));
    }
}
