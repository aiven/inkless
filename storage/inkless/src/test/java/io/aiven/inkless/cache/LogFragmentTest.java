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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogFragmentTest {

    private static final Time time = Time.SYSTEM;

    private CacheBatchCoordinate createBatch(long baseOffset, int recordCount) {
        return createBatch(baseOffset, recordCount, 0L);
    }

    private CacheBatchCoordinate createBatch(long baseOffset, int recordCount, long logStartOffset) {
        return new CacheBatchCoordinate(
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
    void logFragmentWithLSOEqualsZero() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(1), time);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.highWaterMark());

        // Add new batch: increase HW
        CacheBatchCoordinate batch1 = createBatch(0, 10); // [0-9]
        long newHighWaterMark = logFragment.addBatch(batch1);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(10L, newHighWaterMark);
        assertEquals(10L, logFragment.highWaterMark());

        // Adding a new batch that has base offset < current HW is invalid
        assertThrows(IllegalArgumentException.class, () -> logFragment.addBatch(createBatch(8, 5)));
        // New batch must be contiguous
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(11, 5)));
        // Cannot add a new batch that has LSO != current LSO
        assertThrows(IncreasedLogStartOffsetException.class, () -> logFragment.addBatch(createBatch(10, 5, 6)));

        // Add new batch: increase HW
        CacheBatchCoordinate batch2 = createBatch(10, 10); // [10-19]
        newHighWaterMark = logFragment.addBatch(batch2);

        assertFalse(logFragment.isEmpty());
        assertEquals(2, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(20L, newHighWaterMark);
        assertEquals(20L, logFragment.highWaterMark());
    }

    @Test
    void logFragmentWithLSOGreaterThanZero() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(50, Duration.ofSeconds(1), time);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(50, logFragment.logStartOffset());
        assertEquals(50, logFragment.highWaterMark());

        // Adding a new batch that has LSO < current LSO is invalid
        assertThrows(IllegalArgumentException.class, () -> logFragment.addBatch(createBatch(50, 5, 40)));
        // Cannot add new batch that has a base offset < HW
        assertThrows(IllegalArgumentException.class, () -> logFragment.addBatch(createBatch(10, 5, 50)));
        // Cannot add a new batch that has LSO > current LSO
        assertThrows(IncreasedLogStartOffsetException.class, () -> logFragment.addBatch(createBatch(50, 5, 60)));

        // Add new batch: increase HW
        CacheBatchCoordinate batch1 = createBatch(50, 10, 50); // [50-59]
        long newHighWaterMark = logFragment.addBatch(batch1);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(50L, logFragment.logStartOffset());
        assertEquals(60L, newHighWaterMark);
        assertEquals(60L, logFragment.highWaterMark());

        // Adding a new batch that has base offset < current HW is invalid
        assertThrows(IllegalArgumentException.class, () -> logFragment.addBatch(createBatch(30, 5, 50)));
        // New batch must be contiguous
        assertThrows(NonContiguousLogFragmentException.class, () -> logFragment.addBatch(createBatch(70, 5, 50)));

        // Add new batch: increase HW
        CacheBatchCoordinate batch2 = createBatch(60, 10, 50); // [60-69]
        newHighWaterMark = logFragment.addBatch(batch2);

        assertFalse(logFragment.isEmpty());
        assertEquals(2, logFragment.size());
        assertEquals(50L, logFragment.logStartOffset());
        assertEquals(70L, newHighWaterMark);
        assertEquals(70L, logFragment.highWaterMark());
    }

    @Test
    void logFragmentWithLSOEqualsZeroJumpToMiddle() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(1), time);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.highWaterMark());

        // If LogFragment is empty it's possible to add a new batch with base offset > HW
        CacheBatchCoordinate batch = createBatch(100, 10); // [100-109]
        long newHighWaterMark = logFragment.addBatch(batch);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(110L, newHighWaterMark);
        assertEquals(110L, logFragment.highWaterMark());
    }

    @Test
    void logFragmentWithLSOGreaterThanZeroJumpToMiddle() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(50, Duration.ofSeconds(1), time);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(50, logFragment.logStartOffset());
        assertEquals(50, logFragment.highWaterMark());

        // Cannot add new batch with base offset < HW
        assertThrows(IllegalArgumentException.class, () -> logFragment.addBatch(createBatch(30, 5, 50)));

        // If LogFragment is empty it's possible to add a new batch with starting offset > HW
        CacheBatchCoordinate batch = createBatch(100, 10, 50); // [100-109]
        long newHighWaterMark = logFragment.addBatch(batch);

        assertFalse(logFragment.isEmpty());
        assertEquals(1, logFragment.size());
        assertEquals(50L, logFragment.logStartOffset());
        assertEquals(110L, newHighWaterMark);
        assertEquals(110L, logFragment.highWaterMark());
    }


    @Test
    void subFragmentOnEmptyLogReturnsNull() {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(1), time);
        assertNull(logFragment.subFragment(0));
    }

    @Test
    void subFragmentWhenStartOffsetIsInMiddleOfBatchReturnsCorrectSublist() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(1), time);
        CacheBatchCoordinate batch1 = createBatch(0, 10);
        CacheBatchCoordinate batch2 = createBatch(10, 10);
        CacheBatchCoordinate batch3 = createBatch(20, 10);
        logFragment.addBatch(batch1);
        logFragment.addBatch(batch2);
        logFragment.addBatch(batch3);
        // LogFragment current state: [0-9] -> [10-19] -> [20-29]

        LogFragment subFragment = logFragment.subFragment(15);

        // Original fragment size should remain unchanged
        assertEquals(3, logFragment.size());

        assertNotNull(subFragment);
        assertEquals(2, subFragment.size());
        assertEquals(batch2, subFragment.batches().get(0));
        assertEquals(batch3, subFragment.batches().get(1));
        assertEquals(0, subFragment.logStartOffset());
        assertEquals(30, subFragment.highWaterMark());
    }

    @Test
    void subFragmentWhenStartOffsetIsTooHighReturnsEmptyFragment() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(1), time);
        logFragment.addBatch(createBatch(0, 10)); // HWM = 10

        LogFragment subFragment1 = logFragment.subFragment(10);
        assertNotNull(subFragment1);
        assertTrue(subFragment1.isEmpty());
        assertEquals(0, subFragment1.logStartOffset());
        assertEquals(10, subFragment1.highWaterMark());


        LogFragment subFragment2 = logFragment.subFragment(99);
        assertNotNull(subFragment2);
        assertTrue(subFragment2.isEmpty());
        assertEquals(0, subFragment2.logStartOffset());
        assertEquals(10, subFragment2.highWaterMark());
    }

    @Test
    void subFragmentWhenStartOffsetIsNotFoundBeforeFirstBatchReturnsNull() throws StaleLogFragmentException {
        LogFragment logFragment = new LogFragment(5, Duration.ofSeconds(1), time);
        logFragment.addBatch(createBatch(5, 10, 5)); // [5-14]

        LogFragment subFragment = logFragment.subFragment(3);
        assertNull(subFragment);
    }

    @Test
    void evictOnNonEmptyFragmentRemovesExpiredBatch() throws StaleLogFragmentException {
        var time = new MockTime();
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(30), time);

        CacheBatchCoordinate batch1 = createBatch(0, 10); // [0-9], expires at T=30
        logFragment.addBatch(batch1);
        assertEquals(0, logFragment.evictExpired()); // No batch is expired yet

        // T=40, batch1 is expired
        time.sleep(40 * 1000);
        CacheBatchCoordinate batch2 = createBatch(10, 10); // [10-19], expires at T=70
        logFragment.addBatch(batch2);

        long originalHwm = logFragment.highWaterMark(); // 20
        long originalLogStartOffset = logFragment.logStartOffset(); // This is 0, as is not changed by addBatch

        int evicted = logFragment.evictExpired();

        assertEquals(1, evicted);
        assertEquals(1, logFragment.size());
        assertEquals(batch2, logFragment.batches().getFirst());
        assertEquals(originalHwm, logFragment.highWaterMark());
        assertEquals(originalLogStartOffset, logFragment.logStartOffset());
    }

    @Test
    void evictOnEmptyFragmentReturnsFalse() {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(30), time);
        int evicted = logFragment.evictExpired();

        assertEquals(0, evicted);
        assertTrue(logFragment.isEmpty());
        assertEquals(0, logFragment.size());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(0, logFragment.highWaterMark());
    }

    @Test
    void subFragmentWhenStartOffsetIsLessThanFirstBatchOffsetReturnsNull() throws StaleLogFragmentException {
        var time = new MockTime();
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(1), time);
        logFragment.addBatch(createBatch(0, 5)); // [0-4], expires at T=1

        // T=5, first batch is expired
        time.sleep(5 * 1000);
        logFragment.addBatch(createBatch(5, 5)); // [5-9], expires at T=6

        logFragment.evictExpired(); // Evicts [0-4], leaving [5-9]
        assertEquals(1, logFragment.size());
        assertEquals(5, logFragment.firstOffset());
        assertEquals(0, logFragment.logStartOffset());
        assertEquals(10, logFragment.highWaterMark());

        assertNull(logFragment.subFragment(3));
    }

    @Test
    void subFragmentOnEmptyFragmentReturnsNull() {
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(30), time);
        assertNull(logFragment.subFragment(0));
    }

    @Test
    void subFragmentReturnsNullIfFirstMatchingBatchIsExpired() throws StaleLogFragmentException {
        var time = new MockTime();
        LogFragment logFragment = new LogFragment(0, Duration.ofSeconds(30), time);

        CacheBatchCoordinate batch1 = createBatch(0, 10); // [0-9], expires at T=30
        logFragment.addBatch(batch1);

        // T=5
        time.sleep(5 * 1000);

        CacheBatchCoordinate batch2 = createBatch(10, 10); // [10-19], expires at T=35
        logFragment.addBatch(batch2);

        // T=10
        time.sleep(5 * 1000);

        CacheBatchCoordinate batch3 = createBatch(20, 10); // [20-19], expires at T=40
        logFragment.addBatch(batch3);

        // T=31, first batch is expired
        time.sleep(21 * 1000);

        LogFragment expiredResult1 = logFragment.subFragment(5);
        assertNull(expiredResult1, "Should return null because the first matching batch [0-9] is expired.");

        LogFragment validResult1 = logFragment.subFragment(15);
        assertNotNull(validResult1, "Should return a valid fragment for the non-expired batches [10-19] and [20-29].");
        assertEquals(2, validResult1.size());
        assertEquals(batch2, validResult1.batches().getFirst());

        LogFragment validResult2 = logFragment.subFragment(21);
        assertNotNull(validResult2, "Should return a valid fragment for the non-expired batch [20-29].");
        assertEquals(1, validResult2.size());
        assertEquals(batch3, validResult2.batches().getFirst());

        // T=36, second batch is expired
        time.sleep(5 * 1000);

        LogFragment expiredResult2 = logFragment.subFragment(5);
        assertNull(expiredResult2, "Should return null because the first matching batch [0-9] is expired.");

        LogFragment expiredResult3 = logFragment.subFragment(15);
        assertNull(expiredResult3, "Should return null because the first matching batch [10-19] is expired.");

        LogFragment validResult3 = logFragment.subFragment(25);
        assertNotNull(validResult3, "Should return a valid fragment for the non-expired batch [20-29].");
        assertEquals(1, validResult3.size());
        assertEquals(batch3, validResult3.batches().getFirst());
    }
}
