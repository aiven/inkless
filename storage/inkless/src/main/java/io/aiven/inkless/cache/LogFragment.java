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

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.BatchCoordinate;

public class LogFragment {
    private final TopicIdPartition topicIdPartition;
    private final LinkedList<TimedBatchCoordinate> batches;
    private long logStartOffset;
    private long highWaterMark;
    private final Duration ttl;
    private final Clock clock;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param expirationTime Unix timestamp in ms.
     */
    private record TimedBatchCoordinate(BatchCoordinate batch, long expirationTime) {
        boolean isExpired(long now) {
            return now > expirationTime;
        }
    }

    private LogFragment(TopicIdPartition topicIdPartition, LinkedList<TimedBatchCoordinate> batches, long logStartOffset, long highWaterMark, Duration ttl, Clock clock) {
        if (topicIdPartition == null) {
            throw new IllegalArgumentException("topicIdPartition cannot be null");
        }
        this.topicIdPartition = topicIdPartition;
        if (batches == null) {
            throw new IllegalArgumentException("batches cannot be null");
        }
        this.batches = batches;
        if (logStartOffset < 0) {
            throw new IllegalArgumentException("logStartOffset cannot be negative");
        }
        this.logStartOffset = logStartOffset;
        if (highWaterMark < 0) {
            throw new IllegalArgumentException("highWaterMark cannot be negative");
        } else if (highWaterMark < logStartOffset) {
            throw new IllegalArgumentException("highWaterMark cannot be less than logStartOffset");
        }
        this.highWaterMark = highWaterMark;
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be a positive duration.");
        }
        this.ttl = ttl;
        if (clock == null) {
            throw new IllegalArgumentException("Clock must not be null.");
        }
        this.clock = clock;
    }

    public LogFragment(TopicIdPartition topicIdPartition, long logStartOffset, Duration ttl, Clock clock) {
        this(topicIdPartition, new LinkedList<>(), logStartOffset, logStartOffset, ttl, clock);
    }

    private LogFragment subFragment(LinkedList<TimedBatchCoordinate> batches) {
        return new LogFragment(topicIdPartition, batches, logStartOffset, highWaterMark, ttl, clock);
    }

    protected long addBatch(BatchCoordinate batch) throws StaleCacheEntryException {
        if (!batch.topicIdPartition().equals(topicIdPartition)) {
            throw new IllegalArgumentException("Batch coordinate topic id partition does not match LogFragment topic id partition");
        }

        lock.writeLock().lock();
        try {
            if (batch.baseOffset() < highWaterMark || (!this.isEmpty() && batch.baseOffset() != highWaterMark)) {
                throw new NonContiguousLogFragmentException(this, batch);
            } else if (batch.logStartOffset() > logStartOffset) {
                throw new LogTruncationException(this, batch);
            } else if (batch.logStartOffset() < logStartOffset) {
                throw new IllegalStateException("Log start offset of new batch ("+ batch.logStartOffset() +") < current log start offset (" + logStartOffset + ")");
            }

            final long expirationTime = clock.millis() + ttl.toMillis();
            batches.add(new TimedBatchCoordinate(batch, expirationTime));

            highWaterMark = batch.lastOffset() + 1;
            return highWaterMark;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void truncateLog(long newLogStartOffset) {
        lock.writeLock().lock();
        try {
            if (newLogStartOffset == logStartOffset) {
                return;
            }
            if (newLogStartOffset > highWaterMark) {
                throw new IllegalArgumentException("New log start offset exceeds high watermark");
            }
            while (!batches.isEmpty() && batches.peekFirst().batch.lastOffset() < newLogStartOffset) {
                batches.removeFirst();
            }
            this.logStartOffset = newLogStartOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected int evictExpired() {
        long now = clock.millis();
        lock.writeLock().lock();
        try {
            int removedCount = 0;
            while (!batches.isEmpty() && batches.peekFirst().isExpired(now)) {
                batches.removeFirst();
                removedCount++;
            }
            return removedCount;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LogFragment subFragment(long startOffset) {
        lock.readLock().lock();
        try {
            var firstOffset = firstOffset();
            if (firstOffset == null || firstOffset > startOffset) {
                return null;
            }
            if (startOffset > highWaterMark) {
                return subFragment(new LinkedList<>());
            }

            final long now = clock.millis();

            final LinkedList<TimedBatchCoordinate> subList = batches.stream()
                // First, only consider batches that are not expired.
                .filter(timedBatch -> !timedBatch.isExpired(now))
                // Next, drop all valid batches that end before our desired startOffset.
                .dropWhile(timedBatch -> timedBatch.batch.lastOffset() < startOffset)
                // Finally, collect the remaining batches into a list.
                .collect(Collectors.toCollection(LinkedList::new));

            return subFragment(subList);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Long firstOffset() {
        lock.readLock().lock();
        try {
            if (batches.isEmpty()) {
                return null;
            }
            return batches.peekFirst().batch.baseOffset();
        } finally {
            lock.readLock().unlock();
        }
    }

    public LinkedList<BatchCoordinate> batches() {
        lock.readLock().lock();
        try {
            return batches.stream()
                .map(timedBatch -> timedBatch.batch)
                .collect(Collectors.toCollection(LinkedList::new));
        } finally {
            lock.readLock().unlock();
        }
    }

    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    public long highWaterMark() {
        lock.readLock().lock();
        try {
            return highWaterMark;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long logStartOffset() {
        lock.readLock().lock();
        try {
            return logStartOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return batches.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return batches.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        return "LogFragment[" + topicIdPartition + ": [" + logStartOffset() + ", " + highWaterMark() + ")]";
    }
}