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

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import io.aiven.inkless.control_plane.BatchCoordinate;

public class LogFragment {
    private final TopicIdPartition topicIdPartition;
    private final LinkedList<BatchCoordinate> batches;
    private long logStartOffset;
    private long highWaterMark;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LogFragment(TopicIdPartition topicIdPartition) {
        this(topicIdPartition, new LinkedList<>(), 0, 0);
    }

    public LogFragment(TopicIdPartition topicIdPartition, long logStartOffset) {
        this.topicIdPartition = topicIdPartition;
        this.batches = new LinkedList<>();
        this.logStartOffset = logStartOffset;
        this.highWaterMark = logStartOffset;
    }

    private LogFragment(TopicIdPartition topicIdPartition, LinkedList<BatchCoordinate> batches, long logStartOffset, long highWaterMark) {
        this.topicIdPartition = topicIdPartition;
        this.batches = batches;
        this.logStartOffset = logStartOffset;
        this.highWaterMark = highWaterMark;
    }

    private LogFragment subList(LinkedList<BatchCoordinate> batches) {
        return new LogFragment(topicIdPartition, batches, logStartOffset, highWaterMark);
    }

    public long addBatch(BatchCoordinate batch) throws NonContiguousLogFragmentException {
        if (!batch.topicIdPartition().equals(topicIdPartition)) {
            throw new IllegalArgumentException("Batch coordinate topic id partition does not match LogFragment topic id partition");
        }

        lock.writeLock().lock();
        try {
            // New batch must be contiguous to the already existing batches, if those exist.
            if (batch.baseOffset() < highWaterMark || (!this.isEmpty() && batch.baseOffset() != highWaterMark)) {
                throw new NonContiguousLogFragmentException(this, batch);
            }
            if (batch.logStartOffset() != logStartOffset) {
                throw new IllegalStateException("Log start offset of new batch ("+ batch.logStartOffset() +") is not equal to the current log start offset (" + logStartOffset + ")");
            }
            batches.add(batch);
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
            while (!batches.isEmpty() && batches.peekFirst().lastOffset() < newLogStartOffset) {
                batches.removeFirst();
            }
            this.logStartOffset = newLogStartOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns a subset of the current LogFragment, containing the batch that contains the specified
     * start offset and all the subsequent batches.
     * @param startOffset The offset that must be contained in the first batch.
     * @return A new LogFragment containing the matching sublist of batches, null if this LogFragment is empty or
     * the first batch does not contain the start offset.
     *
     */
    public LogFragment subFragment(long startOffset) {
        lock.readLock().lock();
        try {
            var firstOffset = firstOffset();
            if (firstOffset == null || firstOffset > startOffset) {
                return null;
            }
            if (startOffset > highWaterMark) {
                return subList(new LinkedList<>());
            }

            Predicate<BatchCoordinate> filter = batchCoordinate -> startOffset <= batchCoordinate.lastOffset() && startOffset >= batchCoordinate.baseOffset();
            LinkedList<BatchCoordinate> subList = new LinkedList<>();
            ListIterator<BatchCoordinate> iterator = new LinkedList<>(batches).listIterator();
            while (iterator.hasNext()) {
                BatchCoordinate currentElement = iterator.next();
                if (filter.test(currentElement)) {
                    subList.add(currentElement);
                    while (iterator.hasNext()) {
                        subList.add(iterator.next());
                    }
                }
            }
            return subList(subList);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean evictOldestBatch() {
        lock.writeLock().lock();
        try {
            if (batches.isEmpty()) {
                return false;
            }
            batches.removeFirst();
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Long firstOffset() {
        lock.readLock().lock();
        try {
            if (batches.isEmpty()) {
                return null;
            }
            return batches.peekFirst().baseOffset();
        } finally {
            lock.readLock().unlock();
        }
    }

    public LinkedList<BatchCoordinate> batches() {
        lock.readLock().lock();
        try {
            return new LinkedList<>(batches);
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