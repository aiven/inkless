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
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Represents a thread-safe, in-memory view of a <b>contiguous fragment</b> of the batch coordinates
 * that constitute the metadata of a diskless TopicPartition.
 *
 * <p>This class stores a list of {@link CacheBatchCoordinate} objects. Its primary purpose
 * is to track the coordinates of the batches that constitute the tail of a diskless log,
 * and provide fast, offset-based read access.
 *
 * <p>The fragment maintains a list of batches, each with an expiration time. This allows
 * it to function as a time-aware cache over the log's most recent metadata.
 *
 * <p>This class is designed to be fully <b>thread-safe</b>; it uses internal locking
 * to allow for concurrent, non-blocking reads while ensuring that all write operations
 * (like adding new batches or clearing old ones) are exclusive and atomic.
 *
 * <p>It maintains a <b>contiguous</b> view of the log. New batches are
 * only accepted if their starting offset *exactly* matches the fragment's current
 * high-water mark. This check guarantees that the fragment never contains gaps
 * or overlapping data, thus representing a sequential "fragment" of the log.
 *
 * <p>To manage memory, the fragment also implements a <b>time-to-live (TTL)</b> policy.
 * Each batch is tagged with an expiration timestamp when it's added. A separate
 * eviction process ({@link #evictExpired()}) can then be called to efficiently
 * remove any stale batches from the oldest end (the head) of the list.
 * This mechanism maintains prevents the cache from growing indefinitely and guarantees
 * that it will not contain stale entries (for example batches that are not anymore present
 * due to retention).
 */
public class LogFragment {
    private final TopicIdPartition topicIdPartition;
    private final LinkedList<TimedBatchCoordinate> batches;
    private final long logStartOffset;
    private long highWaterMark;
    private final Duration ttl;
    private final Clock clock;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param expirationTime Unix timestamp in ms.
     */
    private record TimedBatchCoordinate(CacheBatchCoordinate batch, long expirationTime) {
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

    /**
     * Appends the coordinates of a new batch to the end of this log fragment, advancing the high-water mark.
     *
     * <p>It stringently enforces log <b>contiguity</b>, ensuring the new batch
     * fits perfectly onto the end of the fragment. Any batch that would create a
     * gap or an overlap is rejected. This ensures the fragment always represents
     * an unbroken sequence of the log.
     *
     * <p>Furthermore, this method detects potential <b>staleness</b>. If the
     * incoming batch indicates that the start offset of the underlying log has moved forward,
     * this fragment is considered obsolete.
     *
     * @param batch The batch coordinate to append.
     * @return The new high-water mark, representing the next expected offset
     * in the log.
     * @throws IllegalArgumentException If the batch does not belong to this fragment's topic partition.
     * @throws IllegalStateException If the batch is out-of-order or its internal offsets are inconsistent with
     * the fragment's state.
     * @throws NonContiguousLogFragmentException If the batch's starting offset does not exactly match the fragment's
     * current end, creating a gap.
     * @throws IncreasedLogStartOffsetException If the batch indicates the log start offset has been increased, making
     * this fragment stale.
     */
    protected long addBatch(CacheBatchCoordinate batch) throws StaleCacheEntryException, IllegalStateException {
        if (!batch.topicIdPartition().equals(topicIdPartition)) {
            throw new IllegalArgumentException("Batch coordinate topic id partition does not match LogFragment topic id partition");
        }

        lock.writeLock().lock();
        try {
            if (batch.logStartOffset() < logStartOffset) {
                throw new IllegalStateException("Log start offset of new batch ("+ batch.logStartOffset() +") < current log start offset (" + logStartOffset + ")");
            } else if (batch.baseOffset() < highWaterMark) {
                throw new IllegalStateException("Base offset of the new batch ("+ batch.baseOffset() +") < high watermark (" + highWaterMark + ")");
            } else if (!this.isEmpty() && batch.baseOffset() != highWaterMark) {
                throw new NonContiguousLogFragmentException(this, batch);
            } else if (batch.logStartOffset() > logStartOffset) {
                throw new IncreasedLogStartOffsetException(this, batch);
            }

            final long expirationTime = clock.millis() + ttl.toMillis();
            batches.add(new TimedBatchCoordinate(batch, expirationTime));

            highWaterMark = batch.lastOffset() + 1;
            return highWaterMark;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Evicts all batches from the *start* of the list (the oldest data)
     * that have surpassed their time-to-live (TTL).
     *
     * @return The total number of batches that were evicted from the fragment.
     */
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

    /**
     * Creates a new, immutable view of this fragment, containing all batches from
     * the requested {@code startOffset} to the end.
     * <ul>
     *      <li>If the requested {@code startOffset} is older than the oldest batch currently held in
     *      this fragment (i.e., it has already been evicted), this method returns {@code null}.</li>
     *
     *      <li>If the *first* batch that would satisfy the request is <b>expired</b> at the time of the request,
     *      it returns null. This prevents returning stale data.</li>
     *
     *      <li>If the {@code startOffset} is valid but newer than any data in this fragment
     *      (i.e., it's beyond the current high-water mark), it returns a valid, <b>empty</b> fragment.</li>
     *
     *      <li>If the {@code startOffset} is found within a valid, non-expired batch,
     *      a new {@code LogFragment} instance is created. This new fragment contains all batches
     *      from the located batch to the end of this fragment.</li>
     * </ul>
     *
     * @param startOffset The target offset from which to begin the new fragment.
     * @return A new {@code LogFragment}, which starts from the batch that contains the
     * {@code startOffset} and then all the subsequent batches contained in this log fragment,
     * or {@code null} if the data is not available or is stale.
     */
    protected LogFragment subFragment(long startOffset) {
        lock.readLock().lock();
        try {
            Long firstOffset = firstOffset();
            if (firstOffset == null || firstOffset > startOffset) {
                return null;
            }
            if (startOffset > highWaterMark) {
                return subFragment(new LinkedList<>());
            }

            final long now = clock.millis();

            ListIterator<TimedBatchCoordinate> iterator = batches.listIterator();

            while (iterator.hasNext()) {
                TimedBatchCoordinate currentBatch = iterator.next();

                // Find the first batch that meets the startOffset requirement
                if (currentBatch.batch.lastOffset() >= startOffset) {
                    if (currentBatch.isExpired(now)) {
                        // If the first batch is expired return a cache miss
                        return null;
                    }

                    LinkedList<TimedBatchCoordinate> subList = new LinkedList<>();
                    subList.add(currentBatch);
                    while (iterator.hasNext()) {
                        subList.add(iterator.next());
                    }
                    return subFragment(subList);
                }
            }

            return null;
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

    public LinkedList<CacheBatchCoordinate> batches() {
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