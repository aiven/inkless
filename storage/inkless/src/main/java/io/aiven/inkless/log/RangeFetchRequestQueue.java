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
package io.aiven.inkless.log;

import io.aiven.inkless.common.ObjectKey;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The queue for range fetch requests.
 *
 * <p>It allows to add requests with the specified delay. Until the delay is expired,
 * the requests for this file is not available for taking.
 *
 * <p>It groups requested ranges per file. I.e. if multiple ranges are requested while a file is in the queue,
 * all the ranges will be returned with {@link RangeFetchRequestQueue#take()}.
 */
class RangeFetchRequestQueue {
    private final Time time;
    private final long delayMs;

    private final Lock lock = new ReentrantLock();
    private final Condition nonEmptyCondition = lock.newCondition();
    private final Map<ObjectKey, List<ByteRangeWithFuture>> requests = new HashMap<>();
    private final DelayQueue<ObjectDelayed> keyQueue = new DelayQueue<>();

    RangeFetchRequestQueue(final Time time, final long delayMs) {
        this.time = Objects.requireNonNull(time, "time cannot be null");

        if (delayMs < 0) {
            throw new IllegalArgumentException("delayMs cannot be negative");
        }
        this.delayMs = delayMs;
    }

    void addRequest(final ObjectKey objectKey,
                    final ByteRangeWithFuture range) {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        Objects.requireNonNull(range, "range cannot be null");

        lock.lock();
        try {
            final List<ByteRangeWithFuture> ranges;
            if (requests.containsKey(objectKey)) {
                ranges = requests.get(objectKey);
            } else {
                ranges = new ArrayList<>();
                requests.put(objectKey, ranges);
                keyQueue.put(new ObjectDelayed(objectKey));
                nonEmptyCondition.signal();
            }
            ranges.add(range);
        } finally {
           lock.unlock();
        }
    }

    RangeFetchRequest take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                final ObjectDelayed objectDelayed = keyQueue.poll();
                if (objectDelayed == null) {
                    nonEmptyCondition.await();
                } else {
                    final List<ByteRangeWithFuture> ranges = requests.remove(objectDelayed.objectKey);
                    if (ranges == null) {
                        throw new IllegalStateException();
                    }
                    return new RangeFetchRequest(objectDelayed.objectKey, ranges);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private class ObjectDelayed implements Delayed {
        private final long endTimeNanos;
        private final ObjectKey objectKey;

        private ObjectDelayed(final ObjectKey objectKey) {
            this.objectKey = Objects.requireNonNull(objectKey, "objectKey cannot be null");
            this.endTimeNanos = time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(delayMs);
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(endTimeNanos - time.nanoseconds(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(final Delayed other) {
            Objects.requireNonNull(other, "other cannot be null");
            ObjectDelayed otherChannel = (ObjectDelayed) other;
            return Long.compare(this.endTimeNanos, otherChannel.endTimeNanos);
        }
    }
}
