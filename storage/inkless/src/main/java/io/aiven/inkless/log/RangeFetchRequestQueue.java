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
 * all the ranges will be returned with {@link RangeFetchRequestQueue#poll(long, TimeUnit)}.
 */
class RangeFetchRequestQueue {
    private final Time time;
    private final long delayMs;

    private final DelayQueue<FetchRequestsDelayed> requestQueue = new DelayQueue<>();
    private final Map<ObjectKey, FetchRequestsDelayed> requestMap = new HashMap<>();
    /**
     * The lock that protects access to requestQueue and requestMap.
     */
    private final Lock lock = new ReentrantLock();
    /**
     * The condition that the queue has elements.
     */
    private final Condition nonEmpty = lock.newCondition();

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
            final FetchRequestsDelayed fetchRequests;
            if (requestMap.containsKey(objectKey)) {
                fetchRequests = requestMap.get(objectKey);
            } else {
                fetchRequests = new FetchRequestsDelayed(objectKey);
                requestMap.put(objectKey, fetchRequests);
                requestQueue.put(fetchRequests);
                nonEmpty.signal();
            }
            fetchRequests.addRange(range);
        } finally {
           lock.unlock();
        }
    }

    RangeFetchRequests poll(final long delay, final TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(delay);
        
        lock.lockInterruptibly();
        try {
            do {
                final FetchRequestsDelayed requests = requestQueue.poll();
                if (requests != null) {
                    requestMap.remove(requests.objectKey);
                    return new RangeFetchRequests(requests.objectKey, requests.ranges);
                }
            } while ((remainingNanos = nonEmpty.awaitNanos(remainingNanos)) > 0);
            return null;
        } finally {
            lock.unlock();
        }
    }

    private class FetchRequestsDelayed implements Delayed {
        private final long endTimeNanos;
        private final ObjectKey objectKey;
        private final List<ByteRangeWithFuture> ranges;

        private FetchRequestsDelayed(final ObjectKey objectKey) {
            this.objectKey = Objects.requireNonNull(objectKey, "objectKey cannot be null");
            this.endTimeNanos = time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(delayMs);
            this.ranges = new ArrayList<>();
        }

        private void addRange(final ByteRangeWithFuture range) {
            ranges.add(
                Objects.requireNonNull(range, "range cannot be null")
            );
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(endTimeNanos - time.nanoseconds(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(final Delayed other) {
            Objects.requireNonNull(other, "other cannot be null");
            final FetchRequestsDelayed otherChannel = (FetchRequestsDelayed) other;
            return Long.compare(this.endTimeNanos, otherChannel.endTimeNanos);
        }
    }
}
