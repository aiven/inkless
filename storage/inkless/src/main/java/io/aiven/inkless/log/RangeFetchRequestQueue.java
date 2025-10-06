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

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import org.apache.kafka.common.utils.Time;

import java.util.List;
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

    private final RangeBlockingMultiMap requests = new RangeBlockingMultiMap();
    private final DelayQueue<ObjectDelayed> keyQueue = new DelayQueue<>();

    RangeFetchRequestQueue(final Time time, final long delayMs) {
        this.time = Objects.requireNonNull(time, "time cannot be null");

        if (delayMs < 0) {
            throw new IllegalArgumentException("delayMs cannot be negative");
        }
        this.delayMs = delayMs;
    }

    void addRequest(final ObjectKey objectKey,
                    final ByteRange range) {
        // We permit duplicates in `keyQueue` to simplify synchronization and get the most straightforward access
        // to the blocking `take` method of `keyQueue`. When there are multiple instances of the same key in the queue,
        // the first dequeued will grab all the waiting ranges from `requests`, others will be considered "ghosts" and ignored.
        // An example:
        // requests = {key: [range1]}, keyQueue = [key].
        // 1. Thread A puts `range2` in `requests`: requests = {key: [range1, range2]}
        // 2. Thread B dequeues `key` from `keyQueue`: keyQueue = []
        // 3. Thread B removes `range1,range2` from `requests`: requests = {}
        // 4. Thread A enqueues `key` in `keyQueue`: keyQueue = [key]
        // 5. Thread C dequeues `key` from `keyQueue`.
        // 6. Thread C finds that `requests` has no value for `key`.
        // In this case, thread C just ignores the dequeued and loops.
        // IOW, having duplicates in the queue is safe, albeit undesirable. This should happen rarely.

        // It's important to first put into `requests` map and only after that add to `keyQueue`,
        // because otherwise the following scheduling is possible:
        // 1. Thread A enqueues `key` in `keyQueue`: keyQueue = [key]
        // 2. Thread B dequeues `key` from `keyQueue`: keyQueue = []
        // 3. Thread B checks if there are requests in `requests`. There are none, and it discards the dequeued element.
        // 4. Thread A puts `range` in `requests`: requests = {key: [range1]}
        // Here, `range1` may be effectively lost.

        final boolean shouldEnqueue = requests.put(objectKey, range);
        if (shouldEnqueue) {
            keyQueue.put(new ObjectDelayed(objectKey));
        }
    }

    RangeFetchRequest take() throws InterruptedException {
        do {
            final ObjectDelayed objectDelayed = keyQueue.take();
            final List<ByteRange> ranges = requests.remove(objectDelayed.objectKey);
            // Ignore "ghost" items in the queue by continue iterating.
            if (ranges != null) {
                return new RangeFetchRequest(objectDelayed.objectKey, ranges);
            }
        } while (true);
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
