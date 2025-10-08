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

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MaterializedPartition {
    private static final Logger LOG = LoggerFactory.getLogger(HighWatermarkUpdater.class);

    final TopicIdPartition topicIdPartition;

    private final AtomicLong highWatermark = new AtomicLong(-1);
    private final AtomicLong nextOffsetToRequest = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<ObjectFetchTask> taskQueue = new ConcurrentLinkedQueue<>();

    MaterializedPartition(final TopicIdPartition topicIdPartition) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
    }

    long highWatermark() {
        return highWatermark.get();
    }

    void setHighWatermark(final long newHighWatermark) {
        if (highWatermark.getAndSet(newHighWatermark) != newHighWatermark) {
            // TODO change to debug
            LOG.error("QQQQQQQQQQ Updated HWM of {} to {}", topicIdPartition, newHighWatermark);
        }
    }

    long nextOffsetToRequest() {
        return nextOffsetToRequest.get();
    }

    void setNextOffsetToRequest(final long newNextOffsetToRequest) {
        if (nextOffsetToRequest.getAndSet(newNextOffsetToRequest) != newNextOffsetToRequest) {
            // TODO change to debug
            LOG.error("QQQQQQQQQQ Updated nextOffsetToRequest of {} to {}", topicIdPartition, newNextOffsetToRequest);
        }
    }

    boolean isNoTasks() {
        return taskQueue.isEmpty();
    }

    void addTask(final ObjectFetchTask task) {
        taskQueue.add(task);
    }
}
