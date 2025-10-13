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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The high watermark updater for {@link MaterializedPartition}.
 *
 * <p>It makes sense to have it separate because it may be optimized to e.g. use Postgres LISTEN/NOTIFY.
 */
class HighWatermarkUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(HighWatermarkUpdater.class);

    private static final int MAX_PARTITIONS_PER_REQUEST = 10;  // TODO make configurable?

    private final Time time;
    private final ControlPlane controlPlane;
    private final long delayMs;

    private final int numThreads;
    private final List<HighWatermarkUpdaterWorker> workers = new ArrayList<>();

    HighWatermarkUpdater(final Time time,
                         final ControlPlane controlPlane,
                         final long delayMs,
                         final int numThreads) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");

        if (delayMs < 0) {
            throw new IllegalArgumentException("delayMs cannot be negative");
        }
        this.delayMs = delayMs;

        if (numThreads <= 0) {
            throw new IllegalArgumentException("numThreads must be at least 1");
        }
        this.numThreads = numThreads;

        for (int i = 0; i < numThreads; i++) {
            final HighWatermarkUpdaterWorker worker = new HighWatermarkUpdaterWorker();
            workers.add(worker);
            KafkaThread.daemon(String.format("inkless-high-watermark-updater-%d", i), worker).start();
        }
    }
    void addPartition(final MaterializedPartition partition) {
        workers.get(threadForPartition(partition)).addPartition(partition);
    }

    void removePartition(final MaterializedPartition partition) {
        workers.get(threadForPartition(partition)).removePartition(partition);
    }

    private int threadForPartition(final MaterializedPartition partition) {
        return Utils.abs(31 * partition.topicIdPartition.topic().hashCode() + partition.topicIdPartition.partition())
            % numThreads;
    }

    void shutdown() {
        for (final var thread : workers) {
            thread.running = false;
        }
    }

    private class HighWatermarkUpdaterWorker implements Runnable {
        private final DelayQueue<DelayedMaterializedPartition> queue = new DelayQueue<>();
        private final ConcurrentHashMap<TopicIdPartition, Boolean> knownPartitions = new ConcurrentHashMap<>();

        private volatile boolean running = true;

        private void addPartition(final MaterializedPartition partition) {
            final boolean justAdded = knownPartitions.putIfAbsent(partition.topicIdPartition, true) == null;
            if (justAdded) {
                queue.put(new DelayedMaterializedPartition(time, delayMs, partition));
            }
        }

        private void removePartition(final MaterializedPartition partition) {
            knownPartitions.remove(partition.topicIdPartition);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    runOnce();
                } catch (final InterruptedException e) {
                    // Preserve interrupt status
                    Thread.currentThread().interrupt();
                    return;
                } catch (final Throwable e) {
                    LOG.error("Unexpected exception", e);
                }
            }
        }

        private void runOnce() throws InterruptedException {
            final List<MaterializedPartition> partitions = takePartitions().stream()
                // Only keep known partitions, others seems to be removed.
                .filter(mp -> knownPartitions.containsKey(mp.topicIdPartition))
                .toList();

            if (!partitions.isEmpty()) {
                updateHighWatermarks(partitions);  // never throws

                // Return the partitions back to the queue.
                partitions.stream()
                    .map(p -> new DelayedMaterializedPartition(time, delayMs, p))
                    .forEach(queue::add);
            }
        }

        private List<MaterializedPartition> takePartitions() {
            DelayedMaterializedPartition p;
            final List<MaterializedPartition> partitions = new ArrayList<>();
            try {
                p = queue.poll(1, TimeUnit.SECONDS);
                if (p != null) {
                    partitions.add(p.partition);
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("No partitions available in queue");
                    }
                    return List.of();
                }
            } catch (final InterruptedException e) {
                LOG.error("Thread interrupted");
                return List.of();
            }

            // Take more items if available without blocking.
            while ((p = queue.poll()) != null && partitions.size() < MAX_PARTITIONS_PER_REQUEST) {
                partitions.add(p.partition);
            }

            return partitions;
        }

        private void updateHighWatermarks(final List<MaterializedPartition> partitions) {
            try {
                final List<GetLogInfoRequest> requests = partitions.stream()
                    .map(p -> new GetLogInfoRequest(p.topicIdPartition.topicId(), p.topicIdPartition.partition()))
                    .toList();
                final List<GetLogInfoResponse> responses = controlPlane.getLogInfo(requests);
                for (int i = 0; i < partitions.size(); i++) {
                    final MaterializedPartition partition = partitions.get(i);
                    final GetLogInfoResponse logInfoResponse = responses.get(i);
                    if (logInfoResponse.errors().code() == Errors.NONE.code()) {
                        partition.setHighWatermark(logInfoResponse.highWatermark());
                    } else {
                        LOG.warn("[{}] Error updating high watermark: {}",
                            partition.topicIdPartition.topicPartition(), logInfoResponse.errors());
                    }
                }
            } catch (final Exception e) {
                LOG.error("Error updating high watermarks", e);
            }
        }
    }
}
