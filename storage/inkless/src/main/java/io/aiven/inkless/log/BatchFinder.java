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
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchFinder {
    private static final Logger LOG = LoggerFactory.getLogger(BatchFinder.class);

    // TODO configurable?
    private static final int MAX_PARTITIONS_PER_REQUEST = 10;

    private final Time time;

    private final int numThreads;
    private final List<BatchFinderWorker> workers = new ArrayList<>();
    private final ExponentialBackoff backoff = new ExponentialBackoff(100, 2, 5 * 1000, 0.2);
    private final ControlPlane controlPlane;

    BatchFinder(final Time time,
                final int numThreads,
                final ControlPlane controlPlane) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");

        if (numThreads <= 0) {
            throw new IllegalArgumentException("numThreads must be at least 1");
        }
        this.numThreads = numThreads;

        for (int i = 0; i < numThreads; i++) {
            final BatchFinderWorker worker = new BatchFinderWorker();
            workers.add(worker);
            KafkaThread.daemon(String.format("inkless-batch-finder-%d", i), worker).start();
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
        for (final var worker : workers) {
            worker.running = false;
        }
    }

    private class BatchFinderWorker implements Runnable {
        private final ConcurrentHashMap<TopicIdPartition, MaterializedPartition> partitions = new ConcurrentHashMap<>();
        // TODO use delay queue

//        private long failedAttempts = 0;
        private volatile boolean running = true;

        private void addPartition(final MaterializedPartition partition) {
            partitions.putIfAbsent(partition.topicIdPartition, partition);
        }

        private void removePartition(final MaterializedPartition partition) {
            partitions.remove(partition.topicIdPartition);
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
            if (partitions.isEmpty()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("No partitions, sleeping for 100 ms");
                }
                Thread.sleep(100);
                return;
            }

            final Enumeration<MaterializedPartition> enumeration = partitions.elements();
            while (true) {
                final var partitions = takePartitionsWithDemand(enumeration, MAX_PARTITIONS_PER_REQUEST);
                if (partitions.isEmpty()) {
                    break;
                } else {
                    runForSomePartitions(partitions);
                }
            }
        }

        private List<MaterializedPartition> takePartitionsWithDemand(
            final Enumeration<MaterializedPartition> from,
            final int numPartitions
        ) {
            final List<MaterializedPartition> result = new ArrayList<>();
            while (result.size() < numPartitions && from.hasMoreElements()) {
                final MaterializedPartition partition = from.nextElement();
                if (partition.getBatchDemand() != null) {
                    result.add(partition);
                }
            }
            return result;
        }

        private void runForSomePartitions(final List<MaterializedPartition> partitions) throws InterruptedException {
            // TODO implement bachoff
//            if (failedAttempts > 0) {
//                final long sleep = backoff.backoff(failedAttempts);
//                LOG.debug("Sleeping for {} ms due to backoff", sleep);
//                Thread.sleep(100);
//            }

            final List<FindBatchRequest> findBatchRequests = new ArrayList<>();
            for (final MaterializedPartition partition : partitions) {
                final var batchDemand = partition.getBatchDemand();
                // This shouldn't happen as we checked this before, but as a safety net.
                if (batchDemand == null) {
                    continue;
                }

                findBatchRequests.add(
                    new FindBatchRequest(partition.topicIdPartition, batchDemand.offset(), batchDemand.maxBytes())
                );
            }

            final List<FindBatchResponse> responses;
            try {
                responses = controlPlane.findBatches(findBatchRequests, Integer.MAX_VALUE, 0);
//                failedAttempts = 0;
            } catch (final Exception e) {
                LOG.error("Error finding batches", e);
//                failedAttempts += 1;
                return;
            }

            for (int i = 0; i < responses.size(); i++) {
                final MaterializedPartition partition = partitions.get(i);
                if (partition != null) {
                    final FindBatchResponse response = responses.get(i);
                    if (response.errors().code() == Errors.NONE.code()) {
                        partition.informAboutBatches(response.batches());
                    } else {
                        // TODO what else to do?
                        LOG.error("Error finding batches for partition {}: {}",
                            partition.topicIdPartition.topicPartition(), response.errors());
                    }
                }
            }
        }
    }
}
