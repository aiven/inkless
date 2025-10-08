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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchRequester {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRequester.class);

    private static final int MAX_REQUEST_BYTES = 1 * 1024 * 1024;

    private final Time time;
    private final ControlPlane controlPlane;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectFetchManager objectFetchManager;

    protected final ScheduledExecutorService pool;

    private final ConcurrentHashMap<TopicIdPartition, MaterializedPartition> partitions = new ConcurrentHashMap<>();

    BatchRequester(final Time time,
                   final ControlPlane controlPlane,
                   final ObjectKeyCreator objectKeyCreator,
                   final ObjectFetchManager objectFetchManager,
                   final int numThreads) {
        this(time, controlPlane, objectKeyCreator, objectFetchManager, numThreads,
            Executors.newScheduledThreadPool(
                numThreads,
                ThreadUtils.createThreadFactory("inkless-materialization-batch-requester-%d", true,
                    (t, e) -> LOG.error("Uncaught exception in thread '{}':", t.getName(), e))));
    }

    // Visible for testing
    BatchRequester(final Time time,
                   final ControlPlane controlPlane,
                   final ObjectKeyCreator objectKeyCreator,
                   final ObjectFetchManager objectFetchManager,
                   final int numThreads,
                   final ScheduledExecutorService pool) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectFetchManager = Objects.requireNonNull(objectFetchManager, "objectFetchManager cannot be null");

        this.pool = Objects.requireNonNull(pool, "pool cannot be null");
        if (numThreads <= 0) {
            throw new IllegalArgumentException("numThreads must be at least 1");
        }
        for (int i = 0; i < numThreads; i++) {
            // TODO anything better than this? Probably we should just run threads.
            // 50 ms to break endless loop when nothing happens.
            this.pool.scheduleWithFixedDelay(this::iteration, 0, 50, TimeUnit.MILLISECONDS);
        }
    }

    private void iteration() {
        try {
            innerIteration();
        } catch (final Exception e) {
            LOG.error("Uncaught exception", e);
        }
    }

    private void innerIteration() {
        // TODO this doesn't work with sharing between multiple threads, rework
        while (true) {
            final List<MaterializedPartition> workablePartitions = new ArrayList<>();
            final Enumeration<MaterializedPartition> enumeration = partitions.elements();
            while (enumeration.hasMoreElements()) {
                final MaterializedPartition partition = enumeration.nextElement();
                final boolean neverRequestedBefore = partition.nextOffsetToRequest() == -1;
                final boolean hasNewerBatches = partition.nextOffsetToRequest() < partition.highWatermark();
                if (partition.highWatermark() >= 0
                    && partition.isNoTasks()
                    && (neverRequestedBefore || hasNewerBatches)
                ) {
                    workablePartitions.add(partition);
                }
            }

            if (workablePartitions.isEmpty()) {
                return;
            }

            for (final MaterializedPartition partition : workablePartitions) {
                processPartition(partition);
            }
        }
    }

    private void processPartition(final MaterializedPartition partition) {
        final long fromOffset = partition.nextOffsetToRequest() >= 0
            ? partition.nextOffsetToRequest()
            : partition.highWatermark();
        final var findBatchRequest =
            new FindBatchRequest(partition.topicIdPartition, fromOffset, MAX_REQUEST_BYTES);
        final List<FindBatchResponse> responses;
        try {
            responses = controlPlane.findBatches(List.of(findBatchRequest), MAX_REQUEST_BYTES, 0);
        } catch (final Exception e) {
            LOG.error("Error finding batches for {}", partition.topicIdPartition, e);
            return;
        }

        final FindBatchResponse response = responses.get(0);
        if (response.errors().code() == Errors.NONE.code()) {
            for (final BatchInfo batch : response.batches()) {
                final ObjectFetchTask task =
                    objectFetchManager.request(objectKeyCreator.from(batch.objectKey()), batch, batch.metadata().range());
                partition.addTask(task);
                partition.setNextOffsetToRequest(batch.metadata().lastOffset() + 1);
                LOG.error("QQQQQQQQQQ Added batch for {}", partition.topicIdPartition);
            }
        } else {
            LOG.error("Error finding batches for {}: {}", partition.topicIdPartition, response.errors());
        }
    }

    void addPartition(final MaterializedPartition partition) {
        partitions.putIfAbsent(partition.topicIdPartition, partition);
    }

    void removePartition(final MaterializedPartition partition) {
        partitions.remove(partition.topicIdPartition);
    }

    void shutdown() {
        ThreadUtils.shutdownExecutorServiceQuietly(this.pool, 0, TimeUnit.SECONDS);
    }
}
