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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MaterializedPartition {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializedPartition.class);

    final TopicIdPartition topicIdPartition;

    private UnifiedLog log;

    private final AtomicLong highWatermark = new AtomicLong(-1);
    private final AtomicLong nextOffsetToRequest = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<ObjectFetchTask> taskQueue = new ConcurrentLinkedQueue<>();

    private final Time time;
    private final ControlPlane controlPlane;
    private final Path materializationDirectory;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectFetchManager objectFetchManager;

    private final KafkaScheduler unifiedLogScheduler;
    private final BrokerTopicStats brokerTopicStats;
    private final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(1, false);
    private final LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(1);

    private final BatchRequester batchRequester;
    private final DiskWriter diskWriter;

    MaterializedPartition(final TopicIdPartition topicIdPartition,
                          final Time time,
                          final ControlPlane controlPlane,
                          final Path materializationDirectory,
                          final ExecutorService batchRequestExecutor,
                          final ExecutorService diskWriteExecutor,
                          final ObjectKeyCreator objectKeyCreator,
                          final ObjectFetchManager objectFetchManager,
                          final KafkaScheduler unifiedLogScheduler,
                          final BrokerTopicStats brokerTopicStats) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.materializationDirectory = Objects.requireNonNull(materializationDirectory, "materializationDirectory cannot be null");
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectFetchManager = Objects.requireNonNull(objectFetchManager, "objectFetchManager cannot be null");
        this.unifiedLogScheduler = Objects.requireNonNull(unifiedLogScheduler, "unifiedLogScheduler cannot be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats cannot be null");

        this.batchRequester = new BatchRequester(
            batchRequestExecutor,
            new EmptyQueueBatchRequestPolicy(),
            this::downloadCompletedCallback);
        this.diskWriter = new DiskWriter(
            diskWriteExecutor,
            this::writeCompletedCallback
        );
    }

    void setHighWatermark(final long newHighWatermark) {
        if (highWatermark.getAndSet(newHighWatermark) != newHighWatermark) {
            // TODO change to debug
            LOG.error("QQQQQQQQQQ Updated HWM of {} to {}", topicIdPartition, newHighWatermark);

            nextOffsetToRequest.compareAndExchange(-1, newHighWatermark);
            batchRequester.requestMoreIfNeeded();
        }
    }

    private void downloadCompletedCallback() {
        diskWriter.writeIfNeeded();
    }

    private void writeCompletedCallback() {
        batchRequester.requestMoreIfNeeded();
    }

    private interface BatchRequestPolicy {
        int howMuchToRequest();
    }

    private class EmptyQueueBatchRequestPolicy implements BatchRequestPolicy {
        private static final int MAX_REQUEST_BYTES = 1 * 1024 * 1024;

        @Override
        public int howMuchToRequest() {
            if (highWatermark.get() == -1
                || nextOffsetToRequest.get() == -1
                || nextOffsetToRequest.get() >= highWatermark.get()
                || !taskQueue.isEmpty()) {
                return -1;
            } else {
                return MAX_REQUEST_BYTES;
            }
        }
    }

    private class BatchRequester {
        private final AtomicBoolean inProgress = new AtomicBoolean(false);
        private final ExecutorService executor;
        private final BatchRequestPolicy policy;

        private final Runnable downloadCompletedCallback;

        private BatchRequester(final ExecutorService executor,
                               final BatchRequestPolicy policy,
                               final Runnable downloadCompletedCallback) {
            this.executor = Objects.requireNonNull(executor, "executor cannot be null");
            this.policy = Objects.requireNonNull(policy, "policy cannot be null");
            this.downloadCompletedCallback =
                Objects.requireNonNull(downloadCompletedCallback, "downloadCompletedCallback cannot be null");
        }

        private void requestMoreIfNeeded() {
            // Skip even submitting a task if we shouldn't request right now or if one is in progress.
            if (policy.howMuchToRequest() > 0
                && inProgress.compareAndSet(false, true)) {
                executor.submit(this::requestInternal);
            }
        }

        private void requestInternal() {
            try {
                final int howMuchToRequest = policy.howMuchToRequest();
                if (howMuchToRequest == -1) {
                    return;
                }

                LOG.error("QQQQQQQQQQ Requesting {} bytes for {}", howMuchToRequest, topicIdPartition);
                final var findBatchRequest = new FindBatchRequest(topicIdPartition, nextOffsetToRequest.get(), howMuchToRequest);
                final FindBatchResponse response;
                try {
                    response = controlPlane.findBatches(List.of(findBatchRequest), howMuchToRequest, 0).get(0);
                } catch (final Exception e) {
                    LOG.error("Error finding batches for {}", topicIdPartition, e);
                    return;
                }

                if (response.errors().code() == Errors.NONE.code()) {
                    for (final BatchInfo batch : response.batches()) {
                        final ObjectFetchTask task =
                            objectFetchManager.request(objectKeyCreator.from(batch.objectKey()), batch, batch.metadata().range());

                        task.future()
                            .whenComplete((_ignored1, _ignored2) -> downloadCompletedCallback.run());
                        taskQueue.add(task);
                        final long newNextOffsetToRequest = batch.metadata().lastOffset() + 1;
                        if (nextOffsetToRequest.getAndSet(newNextOffsetToRequest) != newNextOffsetToRequest) {
                            // TODO change to debug
                            LOG.error("QQQQQQQQQQ Updated nextOffsetToRequest of {} to {}", topicIdPartition, newNextOffsetToRequest);
                        }
                        LOG.error("QQQQQQQQQQ Added batch for {}", topicIdPartition);
                    }
                } else {
                    LOG.error("Error finding batches for {}: {}", topicIdPartition, response.errors());
                }
            } finally {
                inProgress.set(false);
            }
        }
    }

    private class DiskWriter {
        private final AtomicBoolean inProgress = new AtomicBoolean(false);
        private final ExecutorService executor;

        private final Runnable writeCompletedCallback;

        private DiskWriter(final ExecutorService executor,
                           final Runnable writeCompletedCallback) {
            this.executor = Objects.requireNonNull(executor, "executor cannot be null");
            this.writeCompletedCallback = Objects.requireNonNull(writeCompletedCallback, "writeCompletedCallback cannot be null");
        }

        private void writeIfNeeded() {
            if (taskQueue.peek().future().isDone()
                && inProgress.compareAndSet(false, true)) {
                executor.submit(this::writeInternal);
            }
        }

        private void writeInternal() {
            try {
                while (taskQueue.peek() != null && taskQueue.peek().future().isDone()) {
                    final ObjectFetchTask task = taskQueue.poll();
                    if (task == null) {
                        throw new RuntimeException("Not expected");
                    }

                    initLogIfNeeded(task.batchInfo().metadata().baseOffset());

                    final ByteBuffer byteBuffer;
                    try {
                        byteBuffer = task.future().get();
                    } catch (final InterruptedException | ExecutionException e) {
                        // TODO handle gracefully
                        LOG.error("QQQQQQQQQQ Error processing future in {}", topicIdPartition, e);
                        throw new RuntimeException(e);
                    }

                    // TODO move createMemoryRecords to more suitable place
                    final MemoryRecords records = FetchCompleter.createMemoryRecords(byteBuffer, task.batchInfo());
                    final LogAppendInfo logAppendInfo = log.appendAsFollower(records, 0);
                    LOG.error("QQQQQQQQQQ Appended to {}: {}", topicIdPartition, logAppendInfo);
                }
                writeCompletedCallback.run();
            } finally {
                inProgress.set(false);
            }
        }

        private void initLogIfNeeded(final long logStartOffset) {
            if (log != null) {
                return;
            }
            final File dir = materializationDirectory.resolve(
                String.format("%s-%d", topicIdPartition.topicPartition().topic(), topicIdPartition.partition())
            ).toFile();
            try {
                // Always start anew
                // TODO optimize, check HWM
                Utils.delete(dir);
                final boolean _ignored = dir.mkdir();

                // TODO retention, segment size
                final LogConfig logConfig = LogConfig.fromProps(Map.of(), new Properties());
                final long recoveryPoint = logStartOffset;  // TODO change this is we start not from beginning
                log = UnifiedLog.create(dir, logConfig, logStartOffset, recoveryPoint, unifiedLogScheduler,
                    brokerTopicStats, time, 1, producerStateManagerConfig,
                    1, logDirFailureChannel, true,
                    Optional.of(topicIdPartition.topicId()));

                LOG.error("QQQQQQQQQQ Initialized log {} with start offset {}", topicIdPartition, logStartOffset);
            } catch (final IOException e) {
                LOG.error("QQQQQQQQQQ Error in {}", topicIdPartition, e);
                throw new RuntimeException(e);
            }
        }
    }
}
