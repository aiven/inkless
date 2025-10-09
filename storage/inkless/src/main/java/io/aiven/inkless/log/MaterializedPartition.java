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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MaterializedPartition {
    private static final Logger LOG = LoggerFactory.getLogger(HighWatermarkUpdater.class);

    private static final int MAX_REQUEST_BYTES = 1 * 1024 * 1024;

    final TopicIdPartition topicIdPartition;

    private UnifiedLog log;

    private final AtomicBoolean batchRequestInProgress = new AtomicBoolean(false);
    private final AtomicBoolean writingInProgress = new AtomicBoolean(false);
    private final AtomicLong highWatermark = new AtomicLong(-1);
    private final AtomicLong nextOffsetToRequest = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<ObjectFetchTask> taskQueue = new ConcurrentLinkedQueue<>();

    private final Time time;
    private final ControlPlane controlPlane;
    private final ExecutorService batchRequestExecutor;
    private final ExecutorService diskWriteExecutor;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectFetchManager objectFetchManager;

    private final KafkaScheduler unifiedLogScheduler;
    private final BrokerTopicStats brokerTopicStats;
    private final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(1, false);
    private final LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(1);

    MaterializedPartition(final TopicIdPartition topicIdPartition,
                          final Time time,
                          final ControlPlane controlPlane,
                          final ExecutorService batchRequestExecutor,
                          final ExecutorService diskWriteExecutor,
                          final ObjectKeyCreator objectKeyCreator,
                          final ObjectFetchManager objectFetchManager,
                          final KafkaScheduler unifiedLogScheduler,
                          final BrokerTopicStats brokerTopicStats) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.batchRequestExecutor = Objects.requireNonNull(batchRequestExecutor, "batchRequestExecutor cannot be null");
        this.diskWriteExecutor = Objects.requireNonNull(diskWriteExecutor, "diskWriteExecutor cannot be null");
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectFetchManager = Objects.requireNonNull(objectFetchManager, "objectFetchManager cannot be null");
        this.unifiedLogScheduler = Objects.requireNonNull(unifiedLogScheduler, "unifiedLogScheduler cannot be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats cannot be null");
    }

//    long highWatermark() {
//        return highWatermark.get();
//    }

    void setHighWatermark(final long newHighWatermark) {
        if (highWatermark.getAndSet(newHighWatermark) != newHighWatermark) {
            // TODO change to debug
            LOG.error("QQQQQQQQQQ Updated HWM of {} to {}", topicIdPartition, newHighWatermark);

            nextOffsetToRequest.compareAndExchange(-1, newHighWatermark);
            requestMoreIfNeeded();
        }
    }

    private void requestMoreIfNeeded() {
        if (batchRequestInProgress.get()) {
            return;
        }
        if (highWatermark.get() == -1
            || nextOffsetToRequest.get() == -1
            || nextOffsetToRequest.get() >= highWatermark.get()) {
            return;
        }
        if (!taskQueue.isEmpty()) {
            LOG.error("QQQQQQQQQQ Not adding more for {} because there are tasks in queue", topicIdPartition);
            return;
        }

        LOG.error("QQQQQQQQQQ Requesting more for {}", topicIdPartition);
        CompletableFuture.runAsync(() -> {
            if (batchRequestInProgress.get()) {
                return;
            }

            // TODO better synchronization and task scheduling
            batchRequestInProgress.set(true);
            try {
                final long fromOffset = nextOffsetToRequest.get() >= 0
                    ? nextOffsetToRequest.get()
                    : highWatermark.get();
                final var findBatchRequest =
                    new FindBatchRequest(topicIdPartition, fromOffset, MAX_REQUEST_BYTES);
                final List<FindBatchResponse> responses;
                try {
                    responses = controlPlane.findBatches(List.of(findBatchRequest), MAX_REQUEST_BYTES, 0);
                } catch (final Exception e) {
                    LOG.error("Error finding batches for {}", topicIdPartition, e);
                    return;
                }

                final FindBatchResponse response = responses.get(0);
                if (response.errors().code() == Errors.NONE.code()) {
                    for (final BatchInfo batch : response.batches()) {
                        final ObjectFetchTask task =
                            objectFetchManager.request(objectKeyCreator.from(batch.objectKey()), batch, batch.metadata().range());

                        task.future().whenCompleteAsync(this::someBatchFetchCompleted, diskWriteExecutor);
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
                batchRequestInProgress.set(false);
            }
        }, batchRequestExecutor);
    }

    private void someBatchFetchCompleted(final ByteBuffer _notUsed1, final Throwable _notUsed2) {
        // This is a signal to look at the head of the queue and act if it's completed.

        if (writingInProgress.get()) {
            return;
        }

        writingInProgress.set(true);
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
                LOG.error("QQQQQQQQQQ Appended: {}", logAppendInfo);
            }
            requestMoreIfNeeded();
        } finally {
            writingInProgress.set(false);
        }
    }

    private void initLogIfNeeded(final long logStartOffset) {
        if (log != null) {
            return;
        }
        final File dir = new File(String.format("_unified_log/%s-%d",
            topicIdPartition.topicPartition().topic(), topicIdPartition.partition()));
        try {
            // Always start anew
            // TODO optimize, check HWM
            Utils.delete(dir);
            dir.mkdir();

            // TODO retention, segment size
            final LogConfig logConfig = LogConfig.fromProps(Map.of(), new Properties());
            final long recoveryPoint = logStartOffset;  // TODO change this is we start not from beginning
            this.log = UnifiedLog.create(dir, logConfig, logStartOffset, recoveryPoint, unifiedLogScheduler,
                brokerTopicStats, time, 1, producerStateManagerConfig,
                1, logDirFailureChannel, true, Optional.of(this.topicIdPartition.topicId()));

            LOG.error("QQQQQQQQQQ Initialized log {} with start offset {}", topicIdPartition, logStartOffset);
        } catch (final IOException e) {
            LOG.error("QQQQQQQQQQ Error in {}", topicIdPartition, e);
            throw new RuntimeException(e);
        }
    }
}
