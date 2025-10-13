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
import java.util.Objects;
import java.util.Optional;
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

import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.ControlPlane;

import com.antithesis.sdk.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MaterializedPartition {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializedPartition.class);

    final TopicIdPartition topicIdPartition;

    private UnifiedLog log;

    private long highWatermark = -1;
    private long nextOffsetToRequest = -1;
    private final ObjectFetchTaskQueue taskQueue = new ObjectFetchTaskQueue();

    private final Time time;
    private final Path materializationDirectory;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectFetchManager objectFetchManager;
    private final int maxBytesInQueue;

    private final KafkaScheduler unifiedLogScheduler;
    private final BrokerTopicStats brokerTopicStats;
    private final LogConfig logConfig;
    private final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(1, false);
    private final LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(1);

    MaterializedPartition(final TopicIdPartition topicIdPartition,
                          final Time time,
                          final Path materializationDirectory,
                          final int maxBytesInQueue,
                          final ObjectKeyCreator objectKeyCreator,
                          final ObjectFetchManager objectFetchManager,
                          final KafkaScheduler unifiedLogScheduler,
                          final BrokerTopicStats brokerTopicStats,
                          final LogConfig logConfig) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.materializationDirectory = Objects.requireNonNull(materializationDirectory, "materializationDirectory cannot be null");
        if (maxBytesInQueue <=  0) {
            throw new IllegalArgumentException("maxBytesInQueue must be at least 1");
        }
        this.maxBytesInQueue = maxBytesInQueue;
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectFetchManager = Objects.requireNonNull(objectFetchManager, "objectFetchManager cannot be null");
        this.unifiedLogScheduler = Objects.requireNonNull(unifiedLogScheduler, "unifiedLogScheduler cannot be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats cannot be null");
        this.logConfig = Objects.requireNonNull(logConfig, "logConfig cannot be null");
    }

    synchronized void setHighWatermark(final long newHighWatermark) {
        if (highWatermark != newHighWatermark) {
            highWatermark = newHighWatermark;
            LOG.error("[{}] QQQQQQQQQQ Updated HWM to {}", topicIdPartition.topicPartition(), newHighWatermark);

            if (nextOffsetToRequest == -1) {
                nextOffsetToRequest = highWatermark;
            }
        }
    }

    synchronized BatchDemand getBatchDemand() {
        if (nextOffsetToRequest >= highWatermark
        || taskQueue.byteSize() >= maxBytesInQueue) {
            return null;
        } else {
            return new BatchDemand(
                nextOffsetToRequest,
                maxBytesInQueue - Math.toIntExact(taskQueue.byteSize())
            );
        }
    }

    synchronized void informAboutBatches(final List<BatchInfo> batches) {
        if (batches.isEmpty()) {
            return;
        }
        if (batches.get(0).metadata().baseOffset() != nextOffsetToRequest) {
            LOG.error("[{}] QQQQQQQQQQ Got base offset {}, but expected {}",
                topicIdPartition.topicPartition(),
                batches.get(0).metadata().baseOffset(), nextOffsetToRequest);
            return;
        }

        LOG.error("[{}] QQQQQQQQQQ Added {} batches", topicIdPartition.topicPartition(), batches.size());
        for (final BatchInfo batch : batches) {
            LOG.error("QQQQQQQQQQ Time between logAppendTimestamp and starting fetching batch: {} ms",
                time.milliseconds() - batch.metadata().logAppendTimestamp());

            nextOffsetToRequest = batch.metadata().lastOffset() + 1;

            final ObjectFetchTask task =
                objectFetchManager.request(objectKeyCreator.from(batch.objectKey()), batch, batch.metadata().range());

            // If this is the first task in the queue, attach the fetch completed callback.
            if (taskQueue.peek() == null) {
                task.future().whenComplete(this::fetchCompletedCallback);
            }
            taskQueue.add(task);
        }
    }

    private synchronized void fetchCompletedCallback(final ByteBuffer _notUsed1, final Throwable _notUsed2) {
        // TODO fine-grained synchronization
        // TODO separate executor?

        ObjectFetchTask task = taskQueue.poll();
        Assert.always(task != null, "Task in fetchCompletedCallback cannot be null", null);
        if (task == null) {
            // TODO This should never happen, track this somehow.
            LOG.error("[{}] fetchCompletedCallback called but queue is empty",
                topicIdPartition.topicPartition());
            return;
        }
        Assert.always(task.future().isDone(), "Task in fetchCompletedCallback must be done", null);
        if (!task.future().isDone()) {
            // TODO This should never happen, track this somehow.
            LOG.error("[{}] fetchCompletedCallback called but first future is not done",
                topicIdPartition.topicPartition());
            return;
        }

        initLogIfNeeded(task.batchInfo().metadata().baseOffset());
        // Write this task result and continue while there are more completed futures.
        writeBufferFromTask(task);
        while ((task = taskQueue.poll()) != null) {
            if (task.future().isDone()) {
                writeBufferFromTask(task);
            } else {
                // When we see a not-done future, attach the callback and exit.
                task.future().whenComplete(this::fetchCompletedCallback);
                break;
            }
        }
    }

    private void writeBufferFromTask(final ObjectFetchTask task) {
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

        LOG.error("QQQQQQQQQQ Time between logAppendTimestamp and appending to log: {} ms",
            time.milliseconds() - task.batchInfo().metadata().logAppendTimestamp());
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

    record BatchDemand(long offset, int maxBytes) { }
}
