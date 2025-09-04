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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;

/**
 * The entry point for diskless writing.
 *
 * <p>This class encapsulates all the machinery of writing to diskless:
 * buffers, timers, uploading files, committing to the control plane.
 *
 * <p>The Writer has the active file, the queue of files being uploaded.
 * It schedules commit ticks at the specified interval.
 *
 * <p>The class is thread-safe: all the event entry points are protected with the lock.</p>
 */
class Writer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

    private final Lock lock = new ReentrantLock();
    private ActiveFile activeFile;
    private final FileCommitter fileCommitter;
    private final Time time;
    private final Duration commitInterval;
    private final int maxBufferSize;
    private final ScheduledExecutorService commitTickScheduler;
    private boolean closed = false;
    private final WriterMetrics writerMetrics;
    private final BrokerTopicStats brokerTopicStats;
    private Instant openedAt;
    private ScheduledFuture<?> scheduledTick;

    @DoNotMutate
    Writer(final Time time,
           final int brokerId,
           final ObjectKeyCreator objectKeyCreator,
           final StorageBackend storage,
           final KeyAlignmentStrategy keyAlignmentStrategy,
           final ObjectCache objectCache,
           final ControlPlane controlPlane,
           final Duration commitInterval,
           final int maxBufferSize,
           final int maxFileUploadAttempts,
           final Duration fileUploadRetryBackoff,
           final int fileUploaderThreadPoolSize,
           final BrokerTopicStats brokerTopicStats
    ) {
        this(
            time,
            commitInterval,
            maxBufferSize,
            Executors.newScheduledThreadPool(1, new InklessThreadFactory("inkless-file-commit-ticker-", true)),
            new FileCommitter(
                brokerId, controlPlane, objectKeyCreator, storage,
                keyAlignmentStrategy, objectCache, time,
                maxFileUploadAttempts, fileUploadRetryBackoff,
                fileUploaderThreadPoolSize),
            new WriterMetrics(time),
            brokerTopicStats
        );
    }

    // Visible for testing
    Writer(final Time time,
           final Duration commitInterval,
           final int maxBufferSize,
           final ScheduledExecutorService commitTickScheduler,
           final FileCommitter fileCommitter,
           final WriterMetrics writerMetrics,
           final BrokerTopicStats brokerTopicStats) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.commitInterval = Objects.requireNonNull(commitInterval, "commitInterval cannot be null");
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive");
        }
        this.maxBufferSize = maxBufferSize;
        this.commitTickScheduler = Objects.requireNonNull(commitTickScheduler, "commitTickScheduler cannot be null");
        this.fileCommitter = Objects.requireNonNull(fileCommitter, "fileCommitter cannot be null");
        this.writerMetrics = Objects.requireNonNull(writerMetrics, "writerMetrics cannot be null");
        this.brokerTopicStats = brokerTopicStats;
        this.activeFile = new ActiveFile(time, brokerTopicStats);
    }

    CompletableFuture<Map<TopicIdPartition, PartitionResponse>> write(
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        final Map<String, LogConfig> topicConfigs,
        final RequestLocal requestLocal
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(topicConfigs, "topicConfigs cannot be null");
        Objects.requireNonNull(requestLocal, "requestLocal cannot be null");

        if (entriesPerPartition.isEmpty()) {
            throw new IllegalArgumentException("entriesPerPartition cannot be empty");
        }

        if (entriesPerPartition.keySet().stream().map(TopicIdPartition::topic).distinct().noneMatch(topicConfigs::containsKey)) {
            throw new IllegalArgumentException("Configs are not including all the topics requested");
        }

        // TODO add back pressure

        lock.lock();
        try {
            if (closed) {
                return CompletableFuture.failedFuture(new RuntimeException("Writer already closed"));
            }

            if (openedAt == null) {
                openedAt = TimeUtils.durationMeasurementNow(time);
            }

            // Prefer smaller files, than going above the buffer limit by rotating early if the active file is not empty
            // and the request size (which may contain invalid batches/records) exceeds the max buffer size.
            int nonValidatedRequestSize = entriesPerPartition.values().stream().mapToInt(MemoryRecords::sizeInBytes).sum();
            final int beforeSize = this.activeFile.size();
            if (!this.activeFile.isEmpty() && beforeSize + nonValidatedRequestSize > maxBufferSize) {
                if (this.scheduledTick != null) {
                    this.scheduledTick.cancel(false);
                    this.scheduledTick = null;
                }
                rotateFile(false);
            }

            final var result = this.activeFile.add(entriesPerPartition, topicConfigs, requestLocal);
            writerMetrics.requestAdded();
            final int afterSize = this.activeFile.size();
            if (afterSize >= maxBufferSize) {
                if (this.scheduledTick != null) {
                    this.scheduledTick.cancel(false);
                    this.scheduledTick = null;
                }
                rotateFile(false);
            } else if (this.scheduledTick == null) {
                this.scheduledTick = commitTickScheduler.schedule(this::tick, commitInterval.toMillis(), TimeUnit.MILLISECONDS);
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    void tick() {
        lock.lock();
        try {
            this.scheduledTick = null;

            if (closed) {
                return;
            }

            if (!this.activeFile.isEmpty()) {
                rotateFile(false);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            commitTickScheduler.shutdownNow();
            // Rotate file before closing the uploader so the file gets into the queue first.
            rotateFile(true);
            fileCommitter.close();
            writerMetrics.close();
        } finally {
            lock.unlock();
        }
    }

    private void rotateFile(final boolean swallowInterrupted) {
        LOGGER.debug("Rotating active file");
        final ActiveFile prevActiveFile = this.activeFile;
        this.activeFile = new ActiveFile(time, brokerTopicStats);

        try {
            this.fileCommitter.commit(prevActiveFile.close());
            // mark metrics that the file is committed
            if (openedAt != null) {
                writerMetrics.fileRotated(openedAt);
                openedAt = null;
            }
        } catch (final InterruptedException e) {
            if (!swallowInterrupted) {
                // This is not expected as this is probably closing of the Writer, and
                // we try to shut down the executors gracefully. To be sure, redo closing.
                LOGGER.error("Interrupted", e);
                Utils.closeQuietly(this, "Inkless Writer");
                throw new RuntimeException(e);
            } else {
                LOGGER.info("Interrupted, ignoring (probably recursive call)", e);
            }
        }
    }
}
