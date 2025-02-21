// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
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

import io.aiven.inkless.FutureUtils;
import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;

/**
 * The entry point for Inkless writing.
 *
 * <p>This class encapsulates all the machinery of writing to Inkless:
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
           final BrokerTopicStats brokerTopicStats) {
        this(
            time,
            commitInterval,
            maxBufferSize,
            Executors.newScheduledThreadPool(1, new InklessThreadFactory("inkless-file-commit-ticker-", true)),
            new FileCommitter(
                    brokerId, controlPlane, objectKeyCreator, storage,
                    keyAlignmentStrategy, objectCache, time,
                    maxFileUploadAttempts, fileUploadRetryBackoff),
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

    CompletableFuture<Map<TopicPartition, PartitionResponse>> write(
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        final Map<String, TimestampType> timestampTypes
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(timestampTypes, "timestampTypes cannot be null");

        if (entriesPerPartition.isEmpty()) {
            throw new IllegalArgumentException("entriesPerPartition cannot be empty");
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

            final var result = this.activeFile.add(entriesPerPartition, timestampTypes);
            writerMetrics.requestAdded();
            if (this.activeFile.size() >= maxBufferSize) {
                if (this.scheduledTick != null) {
                    this.scheduledTick.cancel(false);
                    this.scheduledTick = null;
                }
                rotateFile(false);
            } else if (this.scheduledTick == null) {
                this.scheduledTick = commitTickScheduler.schedule(this::tick, commitInterval.toMillis(), TimeUnit.MILLISECONDS);
            }

            return FutureUtils.combineMapOfFutures(result);
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
            final ClosedFile closedFile = prevActiveFile.close();
            if (closedFile.isEmpty()) {
                LOGGER.debug("Active file is empty, skipping rotation");
                return;
            }

            this.fileCommitter.commit(closedFile);
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
