// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectUploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for Inkless writing.
 *
 * <p>This class encapsulates all the machinery of writing to Inkless:
 * buffers, timers, uploading files, committing to the control plane.
 *
 * <p>The major part of the logic is concentrated in the writer states. The Writer starts in the {@code Empty} state
 * and reacts at events ({@code Add} data, {@code Commit tick}, and {@code Commit finished}) with state transition
 * and side effects like scheduling the commit tick or initiating the commit process.
 * See the {@link WriterState} javadoc for more details.
 *
 * <p>The class is thread-safe: all the event entry points are protected with the lock.</p>
 */
class Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

    private final Lock lock = new ReentrantLock();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private WriterState state;

    Writer(final Duration commitInterval, final int maxBufferSize,
           final int maxFileUploadAttempts, final Duration fileUploadRetryBackoff,
           final ObjectKeyCreator objectKeyCreator,
           final ObjectUploader objectUploader,
           final ControlPlane controlPlane,
           final Time time) {
        final CommitTickScheduler commitTickScheduler =
            (sessionStarted, currentState) ->
                scheduler.schedule(
                    () -> tick(currentState),
                    TimeUtils.durationFromNow(Instant.ofEpochMilli(time.milliseconds()), sessionStarted, commitInterval).toMillis(),
                    TimeUnit.MILLISECONDS);
        final FileCommitter fileCommitter = new FileCommitter(
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-writer-", false)),
            objectKeyCreator,
            objectUploader,
            controlPlane,
            time,
            maxFileUploadAttempts,
            fileUploadRetryBackoff,
            this::commitFinished
        );
        state = new EmptyState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, maxBufferSize, commitInterval)
        );
    }

    // Visible for testing
    Writer(final CommitTickScheduler commitTickScheduler,
           final FileCommitter fileCommitter,
           final Time time,
           final Duration commitInterval, final int maxBufferSize) {
        state = new EmptyState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, maxBufferSize, commitInterval)
        );
    }

    CompletableFuture<Map<TopicPartition, PartitionResponse>> write(
        final Map<TopicPartition, MemoryRecords> entriesPerPartition
    ) {
        lock.lock();
        try {
            final WriterState.AddResult result = this.state.add(entriesPerPartition);
            this.state = result.newState();
            return result.result();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The entry point for the {@code Commit tick} event.
     *
     * @param expectedCurrentState the current state that the Writer is expected to be in (or the handling is no-op).
     */
    private void tick(final WriterState expectedCurrentState) {
        lock.lock();
        try {
            // Skip the tick if it arrived at an irrelevant state.
            if (this.state == expectedCurrentState) {
                this.state = this.state.commitTick().newState();
            } else {
                LOGGER.debug("Received Tick targeted at state {}, but current state is {}, skipping",
                    expectedCurrentState, this.state);
            }
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    void tickTest() {
        tick(state);
    }

    /**
     * The entry point for the {@code Commit finished} event.
     */
    // Visible for testing
    void commitFinished(final List<CommitBatchResponse> commitBatchResponses,
                        final Throwable error) {
        lock.lock();
        try {
            this.state = this.state.commitFinished(commitBatchResponses, error).newState();
        } finally {
            lock.unlock();
        }
    }
}
