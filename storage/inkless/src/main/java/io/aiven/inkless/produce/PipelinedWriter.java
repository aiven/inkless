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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogValidator;
import org.apache.kafka.storage.internals.log.RecordValidationException;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import static org.apache.kafka.storage.internals.log.UnifiedLog.UNKNOWN_OFFSET;
import static org.apache.kafka.storage.internals.log.UnifiedLog.newValidatorMetricsRecorder;

/**
 * Pipelined writer implementation using staged event-driven architecture (SEDA).
 *
 * <h2>Architecture</h2>
 * <pre>
 * Request Handler Threads (Kafka network threads)
 *           │
 *           ▼
 * ┌─────────────────────────────────────┐
 * │     Stage 1: Validation Workers     │  ← N workers (CPU-bound, parallel)
 * │  • CRC validation                   │
 * │  • Size checks                      │
 * │  • Offset assignment                │
 * └─────────────────────────────────────┘
 *           │
 *           ▼
 * ┌─────────────────────────────────────┐
 * │        Buffer Queue (bounded)       │  ← Backpressure point
 * └─────────────────────────────────────┘
 *           │
 *           ▼
 * ┌─────────────────────────────────────┐
 * │   Stage 2: Single Buffer Writer     │  ← 1 thread (NO LOCK needed!)
 * │  • buffer.addBatch()                │
 * │  • Rotation management              │
 * │  • Complete futures                 │
 * └─────────────────────────────────────┘
 * </pre>
 *
 * <h2>Benefits</h2>
 * <ul>
 *   <li>Zero lock contention - buffer writer owns all mutable state</li>
 *   <li>Parallel validation across N workers</li>
 *   <li>Natural batching under load (buffer writer drains queue)</li>
 *   <li>Bounded queue provides backpressure</li>
 * </ul>
 */
class PipelinedWriter implements ProduceWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelinedWriter.class);

    /**
     * Queue capacity for validated requests waiting for buffer writer.
     * Provides ~1000 requests of buffering before backpressure kicks in.
     */
    private static final int BUFFER_QUEUE_CAPACITY = 1000;

    /**
     * Maximum requests to drain from queue in one batch.
     * Balances latency vs throughput.
     */
    private static final int MAX_BATCH_DRAIN_SIZE = 100;

    /**
     * Poll timeout for buffer writer when queue is empty.
     */
    private static final long BUFFER_WRITER_POLL_TIMEOUT_MS = 50;

    // Stage 1: Validation worker pool
    private final ExecutorService validationExecutor;
    private final int validationWorkerCount;

    // Stage 2: Single buffer writer
    private final ExecutorService bufferWriterExecutor;
    private final BlockingQueue<ValidatedRequest> bufferQueue;

    // Buffer writer state (owned exclusively by buffer writer thread)
    private final Time time;
    private final Duration commitInterval;
    private final int maxBufferSize;
    private final StorageBackend storage;
    private final FileCommitter fileCommitter;
    private final WriterMetrics writerMetrics;
    private final BrokerTopicStats brokerTopicStats;

    // Shared state
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Tick scheduling (managed by buffer writer thread)
    private final ScheduledExecutorService tickScheduler;
    private ScheduledFuture<?> scheduledTick;

    // Buffer writer thread-local state
    private ActiveFile activeFile;
    private Instant openedAt;
    private int nextRequestId = 0;

    // Metrics recorder for validation
    private final LogValidator.MetricsRecorder validatorMetricsRecorder;

    /**
     * Holds the result of validation, used to transfer between validateRequest() and write().
     * This avoids creating a throwaway CompletableFuture in validateRequest().
     */
    private record ValidationResult(
        Map<TopicIdPartition, ValidatedRequest.ValidatedBatch> validatedBatches,
        Map<TopicIdPartition, PartitionResponse> invalidBatches
    ) {}

    @DoNotMutate
    PipelinedWriter(
        final Time time,
        final int brokerId,
        final ObjectKeyCreator objectKeyCreator,
        final StorageBackend storage,
        final KeyAlignmentStrategy keyAlignmentStrategy,
        final ObjectCache objectCache,
        final BatchCoordinateCache batchCoordinateCache,
        final ControlPlane controlPlane,
        final Duration commitInterval,
        final int maxBufferSize,
        final int maxFileUploadAttempts,
        final Duration fileUploadRetryBackoff,
        final int fileUploaderThreadPoolSize,
        final BrokerTopicStats brokerTopicStats,
        final int validationWorkerCount
    ) {
        this(
            time,
            commitInterval,
            maxBufferSize,
            Executors.newScheduledThreadPool(1, new InklessThreadFactory("inkless-tick-scheduler-", true)),
            storage,
            new FileCommitter(
                brokerId, controlPlane, objectKeyCreator, storage,
                keyAlignmentStrategy, objectCache, batchCoordinateCache, time,
                maxFileUploadAttempts, fileUploadRetryBackoff,
                fileUploaderThreadPoolSize),
            new WriterMetrics(time),
            brokerTopicStats,
            validationWorkerCount
        );
    }

    // Visible for testing
    PipelinedWriter(
        final Time time,
        final Duration commitInterval,
        final int maxBufferSize,
        final ScheduledExecutorService tickScheduler,
        final StorageBackend storage,
        final FileCommitter fileCommitter,
        final WriterMetrics writerMetrics,
        final BrokerTopicStats brokerTopicStats,
        final int validationWorkerCount
    ) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.commitInterval = Objects.requireNonNull(commitInterval, "commitInterval cannot be null");
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive");
        }
        this.maxBufferSize = maxBufferSize;
        this.tickScheduler = Objects.requireNonNull(tickScheduler, "tickScheduler cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.fileCommitter = Objects.requireNonNull(fileCommitter, "fileCommitter cannot be null");
        this.writerMetrics = Objects.requireNonNull(writerMetrics, "writerMetrics cannot be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats cannot be null");
        this.validationWorkerCount = validationWorkerCount > 0 ? validationWorkerCount : Runtime.getRuntime().availableProcessors();

        // Initialize validation worker pool
        this.validationExecutor = Executors.newFixedThreadPool(
            this.validationWorkerCount,
            new InklessThreadFactory("inkless-validator-", true)
        );

        // Initialize buffer queue (bounded for backpressure)
        this.bufferQueue = new ArrayBlockingQueue<>(BUFFER_QUEUE_CAPACITY);

        // Initialize single buffer writer thread
        this.bufferWriterExecutor = Executors.newSingleThreadExecutor(
            new InklessThreadFactory("inkless-buffer-writer-", false)
        );

        // Initialize ActiveFile on buffer writer thread
        this.activeFile = new ActiveFile(time, this.brokerTopicStats);

        // Metrics recorder (thread-safe)
        this.validatorMetricsRecorder = newValidatorMetricsRecorder(this.brokerTopicStats.allTopicsStats());

        // Start buffer writer loop
        bufferWriterExecutor.submit(this::bufferWriterLoop);

        LOGGER.info("PipelinedWriter started with {} validation workers", this.validationWorkerCount);
    }

    /**
     * Entry point for write requests from Kafka request handler threads.
     *
     * <p>This method submits the request to the validation stage and returns immediately
     * with a future that will be completed when the request is fully processed.
     */
    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> write(
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

        // Verify ALL requested topics have configs (fail if ANY is missing)
        if (!entriesPerPartition.keySet().stream().map(TopicIdPartition::topic).distinct().allMatch(topicConfigs::containsKey)) {
            throw new IllegalArgumentException("Configs are not including all the topics requested");
        }

        if (closed.get()) {
            return CompletableFuture.failedFuture(new RuntimeException("Writer already closed"));
        }

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture = new CompletableFuture<>();

        // Submit to validation stage (async, non-blocking for caller)
        // The exceptionally handler ensures resultFuture is completed even if runAsync submission fails
        // (e.g., RejectedExecutionException if executor is shutting down)
        CompletableFuture.runAsync(() -> {
            try {
                // Each validation worker must use its own RequestLocal instance to
                // avoid sharing thread-confined state from the request handler thread.
                RequestLocal validationRequestLocal = RequestLocal.withThreadConfinedCaching();

                // ══════════════════════════════════════════════════════════════
                // STAGE 1: Validation (runs in parallel on validation workers)
                // ══════════════════════════════════════════════════════════════
                ValidationResult validated = validateRequest(
                    entriesPerPartition,
                    topicConfigs,
                    validationRequestLocal
                );

                // Create the validated request with the future
                ValidatedRequest request = new ValidatedRequest(
                    entriesPerPartition,
                    validated.validatedBatches(),
                    validated.invalidBatches(),
                    resultFuture
                );

                // Hand off to buffer writer stage (blocks if queue full = backpressure)
                if (!bufferQueue.offer(request, 30, TimeUnit.SECONDS)) {
                    resultFuture.completeExceptionally(
                        new KafkaStorageException("Buffer queue full, request timed out"));
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                resultFuture.completeExceptionally(e);
            } catch (Exception e) {
                LOGGER.error("Validation failed", e);
                resultFuture.completeExceptionally(e);
            }
        }, validationExecutor).exceptionally(e -> {
            // Handle case where runAsync itself fails (e.g., executor shutdown)
            resultFuture.completeExceptionally(e);
            return null;
        });

        return resultFuture;
    }

    /**
     * Stage 1: Validates all batches in a request.
     *
     * <p>This method performs CPU-intensive validation work:
     * <ul>
     *   <li>CRC validation</li>
     *   <li>Size checks</li>
     *   <li>Offset assignment</li>
     *   <li>Timestamp validation</li>
     * </ul>
     *
     * <p>This runs on validation worker threads, NOT under any lock.
     *
     * @return a ValidationResult containing validated and invalid batches
     */
    private ValidationResult validateRequest(
        Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        Map<String, LogConfig> topicConfigs,
        RequestLocal requestLocal
    ) {
        Map<TopicIdPartition, ValidatedRequest.ValidatedBatch> validatedBatches = new HashMap<>();
        Map<TopicIdPartition, PartitionResponse> invalidBatches = new HashMap<>();

        for (var entry : entriesPerPartition.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            MemoryRecords records = entry.getValue();
            LogConfig config = topicConfigs.get(topicIdPartition.topic());

            if (config == null) {
                throw new IllegalArgumentException("Config not provided for topic " + topicIdPartition.topic());
            }

            try {
                // Mark metrics
                brokerTopicStats.topicStats(topicIdPartition.topic()).totalProduceRequestRate().mark();
                brokerTopicStats.allTopicsStats().totalProduceRequestRate().mark();

                // ═══════════════════════════════════════════════════════════
                // CPU-intensive validation work (NO LOCK!)
                // ═══════════════════════════════════════════════════════════
                LogAppendInfo appendInfo = UnifiedLog.analyzeAndValidateRecords(
                    topicIdPartition.topicPartition(),
                    config,
                    records,
                    UNKNOWN_OFFSET,  // logStartOffset - set on control-plane, use unknown to fulfill validation
                    UnifiedLog.APPEND_ORIGIN,
                    false,  // ignoreRecordSize
                    true,   // requireOffsetsMonotonic
                    UnifiedLog.LEADER_EPOCH,
                    brokerTopicStats
                );

                if (appendInfo.validBytes() <= 0) {
                    // Empty batch
                    invalidBatches.put(topicIdPartition, new PartitionResponse(Errors.NONE));
                    continue;
                }

                MemoryRecords validRecords = UnifiedLog.trimInvalidBytes(
                    topicIdPartition.topicPartition(),
                    records,
                    appendInfo
                );

                // Offset assignment
                PrimitiveRef.LongRef offset = PrimitiveRef.ofLong(appendInfo.firstOffset());
                Compression targetCompression = BrokerCompressionType.targetCompression(
                    config.compression,
                    appendInfo.sourceCompression()
                );

                LogValidator validator = new LogValidator(
                    validRecords,
                    topicIdPartition.topicPartition(),
                    time,
                    appendInfo.sourceCompression(),
                    targetCompression,
                    config.compact,
                    RecordBatch.CURRENT_MAGIC_VALUE,
                    config.messageTimestampType,
                    config.messageTimestampBeforeMaxMs,
                    config.messageTimestampAfterMaxMs,
                    UnifiedLog.LEADER_EPOCH,
                    UnifiedLog.APPEND_ORIGIN
                );

                LogValidator.ValidationResult result = validator.validateMessagesAndAssignOffsets(
                    offset,
                    validatorMetricsRecorder,
                    requestLocal.bufferSupplier()
                );

                MemoryRecords finalRecords = result.validatedRecords();
                appendInfo.setMaxTimestamp(result.maxTimestampMs());
                appendInfo.setLastOffset(offset.value - 1);
                appendInfo.setRecordValidationStats(result.recordValidationStats());
                if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME) {
                    appendInfo.setLogAppendTime(result.logAppendTimeMs());
                }

                // Re-validate sizes if changed
                if (result.messageSizeMaybeChanged()) {
                    for (MutableRecordBatch batch : finalRecords.batches()) {
                        if (batch.sizeInBytes() > config.maxMessageSize()) {
                            brokerTopicStats.topicStats(topicIdPartition.topicPartition().topic())
                                .bytesRejectedRate().mark(records.sizeInBytes());
                            brokerTopicStats.allTopicsStats().bytesRejectedRate().mark(records.sizeInBytes());
                            throw new RecordTooLargeException("Message batch size is " + batch.sizeInBytes() +
                                " bytes which exceeds the maximum configured size of " + config.maxMessageSize());
                        }
                    }
                }

                // Update metrics for successfully validated
                brokerTopicStats.topicStats(topicIdPartition.topic()).bytesInRate(true).mark(records.sizeInBytes());
                brokerTopicStats.allTopicsStats().bytesInRate(true).mark(records.sizeInBytes());
                brokerTopicStats.topicStats(topicIdPartition.topic()).messagesInRate().mark(appendInfo.numMessages());
                brokerTopicStats.allTopicsStats().messagesInRate().mark(appendInfo.numMessages());

                validatedBatches.put(topicIdPartition, new ValidatedRequest.ValidatedBatch(
                    topicIdPartition,
                    finalRecords,
                    appendInfo
                ));

            } catch (RecordTooLargeException | CorruptRecordException | KafkaStorageException e) {
                invalidBatches.put(topicIdPartition, new PartitionResponse(Errors.forException(e)));
            } catch (RecordValidationException rve) {
                processFailedRecords(topicIdPartition.topicPartition(), rve.invalidException());
                invalidBatches.put(topicIdPartition,
                    new PartitionResponse(
                        Errors.forException(rve.invalidException()),
                        ProduceResponse.INVALID_OFFSET,
                        RecordBatch.NO_TIMESTAMP,
                        ProduceResponse.INVALID_OFFSET,
                        rve.recordErrors(),
                        rve.getMessage()
                    ));
            } catch (Throwable t) {
                processFailedRecords(topicIdPartition.topicPartition(), t);
                invalidBatches.put(topicIdPartition, new PartitionResponse(Errors.forException(t)));
            }
        }

        return new ValidationResult(validatedBatches, invalidBatches);
    }

    private void processFailedRecords(TopicPartition topicPartition, Throwable t) {
        brokerTopicStats.topicStats(topicPartition.topic()).failedProduceRequestRate().mark();
        brokerTopicStats.allTopicsStats().failedProduceRequestRate().mark();
        if (t instanceof InvalidProducerEpochException) {
            // InvalidProducerEpochException is expected during producer fencing, log at INFO
            LOGGER.info("Error processing append operation on partition {}", topicPartition, t);
        } else {
            LOGGER.error("Error processing append operation on partition {}", topicPartition, t);
        }
    }

    /**
     * Stage 2: Buffer writer loop (single thread, NO LOCK!).
     *
     * <p>This method runs on a dedicated thread and owns all mutable buffer state.
     * It drains validated requests from the queue and appends them to the active file.
     */
    private void bufferWriterLoop() {
        LOGGER.info("Buffer writer thread started");

        while (!closed.get() || !bufferQueue.isEmpty()) {
            try {
                // Drain multiple requests for efficiency (batch processing)
                List<ValidatedRequest> batch = new ArrayList<>();

                // Block waiting for at least one request
                ValidatedRequest first = bufferQueue.poll(BUFFER_WRITER_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (first != null) {
                    batch.add(first);
                    // Drain any additional waiting requests (non-blocking)
                    bufferQueue.drainTo(batch, MAX_BATCH_DRAIN_SIZE - 1);
                }

                // Process batch (NO LOCK - single thread owns state!)
                for (ValidatedRequest request : batch) {
                    processValidatedRequest(request);
                }

                // Check for tick-based rotation if no requests processed
                if (batch.isEmpty()) {
                    maybeTickRotate();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("Buffer writer thread interrupted");
                break;
            } catch (Exception e) {
                LOGGER.error("Error in buffer writer loop", e);
            }
        }

        LOGGER.info("Buffer writer thread exiting");
    }

    /**
     * Process a single validated request (runs on buffer writer thread only).
     */
    private void processValidatedRequest(ValidatedRequest request) {
        try {
            // Check if this is a rotation trigger from tick scheduler
            if (isRotationTrigger(request)) {
                if (!activeFile.isEmpty()) {
                    LOGGER.debug("Processing tick-based rotation trigger");
                    cancelScheduledTick();
                    rotateFile(false);
                }
                return;  // Don't process rotation triggers as normal requests
            }

            if (openedAt == null && !request.hasNoValidBatches()) {
                openedAt = TimeUtils.durationMeasurementNow(time);
            }

            // Check if rotation needed before adding
            int incomingSize = request.validatedSize();
            int currentSize = activeFile.size();

            if (!activeFile.isEmpty() && currentSize + incomingSize > maxBufferSize) {
                cancelScheduledTick();
                rotateFile(false);
            }

            // Assign request ID and add batches to buffer
            int requestId = nextRequestId++;

            for (var entry : request.validatedBatches().entrySet()) {
                TopicIdPartition tip = entry.getKey();
                ValidatedRequest.ValidatedBatch batch = entry.getValue();

                // Add each record batch to the buffer
                for (MutableRecordBatch recordBatch : batch.validatedRecords().batches()) {
                    activeFile.addBatchDirect(tip, recordBatch, requestId);
                }
            }

            // Store future for completion on file commit (responses built during commit phase by AppendCompleter)
            activeFile.addAwaitingFuture(requestId, request.resultFuture(), request.originalRecords(), request.invalidBatches());

            writerMetrics.requestAdded();

            // Check if rotation needed after adding
            if (activeFile.size() >= maxBufferSize) {
                cancelScheduledTick();
                rotateFile(false);
            } else if (scheduledTick == null && !activeFile.isEmpty()) {
                scheduleTickRotation();
            }

        } catch (Exception e) {
            LOGGER.error("Error processing validated request", e);
            request.resultFuture().completeExceptionally(e);
        }
    }

    private void maybeTickRotate() {
        // Called when no requests were processed in this iteration.
        // Ensure rotation is scheduled if there's buffered data but no scheduled tick.
        if (!activeFile.isEmpty() && scheduledTick == null) {
            scheduleTickRotation();
        }
    }

    private void scheduleTickRotation() {
        scheduledTick = tickScheduler.schedule(
            this::tickRotate,
            commitInterval.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    private void cancelScheduledTick() {
        if (scheduledTick != null) {
            scheduledTick.cancel(false);
            scheduledTick = null;
        }
    }

    /**
     * Called by tick scheduler to trigger rotation.
     * Submits a rotation command to the buffer queue.
     *
     * <p>Note: This method runs on the tick scheduler thread, not the buffer writer thread.
     * We only check the closed flag here (which is thread-safe via AtomicBoolean).
     * The buffer writer thread will decide whether to actually rotate based on activeFile state.
     */
    private void tickRotate() {
        if (!closed.get()) {
            try {
                // Submit a rotation trigger to the buffer queue.
                // The buffer writer thread will check if rotation is needed.
                bufferQueue.offer(createRotationTrigger(), 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private ValidatedRequest createRotationTrigger() {
        return ValidatedRequest.rotationTrigger();
    }

    private boolean isRotationTrigger(ValidatedRequest request) {
        return request.isRotationTrigger();
    }

    private void rotateFile(boolean swallowInterrupted) {
        LOGGER.debug("Rotating active file");
        ActiveFile prevActiveFile = activeFile;
        activeFile = new ActiveFile(time, brokerTopicStats);
        scheduledTick = null;

        try {
            fileCommitter.commit(prevActiveFile.close());
            if (openedAt != null) {
                writerMetrics.fileRotated(openedAt);
                openedAt = null;
            }
        } catch (InterruptedException e) {
            if (!swallowInterrupted) {
                LOGGER.error("Interrupted during rotation", e);
                throw new RuntimeException(e);
            } else {
                LOGGER.info("Interrupted during rotation, ignoring", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        LOGGER.info("Closing PipelinedWriter");

        // Stop accepting new validation work
        validationExecutor.shutdown();

        // Wait for validation tasks to complete and enqueue their results
        try {
            if (!validationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warn("Validation executor did not terminate gracefully");
                validationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            validationExecutor.shutdownNow();
        }

        // Cancel tick scheduler
        tickScheduler.shutdownNow();

        // Wait for buffer queue to drain and buffer writer to finish
        bufferWriterExecutor.shutdown();
        try {
            if (!bufferWriterExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warn("Buffer writer did not terminate gracefully");
                bufferWriterExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            bufferWriterExecutor.shutdownNow();
        }

        // Final rotation if there's pending data
        if (!activeFile.isEmpty()) {
            rotateFile(true);
        }

        // Close downstream components
        fileCommitter.close();
        storage.close();
        writerMetrics.close();

        LOGGER.info("PipelinedWriter closed");
    }
}
