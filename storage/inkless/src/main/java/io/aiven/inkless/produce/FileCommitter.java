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

import org.apache.kafka.common.utils.Time;

import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.metrics.ThreadPoolMonitor;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.Storage;

/**
 * The file committer.
 *
 * <p>It uploads files concurrently, but commits them to the control plane sequentially.
 *
 * <p><b>Threading:</b> This class is designed to be called from a single thread (the buffer-writer
 * thread in {@link PipelinedWriter} or the lock-protected rotateFile in {@link Writer}).
 * All internal operations use atomic/thread-safe primitives (AtomicInteger for counters,
 * thread-safe ExecutorService for task submission, CompletableFuture for async chaining).
 * The class does NOT support concurrent commit() calls - commits must be serialized by the caller.
 */
class FileCommitter implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitter.class);

    private final FileCommitterMetrics metrics;

    private final int brokerId;
    private final ControlPlane controlPlane;
    private final ObjectKeyCreator objectKeyCreator;
    private final Storage storage;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache objectCache;
    private final BatchCoordinateCache batchCoordinateCache;
    private final Time time;
    private final int maxFileUploadAttempts;
    private final Duration fileUploadRetryBackoff;
    private final ExecutorService executorServiceUpload;
    private final ExecutorService executorServiceCommit;
    private final ExecutorService executorServiceCacheStore;
    private ThreadPoolMonitor threadPoolMonitor;

    private final AtomicInteger totalFilesInProgress = new AtomicInteger(0);
    private final AtomicInteger totalBytesInProgress = new AtomicInteger(0);

    @DoNotMutate
    FileCommitter(final int brokerId,
                  final ControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final Storage storage,
                  final KeyAlignmentStrategy keyAlignmentStrategy,
                  final ObjectCache objectCache,
                  final BatchCoordinateCache batchCoordinateCache,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff,
                  final int fileUploaderThreadPoolSize) {
        this(brokerId, controlPlane, objectKeyCreator, storage, keyAlignmentStrategy, objectCache, batchCoordinateCache, time, maxFileUploadAttempts, fileUploadRetryBackoff,
            Executors.newFixedThreadPool(fileUploaderThreadPoolSize, new InklessThreadFactory("inkless-file-uploader-", false)),
            // It must be single-thread to preserve the commit order.
            Executors.newSingleThreadExecutor(new InklessThreadFactory("inkless-file-committer-", false)),
            // Cache store uses non-daemon threads to ensure proper buffer release during shutdown.
            // While caching is optional for correctness, we want buffers to be cleanly released
            // to avoid leaving the buffer pool in an inconsistent state. The close() method
            // provides a graceful shutdown timeout for these tasks.
            Executors.newFixedThreadPool(fileUploaderThreadPoolSize, new InklessThreadFactory("inkless-file-cache-store-", false)),
            new FileCommitterMetrics(time)
        );
    }

    // Visible for testing
    FileCommitter(final int brokerId,
                  final ControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final Storage storage,
                  final KeyAlignmentStrategy keyAlignmentStrategy,
                  final ObjectCache objectCache,
                  final BatchCoordinateCache batchCoordinateCache,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff,
                  final ExecutorService executorServiceUpload,
                  final ExecutorService executorServiceCommit,
                  final ExecutorService executorServiceCacheStore,
                  final FileCommitterMetrics metrics) {
        this.brokerId = brokerId;
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.objectCache = Objects.requireNonNull(objectCache, "objectCache cannot be null");
        this.batchCoordinateCache = Objects.requireNonNull(batchCoordinateCache, "batchCoordinateCache cannot be null");
        this.keyAlignmentStrategy = Objects.requireNonNull(keyAlignmentStrategy, "keyAlignmentStrategy cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (maxFileUploadAttempts <= 0) {
            throw new IllegalArgumentException("maxFileUploadAttempts must be positive");
        }
        this.maxFileUploadAttempts = maxFileUploadAttempts;
        this.fileUploadRetryBackoff = Objects.requireNonNull(fileUploadRetryBackoff,
            "fileUploadRetryBackoff cannot be null");
        this.executorServiceUpload = Objects.requireNonNull(executorServiceUpload,
            "executorServiceUpload cannot be null");
        this.executorServiceCommit = Objects.requireNonNull(executorServiceCommit,
            "executorServiceCommit cannot be null");
        this.executorServiceCacheStore = Objects.requireNonNull(executorServiceCacheStore,
                "executorServiceCacheStore cannot be null");

        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        // Can't do this in the FileCommitterMetrics constructor, so initializing this way.
        this.metrics.initTotalFilesInProgressMetric(totalFilesInProgress::get);
        this.metrics.initTotalBytesInProgressMetric(totalBytesInProgress::get);
        try {
            this.threadPoolMonitor = new ThreadPoolMonitor("inkless-produce-uploader", executorServiceUpload);
        } catch (final Exception e) {
            // only expected to happen on tests passing other types of pools
            LOGGER.warn("Failed to create ThreadPoolMonitor for inkless-produce-uploader", e);
        }
    }

    void commit(final ClosedFile file) throws InterruptedException {
        Objects.requireNonNull(file, "file cannot be null");

        final Instant uploadAndCommitStart = TimeUtils.durationMeasurementNow(time);
        CompletableFuture<List<CommitBatchResponse>> commitFuture;

        if (file.isEmpty()) {
            // If the file is empty, skip uploading and committing, but proceed with the later steps.
            commitFuture = CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            metrics.fileAdded(file.size());
            metrics.batchesAdded(file.commitBatchRequests().size());
            totalFilesInProgress.addAndGet(1);
            totalBytesInProgress.addAndGet(file.size());

            // Buffer refCount starts at 1. retain() increments for CacheStoreJob.
            // FileUploadJob and CacheStoreJob each release; buffer returns to pool when both complete.
            try {
                file.data().retain();
            } catch (final IllegalStateException e) {
                LOGGER.error("Failed to retain buffer for file upload - buffer already released. "
                    + "This indicates a bug in buffer lifecycle management.", e);
                throw e;
            }

            // Use AsyncFileUploadJob for true non-blocking uploads.
            // When CRT is enabled, this allows the upload to proceed without blocking any thread,
            // fully utilizing the async I/O capabilities.
            final AsyncFileUploadJob uploadJob = AsyncFileUploadJob.createFromBatchBufferData(
                objectKeyCreator,
                storage,
                time,
                maxFileUploadAttempts,
                fileUploadRetryBackoff,
                file.data(),
                metrics::fileUploadFinished
            );

            // uploadAsync() returns a future that completes when upload is done.
            // With CRT, this is truly non-blocking - no thread is held during S3 I/O.
            final CompletableFuture<ObjectKey> uploadFuture = uploadJob.uploadAsync()
                .exceptionally(e -> {
                    // Wrap the exception for consistent error handling downstream
                    throw new FileUploadException(e);
                });

            final FileCommitJob commitJob = new FileCommitJob(
                brokerId,
                file,
                time,
                controlPlane,
                storage,
                metrics::fileCommitFinished,
                metrics::fileCommitWaitFinished
            );

            // Chain the commit as a callback on upload completion.
            // This eliminates the previous pattern where the commit thread would block
            // on uploadFuture.get(), wasting thread resources waiting for S3 latency.
            // The callback runs on executorServiceCommit (single-threaded) to maintain
            // serialization of commits, but the thread only wakes when uploads complete.
            commitFuture = uploadFuture.handleAsync(commitJob::onUploadComplete, executorServiceCommit)
                .whenComplete((result, error) -> {
                    totalFilesInProgress.addAndGet(-1);
                    totalBytesInProgress.addAndGet(-file.size());
                    if (error != null) {
                        // at this point the commit has failed and need to check whether it failed on upload or commit
                        LOGGER.error("Failed to commit diskless file {}", file, error);
                        if (error.getCause() instanceof FileUploadException) {
                            metrics.fileUploadFailed();
                        } else {
                            metrics.fileCommitFailed();
                        }
                    } else {
                        // only mark as finished if everything succeeded
                        metrics.fileFinished(file.start(), uploadAndCommitStart);
                    }
                });

            // Chain cache store as a non-blocking callback on upload completion.
            // This eliminates the previous pattern where cache store threads would block
            // on uploadFuture.get(), wasting thread resources waiting for S3 latency.
            // The callback runs on executorServiceCacheStore to maintain thread isolation.
            final CacheStoreJob cacheStoreJob = new CacheStoreJob(
                time,
                objectCache,
                keyAlignmentStrategy,
                file.data(),
                metrics::cacheStoreFinished
            );
            uploadFuture.whenCompleteAsync(cacheStoreJob::onUploadComplete, executorServiceCacheStore);
        }

        commitFuture.whenComplete((commitBatchResponses, throwable) -> {
            final AppendCompleter completerJob = new AppendCompleter(file, batchCoordinateCache);
            if (commitBatchResponses != null) {
                completerJob.finishCommitSuccessfully(commitBatchResponses);
                metrics.writeCompleted();
            } else {
                completerJob.finishCommitWithError();
                metrics.writeFailed();
            }
        });
    }

    int totalFilesInProgress() {
        return totalFilesInProgress.get();
    }

    int totalBytesInProgress() {
        return totalBytesInProgress.get();
    }

    @Override
    public void close() throws IOException {
        executorServiceUpload.shutdown();
        executorServiceCommit.shutdown();
        executorServiceCacheStore.shutdown();

        // Wait for cache store to release buffers back to pool
        try {
            if (!executorServiceCacheStore.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("Cache store executor did not terminate within timeout. "
                    + "Some buffers may not be released cleanly.");
                executorServiceCacheStore.shutdownNow();
            }
        } catch (final InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for cache store shutdown", e);
            executorServiceCacheStore.shutdownNow();
            Thread.currentThread().interrupt();
        }

        metrics.close();
        if (threadPoolMonitor != null) threadPoolMonitor.close();
    }
}
