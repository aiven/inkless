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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import io.aiven.inkless.storage_backend.common.StorageBackend;

/**
 * The file committer.
 *
 * <p>It uploads files concurrently, but commits them to the control plan sequentially.
 */
class FileCommitter implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitter.class);

    private final FileCommitterMetrics metrics;

    private final Lock lock = new ReentrantLock();

    private final int brokerId;
    private final ControlPlane controlPlane;
    private final ObjectKeyCreator objectKeyCreator;
    private final StorageBackend storage;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache objectCache;
    private final BatchCoordinateCache batchCoordinateCache;
    private final Time time;
    private final int maxFileUploadAttempts;
    private final Duration fileUploadRetryBackoff;
    private final ExecutorService executorServiceUpload;
    private final ExecutorService executorServiceCommit;
    private final ExecutorService executorServiceCacheStore;
    private final boolean asyncCommitPipeline;
    private ThreadPoolMonitor threadPoolMonitor;

    private final AtomicInteger totalFilesInProgress = new AtomicInteger(0);
    private final AtomicInteger totalBytesInProgress = new AtomicInteger(0);

    // Tracks the previous commit to ensure commits happen in submission order, not upload completion order.
    // Each new commit waits for both its upload AND the previous commit to complete before starting.
    // Only used when asyncCommitPipeline is true.
    private CompletableFuture<?> previousCommitFuture = CompletableFuture.completedFuture(null);

    @DoNotMutate
    FileCommitter(final int brokerId,
                  final ControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final StorageBackend storage,
                  final KeyAlignmentStrategy keyAlignmentStrategy,
                  final ObjectCache objectCache,
                  final BatchCoordinateCache batchCoordinateCache,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff,
                  final int fileUploaderThreadPoolSize,
                  final boolean asyncCommitPipeline) {
        this(brokerId, controlPlane, objectKeyCreator, storage, keyAlignmentStrategy, objectCache, batchCoordinateCache, time, maxFileUploadAttempts, fileUploadRetryBackoff,
            Executors.newFixedThreadPool(fileUploaderThreadPoolSize, new InklessThreadFactory("inkless-file-uploader-", false)),
            // It must be single-thread to preserve the commit order.
            Executors.newSingleThreadExecutor(new InklessThreadFactory("inkless-file-committer-", false)),
            // Reuse the same thread pool size as uploads, as there are no more concurrency expected to handle
            Executors.newFixedThreadPool(fileUploaderThreadPoolSize, new InklessThreadFactory("inkless-file-cache-store-", false)),
            new FileCommitterMetrics(time),
            asyncCommitPipeline
        );
    }

    // Visible for testing
    FileCommitter(final int brokerId,
                  final ControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final StorageBackend storage,
                  final KeyAlignmentStrategy keyAlignmentStrategy,
                  final ObjectCache objectCache,
                  final BatchCoordinateCache batchCoordinateCache,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff,
                  final ExecutorService executorServiceUpload,
                  final ExecutorService executorServiceCommit,
                  final ExecutorService executorServiceCacheStore,
                  final FileCommitterMetrics metrics,
                  final boolean asyncCommitPipeline) {
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
        this.asyncCommitPipeline = asyncCommitPipeline;

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

        lock.lock();
        try {
            final Instant uploadAndCommitStart = TimeUtils.durationMeasurementNow(time);
            final CompletableFuture<List<CommitBatchResponse>> commitFuture;

            if (file.isEmpty()) {
                commitFuture = CompletableFuture.completedFuture(Collections.emptyList());
            } else {
                commitFuture = commitNonEmptyFile(file, uploadAndCommitStart);
            }

            attachCompletionHandler(file, commitFuture);
        } finally {
            lock.unlock();
        }
    }

    private CompletableFuture<List<CommitBatchResponse>> commitNonEmptyFile(
            final ClosedFile file,
            final Instant uploadAndCommitStart) {

        updateMetricsOnStart(file);

        final CompletableFuture<ObjectKey> uploadFuture = startUpload(file);
        final FileCommitJob commitJob = createCommitJob(file);
        final CacheStoreJob cacheStoreJob = createCacheStoreJob(file);

        if (asyncCommitPipeline) {
            return commitAsync(file, uploadFuture, commitJob, cacheStoreJob, uploadAndCommitStart);
        } else {
            return commitSync(file, uploadFuture, commitJob, cacheStoreJob, uploadAndCommitStart);
        }
    }

    private void updateMetricsOnStart(final ClosedFile file) {
        metrics.fileAdded(file.size());
        metrics.batchesAdded(file.commitBatchRequests().size());
        totalFilesInProgress.addAndGet(1);
        totalBytesInProgress.addAndGet(file.size());
    }

    private CompletableFuture<ObjectKey> startUpload(final ClosedFile file) {
        final FileUploadJob uploadJob = FileUploadJob.createFromByteArray(
                objectKeyCreator, storage, time,
                maxFileUploadAttempts, fileUploadRetryBackoff,
                file.data(), metrics::fileUploadFinished
        );
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return uploadJob.call();
                    } catch (final Exception e) {
                        throw new FileUploadException(e);
                    }
                },
                executorServiceUpload
        );
    }

    private FileCommitJob createCommitJob(final ClosedFile file) {
        return new FileCommitJob(
                brokerId, file, time, controlPlane, storage,
                metrics::fileCommitFinished, metrics::fileCommitWaitFinished
        );
    }

    private CacheStoreJob createCacheStoreJob(final ClosedFile file) {
        return new CacheStoreJob(
                time, objectCache, keyAlignmentStrategy,
                file.data(), metrics::cacheStoreFinished
        );
    }

    /**
     * Async commit pipeline: callbacks triggered when upload completes, no thread blocking.
     * Commits are chained to ensure submission order is preserved.
     */
    private CompletableFuture<List<CommitBatchResponse>> commitAsync(
            final ClosedFile file,
            final CompletableFuture<ObjectKey> uploadFuture,
            final FileCommitJob commitJob,
            final CacheStoreJob cacheStoreJob,
            final Instant uploadAndCommitStart) {

        // Wait for previous commit to complete (success OR failure) before starting this one.
        // Using handle() ensures we don't propagate previous failures - each commit is independent.
        // We only use previousCommitFuture for ordering, not for error propagation.
        final CompletableFuture<?> prevCommitBarrier = previousCommitFuture.handle((result, error) -> null);
        final CompletableFuture<List<CommitBatchResponse>> commitFuture = uploadFuture
                .thenCombine(prevCommitBarrier, (objectKey, ignored) -> {
                    // Reset the submit time now that we're actually ready to commit.
                    // This ensures FileCommitWaitTime measures only the executor queue wait,
                    // not the time waiting for previous commits in the chain.
                    commitJob.markReadyToCommit();
                    return objectKey;
                })
                .handleAsync(commitJob, executorServiceCommit)
                .whenComplete((result, error) -> handleCommitResult(file, uploadAndCommitStart, error));

        // Cache store as non-blocking callback
        uploadFuture.whenCompleteAsync(cacheStoreJob, executorServiceCacheStore);

        // Update chain for next commit
        previousCommitFuture = commitFuture;
        return commitFuture;
    }

    /**
     * Sync commit pipeline: threads block on uploadFuture.get() waiting for upload.
     * This is the original behavior before the async pipeline was introduced.
     */
    private CompletableFuture<List<CommitBatchResponse>> commitSync(
            final ClosedFile file,
            final CompletableFuture<ObjectKey> uploadFuture,
            final FileCommitJob commitJob,
            final CacheStoreJob cacheStoreJob,
            final Instant uploadAndCommitStart) {

        final CompletableFuture<List<CommitBatchResponse>> commitFuture = CompletableFuture.supplyAsync(
                () -> waitForUploadAndCommit(uploadFuture, commitJob),
                executorServiceCommit
        ).whenComplete((result, error) -> handleCommitResult(file, uploadAndCommitStart, error));

        // Cache store with blocking wait
        CompletableFuture.runAsync(
                () -> waitForUploadAndCache(uploadFuture, cacheStoreJob),
                executorServiceCacheStore
        );

        return commitFuture;
    }

    private List<CommitBatchResponse> waitForUploadAndCommit(
            final CompletableFuture<ObjectKey> uploadFuture,
            final FileCommitJob commitJob) {
        ObjectKey objectKey = null;
        Throwable error = null;
        try {
            objectKey = uploadFuture.get();
        } catch (final Exception e) {
            error = e;
        }
        return commitJob.onUploadComplete(objectKey, error);
    }

    private void waitForUploadAndCache(
            final CompletableFuture<ObjectKey> uploadFuture,
            final CacheStoreJob cacheStoreJob) {
        ObjectKey objectKey = null;
        Throwable error = null;
        try {
            objectKey = uploadFuture.get();
        } catch (final Exception e) {
            error = e;
        }
        cacheStoreJob.onUploadComplete(objectKey, error);
    }

    private void handleCommitResult(final ClosedFile file, final Instant uploadAndCommitStart, final Throwable error) {
        totalFilesInProgress.addAndGet(-1);
        totalBytesInProgress.addAndGet(-file.size());
        if (error != null) {
            LOGGER.error("Failed to commit diskless file {}", file, error);
            if (error.getCause() instanceof FileUploadException) {
                metrics.fileUploadFailed();
            } else {
                metrics.fileCommitFailed();
            }
        } else {
            metrics.fileFinished(file.start(), uploadAndCommitStart);
        }
    }

    private void attachCompletionHandler(
            final ClosedFile file,
            final CompletableFuture<List<CommitBatchResponse>> commitFuture) {
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

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    @Override
    public void close() throws IOException {
        executorServiceUpload.shutdown();
        executorServiceCommit.shutdown();
        executorServiceCacheStore.shutdown();
        try {
            if (!executorServiceUpload.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("Upload executor did not terminate in time, forcing shutdown");
                executorServiceUpload.shutdownNow();
            }
            if (!executorServiceCommit.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("Commit executor did not terminate in time, forcing shutdown");
                executorServiceCommit.shutdownNow();
            }
            if (!executorServiceCacheStore.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("Cache store executor did not terminate in time, forcing shutdown");
                executorServiceCacheStore.shutdownNow();
            }
        } catch (final InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for executor shutdown", e);
            executorServiceUpload.shutdownNow();
            executorServiceCommit.shutdownNow();
            executorServiceCacheStore.shutdownNow();
            Thread.currentThread().interrupt();
        }
        metrics.close();
        if (threadPoolMonitor != null) threadPoolMonitor.close();
    }
}
