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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;

/**
 * The file committer.
 *
 * <p>It uploads files concurrently, but commits them to the control plan sequentially.
 */
class FileCommitter implements Closeable {
    private final FileCommitterMetrics metrics;

    private final Lock lock = new ReentrantLock();

    private final int brokerId;
    private final ControlPlane controlPlane;
    private final ObjectKeyCreator objectKeyCreator;
    private final StorageBackend storage;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache objectCache;
    private final Time time;
    private final int maxFileUploadAttempts;
    private final Duration fileUploadRetryBackoff;
    private final ExecutorService executorServiceUpload;
    private final ExecutorService executorServiceCommit;
    private final ExecutorService executorServiceComplete;
    private final ExecutorService executorServiceCacheStore;

    private final AtomicInteger totalFilesInProgress = new AtomicInteger(0);
    private final AtomicInteger totalBytesInProgress = new AtomicInteger(0);

    @DoNotMutate
    FileCommitter(final int brokerId,
                  final ControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final StorageBackend storage,
                  final KeyAlignmentStrategy keyAlignmentStrategy,
                  final ObjectCache objectCache,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff) {
        this(brokerId, controlPlane, objectKeyCreator, storage, keyAlignmentStrategy, objectCache, time, maxFileUploadAttempts, fileUploadRetryBackoff,
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-file-uploader-", false)),
            // It must be single-thread to preserve the commit order.
            Executors.newSingleThreadExecutor(new InklessThreadFactory("inkless-file-committer-", false)),
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-file-uploader-finisher-", false)),
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-file-cache-store-", false)),
            new FileCommitterMetrics(time)
        );
    }

    // Visible for testing
    FileCommitter(final int brokerId,
                  final ControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final StorageBackend storage,
                  final KeyAlignmentStrategy keyAlignmentStrategy,
                  final ObjectCache objectCache,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff,
                  final ExecutorService executorServiceUpload,
                  final ExecutorService executorServiceCommit,
                  final ExecutorService executorServiceComplete,
                  final ExecutorService executorServiceCacheStore,
                  final FileCommitterMetrics metrics) {
        this.brokerId = brokerId;
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.objectCache = Objects.requireNonNull(objectCache, "objectCache cannot be null");
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
        this.executorServiceComplete = Objects.requireNonNull(executorServiceComplete,
                "executorServiceComplete cannot be null");
        this.executorServiceCacheStore = Objects.requireNonNull(executorServiceCacheStore,
                "executorServiceCacheStore cannot be null");

        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        // Can't do this in the FileCommitterMetrics constructor, so initializing this way.
        this.metrics.initTotalFilesInProgressMetric(totalFilesInProgress::get);
        this.metrics.initTotalBytesInProgressMetric(totalBytesInProgress::get);
    }

    void commit(final ClosedFile file) throws InterruptedException {
        Objects.requireNonNull(file, "file cannot be null");

        lock.lock();
        try {
            final Instant uploadAndCommitStart = TimeUtils.durationMeasurementNow(time);
            Future<List<CommitBatchResponse>> commitFuture;
            if (file.isEmpty()) {
                // If the file is empty, skip uploading and committing, but proceed with the later steps.
                commitFuture = CompletableFuture.completedFuture(Collections.emptyList());
            } else {
                metrics.fileAdded(file.size());
                totalFilesInProgress.addAndGet(1);
                totalBytesInProgress.addAndGet(file.size());

                // Start uploading and add to the commit queue (as Runnable).
                // This ensures files are uploaded in concurrently, but committed to the control plane sequentially,
                // because `executorServiceCommit` is single-threaded.
                final FileUploadJob uploadJob = FileUploadJob.createFromByteArray(
                        objectKeyCreator,
                        storage,
                        time,
                        maxFileUploadAttempts,
                        fileUploadRetryBackoff,
                        file.data(),
                        metrics::fileUploadFinished
                );
                final Future<ObjectKey> uploadFuture = executorServiceUpload.submit(uploadJob);

                final FileCommitJob commitJob = new FileCommitJob(
                        brokerId,
                        file,
                        uploadFuture,
                        time,
                        controlPlane,
                        storage,
                        metrics::fileCommitFinished
                );
                commitFuture = executorServiceCommit.submit(commitJob);


                final CacheStoreJob cacheStoreJob = new CacheStoreJob(
                        time,
                        objectCache,
                        keyAlignmentStrategy,
                        file.data(),
                        uploadFuture,
                        metrics::cacheStoreFinished
                );
                CompletableFuture.runAsync(cacheStoreJob, executorServiceCacheStore)
                    .whenComplete((result, error) -> {
                        totalFilesInProgress.addAndGet(-1);
                        totalBytesInProgress.addAndGet(-file.size());
                        metrics.fileFinished(file.start(), uploadAndCommitStart);
                    });
            }
            final AppendCompleterJob completerJob = new AppendCompleterJob(file, commitFuture, time, metrics::appendCompletionFinished);
            executorServiceComplete.submit(completerJob);
        } finally {
            lock.unlock();
        }
    }

    int totalFilesInProgress() {
        return totalFilesInProgress.get();
    }

    int totalBytesInProgress() {
        return totalBytesInProgress.get();
    }

    @Override
    public void close() throws IOException {
        // Don't wait here, they should try to finish their work.
        executorServiceUpload.shutdown();
        executorServiceCommit.shutdown();
        metrics.close();
    }
}
