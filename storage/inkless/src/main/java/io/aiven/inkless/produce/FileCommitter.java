// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.utils.Time;

import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectUploader;

/**
 * The file committer.
 *
 * <p>It uploads files concurrently, but commits them to the control plan sequentially.
 */
class FileCommitter implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitter.class);

    private final FileCommitterMetrics metrics;

    private final Lock lock = new ReentrantLock();

    private final InMemoryControlPlane controlPlane;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectUploader objectUploader;
    private final Time time;
    private final int maxFileUploadAttempts;
    private final Duration fileUploadRetryBackoff;
    private final ExecutorService executorServiceUpload;
    private final ExecutorService executorServiceCommit;

    private final AtomicInteger totalFilesInProgress = new AtomicInteger(0);
    private final AtomicInteger totalBytesInProgress = new AtomicInteger(0);

    @DoNotMutate
    FileCommitter(final InMemoryControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final ObjectUploader objectUploader,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff) {
        this(controlPlane, objectKeyCreator, objectUploader, time, maxFileUploadAttempts, fileUploadRetryBackoff,
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-file-uploader-", false)),
            // It must be single-thread to preserve the commit order.
            Executors.newSingleThreadExecutor(new InklessThreadFactory("inkless-file-uploader-finisher-", false)),
            new FileCommitterMetrics(time)
        );
    }

    // Visible for testing
    FileCommitter(final InMemoryControlPlane controlPlane,
                  final ObjectKeyCreator objectKeyCreator,
                  final ObjectUploader objectUploader,
                  final Time time,
                  final int maxFileUploadAttempts,
                  final Duration fileUploadRetryBackoff,
                  final ExecutorService executorServiceUpload,
                  final ExecutorService executorServiceCommit,
                  final FileCommitterMetrics metrics) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
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

        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        // Can't do this in the FileCommitterMetrics constructor, so initializing this way.
        this.metrics.initTotalFilesInProgressMetric(totalFilesInProgress::get);
        this.metrics.initTotalBytesInProgressMetric(totalBytesInProgress::get);
    }

    void commit(final ClosedFile file) throws InterruptedException {
        Objects.requireNonNull(file, "file cannot be null");
        metrics.fileAdded(file.size());

        lock.lock();
        try {
            final Instant uploadAndCommitStart = TimeUtils.monotonicNow(time);

            totalFilesInProgress.addAndGet(1);
            totalBytesInProgress.addAndGet(file.size());

            // Start uploading and add to the commit queue (as Runnable).
            // This ensures files are uploaded in concurrently, but committed to the control plane sequentially,
            // because `executorServiceCommit` is single-threaded.
            final FileUploadJob uploadJob = new FileUploadJob(
                this.objectKeyCreator, this.objectUploader, this.time,
                this.maxFileUploadAttempts, this.fileUploadRetryBackoff, file.data(), metrics::fileUploadFinished);
            final Future<ObjectKey> uploadFuture = executorServiceUpload.submit(uploadJob);

            final FileCommitJob commitJob =
                new FileCommitJob(file, uploadFuture, time, controlPlane, metrics::fileCommitFinished);
            CompletableFuture.runAsync(commitJob, executorServiceCommit)
                .whenComplete((result, error) -> {
                    totalFilesInProgress.addAndGet(-1);
                    totalBytesInProgress.addAndGet(-file.size());
                    metrics.fileFinished(file.start(), uploadAndCommitStart);
                });
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
