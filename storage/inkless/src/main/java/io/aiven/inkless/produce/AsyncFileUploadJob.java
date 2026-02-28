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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.storage_backend.common.Storage;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;

/**
 * Async file upload job using the unified Storage interface.
 *
 * <p>This job returns a CompletableFuture that completes when the upload is done,
 * with built-in retry logic using delayed executor for backoff.
 */
public class AsyncFileUploadJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFileUploadJob.class);

    private final ObjectKeyCreator objectKeyCreator;
    private final Storage storage;
    private final Time time;
    private final int maxAttempts;
    private final Duration retryBackoff;
    private final BatchBufferData batchBufferData;
    private final Consumer<Long> durationCallback;

    /**
     * Creates an AsyncFileUploadJob from BatchBufferData.
     *
     * <p>This factory method handles reference-counted buffers. The buffer's reference is
     * released when the upload future completes (success or failure).
     *
     * <p>The job uses the underlying ByteBuffer directly when available (pooled buffers),
     * avoiding unnecessary memory copies.
     *
     * @param objectKeyCreator creator for generating object keys
     * @param storage unified storage interface
     * @param time time source for metrics
     * @param maxAttempts maximum number of upload attempts
     * @param retryBackoff duration to wait between retry attempts
     * @param batchBufferData the buffer data to upload (reference will be released on completion)
     * @param durationCallback callback for recording upload duration
     * @return a new AsyncFileUploadJob
     */
    public static AsyncFileUploadJob createFromBatchBufferData(
            final ObjectKeyCreator objectKeyCreator,
            final Storage storage,
            final Time time,
            final int maxAttempts,
            final Duration retryBackoff,
            final BatchBufferData batchBufferData,
            final Consumer<Long> durationCallback) {
        Objects.requireNonNull(batchBufferData, "batchBufferData cannot be null");

        return new AsyncFileUploadJob(
            objectKeyCreator, storage, time, maxAttempts, retryBackoff,
            batchBufferData, durationCallback
        );
    }

    private AsyncFileUploadJob(final ObjectKeyCreator objectKeyCreator,
                               final Storage storage,
                               final Time time,
                               final int maxAttempts,
                               final Duration retryBackoff,
                               final BatchBufferData batchBufferData,
                               final Consumer<Long> durationCallback) {
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.maxAttempts = maxAttempts;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff cannot be null");
        this.batchBufferData = Objects.requireNonNull(batchBufferData, "batchBufferData cannot be null");
        this.durationCallback = Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
    }

    /**
     * Starts the async upload with retry support.
     *
     * <p>The buffer reference is released when the returned future completes (success or failure).
     *
     * @return CompletableFuture that completes with the ObjectKey on success
     */
    public CompletableFuture<ObjectKey> uploadAsync() {
        final Instant start = TimeUtils.durationMeasurementNow(time);
        final ObjectKey objectKey = objectKeyCreator.create(Uuid.randomUuid().toString());

        return uploadWithRetryAsync(objectKey, 0)
            .whenComplete((result, error) -> {
                final Instant end = TimeUtils.durationMeasurementNow(time);
                final long durationMs = Duration.between(start, end).toMillis();
                durationCallback.accept(durationMs);

                // Release buffer reference
                batchBufferData.release();
            });
    }

    private CompletableFuture<ObjectKey> uploadWithRetryAsync(final ObjectKey objectKey, final int attempt) {
        LOGGER.debug("Uploading {} (attempt {})", objectKey, attempt + 1);

        // Get ByteBuffer directly from BatchBufferData - avoids memory copy for pooled buffers.
        // Use duplicate() to get independent position/limit for this attempt.
        final ByteBuffer buffer = batchBufferData.asByteBuffer()
            .map(ByteBuffer::duplicate)
            .orElseGet(() -> {
                // Fallback for HeapBatchBufferData: copy to byte array
                final byte[] data = new byte[batchBufferData.size()];
                batchBufferData.copyTo(0, data, 0, data.length);
                return ByteBuffer.wrap(data);
            });
        // Ensure position=0 for this attempt, in case a previous attempt modified it
        buffer.rewind();

        return storage.upload(objectKey, buffer)
            .thenApply(v -> {
                LOGGER.debug("Successfully uploaded {}", objectKey);
                return objectKey;
            })
            .exceptionallyCompose(error -> handleUploadError(objectKey, attempt, error));
    }

    private CompletableFuture<ObjectKey> handleUploadError(final ObjectKey objectKey,
                                                           final int attempt,
                                                           final Throwable error) {
        final Throwable cause = unwrapError(error);
        final boolean isLastAttempt = attempt >= maxAttempts - 1;
        final boolean isRetryable = isRetryableError(cause);

        if (isLastAttempt || !isRetryable) {
            if (isTimeout(cause)) {
                LOGGER.error("Error uploading {} due to timeout, giving up: {}",
                    objectKey, safeGetMessage(cause));
            } else {
                LOGGER.error("Error uploading {}, giving up", objectKey, cause);
            }
            return CompletableFuture.failedFuture(wrapError(cause));
        }

        // Retry with backoff
        if (isTimeout(cause)) {
            LOGGER.warn("Error uploading {} due to timeout, retrying in {} ms: {}",
                objectKey, retryBackoff.toMillis(), safeGetMessage(cause));
        } else {
            LOGGER.warn("Error uploading {}, retrying in {} ms",
                objectKey, retryBackoff.toMillis(), cause);
        }

        return CompletableFuture.supplyAsync(
                () -> null,
                delayedExecutor(retryBackoff.toMillis(), TimeUnit.MILLISECONDS))
            .thenCompose(v -> uploadWithRetryAsync(objectKey, attempt + 1));
    }

    private Throwable unwrapError(final Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }

    private boolean isRetryableError(final Throwable error) {
        // Retry on storage backend exceptions (timeouts, transient errors)
        // Don't retry on IllegalArgumentException or other client-side errors
        return error instanceof StorageBackendException;
    }

    private boolean isTimeout(final Throwable error) {
        return error instanceof StorageBackendTimeoutException;
    }

    private Throwable wrapError(final Throwable error) {
        if (error instanceof StorageBackendException) {
            return error;
        }
        return new StorageBackendException("Upload failed", error);
    }

    private static String safeGetMessage(final Throwable e) {
        return e != null ? e.getMessage() : "";
    }

    private static Executor delayedExecutor(final long delay, final TimeUnit unit) {
        return CompletableFuture.delayedExecutor(delay, unit);
    }
}
