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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.Storage;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;

/**
 * The job of uploading a file to the object storage.
 */
public class FileUploadJob implements Callable<ObjectKey> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadJob.class);

    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectUploader objectUploader;
    private final Time time;
    private final int attempts;
    private final Duration retryBackoff;
    private final Supplier<InputStream> dataStream;
    private final ByteBuffer dataBuffer;
    private final long length;
    private final Consumer<Long> durationCallback;

    /**
     * Constructor for InputStream-based uploads.
     */
    public FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                         final ObjectUploader objectUploader,
                         final Time time,
                         final int attempts,
                         final Duration retryBackoff,
                         final Supplier<InputStream> data,
                         final long length,
                         final Consumer<Long> durationCallback) {
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (attempts <= 0) {
            throw new IllegalArgumentException("attempts must be positive");
        }
        this.attempts = attempts;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff cannot be null");
        this.dataStream = Objects.requireNonNull(data, "data cannot be null");
        this.dataBuffer = null;
        this.length = length;
        this.durationCallback = Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
    }

    /**
     * Constructor for ByteBuffer-based uploads (zero-copy path).
     * Note: The buffer is stored by reference, not copied. The caller must ensure the buffer
     * is not modified between construction and when call() completes. The buffer's position
     * and limit are captured at construction time via duplicate() during upload.
     */
    private FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                          final ObjectUploader objectUploader,
                          final Time time,
                          final int attempts,
                          final Duration retryBackoff,
                          final ByteBuffer dataBuffer,
                          final Consumer<Long> durationCallback) {
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (attempts <= 0) {
            throw new IllegalArgumentException("attempts must be positive");
        }
        this.attempts = attempts;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff cannot be null");
        this.dataStream = null;
        // Store the buffer reference - position/limit are preserved via duplicate() during upload
        this.dataBuffer = Objects.requireNonNull(dataBuffer, "dataBuffer cannot be null");
        this.length = dataBuffer.remaining();
        this.durationCallback = Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
    }

    public static FileUploadJob createFromByteArray(final ObjectKeyCreator objectKeyCreator,
                                       final ObjectUploader objectUploader,
                                       final Time time,
                                       final int attempts,
                                       final Duration retryBackoff,
                                       final byte[] data,
                                       final Consumer<Long> durationCallback) {
        Objects.requireNonNull(data, "data cannot be null");
        return new FileUploadJob(
            objectKeyCreator,
            objectUploader,
            time,
            attempts,
            retryBackoff,
            () -> new ByteArrayInputStream(data),
            data.length,
            durationCallback
        );
    }

    /**
     * Creates a FileUploadJob for ByteBuffer data using the zero-copy upload path.
     * The ByteBuffer's position will not be modified (uses duplicate internally for retries).
     */
    public static FileUploadJob createFromByteBuffer(final ObjectKeyCreator objectKeyCreator,
                                        final ObjectUploader objectUploader,
                                        final Time time,
                                        final int attempts,
                                        final Duration retryBackoff,
                                        final ByteBuffer data,
                                        final Consumer<Long> durationCallback) {
        Objects.requireNonNull(data, "data cannot be null");
        if (data.remaining() <= 0) {
            throw new IllegalArgumentException("data must have remaining bytes");
        }
        return new FileUploadJob(
            objectKeyCreator,
            objectUploader,
            time,
            attempts,
            retryBackoff,
            data,
            durationCallback
        );
    }

    /**
     * Creates a FileUploadJob using the unified Storage interface.
     *
     * <p>This method wraps the async Storage interface to work with the sync FileUploadJob.
     * The async upload is converted to sync by blocking on the CompletableFuture.
     *
     * @param objectKeyCreator creator for generating object keys
     * @param storage unified storage interface (async-first)
     * @param time time source for metrics
     * @param attempts maximum number of upload attempts
     * @param retryBackoff duration to wait between retry attempts
     * @param data batch buffer data to upload
     * @param durationCallback callback for recording upload duration
     * @return a FileUploadJob configured to use the Storage interface
     */
    public static FileUploadJob createFromBatchBufferData(final ObjectKeyCreator objectKeyCreator,
                                       final Storage storage,
                                       final Time time,
                                       final int attempts,
                                       final Duration retryBackoff,
                                       final BatchBufferData data,
                                       final Consumer<Long> durationCallback) {
        Objects.requireNonNull(storage, "storage cannot be null");
        Objects.requireNonNull(data, "data cannot be null");

        // Adapter that converts async Storage to sync ObjectUploader interface
        final ObjectUploader adapter = new StorageObjectUploaderAdapter(storage);

        return new FileUploadJob(
            objectKeyCreator,
            adapter,
            time,
            attempts,
            retryBackoff,
            data.asInputStreamSupplier(),
            data.size(),
            durationCallback,
            data
        );
    }

    /**
     * Adapter that wraps the async Storage interface as a sync ObjectUploader.
     *
     * <p>This adapter blocks on CompletableFuture to convert async operations to sync.
     * Used to integrate Storage interface with the existing sync FileUploadJob.
     */
    private static final class StorageObjectUploaderAdapter implements ObjectUploader {
        private final Storage storage;

        StorageObjectUploaderAdapter(Storage storage) {
            this.storage = storage;
        }

        @Override
        public void upload(ObjectKey key, InputStream inputStream, long length) throws StorageBackendException {
            // Convert InputStream to ByteBuffer
            try {
                final byte[] bytes = inputStream.readAllBytes();
                upload(key, ByteBuffer.wrap(bytes));
            } catch (IOException e) {
                throw new StorageBackendException("Failed to read input stream", e);
            }
        }

        @Override
        public void upload(ObjectKey key, ByteBuffer byteBuffer) throws StorageBackendException {
            try {
                storage.upload(key, byteBuffer).join();
            } catch (java.util.concurrent.CompletionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof StorageBackendException) {
                    throw (StorageBackendException) cause;
                }
                throw new StorageBackendException("Upload failed", cause != null ? cause : e);
            }
        }

        @Override
        public void close() {
            // Don't close storage here - it's managed externally
        }
    }

    @Override
    public ObjectKey call() throws Exception {
        return TimeUtils.measureDurationMs(time, this::callInternal, durationCallback);
    }

    private ObjectKey callInternal() throws Exception {
        final ObjectKey objectKey;
        final Exception uploadError;
        try {
            objectKey = objectKeyCreator.create(Uuid.randomUuid().toString());
            if (dataBuffer != null) {
                LOGGER.debug("Uploading {} via ByteBuffer (zero-copy)", objectKey);
                uploadError = uploadWithRetry(objectKey, () -> objectUploader.upload(objectKey, dataBuffer));
            } else {
                LOGGER.debug("Uploading {} via InputStream", objectKey);
                uploadError = uploadWithRetry(objectKey, () -> {
                    try (InputStream stream = dataStream.get()) {
                        objectUploader.upload(objectKey, stream, length);
                    }
                });
            }
        } catch (final Exception e) {
            LOGGER.error("Unexpected exception", e);
            throw e;
        }

        if (uploadError == null) {
            return objectKey;
        } else {
            throw uploadError;
        }
    }

    /**
     * Executes the upload operation with retry logic.
     * @param objectKey the object key being uploaded (for logging)
     * @param uploadOperation the upload operation to execute
     * @return null on success, or the last exception on failure after all retries exhausted
     */
    private Exception uploadWithRetry(final ObjectKey objectKey, final UploadOperation uploadOperation) {
        Exception error = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                uploadOperation.execute();
                LOGGER.debug("Successfully uploaded {}", objectKey);
                return null;
            } catch (final StorageBackendException | IOException e) {
                error = e;
                final boolean lastAttempt = attempt == attempts - 1;
                logRetryableError(objectKey, lastAttempt, e);
                if (!lastAttempt) {
                    time.sleep(retryBackoff.toMillis());
                }
            }
        }
        return error;
    }

    private void logRetryableError(final ObjectKey objectKey, final boolean lastAttempt, final Exception e) {
        if (lastAttempt) {
            if (e instanceof StorageBackendTimeoutException) {
                LOGGER.error("Error uploading {} due to timeout, giving up: {}", objectKey, safeGetCauseMessage(e));
            } else {
                LOGGER.error("Error uploading {}, giving up", objectKey, e);
            }
        } else {
            if (e instanceof StorageBackendTimeoutException) {
                LOGGER.error("Error uploading {} due to timeout, retrying in {} ms: {}",
                    objectKey, retryBackoff.toMillis(), safeGetCauseMessage(e));
            } else {
                LOGGER.error("Error uploading {}, retrying in {} ms",
                    objectKey, retryBackoff.toMillis(), e);
            }
        }
    }

    @FunctionalInterface
    private interface UploadOperation {
        void execute() throws StorageBackendException, IOException;
    }

    private static String safeGetCauseMessage(final Exception e) {
        return e.getCause() != null ? e.getCause().getMessage() : "";
    }
}
