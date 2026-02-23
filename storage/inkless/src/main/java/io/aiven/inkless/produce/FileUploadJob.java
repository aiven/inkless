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
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
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
    private final Supplier<InputStream> data;
    private final long length;
    private final Consumer<Long> durationCallback;
    // May be null when created via createFromByteArray().
    // When non-null, this job owns one reference and releases it in finally block.
    private final BatchBufferData batchBufferData;

    public FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                         final ObjectUploader objectUploader,
                         final Time time,
                         final int attempts,
                         final Duration retryBackoff,
                         final Supplier<InputStream> data,
                         final long length,
                         final Consumer<Long> durationCallback) {
        this(objectKeyCreator, objectUploader, time, attempts, retryBackoff, data, length, durationCallback, null);
    }

    private FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                         final ObjectUploader objectUploader,
                         final Time time,
                         final int attempts,
                         final Duration retryBackoff,
                         final Supplier<InputStream> data,
                         final long length,
                         final Consumer<Long> durationCallback,
                         final BatchBufferData batchBufferData) {
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (attempts <= 0) {
            throw new IllegalArgumentException("attempts must be positive");
        }
        this.attempts = attempts;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff cannot be null");
        this.data = Objects.requireNonNull(data, "data cannot be null");
        this.length = length;
        this.durationCallback = Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
        this.batchBufferData = batchBufferData;
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

    public static FileUploadJob createFromBatchBufferData(final ObjectKeyCreator objectKeyCreator,
                                       final ObjectUploader objectUploader,
                                       final Time time,
                                       final int attempts,
                                       final Duration retryBackoff,
                                       final BatchBufferData data,
                                       final Consumer<Long> durationCallback) {
        Objects.requireNonNull(data, "data cannot be null");
        return new FileUploadJob(
            objectKeyCreator,
            objectUploader,
            time,
            attempts,
            retryBackoff,
            data.asInputStreamSupplier(),
            data.size(),
            durationCallback,
            data
        );
    }

    @Override
    public ObjectKey call() throws Exception {
        try {
            return TimeUtils.measureDurationMs(time, this::callInternal, durationCallback);
        } finally {
            // Release the buffer reference when upload is done
            if (batchBufferData != null) {
                batchBufferData.release();
            }
        }
    }

    private ObjectKey callInternal() throws Exception {
        final ObjectKey objectKey;
        final Exception uploadError;
        try {
            objectKey = objectKeyCreator.create(Uuid.randomUuid().toString());
            uploadError = uploadWithRetry(objectKey, data, length);
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

    private Exception uploadWithRetry(final ObjectKey objectKey, final Supplier<InputStream> data, final long length) {
        LOGGER.debug("Uploading {}", objectKey);

        // Try ByteBuffer path if available
        final Optional<ByteBuffer> directBuffer = batchBufferData != null
            ? batchBufferData.asByteBuffer()
            : Optional.empty();

        if (directBuffer.isPresent()) {
            LOGGER.debug("Using ByteBuffer upload for {}", objectKey);
            return uploadWithRetryByteBuffer(objectKey, directBuffer.get());
        }

        // Fall back to InputStream path
        return uploadWithRetryInputStream(objectKey, data, length);
    }

    /**
     * Uploads using ByteBuffer path with retry on StorageBackendException.
     * Buffer release is handled by the finally block in {@link #call()}.
     */
    private Exception uploadWithRetryByteBuffer(final ObjectKey objectKey, final ByteBuffer buffer) {
        // Store original position and limit to ensure correct state for each retry.
        // Although AWS SDK uses duplicate() internally, we defensively restore state
        // to protect against SDK behavior changes or other consumers modifying the buffer.
        final int originalPosition = buffer.position();
        final int originalLimit = buffer.limit();

        Exception error = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                // Restore buffer to original state for retry (position=0, limit=dataSize)
                buffer.position(originalPosition);
                buffer.limit(originalLimit);
                objectUploader.upload(objectKey, buffer);
                LOGGER.debug("Successfully uploaded {}", objectKey);
                return null;
            } catch (final StorageBackendException e) {
                error = e;
                handleUploadError(objectKey, e, attempt);
            }
        }
        return error;
    }

    private Exception uploadWithRetryInputStream(final ObjectKey objectKey, final Supplier<InputStream> data, final long length) {
        Exception error = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try (InputStream stream = data.get()) {
                objectUploader.upload(objectKey, stream, length);
                LOGGER.debug("Successfully uploaded {}", objectKey);
                return null;
            } catch (final StorageBackendException | IOException e) {
                error = e;
                handleUploadError(objectKey, e, attempt);
            }
        }
        return error;
    }

    private void handleUploadError(final ObjectKey objectKey, final Exception e, final int attempt) {
        final boolean lastAttempt = attempt == attempts - 1;
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
            time.sleep(retryBackoff.toMillis());
        }
    }

    private static String safeGetCauseMessage(final Exception e) {
        return e.getCause() != null ? e.getCause().getMessage() : "";
    }
}
