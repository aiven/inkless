// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
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
    private final byte[] data;
    private final Consumer<Long> durationCallback;

    public FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                  final ObjectUploader objectUploader,
                  final Time time,
                  final int attempts,
                  final Duration retryBackoff,
                  final byte[] data,
                  final Consumer<Long> durationCallback) {
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (attempts <= 0) {
            throw new IllegalArgumentException("attempts must be positive");
        }
        this.attempts = attempts;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff cannot be null");
        this.data = Objects.requireNonNull(data, "data cannot be null");
        this.durationCallback = Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
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
            uploadError = uploadWithRetry(objectKey, data);
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

    private Exception uploadWithRetry(final ObjectKey objectKey, final byte[] data) {
        LOGGER.debug("Uploading {}", objectKey);
        Exception error = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                objectUploader.upload(objectKey, data);
                LOGGER.debug("Successfully uploaded {}", objectKey);
                return null;
            } catch (final StorageBackendException e) {
                error = e;
                // Sleep on all attempts but last.
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
        }
        return error;
    }

    private static String safeGetCauseMessage(final Exception e) {
        return e.getCause() != null ? e.getCause().getMessage() : "";
    }
}
