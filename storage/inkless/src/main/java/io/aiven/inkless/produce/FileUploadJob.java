// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The job of uploading a file to the object storage.
 */
class FileUploadJob implements Callable<ObjectKey> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadJob.class);

    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectUploader objectUploader;
    private final Time time;
    private final int attempts;
    private final Duration retryBackoff;
    private final byte[] data;

    FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                  final ObjectUploader objectUploader,
                  final Time time,
                  final int attempts,
                  final Duration retryBackoff,
                  final byte[] data) {
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        if (attempts <= 0) {
            throw new IllegalArgumentException("attempts must be positive");
        }
        this.attempts = attempts;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff cannot be null");
        this.data = Objects.requireNonNull(data, "data cannot be null");
    }

    @Override
    public ObjectKey call() throws Exception {
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
            LOGGER.error("Error uploading {}, giving up", objectKey, uploadError);
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
                    LOGGER.error("Error uploading {}, giving up", objectKey, e);
                } else {
                    LOGGER.error("Error uploading {}, retrying in {}", objectKey, retryBackoff, e);
                    time.sleep(retryBackoff.toMillis());
                }
            }
        }
        return error;
    }
}
