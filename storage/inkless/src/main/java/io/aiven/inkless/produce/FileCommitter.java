// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitter.class);

    private final Executor executor;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectUploader objectUploader;
    private final ControlPlane controlPlane;
    private final Time time;
    private final int attempts;
    private final Duration retryBackoff;
    private final BiFunction<List<CommitBatchResponse>, Throwable, Void> callback;

    FileCommitter(final Executor executor,
                  final ObjectKeyCreator objectKeyCreator,
                  final ObjectUploader objectUploader,
                  final ControlPlane controlPlane,
                  final Time time,
                  final int attempts,
                  final Duration retryBackoff,
                  final BiFunction<List<CommitBatchResponse>, Throwable, Void> callback) {
        this.executor = executor;
        this.objectKeyCreator = objectKeyCreator;
        this.objectUploader = objectUploader;
        this.controlPlane = controlPlane;
        this.time = time;
        this.attempts = attempts;
        this.retryBackoff = retryBackoff;
        this.callback = callback;
    }

    public void upload(final List<CommitBatchRequest> commitBatchRequests,
                       final byte[] data) {
        executor.execute(() -> uploadInternal(commitBatchRequests, data));
    }

    private void uploadInternal(final List<CommitBatchRequest> commitBatchRequests,
                                final byte[] data) {
        final ObjectKey objectKey = objectKeyCreator.create(Uuid.randomUuid().toString());
        final Throwable uploadError = uploadWithRetry(objectKey, data);
        if (uploadError == null) {
            LOGGER.debug("Committing {}", objectKey);
            final var commitResult = controlPlane.commitFile(objectKey, commitBatchRequests);
            callback.apply(commitResult, null);
        } else {
            LOGGER.error("Error uploading {}, giving up", objectKey, uploadError);
            callback.apply(null, uploadError);
        }
    }

    private Throwable uploadWithRetry(final ObjectKey objectKey, final byte[] data) {
        LOGGER.debug("Uploading {}", objectKey);
        Throwable error = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                objectUploader.upload(objectKey, data);
                LOGGER.debug("Successfully uploaded {}", objectKey);
                return null;
            } catch (final StorageBackendException e) {
                error = e;
                // Retry on all attempts but last.
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
