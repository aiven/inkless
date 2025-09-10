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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.storage_backend.common.ObjectDeleter;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

/**
 * The job of committing the already uploaded file to the control plane.
 *
 * <p>If the file was uploaded successfully, commit to the control plane happens. Otherwise, it doesn't.
 */
class FileCommitJob implements Supplier<List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitJob.class);

    private final int brokerId;
    private final ClosedFile file;
    private final Future<ObjectKey> uploadFuture;
    private final Time time;
    private final ControlPlane controlPlane;
    private final ObjectDeleter objectDeleter;
    private final long commitSubmitTimeMs;
    private final Consumer<Long> durationCallback;
    private final Consumer<Long> commitWaitDurationCallback;

    FileCommitJob(final int brokerId,
                  final ClosedFile file,
                  final Future<ObjectKey> uploadFuture,
                  final Time time,
                  final ControlPlane controlPlane,
                  final ObjectDeleter objectDeleter,
                  final Consumer<Long> durationCallback,
                  final Consumer<Long> commitWaitDurationCallback) {
        this.brokerId = brokerId;
        this.file = file;
        this.uploadFuture = uploadFuture;
        this.controlPlane = controlPlane;
        this.time = time;
        this.objectDeleter = objectDeleter;
        // Record the time when the commit job was submitted, so we can measure the duration of the commit wait.
        this.commitSubmitTimeMs = time.milliseconds();
        this.durationCallback = durationCallback;
        this.commitWaitDurationCallback = commitWaitDurationCallback;
    }

    @Override
    public List<CommitBatchResponse> get() {
        // The wait for upload is already measured, and should take the upload time or less if it was already completed.
        final UploadResult uploadResult = waitForUpload();
        // Measure the duration from the commit job submission to the moment we start committing.
        // and should account for the wait time to execute the commit job on a single-threaded executor.
        commitWaitDurationCallback.accept(time.milliseconds() - commitSubmitTimeMs);
        return TimeUtils.measureDurationMsSupplier(time, () -> doCommit(uploadResult), durationCallback);
    }

    private UploadResult waitForUpload() {
        try {
            final ObjectKey objectKey = uploadFuture.get();
            return new UploadResult(objectKey, null);
        } catch (final ExecutionException e) {
            LOGGER.error("Failed upload", e);
            return new UploadResult(null, e.getCause());
        } catch (final InterruptedException e) {
            // This is not expected as we try to shut down the executor gracefully.
            LOGGER.error("Interrupted", e);
            throw new RuntimeException(e);
        }
    }

    private List<CommitBatchResponse> doCommit(final UploadResult result) {
        if (result.objectKey != null) {
            LOGGER.debug("Uploaded {} successfully, committing", result.objectKey);
            try {
                final var commitBatchResponses = controlPlane.commitFile(result.objectKey.value(), ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, brokerId, file.size(), file.commitBatchRequests());
                LOGGER.debug("Committed successfully");
                return commitBatchResponses;
            } catch (final Exception e) {
                LOGGER.error("Commit failed", e);
                if (e instanceof ControlPlaneException) {
                    // only attempt to remove the uploaded file if it is a control plane error
                    tryDeleteFile(result.objectKey(), e);
                }
                throw e;
            }
        } else {
            throw new FileUploadException(result.uploadError);
        }
    }

    private void tryDeleteFile(ObjectKey objectKey, Exception e) {
        boolean safeToDeleteFile;
        try {
            safeToDeleteFile = controlPlane.isSafeToDeleteFile(objectKey.value());
        } catch (final ControlPlaneException cpe) {
            LOGGER.error("Error checking if it is safe to delete the uploaded file {}", objectKey, cpe);
            safeToDeleteFile = false;
        }

        if (safeToDeleteFile) {
            LOGGER.error("Error commiting data, attempting to remove the uploaded file {}", objectKey, e);
            try {
                objectDeleter.delete(objectKey);
            } catch (final StorageBackendException e2) {
                LOGGER.error("Error removing the uploaded file {}", objectKey, e2);
            }
        } else {
            LOGGER.error("Error commiting data, but not removing the uploaded file {} as it is not safe", objectKey, e);
        }
    }

    private record UploadResult(ObjectKey objectKey, Throwable uploadError) {
    }
}
