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
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.storage_backend.common.Storage;

/**
 * The job of committing the already uploaded file to the control plane.
 *
 * <p>This class uses a non-blocking callback pattern. When the upload completes,
 * {@link #onUploadComplete} is invoked to perform the actual commit to the control plane.
 * This eliminates blocking wait on upload completion, allowing the commit executor to
 * only do actual commit work instead of waiting for S3 latency.
 *
 * <p>If the file was uploaded successfully, commit to the control plane happens. Otherwise, it doesn't.
 */
class FileCommitJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitJob.class);

    private final int brokerId;
    private final ClosedFile file;
    private final Time time;
    private final ControlPlane controlPlane;
    private final Storage storage;
    private final long commitSubmitTimeMs;
    private final Consumer<Long> durationCallback;
    private final Consumer<Long> commitWaitDurationCallback;

    FileCommitJob(final int brokerId,
                  final ClosedFile file,
                  final Time time,
                  final ControlPlane controlPlane,
                  final Storage storage,
                  final Consumer<Long> durationCallback,
                  final Consumer<Long> commitWaitDurationCallback) {
        this.brokerId = brokerId;
        this.file = file;
        this.controlPlane = controlPlane;
        this.time = time;
        this.storage = storage;
        // Record the time when the commit job was submitted, so we can measure the duration of the commit wait.
        this.commitSubmitTimeMs = time.milliseconds();
        this.durationCallback = durationCallback;
        this.commitWaitDurationCallback = commitWaitDurationCallback;
    }

    /**
     * Callback to be invoked when the upload completes.
     *
     * <p>If the upload succeeded, commits the file to the control plane.
     * If the upload failed, throws a {@link FileUploadException}.
     *
     * <p>This method is designed to be used with {@code CompletableFuture.handleAsync()}:
     * <pre>{@code
     * uploadFuture.handleAsync(commitJob::onUploadComplete, commitExecutor);
     * }</pre>
     *
     * @param objectKey the uploaded object key (null if upload failed)
     * @param uploadError the upload error (null if upload succeeded)
     * @return the list of commit batch responses
     * @throws FileUploadException if the upload failed
     * @throws RuntimeException if the commit fails
     */
    public List<CommitBatchResponse> onUploadComplete(final ObjectKey objectKey, final Throwable uploadError) {
        // Measure the duration from the commit job submission to the moment we start committing.
        // This accounts for the wait time for upload completion plus queue time on the commit executor.
        commitWaitDurationCallback.accept(time.milliseconds() - commitSubmitTimeMs);

        final UploadResult uploadResult;
        if (uploadError != null) {
            LOGGER.error("Failed upload", uploadError);
            uploadResult = new UploadResult(null, uploadError);
        } else {
            uploadResult = new UploadResult(objectKey, null);
        }

        return TimeUtils.measureDurationMsSupplier(time, () -> doCommit(uploadResult), durationCallback);
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
            // no need to log here, it was already logged in waitForUpload
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
            // Fire-and-forget delete - don't block the error path waiting for S3
            storage.delete(objectKey)
                .exceptionally(e2 -> {
                    LOGGER.error("Error removing the uploaded file {}", objectKey, e2);
                    return null;
                });
        } else {
            LOGGER.error("Error commiting data, but not removing the uploaded file {} as it is not safe", objectKey, e);
        }
    }

    private record UploadResult(ObjectKey objectKey, Throwable uploadError) {
    }
}
