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
import java.util.function.Function;

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
 * <p>This class implements {@link Function} for use with {@code CompletableFuture.thenApplyAsync()}.
 * When the upload completes successfully, {@link #apply} is invoked to perform the actual commit.
 * This eliminates blocking wait on upload completion, allowing the commit executor to
 * only do actual commit work instead of waiting for S3 latency.
 *
 * <p>Upload failures are handled separately in the future chain and don't reach this job.
 */
class FileCommitJob implements Function<ObjectKey, List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitJob.class);

    private final int brokerId;
    private final ClosedFile file;
    private final Time time;
    private final ControlPlane controlPlane;
    private final ObjectDeleter objectDeleter;
    private volatile long commitSubmitTimeMs;
    private final Consumer<Long> durationCallback;
    private final Consumer<Long> commitWaitDurationCallback;

    FileCommitJob(final int brokerId,
                  final ClosedFile file,
                  final Time time,
                  final ControlPlane controlPlane,
                  final ObjectDeleter objectDeleter,
                  final Consumer<Long> durationCallback,
                  final Consumer<Long> commitWaitDurationCallback) {
        this.brokerId = brokerId;
        this.file = file;
        this.controlPlane = controlPlane;
        this.time = time;
        this.objectDeleter = objectDeleter;
        // Record the time when the commit job was submitted, so we can measure the duration of the commit wait.
        this.commitSubmitTimeMs = time.milliseconds();
        this.durationCallback = durationCallback;
        this.commitWaitDurationCallback = commitWaitDurationCallback;
    }

    /**
     * Resets the submit time to now. Call this when the commit is actually ready to be executed
     * (e.g., after upload completes in async mode) to measure only the executor queue wait time,
     * not the time waiting for previous commits in the chain.
     */
    void markReadyToCommit() {
        this.commitSubmitTimeMs = time.milliseconds();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Commits the uploaded file to the control plane.
     * This method is only called when upload succeeds (via thenApplyAsync).
     */
    @Override
    public List<CommitBatchResponse> apply(ObjectKey objectKey) {
        // Measure the duration from markReadyToCommit() to the moment we start committing.
        // markReadyToCommit() is called after upload completion and chain wait,
        // so this measures only the executor queue wait time.
        commitWaitDurationCallback.accept(time.milliseconds() - commitSubmitTimeMs);

        return TimeUtils.measureDurationMsSupplier(time, () -> doCommit(objectKey), durationCallback);
    }

    private List<CommitBatchResponse> doCommit(final ObjectKey objectKey) {
        LOGGER.debug("Uploaded {} successfully, committing", objectKey);
        try {
            final var commitBatchResponses = controlPlane.commitFile(objectKey.value(), ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, brokerId, file.size(), file.commitBatchRequests());
            LOGGER.debug("Committed successfully");
            return commitBatchResponses;
        } catch (final Exception e) {
            LOGGER.error("Commit failed", e);
            if (e instanceof ControlPlaneException) {
                // only attempt to remove the uploaded file if it is a control plane error
                tryDeleteFile(objectKey, e);
            }
            throw e;
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

}
