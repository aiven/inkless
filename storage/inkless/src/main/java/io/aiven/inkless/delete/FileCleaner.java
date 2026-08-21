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
package io.aiven.inkless.delete;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class FileCleaner implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCleaner.class);

    final Time time;
    final ControlPlane controlPlane;
    final StorageBackend storage;
    final ObjectKeyCreator objectKeyCreator;
    final Duration retentionPeriod;
    final int maxFilesPerCycle;
    final FileCleanerMetrics metrics;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FileCleaner(SharedState sharedState) {
        this(
            sharedState.time(),
            sharedState.controlPlane(),
            sharedState.backgroundStorage(),
            sharedState.objectKeyCreator(),
            sharedState.config().fileCleanerRetentionPeriod(),
            sharedState.config().fileCleanerMaxFilesPerCycle()
        );
    }

    // package-private constructor for testing
    FileCleaner(Time time,
                ControlPlane controlPlane,
                StorageBackend storage,
                ObjectKeyCreator objectKeyCreator,
                Duration retentionPeriod,
                int maxFilesPerCycle) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.storage = storage;
        this.objectKeyCreator = objectKeyCreator;
        this.retentionPeriod = retentionPeriod;
        this.maxFilesPerCycle = maxFilesPerCycle;
        this.metrics = new FileCleanerMetrics(time);
    }


    @Override
    public void run() {
        if (closed.get()) {
            return;
        }
        try {
            final var now = TimeUtils.now(time);

            final Instant markedBefore = now.minus(retentionPeriod);
            // One row beyond the cap distinguishes a saturated cycle from an exactly-full one. The add
            // saturates: at MAX_VALUE there is no room for the probe, and overflowing to a negative
            // limit would read as unbounded and remove the cap the operator asked for.
            final int queryLimit = maxFilesPerCycle > 0 && maxFilesPerCycle < Integer.MAX_VALUE
                ? maxFilesPerCycle + 1
                : maxFilesPerCycle;
            final List<FileToDelete> fetched = controlPlane.getFilesToDelete(markedBefore, queryLimit);
            final boolean saturated = maxFilesPerCycle > 0 && fetched.size() > maxFilesPerCycle;
            final List<FileToDelete> filesToDelete = saturated ? fetched.subList(0, maxFilesPerCycle) : fetched;
            final Set<String> objectKeyPaths = filesToDelete.stream()
                .map(FileToDelete::objectKey)
                .collect(Collectors.toSet());
            if (objectKeyPaths.isEmpty()) {
                LOGGER.debug("No files to delete");
            } else if (closed.get()) {
                // Leave marked files for the next cycle instead of starting deletion during shutdown.
                LOGGER.info("Skipping deletion of {} files: file cleaner closed", objectKeyPaths.size());
            } else {
                if (saturated) {
                    metrics.recordFileCleanerCycleSaturated();
                    LOGGER.info("Running file cleaner: deleting {} files (per-cycle cap reached, more remain)",
                        objectKeyPaths.size());
                } else {
                    LOGGER.info("Running file cleaner: deleting {} files", objectKeyPaths.size());
                }
                metrics.recordFileCleanerStart();
                final int deletedCount = TimeUtils.measureDurationMs(time,
                    () -> cleanFiles(objectKeyPaths),
                    metrics::recordFileCleanerTotalTime);
                LOGGER.info("File cleaner deleted {} of {} files", deletedCount, objectKeyPaths.size());
            }

            metrics.recordFileCleanerCycleSucceeded();
        } catch (final Exception e) {
            metrics.recordFileCleanerError();
            LOGGER.error("Error while deleting files", e);
        }
    }

    private int cleanFiles(Set<String> objectKeyPaths) throws StorageBackendException {
        final Set<ObjectKey> objectKeys = objectKeyPaths.stream()
            .map(objectKeyCreator::from)
            .collect(Collectors.toSet());
        // Delete files from the storage backend. Deletion may be partial (e.g. under S3 throttling):
        // only the keys the backend confirmed deleted are dereferenced in the control plane, so the
        // remaining keys stay marked for deletion and are retried on the next cycle instead of being
        // re-attempted after already being deleted.
        final Set<ObjectKey> deletedKeys = storage.delete(objectKeys);
        metrics.recordFileCleanerFilesFailed(objectKeyPaths.size() - deletedKeys.size());
        if (deletedKeys.isEmpty()) {
            LOGGER.warn("No files deleted from storage out of {} candidates; retrying next cycle",
                objectKeyPaths.size());
            return 0;
        }
        final Set<String> deletedPaths = deletedKeys.stream()
            .map(ObjectKey::value)
            .collect(Collectors.toSet());
        // update control plane
        final DeleteFilesRequest request = new DeleteFilesRequest(deletedPaths);
        controlPlane.deleteFiles(request);

        metrics.recordFileCleanerCompleted(deletedPaths.size());
        return deletedPaths.size();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            // SharedState owns the storage backend lifecycle; only close component metrics here.
            metrics.close();
        }
    }
}
