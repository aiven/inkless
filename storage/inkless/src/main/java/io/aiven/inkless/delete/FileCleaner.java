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

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
    final FileCleanerMetrics metrics;
    private final ExponentialBackoff errorBackoff = new ExponentialBackoff(100, 2, 60 * 1000, 0.2);
    private final Supplier<Long> noWorkBackoffSupplier;

    /**
     * The counter of cleaning attempts.
     */
    private final AtomicInteger attempts = new AtomicInteger();

    public FileCleaner(SharedState sharedState) {
        this(
            sharedState.time(),
            sharedState.controlPlane(),
            sharedState.backgroundStorage(),
            sharedState.objectKeyCreator(),
            sharedState.config().fileCleanerRetentionPeriod()
        );
    }

    // package-private constructor for testing
    FileCleaner(Time time,
                ControlPlane controlPlane,
                StorageBackend storage,
                ObjectKeyCreator objectKeyCreator,
                Duration retentionPeriod) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.storage = storage;
        this.objectKeyCreator = objectKeyCreator;
        this.retentionPeriod = retentionPeriod;
        this.metrics = new FileCleanerMetrics();

        // This backoff is needed only for jitter, there's no exponent in it.
        final int noWorkBackoffDuration = 10 * 1000;
        final var noWorkBackoff = new ExponentialBackoff(noWorkBackoffDuration, 1, noWorkBackoffDuration * 2, 0.2);
        noWorkBackoffSupplier = () -> noWorkBackoff.backoff(1);
    }


    @Override
    public void run() {
        try {
            final var now = TimeUtils.now(time);
            LOGGER.info("Running file cleaner at {}", now);

            // find all files that are marked for deletion
            final List<FileToDelete> filesToDelete = controlPlane.getFilesToDelete();
            final Set<String> objectKeyPaths = filesToDelete.stream()
                .filter(f -> Duration.between(f.markedForDeletionAt(), now).compareTo(retentionPeriod) > 0)
                .map(FileToDelete::objectKey)
                .collect(Collectors.toSet());
            if (objectKeyPaths.isEmpty()) {
                final long sleepMillis = noWorkBackoffSupplier.get();
                final Duration sleepDuration = Duration.ofMillis(sleepMillis);
                LOGGER.info("No files to delete, sleeping for {}", sleepDuration);
                time.sleep(sleepMillis);
                attempts.set(0);
                return;
            }

            metrics.recordFileCleanerStart();
            final int deletedCount = TimeUtils.measureDurationMs(time,
                () -> cleanFiles(objectKeyPaths),
                metrics::recordFileCleanerTotalTime);

            if (deletedCount == 0) {
                // There was work but nothing drained: the backend is likely throttling (S3.delete does
                // not throw for that) or hitting hard errors. Back off before the next cycle so we do
                // not keep retrying at a fixed cadence while under pressure. Request-rate backoff within
                // a cycle is handled by the S3 client's adaptive retry strategy.
                final long backoff = errorBackoff.backoff(attempts.incrementAndGet());
                LOGGER.warn("File cleaner drained no files this cycle, backing off for {}",
                    Duration.ofMillis(backoff));
                time.sleep(backoff);
            } else {
                attempts.set(0);
            }
        } catch (final Exception e) {
            metrics.recordFileCleanerError();
            final long backoff = errorBackoff.backoff(attempts.incrementAndGet());
            LOGGER.error("Error while deleting files, waiting for {}", Duration.ofMillis(backoff), e);
            time.sleep(backoff);
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
        LOGGER.info("Deleted {} of {} files", deletedPaths.size(), objectKeyPaths.size());
        return deletedPaths.size();
    }

    @Override
    public void close() throws IOException {
        // SharedState owns the storage backend lifecycle; only close component metrics here.
        metrics.close();
    }
}
