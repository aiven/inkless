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
            sharedState.buildStorage(),
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
            } else {
                metrics.recordFileCleanerStart();
                TimeUtils.measureDurationMs(time, () -> {
                    try {
                        cleanFiles(objectKeyPaths);
                    } catch (StorageBackendException e) {
                        throw new RuntimeException(e);
                    }
                }, metrics::recordFileCleanerTotalTime);
                metrics.recordFileCleanerCompleted(objectKeyPaths.size());
            }

            attempts.set(0);
        } catch (final Exception e) {
            metrics.recordFileCleanerError();
            final long backoff = errorBackoff.backoff(attempts.incrementAndGet());
            LOGGER.error("Error while deleting files, waiting for {}", Duration.ofMillis(backoff), e);
            time.sleep(backoff);
        }
    }

    private void cleanFiles(Set<String> objectKeyPaths) throws StorageBackendException {
        final Set<ObjectKey> objectKeys = objectKeyPaths.stream()
            .map(objectKeyCreator::from)
            .collect(Collectors.toSet());
        // delete files from storage backend
        storage.delete(objectKeys);
        // update control plane
        final DeleteFilesRequest request = new DeleteFilesRequest(objectKeyPaths);
        controlPlane.deleteFiles(request);

        LOGGER.info("Deleted {} files", objectKeyPaths.size());
    }

    @Override
    public void close() throws IOException {
        storage.close();
        metrics.close();
    }
}
