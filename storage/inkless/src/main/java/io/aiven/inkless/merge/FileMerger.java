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
package io.aiven.inkless.merge;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.FileMergeWorkItem;
import io.aiven.inkless.produce.FileUploadJob;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class FileMerger implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMerger.class);

    private final Path workDir;
    private final int brokerId;
    private final Time time;
    private final InklessConfig config;
    private final ControlPlane controlPlane;
    private final StorageBackend storage;
    private final ObjectKeyCreator objectKeyCreator;
    private final ExponentialBackoff errorBackoff = new ExponentialBackoff(100, 2, 60 * 1000, 0.2);
    private final Supplier<Long> noWorkBackoffSupplier;
    private final FileMergerMetrics metrics;

    /**
     * The counter of merging attempts.
     */
    private final AtomicInteger attempts = new AtomicInteger();

    public FileMerger(final SharedState sharedState) {
        this.brokerId = sharedState.brokerId();
        this.time = sharedState.time();
        this.config = sharedState.config();
        this.controlPlane = sharedState.controlPlane();
        this.storage = sharedState.storage();
        this.objectKeyCreator = sharedState.objectKeyCreator();
        this.metrics = new FileMergerMetrics();

        // This backoff is needed only for jitter, there's no exponent in it.
        final int noWorkBackoffDuration = 10 * 1000;
        final var noWorkBackoff = new ExponentialBackoff(noWorkBackoffDuration, 1, noWorkBackoffDuration * 2, 0.2);
        noWorkBackoffSupplier = () -> noWorkBackoff.backoff(1);

        this.workDir = config.fileMergeWorkDir();
    }

    @Override
    public void run() {
        final var now = TimeUtils.now(time);
        LOGGER.info("Running file merger at {}", now);
        try {
            final FileMergeWorkItem workItem = controlPlane.getFileMergeWorkItem();
            if (workItem == null) {
                final long sleepMillis = noWorkBackoffSupplier.get();
                final Duration sleepDuration = Duration.ofMillis(sleepMillis);
                LOGGER.info("No file merge work items, sleeping for {}", sleepDuration);
                time.sleep(sleepMillis);
            } else {
                try {
                    metrics.recordFileMergeStarted();
                    TimeUtils.measureDurationMs(time, () -> {
                        try {
                            runWithWorkItem(workItem);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, metrics::recordFileMergeTotalTime);
                    metrics.recordFileMergeCompleted(workItem.files().size());
                } catch (final Exception e1) {
                    LOGGER.error("Error merging files, trying to release work item", e1);
                    metrics.recordFileMergeError();
                    try {
                        controlPlane.releaseFileMergeWorkItem(workItem.workItemId());
                    } catch (final Exception e2) {
                        LOGGER.error("Error releasing work item", e1);
                        // The original exception will be thrown.
                    }
                    throw e1;
                }
            }

            // Reset the attempt counter in case of success.
            attempts.set(0);
        } catch (final Exception e) {
            final long backoff = errorBackoff.backoff(attempts.incrementAndGet());
            LOGGER.error("Error while merging files, waiting for {}", Duration.ofMillis(backoff), e);
            time.sleep(backoff);
        }
    }

    private void runWithWorkItem(final FileMergeWorkItem workItem) throws Exception {
        LOGGER.info("Work item received, merging {} files", workItem.files().size());

        // Delete all content within work directory
        if (Files.exists(workDir)) {
            try (var stream = Files.newDirectoryStream(workDir)) {
                for (final var path : stream) {
                    Files.delete(path);
                }
            }
        } else {
            Files.createDirectories(workDir);
        }

        var builder = new MergeBatchesInputStream.Builder();
        var paths = new ArrayList<Path>();
        try {
            // Collect InputStream supplier for each file, to avoid opening all of them at once.
            for (final var file : workItem.files()) {
                final ObjectKey objectKey = objectKeyCreator.from(file.objectKey());

                // Download the file to a temporary location flat in the work directory.
                final String tmpFileName = file.objectKey().replaceAll("/", "_");
                final var target = workDir.resolve(tmpFileName);
                paths.add(target);

                final var in = storage.fetch(objectKey, null);
                try (final var out = Files.newByteChannel(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    out.write(storage.readToByteBuffer(in));
                } catch (Exception e) {
                    e.printStackTrace();
                }

                final Supplier<InputStream> inputStream = () -> {
                    try {
                        return Files.newInputStream(target);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

                final var inputStreamWithPosition = new InputStreamWithPosition(inputStream, file.size());

                for (final var batch : file.batches()) {
                    builder.addBatch(new BatchAndStream(batch, inputStreamWithPosition));
                }
            }

            var mergeMetadata = builder.mergeMetadata();

            final ObjectKey objectKey = new FileUploadJob(
                objectKeyCreator, storage, time,
                config.produceMaxUploadAttempts(),
                config.produceUploadBackoff(),
                builder::build,
                mergeMetadata.mergedFileSize(),
                metrics::recordFileUploadTime
            ).call();

            try {
                controlPlane.commitFileMergeWorkItem(
                    workItem.workItemId(),
                    objectKey.value(),
                    ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                    brokerId,
                    mergeMetadata.mergedFileSize(),
                    mergeMetadata.mergedFileBatch()
                );
            } catch (final Exception e) {
                if (e instanceof ControlPlaneException) {
                    // only attempt to remove the uploaded file if it is a control plane error
                    tryDeleteFile(objectKey, e);
                }
                // The original exception will be thrown.
                throw e;
            }
            LOGGER.info("Merged {} files into {}", workItem.files().size(), objectKey);
        } finally {
            // delete the temporary files
            paths.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
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
            LOGGER.error("Error committing merged file, attempting to remove the uploaded file {}", objectKey, e);
            try {
                storage.delete(objectKey);
            } catch (final StorageBackendException e2) {
                LOGGER.error("Error removing the uploaded file {}", objectKey, e2);
            }
        } else {
            LOGGER.error("Error committing merged file, but not safe to delete the uploaded file {} as it is not safe", objectKey, e);
        }
    }

    @Override
    public void close() throws IOException {
        storage.close();
        metrics.close();
    }
}
