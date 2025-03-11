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

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FileMergeWorkItem;
import io.aiven.inkless.control_plane.MergedFileBatch;
import io.aiven.inkless.produce.MultiPartFileUploadJob;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class FileMerger implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMerger.class);

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
        this.storage = config.storage();
        this.objectKeyCreator = sharedState.objectKeyCreator();
        this.metrics = new FileMergerMetrics();

        // This backoff is needed only for jitter, there's no exponent in it.
        final int noWorkBackoffDuration = 10 * 1000;
        final var noWorkBackoff = new ExponentialBackoff(noWorkBackoffDuration, 1, noWorkBackoffDuration * 2, 0.2);
        noWorkBackoffSupplier = () -> noWorkBackoff.backoff(1);
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

        // Collect all the involved batches into one bag.
        final List<BatchAndStream> batches = new ArrayList<>();
        MergeBatchInputStream mergeBatchInputStream = null;
        try {
            // Collect InputStream supplier for each file, to avoid opening all of them at once.
            for (final var file : workItem.files()) {
                final ObjectKey objectKey = objectKeyCreator.from(file.objectKey());

                final Supplier<InputStream> inputStream = () -> {
                    try {
                        return storage.fetch(objectKey, null);
                    } catch (StorageBackendException e) {
                        throw new RuntimeException(e);
                    }
                };

                final var inputStreamWithPosition = new InputStreamWithPosition(inputStream, file.size());

                for (final var batch : file.batches()) {
                    batches.add(new BatchAndStream(batch, inputStreamWithPosition));
                }
            }
            // Order batches as we want them in the output file.
            batches.sort(BatchAndStream.TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR);

            final List<MergedFileBatch> mergedFileBatches = new ArrayList<>();
            long fileSize = 0;
            for (final BatchAndStream bf : batches) {
                var batchSize = bf.getLength();
                mergedFileBatches.add(new MergedFileBatch(
                    new BatchMetadata(
                        bf.getParentBatch().metadata().topicIdPartition(),
                        fileSize,
                        batchSize,
                        bf.getParentBatch().metadata().baseOffset(),
                        bf.getParentBatch().metadata().lastOffset(),
                        bf.getParentBatch().metadata().logAppendTimestamp(),
                        bf.getParentBatch().metadata().batchMaxTimestamp(),
                        bf.getParentBatch().metadata().timestampType(),
                        bf.getParentBatch().metadata().producerId(),
                        bf.getParentBatch().metadata().producerEpoch(),
                        bf.getParentBatch().metadata().baseSequence(),
                        bf.getParentBatch().metadata().lastSequence()
                    ),
                    List.of(bf.getParentBatch().batchId())
                ));
                fileSize += batchSize;
            }

            mergeBatchInputStream = new MergeBatchInputStream(batches);

            final ObjectKey objectKey = new MultiPartFileUploadJob(
                objectKeyCreator, storage, time,
                config.produceMaxUploadAttempts(),
                config.produceUploadBackoff(),
                mergeBatchInputStream,
                metrics::recordFileUploadTime
            ).call();

            try {
                controlPlane.commitFileMergeWorkItem(
                    workItem.workItemId(),
                    objectKey.value(),
                    brokerId,
                    fileSize,
                    mergedFileBatches
                );
            } catch (final Exception e) {
                LOGGER.error("Error committing merged file, attempting to remove the uploaded file {}", objectKey, e);
                try {
                    storage.delete(objectKey);
                } catch (final StorageBackendException e2) {
                    LOGGER.error("Error removing the uploaded file {}", objectKey, e2);
                }
                // The original exception will be thrown.
                throw e;
            }
            LOGGER.info("Merged {} files into {}", workItem.files().size(), objectKey);
        } finally {
            if (mergeBatchInputStream != null) {
                mergeBatchInputStream.close();
            }
        }
    }

}
