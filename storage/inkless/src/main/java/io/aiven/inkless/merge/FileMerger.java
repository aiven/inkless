// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.merge;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FileMergeWorkItem;
import io.aiven.inkless.control_plane.MergedFileBatch;
import io.aiven.inkless.produce.FileUploadJob;
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

            // `close` is no-op for ByteArrayOutputStream, we can skip closing it.
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            final List<MergedFileBatch> mergedFileBatches = new ArrayList<>();
            long fileSize = 0;
            for (final BatchAndStream bf : batches) {
                final BatchInfo parentBatch = bf.batch;

                // ignore auto closing of the input stream, we'll close them manually
                final InputStream inputStream = bf.inputStreamWithPosition.inputStream();
                if (bf.inputStreamWithPosition.position() < parentBatch.metadata().byteOffset()) {
                    // We're facing a gap, need to fast-forward.
                    final long gapSize = parentBatch.metadata().byteOffset() - bf.inputStreamWithPosition.position();
                    try {
                        inputStream.skipNBytes(gapSize);
                    } catch (final EOFException e) {
                        throw new RuntimeException("Desynchronization between batches and files");
                    }
                    bf.inputStreamWithPosition.advance(gapSize);
                } else if (bf.inputStreamWithPosition.position() > parentBatch.metadata().byteOffset()) {
                    throw new RuntimeException("Desynchronization between batches and files");
                }

                final long batchSize = parentBatch.metadata().byteSize();
                final byte[] readBytes = inputStream.readNBytes((int) batchSize);
                if (readBytes.length < batchSize) {
                    throw new RuntimeException("Desynchronization between batches and files");
                }
                outputStream.writeBytes(readBytes);
                bf.inputStreamWithPosition.advance(batchSize);

                bf.inputStreamWithPosition.closeIfFullyRead();

                final long offset = fileSize;
                fileSize += batchSize;
                mergedFileBatches.add(new MergedFileBatch(
                    new BatchMetadata(
                        parentBatch.metadata().topicIdPartition(),
                        offset,
                        batchSize,
                        parentBatch.metadata().baseOffset(),
                        parentBatch.metadata().lastOffset(),
                        parentBatch.metadata().logAppendTimestamp(),
                        parentBatch.metadata().batchMaxTimestamp(),
                        parentBatch.metadata().timestampType(),
                        parentBatch.metadata().producerId(),
                        parentBatch.metadata().producerEpoch(),
                        parentBatch.metadata().baseSequence(),
                        parentBatch.metadata().lastSequence()
                    ),
                    List.of(parentBatch.batchId())
                ));
            }

            final ObjectKey objectKey = new FileUploadJob(
                objectKeyCreator, storage, time,
                config.produceMaxUploadAttempts(),
                config.produceUploadBackoff(),
                outputStream.toByteArray(),
                metrics::recordFileUploadTime
            ).call();

            controlPlane.commitFileMergeWorkItem(
                workItem.workItemId(),
                objectKey.value(),
                brokerId,
                fileSize,
                mergedFileBatches
            );
            LOGGER.info("Merged {} files into {}", workItem.files().size(), objectKey);
        } finally {
            for (final var batch : batches) {
                batch.inputStreamWithPosition.forceClose();
            }
        }
    }

    // One InputStream per file.
    // Note that BoundedInputStream is by default unbound, we use it only for counting the current position.
    private static class InputStreamWithPosition {
        private final Supplier<InputStream> inputStreamSupplier;
        private final long size;
        private long position = 0;
        private InputStream source = null;

        private InputStreamWithPosition(final Supplier<InputStream> inputStreamSupplier, final long size) {
            this.inputStreamSupplier = inputStreamSupplier;
            this.size = size;
        }

        long position() {
            return position;
        }

        void advance(final long offset) {
            this.position += offset;
        }

        public InputStream inputStream() {
            if (source == null) {
                source = inputStreamSupplier.get();
            }
            return source;
        }

        public void closeIfFullyRead() {
            if (position >= size) close();
        }

        public void forceClose() {
            if (position < size) close();
        }

        private void close() {
            if (source != null) {
                Utils.closeQuietly(source, "object storage input stream");
            }
        }
    }

    private record BatchAndStream(BatchInfo batch,
                                  InputStreamWithPosition inputStreamWithPosition) {
        /**
         * The comparator that sorts batches by the following criteria (in order):
         * <ol>
         *     <li>Topic ID.</li>
         *     <li>Partition.</li>
         *     <li>Base offset.</li>
         * </ol>
         */
        static final Comparator<BatchAndStream> TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR;
        static {
            final Comparator<BatchAndStream> topicIdComparator = Comparator.comparing(bf -> bf.batch.metadata().topicIdPartition().topicId());
            final Comparator<BatchAndStream> partitionComparator = Comparator.comparing(bf -> bf.batch.metadata().topicIdPartition().partition());
            final Comparator<BatchAndStream> offsetComparator = Comparator.comparing(bf -> bf.batch.metadata().baseOffset());
            TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR = topicIdComparator.thenComparing(partitionComparator).thenComparing(offsetComparator);
        }
    }
}
