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
package io.aiven.inkless.consume;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;

public class FetchCompleter implements Supplier<Map<TopicIdPartition, FetchPartitionData>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchCompleter.class);

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final Map<TopicIdPartition, FindBatchResponse> coordinates;
    private final List<FileExtentResult> backingData;
    private final Consumer<Long> durationCallback;
    private final InklessFetchMetrics metrics;

    public FetchCompleter(Time time,
                          ObjectKeyCreator objectKeyCreator,
                          Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                          Map<TopicIdPartition, FindBatchResponse> coordinates,
                          List<FileExtentResult> backingData,
                          Consumer<Long> durationCallback,
                          InklessFetchMetrics metrics) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.fetchInfos = fetchInfos;
        this.coordinates = coordinates;
        this.backingData = backingData;
        this.durationCallback = durationCallback;
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
    }

    @Override
    public Map<TopicIdPartition, FetchPartitionData> get() {
        try {
            final Map<String, List<FileExtent>> files = groupFileData();
            final Map<TopicIdPartition, FetchPartitionData> result =
                TimeUtils.measureDurationMs(time, () -> serveFetch(coordinates, files), durationCallback);
            long totalBytes = 0;
            for (FetchPartitionData d : result.values()) {
                totalBytes += d.records.sizeInBytes();
            }
            // Record every fetch, including 0-byte responses: empty-fetch frequency is signal, and
            // this stays consistent with the other (unguarded) fetch-shape histograms.
            metrics.recordFetchResponseSize(totalBytes);
            return result;
        } catch (Exception e) {
            throw new FetchException("Failed to complete fetch for partitions " + fetchInfos.keySet(), e);
        }
    }

    // Groups file extents by their object keys, handling failures per object.
    // When a failure occurs for an object key, processing stops for that object and only
    // successfully fetched ranges up to that point are used.
    private Map<String, List<FileExtent>> groupFileData() {
        // First, group all results by object key
        final Map<String, List<FileExtentResult>> resultsByObject = new HashMap<>();
        for (FileExtentResult result : backingData) {
            final String objectKey = result.objectKey().value();
            resultsByObject.computeIfAbsent(objectKey, k -> new ArrayList<>()).add(result);
        }

        final Map<String, List<FileExtent>> files = new HashMap<>();
        for (Map.Entry<String, List<FileExtentResult>> entry : resultsByObject.entrySet()) {
            final String objectKey = entry.getKey();
            final List<FileExtentResult> results = entry.getValue();

            // Sort results by range offset to process in order
            results.sort(Comparator.comparingLong(a -> a.byteRange().offset()));

            // Process results in order, stopping at first failure
            final List<FileExtent> successfulExtents = new ArrayList<>();
            for (FileExtentResult result : results) {
                if (result instanceof FileExtentResult.Failure) {
                    // Stop processing this object key on first failure
                    // Only successfully fetched ranges up to this point will be used
                    break;
                } else if (result instanceof FileExtentResult.Success success) {
                    successfulExtents.add(success.extent());
                }
            }

            // Only add object key to files map if we have at least one successful fetch
            if (!successfulExtents.isEmpty()) {
                files.put(objectKey, successfulExtents);
            }
        }
        return files;
    }

    private Map<TopicIdPartition, FetchPartitionData> serveFetch(
        final Map<TopicIdPartition, FindBatchResponse> metadata,
        final Map<String, List<FileExtent>> files
    ) {
        return fetchInfos.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> servePartition(e.getKey(), metadata, files))
            );
    }

    private FetchPartitionData servePartition(
        final TopicIdPartition key,
        final Map<TopicIdPartition, FindBatchResponse> allMetadata,
        final Map<String, List<FileExtent>> allFiles
    ) {
        FindBatchResponse metadata = allMetadata.get(key);
        if (metadata == null) {
            metrics.recordPartitionStorageError();
            return errorResponse(Errors.KAFKA_STORAGE_ERROR);
        }
        if (metadata.errors() != Errors.NONE || metadata.batches().isEmpty()) {
            return new FetchPartitionData(
                metadata.errors(),
                metadata.highWatermark(),
                metadata.logStartOffset(),
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
            );
        }
        final ExtractionResult extraction;
        try {
            extraction = extractRecords(metadata, allFiles);
        } catch (RuntimeException e) {
            // Safety net for the unexpected: constructBatch already classifies expected failures, so a
            // throw here (oversized/negative coordinate, truncated extent, or a bug) is scoped to this
            // partition rather than aborting the whole fetch. KAFKA_STORAGE_ERROR is deliberate -
            // retriable on every client, unlike UNKNOWN_SERVER_ERROR from Errors.forException. Logged
            // without a trace so a wedged consumer's re-fetch loop cannot flood the log.
            LOGGER.warn("Unexpected error extracting diskless records for partition {}; returning storage error: {}", key, e.toString());
            metrics.recordPartitionStorageError();
            return errorResponse(Errors.KAFKA_STORAGE_ERROR);
        }
        final List<MemoryRecords> foundRecords = extraction.records();
        if (foundRecords.isEmpty()) {
            // Nothing extractable: map to the classic per-partition code (CORRUPT_MESSAGE /
            // INVALID_RECORD for integrity failures, KAFKA_STORAGE_ERROR for missing/incomplete data).
            final Errors error = extraction.errorIfEmpty();
            if (error == Errors.CORRUPT_MESSAGE || error == Errors.INVALID_RECORD) {
                metrics.recordPartitionCorruptRecord();
            } else {
                metrics.recordPartitionStorageError();
            }
            return errorResponse(error);
        }
        // Partial: extracted some batches but fewer than the metadata listed (trailing batch dropped).
        if (foundRecords.size() < metadata.batches().size()) {
            metrics.recordPartitionPartialFetch();
        }

        return new FetchPartitionData(
            Errors.NONE,
            metadata.highWatermark(),
            metadata.logStartOffset(),
            new ConcatenatedRecords(foundRecords),
            Optional.empty(),
            OptionalLong.of(metadata.highWatermark()),
            Optional.empty(),
            OptionalInt.empty(),
            false
        );
    }

    private static FetchPartitionData errorResponse(final Errors error) {
        return new FetchPartitionData(
            error, -1, -1, MemoryRecords.EMPTY,
            Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false);
    }

    /**
     * Result of extracting a partition's batches: the (possibly partial) prefix of successfully
     * constructed records, plus the error to surface if that prefix is empty ({@link Errors#NONE}
     * when everything was extracted).
     */
    private record ExtractionResult(List<MemoryRecords> records, Errors errorIfEmpty) {}

    /** Result of constructing a single batch: the records, or {@code null} with the classifying error. */
    private record BatchResult(MemoryRecords records, Errors error) {}

    /**
     * Extracts records for a partition's batches into a coalesced buffer, stopping at the first batch
     * that is incomplete or fails integrity validation. Returns the successfully constructed prefix
     * plus the error to surface if that prefix is empty (KAFKA_STORAGE_ERROR for missing/incomplete
     * data, CORRUPT_MESSAGE / INVALID_RECORD for integrity failures).
     *
     * @param metadata the batch metadata for the partition
     * @param allFiles the map of object keys to fetched file extents
     * @return the extracted prefix and the error to use if the prefix is empty
     */
    private ExtractionResult extractRecords(FindBatchResponse metadata, Map<String, List<FileExtent>> allFiles) {
        final List<BatchInfo> batches = metadata.batches();
        final List<MemoryRecords> foundRecords = new ArrayList<>();

        // One partition-level buffer sliced per batch instead of one allocation per batch: the
        // multi-batch tail drops from N allocations to 1. Fall back to per-batch buffers if the total
        // would overflow an int-sized array (practically unreachable under fetch.max.bytes).
        final long totalSize = computeTotalBufferSize(batches);
        final boolean coalesce = totalSize >= 0 && totalSize <= Integer.MAX_VALUE;
        final byte[] sharedBuffer = coalesce ? new byte[(int) totalSize] : null;
        int writeOffset = 0;
        for (BatchInfo batch : batches) {
            final long rawSize = batch.metadata().byteSize();
            if (rawSize < 0 || rawSize > Integer.MAX_VALUE) {
                // Corrupt coordinate (nonsensical size). Classify as CORRUPT_MESSAGE ("exceeds the
                // valid size") and keep the valid prefix, rather than throwing into the safety net.
                LOGGER.warn("Dropping diskless batch with invalid size {} for {} in object {}",
                    rawSize, batch.metadata().topicIdPartition(), batch.objectKey());
                return new ExtractionResult(foundRecords, Errors.CORRUPT_MESSAGE);
            }
            final int batchSize = (int) rawSize;
            final List<FileExtent> files = allFiles.get(objectKeyCreator.from(batch.objectKey()).value());
            if (files == null || files.isEmpty()) {
                // Missing file extent for this batch: storage error. Return successful prefix so far.
                return new ExtractionResult(foundRecords, Errors.KAFKA_STORAGE_ERROR);
            }
            final byte[] dest = coalesce ? sharedBuffer : new byte[batchSize];
            final int destOffset = coalesce ? writeOffset : 0;
            final BatchResult batchResult = constructBatch(batch, files, dest, destOffset, batchSize);
            if (batchResult.records() == null) {
                return new ExtractionResult(foundRecords, batchResult.error());
            }
            foundRecords.add(batchResult.records());
            writeOffset += batchSize;
        }
        return new ExtractionResult(foundRecords, Errors.NONE);
    }

    /**
     * Constructs one batch, classifying any failure instead of throwing so a bad batch fails only its
     * partition. Missing/non-covering extents -> KAFKA_STORAGE_ERROR; integrity failures -> the classic
     * code (CorruptRecordException -> CORRUPT_MESSAGE, InvalidRecordException -> INVALID_RECORD), as
     * ReplicaManager.readFromLocalLog does via Errors.forException. Other RuntimeExceptions propagate
     * to the per-partition safety net.
     *
     * <p>Only CorruptRecordException is thrown on this path today; the InvalidRecordException arm is
     * defensive for a future record-iterating validation.
     */
    private static BatchResult constructBatch(
        final BatchInfo batch,
        final List<FileExtent> files,
        final byte[] dest,
        final int destOffset,
        final int batchSize
    ) {
        try {
            final MemoryRecords records = constructRecordsIntoSlice(batch, files, dest, destOffset, batchSize);
            return records == null
                ? new BatchResult(null, Errors.KAFKA_STORAGE_ERROR)
                : new BatchResult(records, Errors.NONE);
        } catch (CorruptRecordException | InvalidRecordException e) {
            // Data corruption (bad CRC / size, or a coalesced-run offset mismatch): log object +
            // offsets so operators can locate it, drop the batch, serve the prefix or an error.
            LOGGER.warn("Dropping corrupt diskless batch for {} in object {} offsets [{}, {}]: {}",
                batch.metadata().topicIdPartition(), batch.objectKey(),
                batch.metadata().baseOffset(), batch.metadata().lastOffset(), e.getMessage());
            return new BatchResult(null,
                e instanceof InvalidRecordException ? Errors.INVALID_RECORD : Errors.CORRUPT_MESSAGE);
        }
    }

    private static long computeTotalBufferSize(final List<BatchInfo> batches) {
        long total = 0;
        for (BatchInfo b : batches) {
            final long size = b.metadata().byteSize();
            // A nonsensical size forces the per-batch path so it can't mis-size the shared buffer;
            // the extraction loop then classifies that batch and keeps the prefix.
            if (size < 0 || size > Integer.MAX_VALUE) return -1;
            total += size;
            if (total < 0) return -1; // overflow guard
        }
        return total;
    }

    /**
     * Writes a complete batch into {@code dest[destOffset, destOffset+batchSize)} and returns
     * MemoryRecords over that slice, or {@code null} if the extents don't fully cover the batch
     * range (an incomplete batch is unusable and must be failed).
     *
     * @param batch        the batch metadata
     * @param files        the list of file extents (should be contiguous from groupFileData)
     * @param dest         the destination byte array (typically a partition-level buffer)
     * @param destOffset   start position within {@code dest} to write this batch's bytes
     * @param batchSize    expected size of the batch (must equal {@code batch.metadata().byteSize()})
     * @return MemoryRecords (over a slice of {@code dest}) if the batch is complete, null otherwise
     */
    private static MemoryRecords constructRecordsIntoSlice(
        final BatchInfo batch,
        final List<FileExtent> files,
        final byte[] dest,
        final int destOffset,
        final int batchSize
    ) {
        if (files == null || files.isEmpty()) {
            return null;
        }

        final ByteRange batchRange = batch.metadata().range();

        // Validate that extents fully cover the batch range before writing.
        List<ByteRange> intersections = new ArrayList<>();
        for (FileExtent file : files) {
            final ByteRange fileRange = new ByteRange(file.range().offset(), file.range().length());
            ByteRange intersection = ByteRange.intersect(batchRange, fileRange);
            if (intersection.size() > 0) {
                intersections.add(intersection);
            }
        }

        if (intersections.isEmpty()) {
            return null;
        }

        // Sort by offset to check contiguity
        intersections.sort(Comparator.comparingLong(ByteRange::offset));

        // Check that intersections are contiguous and cover the entire batch range
        long expectedStart = batchRange.offset();
        for (ByteRange intersection : intersections) {
            if (intersection.offset() > expectedStart) {
                return null; // Gap detected - incomplete batch
            }
            expectedStart = Math.max(expectedStart, intersection.offset() + intersection.size());
        }

        if (expectedStart < batchRange.offset() + batchRange.size()) {
            return null; // Doesn't cover entire batch range - incomplete batch
        }

        // All extents cover the batch range, write into dest[destOffset .. destOffset+batchSize].
        for (FileExtent file : files) {
            final ByteRange fileRange = new ByteRange(file.range().offset(), file.range().length());
            ByteRange intersection = ByteRange.intersect(batchRange, fileRange);
            if (intersection.size() > 0) {
                final int positionInBatch = intersection.bufferOffset() - batchRange.bufferOffset();
                final int from = intersection.bufferOffset() - fileRange.bufferOffset();
                final int to = intersection.bufferOffset() - fileRange.bufferOffset() + intersection.bufferSize();
                final byte[] fileData = file.data();
                final int copyLen = Math.min(fileData.length - from, to - from);
                // Keep every write inside this batch's [0, batchSize) slot. A runtime check (not an
                // assert, which is off in production) turns a future miscalc or short extent into a
                // failed batch rather than silent corruption of an adjacent slot.
                if (positionInBatch < 0 || copyLen < 0 || positionInBatch + copyLen > batchSize) {
                    throw new IllegalStateException("batch write out of slot: pos=" + positionInBatch
                        + " len=" + copyLen + " batchSize=" + batchSize);
                }
                System.arraycopy(fileData, from, dest, destOffset + positionInBatch, copyLen);
            }
        }

        // slice() bounds the buffer to batchSize, so createMemoryRecords's in-place header mutations
        // stay within this batch's slot (an out-of-slot write throws rather than corrupting a neighbor).
        return createMemoryRecords(ByteBuffer.wrap(dest, destOffset, batchSize).slice(), batch);
    }

    /**
     * Re-assigns absolute offsets onto the physical record batches contained in a {@link BatchInfo}'s
     * byte range.
     */
    private static MemoryRecords createMemoryRecords(
        final ByteBuffer buffer,
        final BatchInfo batch
    ) {
        final MemoryRecords records = MemoryRecords.readableRecords(buffer);
        final List<MutableRecordBatch> physicalBatches = new ArrayList<>();
        for (final MutableRecordBatch b : records.batches()) {
            physicalBatches.add(b);
        }
        if (physicalBatches.isEmpty()) {
            throw new CorruptRecordException("Backing file should have at least one batch");
        }

        if (physicalBatches.size() == 1) {
            // Classic single-batch coordinate (pg control plane commit_file_v1, in-memory control plane, batch-coordinate
            // cache, or any pre-coalescing row). Behaviour is unchanged: trust the coordinate's last offset.
            final MutableRecordBatch mutableRecordBatch = physicalBatches.get(0);

            // Validate batch integrity (CRC checksum, size) before modifying it.
            // This catches corrupted data that passed size validation but has invalid checksums.
            // We validate before setLastOffset/setMaxTimestamp to avoid modifying corrupted batches.
            mutableRecordBatch.ensureValid();

            mutableRecordBatch.setLastOffset(batch.metadata().lastOffset());

            if (batch.metadata().timestampType() == TimestampType.LOG_APPEND_TIME) {
                mutableRecordBatch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, batch.metadata().logAppendTimestamp());
            }
            return records;
        }

        // Coalesced run (pg control plane commit_file_v2): the coordinate's byte range contains several byte-contiguous
        // physical batches. Relocate each one. Physical batch j's absolute base offset is
        // metadata.baseOffset() + sum(record counts of batches before it). 
        // The offset delta (last - base) is intrinsic to each batch's bytes and independent of the base offset, so
        // setLastOffset(running + delta) places that physical batch's base at `running`.
        long running = batch.metadata().baseOffset();
        for (final MutableRecordBatch mutableRecordBatch : physicalBatches) {
            mutableRecordBatch.ensureValid();

            final long lastOffsetDelta = mutableRecordBatch.lastOffset() - mutableRecordBatch.baseOffset();
            mutableRecordBatch.setLastOffset(running + lastOffsetDelta);

            if (batch.metadata().timestampType() == TimestampType.LOG_APPEND_TIME) {
                mutableRecordBatch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, batch.metadata().logAppendTimestamp());
            }

            running += lastOffsetDelta + 1L;
        }

        // The relocated offsets must exactly fill the coordinate's [baseOffset, lastOffset] span. A
        // mismatch means the stored bytes and the coordinate disagree (corruption or a wrong coordinate),
        // so we reject the batch as corrupt rather than serve incorrectly-offset data. constructBatch
        // maps this to CORRUPT_MESSAGE, scoped to the partition.
        final long expectedLastOffset = batch.metadata().lastOffset();
        if (running - 1 != expectedLastOffset) {
            throw new CorruptRecordException(
                "Coalesced batch offsets do not match metadata: computed last offset " + (running - 1)
                    + " but expected " + expectedLastOffset + " for " + batch.metadata().topicIdPartition());
        }

        return records;
    }
}
