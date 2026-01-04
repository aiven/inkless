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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final Map<TopicIdPartition, FindBatchResponse> coordinates;
    private final List<FileExtentResult> backingData;
    private final Consumer<Long> durationCallback;

    public FetchCompleter(Time time,
                          ObjectKeyCreator objectKeyCreator,
                          Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                          Map<TopicIdPartition, FindBatchResponse> coordinates,
                          List<FileExtentResult> backingData,
                          Consumer<Long> durationCallback) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.fetchInfos = fetchInfos;
        this.coordinates = coordinates;
        this.backingData = backingData;
        this.durationCallback = durationCallback;
    }

    @Override
    public Map<TopicIdPartition, FetchPartitionData> get() {
        try {
            final Map<String, List<FileExtent>> files = groupFileData();
            return TimeUtils.measureDurationMs(time, () -> serveFetch(coordinates, files), durationCallback);
        } catch (Exception e) {
            throw new FetchException("Failed to complete fetch for partitions " + fetchInfos.keySet(), e);
        }
    }

    private Map<String, List<FileExtent>> groupFileData() {
        Map<String, List<FileExtent>> files = new HashMap<>();
        for (FileExtentResult result : backingData) {
            // Only process successful fetches - failures are handled as missing data
            // which results in KAFKA_STORAGE_ERROR in extractRecords/servePartition
            if (result instanceof FileExtentResult.Success success) {
                final FileExtent fileExtent = success.extent();
                files.compute(fileExtent.object(), (k, v) -> {
                    if (v == null) {
                        List<FileExtent> out = new ArrayList<>(1);
                        out.add(fileExtent);
                        return out;
                    } else {
                        v.add(fileExtent);
                        return v;
                    }
                });
            }
            // Failure results are intentionally skipped - they don't contribute to files map
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
            return new FetchPartitionData(
                Errors.KAFKA_STORAGE_ERROR,
                -1,
                -1,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
            );
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
        List<MemoryRecords> foundRecords = extractRecords(metadata, allFiles);
        if (foundRecords.isEmpty()) {
            // If there is no FetchedFile to serve this topic id partition, the earlier steps which prepared the metadata + data have an error.
            return new FetchPartitionData(
                Errors.KAFKA_STORAGE_ERROR,
                -1,
                -1,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
            );
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

    /**
     * Extracts memory records from file extents for a partition's batches.
     *
     * <p><b>Partial Failure Handling:</b>
     * This method returns partial results if some batches fail to extract (missing files or null records).
     * This is intentional to support partial failure scenarios where some batches succeed while others fail.
     * The calling code (servePartition) will check if foundRecords is empty and return KAFKA_STORAGE_ERROR
     * if no records were found, allowing successful batches to be returned when possible.
     *
     * @param metadata the batch metadata for the partition
     * @param allFiles the map of object keys to fetched file extents
     * @return list of memory records (may be partial if some batches failed)
     */
    private List<MemoryRecords> extractRecords(FindBatchResponse metadata, Map<String, List<FileExtent>> allFiles) {
        List<MemoryRecords> foundRecords = new ArrayList<>();
        for (BatchInfo batch : metadata.batches()) {
            List<FileExtent> files = allFiles.get(objectKeyCreator.from(batch.objectKey()).value());
            if (files == null || files.isEmpty()) {
                // Missing file extent for this batch - return partial results (successful batches so far)
                return foundRecords;
            }
            MemoryRecords fileRecords = constructRecordsFromFile(batch, files);
            if (fileRecords == null) {
                // Failed to construct records from file - return partial results (successful batches so far)
                return foundRecords;
            }
            foundRecords.add(fileRecords);
        }
        return foundRecords;
    }

    private static MemoryRecords constructRecordsFromFile(
        final BatchInfo batch,
        final List<FileExtent> files
    ) {
        byte[] buffer = null;
        for (FileExtent file : files) {
            final ByteRange batchRange = batch.metadata().range();
            final ByteRange fileRange = new ByteRange(file.range().offset(), file.range().length());
            ByteRange intersection = ByteRange.intersect(batchRange, fileRange);
            if (intersection.size() > 0) {
                if (buffer == null) {
                    buffer = new byte[Math.toIntExact(batchRange.bufferSize())];
                }
                final int position = intersection.bufferOffset() - batchRange.bufferOffset();
                final int from = intersection.bufferOffset() - fileRange.bufferOffset();
                final int to = intersection.bufferOffset() - fileRange.bufferOffset() + intersection.bufferSize();
                final byte[] fileData = file.data();
                System.arraycopy(fileData, from, buffer, position, Math.min(fileData.length - from, to - from));
            }
        }
        if (buffer == null) {
            return null;
        }
        return createMemoryRecords(ByteBuffer.wrap(buffer), batch);
    }

    private static MemoryRecords createMemoryRecords(
        final ByteBuffer buffer,
        final BatchInfo batch
    ) {
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        if (!iterator.hasNext()) {
            throw new IllegalStateException("Backing file should have at least one batch");
        }
        MutableRecordBatch mutableRecordBatch = iterator.next();

        // set last offset
        mutableRecordBatch.setLastOffset(batch.metadata().lastOffset());

        // set log append timestamp
        if (batch.metadata().timestampType() == TimestampType.LOG_APPEND_TIME) {
            mutableRecordBatch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, batch.metadata().logAppendTimestamp());
        }

        if (iterator.hasNext()) {
            // TODO: support concatenating multiple batches into a single BatchInfo
            throw new IllegalStateException("Backing file should have at only one batch");
        }

        return records;
    }
}
