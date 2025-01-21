// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;

public class FetchCompleterJob implements Supplier<Map<TopicIdPartition, FetchPartitionData>> {

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final Future<Map<TopicIdPartition, FindBatchResponse>> coordinates;
    private final Future<List<Future<FileExtent>>> backingData;
    private final Consumer<Long> durationCallback;

    public FetchCompleterJob(Time time,
                             ObjectKeyCreator objectKeyCreator,
                             Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                             Future<Map<TopicIdPartition, FindBatchResponse>> coordinates,
                             Future<List<Future<FileExtent>>> backingData,
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
            final Map<String, List<FileExtent>> files = waitForFileData();
            final Map<TopicIdPartition, FindBatchResponse> metadata = coordinates.get();
            return TimeUtils.measureDurationMs(time, () -> serveFetch(metadata, files), durationCallback);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, List<FileExtent>> waitForFileData() throws InterruptedException, ExecutionException {
        Map<String, List<FileExtent>> files = new HashMap<>();
        List<Future<FileExtent>> fileFutures = backingData.get();
        for (Future<FileExtent> fileFuture : fileFutures) {
            FileExtent fileExtent = fileFuture.get();
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
        return files;
    }

    private Map<TopicIdPartition, FetchPartitionData> serveFetch(
            Map<TopicIdPartition, FindBatchResponse> metadata,
            Map<String, List<FileExtent>> files
    ) {
        return fetchInfos.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> servePartition(e.getKey(), metadata, files)));
    }

    private FetchPartitionData servePartition(TopicIdPartition key, Map<TopicIdPartition, FindBatchResponse> allMetadata, Map<String, List<FileExtent>> allFiles) {
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
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
        );
    }

    private List<MemoryRecords> extractRecords(FindBatchResponse metadata, Map<String, List<FileExtent>> allFiles) {
        List<MemoryRecords> foundRecords = new ArrayList<>();
        for (BatchInfo batch : metadata.batches()) {
            List<FileExtent> files = allFiles.get(objectKeyCreator.from(batch.objectKey()).value());
            if (files == null || files.isEmpty()) {
                // as soon as we encounter an error
                return foundRecords;
            }
            MemoryRecords fileRecords = constructRecordsFromFile(batch, files);
            if (fileRecords == null) {
                return foundRecords;
            }
            foundRecords.add(fileRecords);
        }
        return foundRecords;
    }

    private static MemoryRecords constructRecordsFromFile(BatchInfo batch, List<FileExtent> files) {
        for (FileExtent file : files) {
            // TODO INK-77: A single batch may be broken up across multiple FileExtents
            if (new ByteRange(file.range().offset(), file.range().length()).contains(batch.range())) {
                ByteBuffer buffer = ByteBuffer.wrap(file.data()).slice(Math.toIntExact(batch.byteOffset() - file.range().offset()), Math.toIntExact(batch.size()));
                MemoryRecords records = MemoryRecords.readableRecords(buffer);
                Iterator<MutableRecordBatch> iterator = records.batches().iterator();
                if (!iterator.hasNext()) {
                    throw new IllegalStateException("Backing file should have at least one batch");
                }
                MutableRecordBatch mutableRecordBatch = iterator.next();

                // set last offset
                mutableRecordBatch.setLastOffset(batch.lastOffset());

                // set log append timestamp
                if (batch.timestampType() == TimestampType.LOG_APPEND_TIME) {
                    mutableRecordBatch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, batch.logAppendTimestamp());
                }
                
                if (iterator.hasNext()) {
                    // TODO: support concatenating multiple batches into a single BatchInfo
                    throw new IllegalStateException("Backing file should have at only one batch");
                }

                return records;
            }
        }
        return null;
    }
}
