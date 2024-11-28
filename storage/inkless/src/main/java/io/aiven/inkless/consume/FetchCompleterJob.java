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
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;

public class FetchCompleterJob implements Supplier<Map<TopicIdPartition, FetchPartitionData>> {

    private final Time time;
    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final Future<Map<TopicIdPartition, FindBatchResponse>> coordinates;
    private final Future<List<Future<FetchedFile>>> backingData;
    private final Consumer<Long> durationCallback;

    public FetchCompleterJob(Time time,
                             Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                             Future<Map<TopicIdPartition, FindBatchResponse>> coordinates,
                             Future<List<Future<FetchedFile>>> backingData,
                             Consumer<Long> durationCallback) {
        this.time = time;
        this.fetchInfos = fetchInfos;
        this.coordinates = coordinates;
        this.backingData = backingData;
        this.durationCallback = durationCallback;
    }

    @Override
    public Map<TopicIdPartition, FetchPartitionData> get() {
        try {
            return TimeUtils.measureDurationMs(time, this::doGet, durationCallback);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<TopicIdPartition, FetchPartitionData> doGet() throws ExecutionException, InterruptedException {
            Map<ObjectKey, List<FetchedFile>> files = waitForFileData();
            return serveFetch(coordinates.get(), files);
    }

    private Map<ObjectKey, List<FetchedFile>> waitForFileData() throws InterruptedException, ExecutionException {
        Map<ObjectKey, List<FetchedFile>> files = new HashMap<>();
        List<Future<FetchedFile>> fileFutures = backingData.get();
        for (Future<FetchedFile> fileFuture : fileFutures) {
            FetchedFile fetchedFile = fileFuture.get();
            files.compute(fetchedFile.key(), (k, v) -> {
                if (v == null) {
                    List<FetchedFile> out = new ArrayList<>(1);
                    out.add(fetchedFile);
                    return out;
                } else {
                    v.add(fetchedFile);
                    return v;
                }
            });
        }
        return files;
    }

    private Map<TopicIdPartition, FetchPartitionData> serveFetch(
            Map<TopicIdPartition, FindBatchResponse> metadata,
            Map<ObjectKey, List<FetchedFile>> files
    ) {
        return fetchInfos.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> servePartition(e.getKey(), metadata, files)));
    }

    private static FetchPartitionData servePartition(TopicIdPartition key, Map<TopicIdPartition, FindBatchResponse> allMetadata, Map<ObjectKey, List<FetchedFile>> allFiles) {
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

    private static List<MemoryRecords> extractRecords(FindBatchResponse metadata, Map<ObjectKey, List<FetchedFile>> allFiles) {
        List<MemoryRecords> foundRecords = new ArrayList<>();
        for (BatchInfo batch : metadata.batches()) {
            List<FetchedFile> files = allFiles.get(batch.objectKey());
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

    private static MemoryRecords constructRecordsFromFile(BatchInfo batch, List<FetchedFile> files) {
        for (FetchedFile file : files) {
            if (file.range().contains(batch.range())) {
                MemoryRecords records = MemoryRecords.readableRecords(file.buffer());
                Iterator<MutableRecordBatch> iterator = records.batches().iterator();
                if (!iterator.hasNext()) {
                    throw new IllegalStateException("Backing file should have at least one batch");
                }
                MutableRecordBatch mutableRecordBatch = iterator.next();

                // set last offset
                long lastOffset = batch.recordOffset() + batch.numberOfRecords() - 1;
                mutableRecordBatch.setLastOffset(lastOffset);

                // set log append timestamp
                if (batch.timestampType() == TimestampType.LOG_APPEND_TIME) {
                    mutableRecordBatch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, batch.logAppendTime());
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
