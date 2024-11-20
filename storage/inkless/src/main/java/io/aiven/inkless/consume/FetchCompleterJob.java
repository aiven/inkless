// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FetchCompleterJob implements Callable<Map<TopicIdPartition, FetchPartitionData>> {

    private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos;
    private final Future<Map<TopicIdPartition, FindBatchResponse>> coordinates;
    private final Future<List<Future<FetchedFile>>> backingData;

    public FetchCompleterJob(Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                             Future<Map<TopicIdPartition, FindBatchResponse>> coordinates,
                             Future<List<Future<FetchedFile>>> backingData) {
        this.fetchInfos = fetchInfos;
        this.coordinates = coordinates;
        this.backingData = backingData;
    }

    @Override
    public Map<TopicIdPartition, FetchPartitionData> call() throws Exception {
        Map<TopicIdPartition, FindBatchResponse> metadata = coordinates.get();
        Map<ObjectKey, List<FetchedFile>> files = waitForFileData();
        return serveFetch(metadata, files);
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
        return metadata.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> servePartition(e.getKey(), metadata, files)));
    }

    private FetchPartitionData servePartition(TopicIdPartition key, Map<TopicIdPartition, FindBatchResponse> allMetadata, Map<ObjectKey, List<FetchedFile>> allFiles) {
        FindBatchResponse metadata = allMetadata.get(key);
        if (metadata.batches().isEmpty()) {
            return new FetchPartitionData(
                    Errors.NONE,
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
        for (BatchInfo batch : metadata.batches()) {
            // TODO: concatenate MemoryRecords together to increase fetch response size up to the defined limits
            List<FetchedFile> files = allFiles.get(batch.objectKey());
            for (FetchedFile file : files) {
                if (file.range().contains(batch.range())) {
                    MemoryRecords records = MemoryRecords.readableRecords(file.buffer());
                    for (MutableRecordBatch mutableRecordBatch : records.batches()) {
                        long lastOffset = batch.recordOffset() + batch.numberOfRecords() - 1;
                        mutableRecordBatch.setLastOffset(lastOffset);
                    }

                    return new FetchPartitionData(
                            Errors.NONE,
                            metadata.highWatermark(),
                            metadata.logStartOffset(),
                            records,
                            Optional.empty(),
                            OptionalLong.empty(),
                            Optional.empty(),
                            OptionalInt.empty(),
                            false
                    );
                }
            }
        }
        // If there is no FetchedFile to serve this topic id partition, the earlier steps which prepared the metadata + data have an error.
        return new FetchPartitionData(
                Errors.KAFKA_STORAGE_ERROR,
                -1,
                -1,
                null,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
        );
    }
}
