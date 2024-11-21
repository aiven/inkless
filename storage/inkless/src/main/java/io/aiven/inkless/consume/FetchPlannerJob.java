// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FetchPlannerJob implements Callable<List<Future<FetchedFile>>> {

    private final ObjectFetcher objectFetcher;
    private final ExecutorService dataExecutor;
    private final Future<Map<TopicIdPartition, FindBatchResponse>> batchCoordinatesFuture;

    public FetchPlannerJob(ObjectFetcher objectFetcher, ExecutorService dataExecutor, Future<Map<TopicIdPartition, FindBatchResponse>> batchCoordinatesFuture) {
        this.objectFetcher = objectFetcher;
        this.dataExecutor = dataExecutor;
        this.batchCoordinatesFuture = batchCoordinatesFuture;
    }

    public List<Future<FetchedFile>> call() {
        try {
            Map<TopicIdPartition, FindBatchResponse> batchCoordinates = batchCoordinatesFuture.get();
            List<Callable<FetchedFile>> jobs = planJobs(batchCoordinates);
            return submitAll(jobs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Unable to plan object fetches without coordinates", e);
        }
    }

    private List<Future<FetchedFile>> submitAll(List<Callable<FetchedFile>> jobs) {
        return jobs.stream()
                .map(dataExecutor::submit)
                .collect(Collectors.toList());
    }

    private List<Callable<FetchedFile>> planJobs(Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        return batchCoordinates.values().stream()
                .filter(findBatch -> findBatch.errors() == Errors.NONE)
                .map(FindBatchResponse::batches)
                .flatMap(List::stream)
                // Merge batch requests
                .collect(Collectors.toMap(BatchInfo::objectKey, BatchInfo::range, ByteRange::merge))
                .entrySet()
                .stream()
                .map(e -> new FileFetchJob(objectFetcher, e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }
}
