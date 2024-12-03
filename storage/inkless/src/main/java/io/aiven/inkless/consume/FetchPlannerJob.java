// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class FetchPlannerJob implements Callable<List<Future<FetchedFile>>> {

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final ObjectFetcher objectFetcher;
    private final ExecutorService dataExecutor;
    private final Future<Map<TopicIdPartition, FindBatchResponse>> batchCoordinatesFuture;
    private final Consumer<Long> durationCallback;
    private final Consumer<Long> fileFetchDurationCallback;

    public FetchPlannerJob(Time time,
                           ObjectKeyCreator objectKeyCreator,
                           ObjectFetcher objectFetcher,
                           ExecutorService dataExecutor,
                           Future<Map<TopicIdPartition, FindBatchResponse>> batchCoordinatesFuture,
                           Consumer<Long> durationCallback,
                           Consumer<Long> fileFetchDurationCallback) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.objectFetcher = objectFetcher;
        this.dataExecutor = dataExecutor;
        this.batchCoordinatesFuture = batchCoordinatesFuture;
        this.durationCallback = durationCallback;
        this.fileFetchDurationCallback = fileFetchDurationCallback;
    }

    public List<Future<FetchedFile>> call() throws Exception {
        final Map<TopicIdPartition, FindBatchResponse> batchCoordinates = batchCoordinatesFuture.get();
        return TimeUtils.measureDurationMs(time, () -> doWork(batchCoordinates), durationCallback);
    }

    private List<Future<FetchedFile>> doWork(final Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        final List<Callable<FetchedFile>> jobs = planJobs(batchCoordinates);
        return submitAll(jobs);
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
                .map(e ->
                    new FileFetchJob(time, objectFetcher, objectKeyCreator.create(e.getKey()), e.getValue(), fileFetchDurationCallback))
                .collect(Collectors.toList());
    }

    private List<Future<FetchedFile>> submitAll(List<Callable<FetchedFile>> jobs) {
        return jobs.stream()
            .map(dataExecutor::submit)
            .collect(Collectors.toList());
    }
}
