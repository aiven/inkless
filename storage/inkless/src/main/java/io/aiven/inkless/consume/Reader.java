// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class Reader implements AutoCloseable {

    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;
    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final ControlPlane controlPlane;
    private final ObjectFetcher objectFetcher;
    private final ExecutorService metadataExecutor;
    private final ExecutorService fetchPlannerExecutor;
    private final ExecutorService dataExecutor;
    private final ExecutorService fetchCompleterExecutor;
    private final InklessFetchMetrics fetchMetrics;

    public Reader(Time time,
                  ObjectKeyCreator objectKeyCreator,
                  ControlPlane controlPlane,
                  ObjectFetcher objectFetcher) {
        this(
            time,
            objectKeyCreator,
            controlPlane,
            objectFetcher,
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-metadata-", false)),
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-planner-", false)),
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-data-", false)),
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-completer-", false))
        );
    }


    public Reader(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        ControlPlane controlPlane,
        ObjectFetcher objectFetcher,
        ExecutorService metadataExecutor,
        ExecutorService fetchPlannerExecutor,
        ExecutorService dataExecutor,
        ExecutorService fetchCompleterExecutor
    ) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.controlPlane = controlPlane;
        this.objectFetcher = objectFetcher;
        this.metadataExecutor = metadataExecutor;
        this.fetchPlannerExecutor = fetchPlannerExecutor;
        this.dataExecutor = dataExecutor;
        this.fetchCompleterExecutor = fetchCompleterExecutor;
        this.fetchMetrics = new InklessFetchMetrics(time);
    }

    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        final FetchParams params,
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos
    ) {
        final Instant startAt = TimeUtils.durationMeasurementNow(time);
        final var batchCoordinates = metadataExecutor.submit(
            new FindBatchesJob(
                time,
                controlPlane,
                params,
                fetchInfos,
                fetchMetrics::findBatchesFinished
            )
        );
        final var fetchedData = fetchPlannerExecutor.submit(
            new FetchPlannerJob(
                time,
                objectKeyCreator,
                objectFetcher,
                dataExecutor,
                batchCoordinates,
                fetchMetrics::fetchPlanFinished,
                fetchMetrics::fetchFileFinished
            )
        );
        return CompletableFuture.supplyAsync(
                new FetchCompleterJob(
                    time,
                    objectKeyCreator,
                    fetchInfos,
                    batchCoordinates,
                    fetchedData,
                    fetchMetrics::fetchCompletionFinished
                ),
                fetchCompleterExecutor
            )
            .whenComplete((topicIdPartitionFetchPartitionDataMap, throwable) -> {
                fetchMetrics.fetchCompleted(startAt);
            });
    }

    @Override
    public void close() {
        ThreadUtils.shutdownExecutorServiceQuietly(metadataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(fetchPlannerExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(dataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(fetchCompleterExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
