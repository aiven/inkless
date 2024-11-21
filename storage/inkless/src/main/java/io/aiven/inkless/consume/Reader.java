// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Reader {

    private final ControlPlane controlPlane;
    private final ObjectFetcher objectFetcher;
    private final ExecutorService metadataExecutor;
    private final ExecutorService fetchPlannerExecutor;
    private final ExecutorService dataExecutor;
    private final ExecutorService fetchCompleterExecutor;

    public Reader(ControlPlane controlPlane, ObjectFetcher objectFetcher) {
        this(
                controlPlane,
                objectFetcher,
                Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-metadata-", false)),
                Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-planner-", false)),
                Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-data-", false)),
                Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-completer-", false))
        );
    }


    public Reader(
            ControlPlane controlPlane,
            ObjectFetcher objectFetcher,
            ExecutorService metadataExecutor,
            ExecutorService fetchPlannerExecutor,
            ExecutorService dataExecutor,
            ExecutorService fetchCompleterExecutor
    ) {
        this.controlPlane = controlPlane;
        this.objectFetcher = objectFetcher;
        this.metadataExecutor = metadataExecutor;
        this.fetchPlannerExecutor = fetchPlannerExecutor;
        this.dataExecutor = dataExecutor;
        this.fetchCompleterExecutor = fetchCompleterExecutor;
    }

    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            final FetchParams params,
            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        Future<Map<TopicIdPartition, FindBatchResponse>> batchCoordinates = metadataExecutor.submit(new FindBatchesJob(controlPlane, params, fetchInfos));
        Future<List<Future<FetchedFile>>> fetchedData = fetchPlannerExecutor.submit(new FetchPlannerJob(objectFetcher, dataExecutor, batchCoordinates));
        return CompletableFuture.supplyAsync(new FetchCompleterJob(fetchInfos, batchCoordinates, fetchedData), fetchCompleterExecutor);
    }
}
