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
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.metrics.ThreadPoolMonitor;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class Reader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reader.class);
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;
    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
    private final ControlPlane controlPlane;
    private final ObjectFetcher objectFetcher;
    private final int maxBatchesPerPartitionToFind;
    private final ExecutorService metadataExecutor;
    private final ExecutorService dataExecutor;
    private final InklessFetchMetrics fetchMetrics;
    private final BrokerTopicStats brokerTopicStats;
    private ThreadPoolMonitor metadataThreadPoolMonitor;
    private ThreadPoolMonitor dataThreadPoolMonitor;

    public Reader(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        ControlPlane controlPlane,
        ObjectFetcher objectFetcher,
        BrokerTopicStats brokerTopicStats,
        int fetchMetadataThreadPoolSize,
        int fetchDataThreadPoolSize,
        int maxBatchesPerPartitionToFind
    ) {
        this(
            time,
            objectKeyCreator,
            keyAlignmentStrategy,
            cache,
            controlPlane,
            objectFetcher,
            maxBatchesPerPartitionToFind,
            Executors.newFixedThreadPool(fetchMetadataThreadPoolSize, new InklessThreadFactory("inkless-fetch-metadata-", false)),
            Executors.newFixedThreadPool(fetchDataThreadPoolSize, new InklessThreadFactory("inkless-fetch-data-", false)),
            new InklessFetchMetrics(time, cache),
            brokerTopicStats
        );
    }

    // visible for testing
    Reader(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        ControlPlane controlPlane,
        ObjectFetcher objectFetcher,
        int maxBatchesPerPartitionToFind,
        ExecutorService metadataExecutor,
        ExecutorService dataExecutor,
        InklessFetchMetrics fetchMetrics,
        BrokerTopicStats brokerTopicStats
    ) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.keyAlignmentStrategy = keyAlignmentStrategy;
        this.cache = cache;
        this.controlPlane = controlPlane;
        this.objectFetcher = objectFetcher;
        this.maxBatchesPerPartitionToFind = maxBatchesPerPartitionToFind;
        this.metadataExecutor = metadataExecutor;
        this.dataExecutor = dataExecutor;
        this.fetchMetrics = fetchMetrics;
        this.brokerTopicStats = brokerTopicStats;
        try {
            this.metadataThreadPoolMonitor = new ThreadPoolMonitor("inkless-fetch-metadata", metadataExecutor);
            this.dataThreadPoolMonitor = new ThreadPoolMonitor("inkless-fetch-data", dataExecutor);
        } catch (final Exception e) {
            // only expected to happen on tests passing other types of pools
            LOGGER.warn("Failed to create thread pool monitors", e);
        }
    }

    /**
     * Asynchronously fetches data for multiple partitions using a staged pipeline:
     *
     * <p>1. Finds batches (metadataExecutor);
     * <p>2. Plans and submits fetches (calling thread);
     * <p>3. Fetches data (dataExecutor);
     * <p>4. Assembles final response (calling thread)
     *
     * <p>Each stage uses a thread pool or the calling thread for efficiency.
     *
     * @param params fetch parameters
     * @param fetchInfos partitions and offsets to fetch
     * @return CompletableFuture with fetch results per partition
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        final FetchParams params,
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos
    ) {
        final Instant startAt = TimeUtils.durationMeasurementNow(time);
        fetchMetrics.fetchStarted(fetchInfos.size());

        // Find Batches (metadataExecutor): Query control plane to find which storage objects contain the requested data
        final var batchCoordinates = CompletableFuture.supplyAsync(
            new FindBatchesJob(
                time,
                controlPlane,
                params,
                fetchInfos,
                maxBatchesPerPartitionToFind,
                fetchMetrics::findBatchesFinished
            ),
            metadataExecutor
        );

        // Plan & Submit (calling thread): Create fetch plan and submit to cache (non-blocking)
        return batchCoordinates.thenApply(coordinates -> {
                // FetchPlanner creates a plan and submits requests to cache
                // Returns List<CompletableFuture<FileExtent>> immediately
                return new FetchPlanner(
                    time,
                    objectKeyCreator,
                    keyAlignmentStrategy,
                    cache,
                    objectFetcher,
                    dataExecutor,
                    coordinates,
                    fetchMetrics
                ).get();
            })
            // Fetch Data (dataExecutor): Flatten list of futures into single future with all results
            .thenCompose(Reader::allOfFileExtents)
            // Complete Fetch (calling thread): Combine fetched data with batch coordinates to build final response
            .thenCombine(batchCoordinates, (fileExtents, coordinates) ->
                new FetchCompleter(
                    time,
                    objectKeyCreator,
                    fetchInfos,
                    coordinates,
                    fileExtents,
                    fetchMetrics::fetchCompletionFinished
                ).get()
            )
            .whenComplete((topicIdPartitionFetchPartitionDataMap, throwable) -> {
                // Mark broker side fetch metrics
                if (throwable != null) {
                    LOGGER.warn("Fetch failed", throwable);
                    for (final var entry : fetchInfos.entrySet()) {
                        final String topic = entry.getKey().topic();
                        brokerTopicStats.allTopicsStats().failedFetchRequestRate().mark();
                        brokerTopicStats.topicStats(topic).failedFetchRequestRate().mark();
                    }
                    // Record specific failure metrics based on exception type
                    // All exceptions are wrapped in CompletionException due to CompletableFuture
                    if (throwable instanceof CompletionException && throwable.getCause() != null) {
                        final Throwable cause = throwable.getCause();
                        if (cause instanceof FindBatchesException) {
                            fetchMetrics.findBatchesFailed();
                        } else if (cause instanceof FileFetchException) {
                            fetchMetrics.fileFetchFailed();
                        }
                    }
                    fetchMetrics.fetchFailed();
                } else {
                    for (final var entry : topicIdPartitionFetchPartitionDataMap.entrySet()) {
                        final String topic = entry.getKey().topic();
                        if (entry.getValue().error == Errors.NONE) {
                            brokerTopicStats.allTopicsStats().totalFetchRequestRate().mark();
                            brokerTopicStats.topicStats(topic).totalFetchRequestRate().mark();
                        } else {
                            brokerTopicStats.allTopicsStats().failedFetchRequestRate().mark();
                            brokerTopicStats.topicStats(topic).failedFetchRequestRate().mark();
                        }
                    }
                    fetchMetrics.fetchCompleted(startAt);
                }
            });
    }

    /**
     * Waits for all file extent futures to complete and collects results in order.
     *
     * @param fileExtentFutures the list of futures to wait for
     * @return a future that completes with a list of file extents in the same order as the input
     */
    static CompletableFuture<List<FileExtent>> allOfFileExtents(
        List<CompletableFuture<FileExtent>> fileExtentFutures
    ) {
        // This does **not** block the calling thread or fork-join pool:
        // 1. allOf() returns immediately with a future (non-blocking)
        // 2. thenApply() registers a callback without waiting (non-blocking)
        // 3. join() inside the callback is safe - it only runs _after_ allOf completes,
        //    meaning all futures are already done, so join() returns immediately
        final CompletableFuture<?>[] futuresArray = fileExtentFutures.toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futuresArray)
            .thenApply(v ->
                fileExtentFutures.stream()
                    .map(CompletableFuture::join)
                    .toList());
    }

    @Override
    public void close() throws IOException {
        ThreadUtils.shutdownExecutorServiceQuietly(metadataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(dataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (metadataThreadPoolMonitor != null) metadataThreadPoolMonitor.close();
        if (dataThreadPoolMonitor != null) dataThreadPoolMonitor.close();
        objectFetcher.close();
        fetchMetrics.close();
    }
}
