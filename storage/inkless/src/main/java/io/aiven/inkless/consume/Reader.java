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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class Reader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reader.class);

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

    /**
     * Creates a Reader with injected executor services.
     *
     * <p>The executor services are managed by the caller (typically SharedState) and will be
     * shut down externally. This Reader does not own the lifecycle of the executors.
     */
    public Reader(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        ControlPlane controlPlane,
        ObjectFetcher objectFetcher,
        int maxBatchesPerPartitionToFind,
        ExecutorService metadataExecutor,
        ExecutorService dataExecutor,
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
        this.fetchMetrics = new InklessFetchMetrics(time, cache);
        this.brokerTopicStats = brokerTopicStats;
    }

    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        final FetchParams params,
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos
    ) {
        final Instant startAt = TimeUtils.durationMeasurementNow(time);
        fetchMetrics.fetchStarted(fetchInfos.size());
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
        return batchCoordinates.thenApply(
                coordinates ->
                    new FetchPlanner(
                        time,
                        objectKeyCreator,
                        keyAlignmentStrategy,
                        cache,
                        objectFetcher,
                        dataExecutor,
                        coordinates,
                        fetchMetrics
                    ).get()
            )
            .thenCombineAsync(batchCoordinates, (fileExtents, coordinates) ->
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
                    // Check if the exception was caused by a fetch related exception and increment the relevant metric
                    if (throwable instanceof CompletionException) {
                        // Finding batches fails on the initial stage
                        if (throwable.getCause() instanceof FindBatchesException) {
                            fetchMetrics.findBatchesFailed();
                        } else if (throwable.getCause() instanceof FetchException) {
                            // but storage-related exceptions are wrapped twice as they happen within the fetch completer
                            final Throwable fetchException = throwable.getCause();
                            if (fetchException.getCause() instanceof FileFetchException) {
                                fetchMetrics.fileFetchFailed();
                            } else if (fetchException.getCause() instanceof CacheFetchException) {
                                fetchMetrics.cacheFetchFailed();
                            }
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

    @Override
    public void close() throws IOException {
        fetchMetrics.close();
    }
}
