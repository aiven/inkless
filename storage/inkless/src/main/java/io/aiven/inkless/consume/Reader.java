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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;

public class Reader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reader.class);
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;

    /**
     * Queue capacity multiplier for lagging consumer executor.
     *
     * <p>The queue capacity is automatically calculated as {@code thread.pool.size * LAGGING_CONSUMER_QUEUE_MULTIPLIER}.
     * This provides burst buffering while preventing unbounded growth. When full, AbortPolicy
     * rejects tasks and exceptions propagate to Kafka's error handler.
     *
     * <p>Example: With default 16 threads and multiplier of 100, capacity = 1600 tasks.
     * At 200 req/s rate limit, this provides ~8 seconds of buffer before rejections occur.
     * Note: These values are derived from defaults and may differ if configuration is changed.
     */
    private static final int LAGGING_CONSUMER_QUEUE_MULTIPLIER = 100;
    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ObjectCache cache;
    private final ControlPlane controlPlane;
    private final ObjectFetcher objectFetcher;
    private final int maxBatchesPerPartitionToFind;
    private final ExecutorService metadataExecutor;
    private final ExecutorService fetchDataExecutor;
    private final long laggingConsumerThresholdMs;
    private final ExecutorService laggingFetchDataExecutor;
    /**
     * Separate ObjectFetcher for lagging consumer requests to provide resource isolation.
     *
     * <p>Prevents lagging consumer bursts from exhausting hot path resources (connection pools,
     * HTTP/2 streams, rate limits). Cold path failures don't propagate to hot path.
     *
     * <p>Trade-off: Doubles connection pools (~500KB) but prevents hot path degradation.
     */
    private final ObjectFetcher laggingConsumerObjectFetcher;
    private final InklessFetchMetrics fetchMetrics;
    private final BrokerTopicStats brokerTopicStats;
    private final Bucket rateLimiter;
    private ThreadPoolMonitor metadataThreadPoolMonitor;
    private ThreadPoolMonitor dataThreadPoolMonitor;
    private ThreadPoolMonitor laggingConsumerThreadPoolMonitor;

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
        ObjectFetcher laggingObjectFetcher,
        long laggingConsumerThresholdMs,
        int laggingConsumerRequestRateLimit,
        int laggingConsumerThreadPoolSize,
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
            laggingObjectFetcher,
            laggingConsumerThresholdMs,
            laggingConsumerRequestRateLimit,
            // Only create lagging consumer executor if feature enabled (pool size > 0)
            laggingConsumerThreadPoolSize > 0
                ? createBoundedThreadPool(laggingConsumerThreadPoolSize)
                : null,
            new InklessFetchMetrics(time, cache),
            brokerTopicStats
        );
    }

    private static ExecutorService createBoundedThreadPool(int poolSize) {
        // Creates a bounded thread pool for lagging consumer fetch requests.
        // Fixed pool design: all threads persist for executor lifetime (never removed when idle).
        final int queueCapacity = poolSize * LAGGING_CONSUMER_QUEUE_MULTIPLIER;
        return new ThreadPoolExecutor(
            poolSize,                    // corePoolSize: fixed pool, always this many threads
            poolSize,                    // maximumPoolSize: no dynamic scaling (core == max)
            0L,                          // keepAliveTime: threads never removed (fixed pool design)
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),  // Bounded queue prevents OOM
            new InklessThreadFactory("inkless-fetch-lagging-consumer-", false),
            // Why AbortPolicy: CallerRunsPolicy would block request handler threads causing broker-wide degradation
            new ThreadPoolExecutor.AbortPolicy()      // Reject when full, don't block callers
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
        ExecutorService fetchDataExecutor,
        ObjectFetcher laggingConsumerObjectFetcher,
        long laggingConsumerThresholdMs,
        int laggingConsumerRequestRateLimit,
        ExecutorService laggingFetchDataExecutor,
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
        this.fetchDataExecutor = fetchDataExecutor;
        this.laggingFetchDataExecutor = laggingFetchDataExecutor;
        this.laggingConsumerThresholdMs = laggingConsumerThresholdMs;
        this.laggingConsumerObjectFetcher = laggingConsumerObjectFetcher;

        // Initialize rate limiter once for reuse across all fetch requests
        if (laggingConsumerRequestRateLimit > 0) {
            final Bandwidth limit = Bandwidth.builder()
                .capacity(laggingConsumerRequestRateLimit)
                .refillGreedy(laggingConsumerRequestRateLimit, java.time.Duration.ofSeconds(1))
                .build();
            this.rateLimiter = Bucket.builder()
                .addLimit(limit)
                .build();
        } else {
            this.rateLimiter = null;
        }

        this.fetchMetrics = fetchMetrics;
        this.brokerTopicStats = brokerTopicStats;
        try {
            this.metadataThreadPoolMonitor = new ThreadPoolMonitor("inkless-fetch-metadata", metadataExecutor);
            this.dataThreadPoolMonitor = new ThreadPoolMonitor("inkless-fetch-data", fetchDataExecutor);
            // Only create monitor if lagging consumer executor exists (feature enabled)
            this.laggingConsumerThreadPoolMonitor = laggingFetchDataExecutor != null
                ? new ThreadPoolMonitor("inkless-fetch-lagging-consumer", laggingFetchDataExecutor)
                : null;
        } catch (final Exception e) {
            // only expected to happen on tests passing other types of pools
            LOGGER.warn("Failed to create thread pool monitors", e);
        }
    }

    /**
     * Asynchronously fetches data for multiple partitions using a staged pipeline:
     *
     * <p>1. Finds batches (metadataExecutor);
     * <p>2. Plans and submits fetches (completing thread);
     * <p>3. Fetches data (dataExecutor or laggingFetchDataExecutor);
     * <p>4. Assembles final response (completing thread)
     *
     * <p>Stages 2 and 4 use non-async methods (thenApply, thenCombine), so they run on
     * whichever thread completes the previous stage. This is acceptable because these
     * are lightweight CPU-bound tasks that complete quickly (1-5ms), and the I/O operations
     * are already fully non-blocking (handled by dataExecutor).
     *
     * <p><b>Note on metadata executor sharing:</b> The metadataExecutor (stage 1) is shared
     * between hot and cold path requests. The hot/cold path separation only applies to data
     * fetching (stage 3), which occurs after metadata is retrieved. A burst of lagging consumer
     * requests can still compete with recent consumer requests at the metadata layer. Consider
     * increasing {@code fetch.metadata.thread.pool.size} if metadata fetching becomes a
     * bottleneck in mixed hot/cold workloads.
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

        // Plan & Submit (completing thread): Create fetch plan and submit to cache (non-blocking)
        // Runs on the thread that completed batchCoordinates (metadataExecutor thread)
        return batchCoordinates.thenApply(coordinates -> {
                // FetchPlanner creates a plan and submits requests to cache
                // Recent vs lagging consumer path separation happens here based on data age
                // Rate limiter is reused across all requests for consistent rate limiting
                return new FetchPlanner(
                    time,
                    objectKeyCreator,
                    keyAlignmentStrategy,
                    cache,
                    objectFetcher,
                    fetchDataExecutor,
                    laggingConsumerObjectFetcher,
                    laggingConsumerThresholdMs,
                    rateLimiter,
                    laggingFetchDataExecutor,
                    coordinates,
                    fetchMetrics
                ).get();
            })
            // Fetch Data (dataExecutor): Flatten list of futures into single future with all results
            // Actual remote fetches happen on dataExecutor only when cache misses
            .thenCompose(Reader::allOfFileExtents)
            // Complete Fetch (completing thread): Combine fetched data with batch coordinates to build final response
            // Runs on whichever thread completes last (typically dataExecutor thread)
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
     * <p><b>Partial Failure Handling:</b>
     * If some futures fail (e.g., lagging consumer rejection) while others succeed (hot path),
     * this method converts failed fetches to Failure results instead of failing the entire request.
     * This allows successful partitions to return data while failed partitions are marked as failures,
     * enabling consumers to retry only the failed partitions.
     *
     * @param fileExtentFutures the list of futures to wait for
     * @return a future that completes with a list of file extent results in the same order as the input
     */
    static CompletableFuture<List<FileExtentResult>> allOfFileExtents(
        List<CompletableFuture<FileExtent>> fileExtentFutures
    ) {
        // Handle each future individually to support partial failures.
        // Convert exceptions to Failure results so successful partitions still get their data.
        final List<CompletableFuture<FileExtentResult>> handledFutures = fileExtentFutures.stream()
            .map(future -> future
                .handle((extent, throwable) -> {
                    if (throwable != null) {
                        // Log at debug level - metrics are recorded in FetchPlanner
                        LOGGER.debug("File extent fetch failed, returning failure result", throwable);
                        return new FileExtentResult.Failure(throwable);
                    } else {
                        return (FileExtentResult) new FileExtentResult.Success(extent);
                    }
                }))
            .toList();

        final CompletableFuture<?>[] futuresArray = handledFutures.toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futuresArray)
            // The thenApply callback runs on whichever thread completes the last file
            // extent future (typically a dataExecutor thread or metadataExecutor when data is cached).
            // The join() calls are safe because all futures are already completed when this callback executes.
            .thenApply(v ->
                handledFutures.stream()
                    .map(CompletableFuture::join)
                    .toList());
    }

    @Override
    public void close() throws IOException {
        ThreadUtils.shutdownExecutorServiceQuietly(metadataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(fetchDataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(laggingFetchDataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (metadataThreadPoolMonitor != null) metadataThreadPoolMonitor.close();
        if (dataThreadPoolMonitor != null) dataThreadPoolMonitor.close();
        if (laggingConsumerThreadPoolMonitor != null) laggingConsumerThreadPoolMonitor.close();
        objectFetcher.close();
        if (laggingConsumerObjectFetcher != null) laggingConsumerObjectFetcher.close();
        fetchMetrics.close();
    }
}
