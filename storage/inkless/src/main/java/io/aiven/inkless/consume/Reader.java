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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.metrics.ThreadPoolMonitor;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;

/**
 * Reader for fetching data from Inkless storage.
 *
 * <h2>Thread Pool Lifecycle</h2>
 * <p>This class manages thread pools for metadata fetching, data fetching, and optionally
 * for lagging consumer requests. All pools must be shut down via {@link #close()}.
 *
 * <p><b>Design Note:</b> Thread pools are created in the constructor arguments before delegation.
 * If construction fails after pool creation (e.g., invalid lagging consumer configuration),
 * the pools may leak. This is acceptable for broker startup components where failure prevents
 * broker startup and JVM exit cleans up resources.
 */
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
    private final boolean ownsLaggingExecutor;
    /**
     * Separate ObjectFetcher for lagging consumer requests to provide resource isolation.
     *
     * <p>Prevents lagging consumer bursts from exhausting hot path resources (connection pools,
     * HTTP/2 streams, rate limits). Cold path failures don't propagate to hot path.
     *
     * <p>Trade-off: Doubles connection pools (~500KB) but prevents hot path degradation.
     */
    private final ObjectFetcher laggingConsumerObjectFetcher;
    private final ScheduledExecutorService hedgeScheduler;
    private final long hedgeTtfbThresholdMs;
    private final long hedgeTotalTimeThresholdMs;
    // Per-key hedge dedup: ensures at most one hedge per primary future across all concurrent callers.
    // Keyed by CF identity (same instance = same cache-deduped primary). Entries are removed on completion.
    private final ConcurrentHashMap<CompletableFuture<?>, AtomicBoolean> hedgeGuards = new ConcurrentHashMap<>();
    private final boolean isConsolidationFetch;
    private final InklessFetchMetrics fetchMetrics;
    private final BrokerTopicStats brokerTopicStats;
    private final Bucket rateLimiter;
    private final Bucket byteRateLimiter;
    private final long byteRateCapacity;
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
        Optional<ObjectFetcher> maybeLaggingFetchStorage,
        long laggingConsumerThresholdMs,
        int laggingConsumerRequestRateLimit,
        long laggingConsumerByteRateLimit,
        int laggingConsumerThreadPoolSize,
        long hedgeTtfbThresholdMs,
        long hedgeTotalTimeThresholdMs,
        int maxBatchesPerPartitionToFind
    ) {
        this(time, objectKeyCreator, keyAlignmentStrategy, cache, controlPlane, objectFetcher, brokerTopicStats,
            fetchMetadataThreadPoolSize, fetchDataThreadPoolSize, maybeLaggingFetchStorage,
            laggingConsumerThresholdMs, laggingConsumerRequestRateLimit, laggingConsumerByteRateLimit, laggingConsumerThreadPoolSize,
            hedgeTtfbThresholdMs, hedgeTotalTimeThresholdMs,
            maxBatchesPerPartitionToFind,
            new KafkaMetricsGroup(InklessFetchMetrics.class.getPackageName(), InklessFetchMetrics.class.getSimpleName()),
            "inkless-");
    }

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
        Optional<ObjectFetcher> maybeLaggingFetchStorage,
        long laggingConsumerThresholdMs,
        int laggingConsumerRequestRateLimit,
        long laggingConsumerByteRateLimit,
        int laggingConsumerThreadPoolSize,
        long hedgeTtfbThresholdMs,
        long hedgeTotalTimeThresholdMs,
        int maxBatchesPerPartitionToFind,
        KafkaMetricsGroup metricsGroup,
        String threadNamePrefix
    ) {
        this(
            time,
            objectKeyCreator,
            keyAlignmentStrategy,
            cache,
            controlPlane,
            objectFetcher,
            brokerTopicStats,
            fetchMetadataThreadPoolSize,
            fetchDataThreadPoolSize,
            maybeLaggingFetchStorage,
            laggingConsumerThresholdMs,
            laggingConsumerRequestRateLimit,
            laggingConsumerByteRateLimit,
            laggingConsumerThreadPoolSize,
            hedgeTtfbThresholdMs,
            hedgeTotalTimeThresholdMs,
            maxBatchesPerPartitionToFind,
            metricsGroup,
            threadNamePrefix,
            false // consumer-fetch reader, not consolidating
        );
    }

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
        Optional<ObjectFetcher> maybeLaggingFetchStorage,
        long laggingConsumerThresholdMs,
        int laggingConsumerRequestRateLimit,
        long laggingConsumerByteRateLimit,
        int laggingConsumerThreadPoolSize,
        long hedgeTtfbThresholdMs,
        long hedgeTotalTimeThresholdMs,
        int maxBatchesPerPartitionToFind,
        KafkaMetricsGroup metricsGroup,
        String threadNamePrefix,
        boolean isConsolidationFetch
    ) {
        this(
            time,
            objectKeyCreator,
            keyAlignmentStrategy,
            cache,
            controlPlane,
            objectFetcher,
            maxBatchesPerPartitionToFind,
            Executors.newFixedThreadPool(fetchMetadataThreadPoolSize, new InklessThreadFactory(threadNamePrefix + "fetch-metadata-", false)),
            Executors.newFixedThreadPool(fetchDataThreadPoolSize, new InklessThreadFactory(threadNamePrefix + "fetch-data-", false)),
            // laggingConsumerObjectFetcher: the storage client the cold path reads through.
            // Pass it through whenever present, regardless of pool size — pool size only controls
            // whether a dedicated executor is created (owned by Reader) or the cold path reuses
            // fetchDataExecutor.
            // Reject pool size > 0 with no fetcher here, so the error names the missing storage config.
            // (Defensive: in production both are gated on the same config, so this only guards
            // direct/test construction. The private constructor rejects the same combination too,
            // but with a more generic executor-without-fetcher message.)
            laggingConsumerThreadPoolSize > 0
                ? maybeLaggingFetchStorage.orElseThrow(() -> new IllegalArgumentException(
                    "Lagging consumer thread pool size is " + laggingConsumerThreadPoolSize
                        + " but no lagging fetch storage was provided"))
                : maybeLaggingFetchStorage.orElse(null),
            laggingConsumerThresholdMs,
            laggingConsumerRequestRateLimit,
            laggingConsumerByteRateLimit,
            // Only create a dedicated lagging pool when pool size > 0.
            // When pool size is 0 and a fetcher is present, the cold path reuses fetchDataExecutor.
            laggingConsumerThreadPoolSize > 0
                ? createBoundedThreadPool(threadNamePrefix, laggingConsumerThreadPoolSize)
                : null,
            // Only create hedge scheduler when any hedging trigger is enabled (threshold > 0).
            // Single-threaded: only schedules timer tasks, actual fetches run on data executors.
            (hedgeTtfbThresholdMs > 0 || hedgeTotalTimeThresholdMs > 0)
                ? createHedgeScheduler(threadNamePrefix)
                : null,
            hedgeTtfbThresholdMs,
            hedgeTotalTimeThresholdMs,
            new InklessFetchMetrics(time, cache, metricsGroup),
            brokerTopicStats,
            threadNamePrefix,
            isConsolidationFetch
        );
    }

    private static ScheduledExecutorService createHedgeScheduler(String threadNamePrefix) {
        final var scheduler = new ScheduledThreadPoolExecutor(
            1, new InklessThreadFactory(threadNamePrefix + "hedge-scheduler-", true));
        // Remove cancelled tasks from the queue immediately to prevent accumulation
        // under high QPS with large hedge thresholds (most timer tasks are cancelled
        // when the primary completes before the threshold).
        scheduler.setRemoveOnCancelPolicy(true);
        // Note on shutdown: executeExistingDelayedTasksAfterShutdownPolicy defaults to true,
        // so already-scheduled timers may still fire after shutdown() is called. This is harmless:
        // tryFireHedge guards with hedgeFired CAS and catches RejectedExecutionException from
        // data executors that are shutting down concurrently. At worst, a timer fires, the CAS
        // succeeds, the data executor rejects the hedge submission, and it's silently discarded.
        return scheduler;
    }

    private static ExecutorService createBoundedThreadPool(String threadNamePrefix, int poolSize) {
        // Creates a bounded thread pool for lagging consumer fetch requests.
        // Fixed pool design: all threads persist for executor lifetime (never removed when idle).
        final int queueCapacity = poolSize * LAGGING_CONSUMER_QUEUE_MULTIPLIER;
        return new ThreadPoolExecutor(
            poolSize,                    // corePoolSize: fixed pool, always this many threads
            poolSize,                    // maximumPoolSize: no dynamic scaling (core == max)
            0L,                          // keepAliveTime: unused for fixed pools (core threads don't time out)
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),  // Bounded queue prevents OOM
            new InklessThreadFactory(threadNamePrefix + "fetch-lagging-", false),
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
        ScheduledExecutorService hedgeScheduler,
        long hedgeTtfbThresholdMs,
        long hedgeTotalTimeThresholdMs,
        InklessFetchMetrics fetchMetrics,
        BrokerTopicStats brokerTopicStats,
        String monitorPrefix
    ) {
        this(time, objectKeyCreator, keyAlignmentStrategy, cache, controlPlane, objectFetcher,
            maxBatchesPerPartitionToFind, metadataExecutor, fetchDataExecutor,
            laggingConsumerObjectFetcher, laggingConsumerThresholdMs, laggingConsumerRequestRateLimit, 0L,
            laggingFetchDataExecutor, hedgeScheduler, hedgeTtfbThresholdMs, hedgeTotalTimeThresholdMs,
            fetchMetrics, brokerTopicStats, monitorPrefix, false);
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
        long laggingConsumerByteRateLimit,
        ExecutorService laggingFetchDataExecutor,
        ScheduledExecutorService hedgeScheduler,
        long hedgeTtfbThresholdMs,
        long hedgeTotalTimeThresholdMs,
        InklessFetchMetrics fetchMetrics,
        BrokerTopicStats brokerTopicStats,
        String monitorPrefix,
        boolean isConsolidationFetch
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
        this.laggingConsumerThresholdMs = laggingConsumerThresholdMs;
        this.laggingConsumerObjectFetcher = laggingConsumerObjectFetcher;
        this.hedgeScheduler = hedgeScheduler;
        this.hedgeTtfbThresholdMs = hedgeTtfbThresholdMs;
        this.hedgeTotalTimeThresholdMs = hedgeTotalTimeThresholdMs;

        // Validate: a dedicated executor exists only to run cold-path fetches, so it is valid
        // only when paired with a fetcher to execute.
        if (laggingFetchDataExecutor != null && laggingConsumerObjectFetcher == null) {
            throw new IllegalArgumentException(
                "Dedicated lagging executor provided but no lagging ObjectFetcher — nothing to execute");
        }
        // Validate: consolidation mode requires a cold-path fetcher to read unconsolidated blobs
        // from remote storage on cache miss.
        if (isConsolidationFetch && laggingConsumerObjectFetcher == null) {
            throw new IllegalArgumentException(
                "isConsolidationFetch=true requires a lagging ObjectFetcher (backgroundStorage) — "
                    + "consolidation must be able to fetch blobs from remote storage on cache miss");
        }
        // Validate: the dedicated lagging executor must be a distinct instance from the hot-path
        // executor. Passing the same instance for both would cause double-shutdown and duplicate
        // thread-pool monitoring.
        if (laggingFetchDataExecutor != null && laggingFetchDataExecutor == fetchDataExecutor) {
            throw new IllegalArgumentException(
                "laggingFetchDataExecutor must be a distinct instance from fetchDataExecutor — "
                    + "passing the same pool would cause double-shutdown and duplicate monitoring");
        }

        // Resolve effective executor: if a dedicated pool is provided, use it and own it.
        // If only a fetcher is provided (no dedicated pool), reuse fetchDataExecutor (cold path
        // enabled but no separate pool — used by consolidation to avoid extra threads).
        this.ownsLaggingExecutor = laggingFetchDataExecutor != null;
        this.laggingFetchDataExecutor = laggingFetchDataExecutor != null
            ? laggingFetchDataExecutor
            : (laggingConsumerObjectFetcher != null ? fetchDataExecutor : null);

        // Initialize rate limiter only if cold path is enabled (resolved executor exists) and rate limit > 0
        // This avoids creating unused objects when the feature is disabled
        if (this.laggingFetchDataExecutor != null && laggingConsumerRequestRateLimit > 0) {
            // Rate limiter configuration:
            // - capacity = rateLimit: Allows initial burst up to full rate limit (e.g., 200 tokens for 200 req/s)
            // - refillGreedy: Refills at rateLimit tokens per second
            // Note: While the rate limiter allows bursts up to capacity, the effective burst is limited by
            // the thread pool size (e.g., 16 threads). Even if 200 tokens are available, only 16 requests
            // can execute concurrently. The rate limiter controls request rate over time, while the thread
            // pool limits concurrent execution, providing both rate limiting and concurrency control.
            final Bandwidth limit = Bandwidth.builder()
                .capacity(laggingConsumerRequestRateLimit)
                .refillGreedy(laggingConsumerRequestRateLimit, Duration.ofSeconds(1))
                .build();
            this.rateLimiter = Bucket.builder()
                .addLimit(limit)
                .build();
        } else {
            this.rateLimiter = null;
        }

        // Byte-rate limiter: bounds cold-path storage-to-broker bandwidth per node. Independent of the
        // request-rate limiter above (that protects storage GET cost; this protects network bandwidth).
        // capacity = byteRateLimit: a 1-second burst, consistent with the request limiter. A single cold
        // fetch covers one storage object and is indivisible; if it exceeds capacity it can never be fully
        // satisfied, so FetchPlanner charges a full bucket and lets it through (see applyRateLimit) rather
        // than deadlocking. That only occurs when the limit is set below the produced object size.
        if (this.laggingFetchDataExecutor != null && laggingConsumerByteRateLimit > 0) {
            final Bandwidth byteLimit = Bandwidth.builder()
                .capacity(laggingConsumerByteRateLimit)
                .refillGreedy(laggingConsumerByteRateLimit, Duration.ofSeconds(1))
                .build();
            this.byteRateLimiter = Bucket.builder()
                .addLimit(byteLimit)
                .build();
            this.byteRateCapacity = laggingConsumerByteRateLimit;
        } else {
            this.byteRateLimiter = null;
            this.byteRateCapacity = 0L;
        }

        this.isConsolidationFetch = isConsolidationFetch;
        this.fetchMetrics = fetchMetrics;
        this.brokerTopicStats = brokerTopicStats;
        try {
            // Initialize all monitors first, then assign to fields to ensure all-or-nothing semantics.
            // If any monitor creation fails, none are assigned, preventing inconsistent monitoring state.
            final ThreadPoolMonitor metadataMonitor = new ThreadPoolMonitor(monitorPrefix + "fetch-metadata", metadataExecutor);
            final ThreadPoolMonitor dataMonitor = new ThreadPoolMonitor(monitorPrefix + "fetch-data", fetchDataExecutor);
            // Only create monitor if we own a dedicated lagging executor (avoids duplicate monitoring
            // when laggingFetchDataExecutor is resolved to fetchDataExecutor).
            final ThreadPoolMonitor laggingMonitor = ownsLaggingExecutor
                ? new ThreadPoolMonitor(monitorPrefix + "fetch-lagging", this.laggingFetchDataExecutor)
                : null;
            // All monitors created successfully, assign to fields
            this.metadataThreadPoolMonitor = metadataMonitor;
            this.dataThreadPoolMonitor = dataMonitor;
            this.laggingConsumerThreadPoolMonitor = laggingMonitor;
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
                    byteRateLimiter,
                    byteRateCapacity,
                    laggingFetchDataExecutor,
                    hedgeScheduler,
                    hedgeTtfbThresholdMs,
                    hedgeTotalTimeThresholdMs,
                    hedgeGuards,
                    coordinates,
                    isConsolidationFetch,
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
                    fetchMetrics::fetchCompletionFinished,
                    fetchMetrics
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
     * <p>Both Success and Failure results include object key and range information to enable
     * proper grouping and ordering when processing results.
     *
     * @param requestsWithFutures the list of fetch requests paired with their futures
     * @return a future that completes with a list of file extent results in the same order as the input
     */
    static CompletableFuture<List<FileExtentResult>> allOfFileExtents(
        List<FetchPlanner.FetchRequestWithFuture> requestsWithFutures
    ) {
        // Handle each future individually to support partial failures.
        // Convert exceptions to Failure results so successful partitions still get their data.
        final List<CompletableFuture<FileExtentResult>> handledFutures = requestsWithFutures.stream()
            .<CompletableFuture<FileExtentResult>>map(requestWithFuture -> {
                final FetchPlanner.ObjectFetchRequest request = requestWithFuture.request();
                final CompletableFuture<FileExtent> future = requestWithFuture.future();
                return future.handle((extent, throwable) -> {
                    if (throwable != null) {
                        // Restore interrupt status if the exception is InterruptedException.
                        // This callback may execute on various threads (executor threads, completing thread, etc.),
                        // but restoring interrupt status is safe and correct: it only sets a flag and doesn't stop execution.
                        // The interrupt flag is informational and allows code that checks Thread.interrupted() to see it.
                        if (throwable instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        // Log at debug level - metrics are recorded in FetchPlanner
                        LOGGER.debug("File extent fetch failed, returning failure result", throwable);
                        // Extract object key and range from request for Failure result
                        return new FileExtentResult.Failure(request.objectKey(), request.byteRange(), throwable);
                    } else {
                        // Extract object key and range from FileExtent for Success result
                        // FileExtent.object() returns String, we need to create ObjectKey from it
                        // For Success, we can use the request's objectKey since it matches
                        final ByteRange byteRange = new ByteRange(extent.range().offset(), extent.range().length());
                        return new FileExtentResult.Success(request.objectKey(), byteRange, extent);
                    }
                });
            })
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
        // Shut down hedge scheduler first to prevent it from submitting new tasks to executors being shut down
        ThreadUtils.shutdownExecutorServiceQuietly(hedgeScheduler, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(metadataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(fetchDataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        // Only shut down lagging executor if we own it (dedicated pool). When the cold path reuses
        // fetchDataExecutor, it was already shut down above.
        if (ownsLaggingExecutor) {
            ThreadUtils.shutdownExecutorServiceQuietly(laggingFetchDataExecutor, EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        if (metadataThreadPoolMonitor != null) metadataThreadPoolMonitor.close();
        if (dataThreadPoolMonitor != null) dataThreadPoolMonitor.close();
        if (laggingConsumerThreadPoolMonitor != null) laggingConsumerThreadPoolMonitor.close();
        // SharedState owns the storage backend lifecycle; only close thread pools and metrics here.
        fetchMetrics.close();
    }
}
