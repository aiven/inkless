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
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.github.bucket4j.Bucket;

/**
 * Plans and executes fetch operations for diskless topics.
 *
 * <p>Execution flow:
 * <ol>
 *   <li><b>Planning:</b> Converts batch coordinates into fetch requests (merges, aligns ranges)</li>
 *   <li><b>Submission:</b> Submits requests to cache with async execution</li>
 *   <li><b>Execution:</b> Cache misses trigger remote storage fetch on executor</li>
 * </ol>
 *
 * If a fetch request hits in the cache, the corresponding future is already completed with the cached value.
 *
 * <p>This class uses async cache operations to avoid blocking threads. The cache returns
 * CompletableFuture immediately, and actual fetches run on the provided executor.
 */
public class FetchPlanner implements Supplier<List<CompletableFuture<FileExtent>>> {

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignment;
    private final ObjectCache cache;
    private final ObjectFetcher objectFetcher;
    // Executor for fetching data from remote storage.
    // Will only be used for recent data (hot path) if lagging consumer support feature is enabled.
    private final ExecutorService fetchDataExecutor;
    private final ExecutorService laggingFetchDataExecutor;
    private final ObjectFetcher laggingObjectFetcher;
    private final long laggingConsumerThresholdMs;
    private final Bucket laggingRateLimiter;
    private final Map<TopicIdPartition, FindBatchResponse> batchCoordinates;
    private final InklessFetchMetrics metrics;

    public FetchPlanner(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignment,
        ObjectCache cache,
        ObjectFetcher objectFetcher,
        ExecutorService fetchDataExecutor,
        ObjectFetcher laggingObjectFetcher,
        long laggingConsumerThresholdMs,
        Bucket laggingRateLimiter,
        ExecutorService laggingFetchDataExecutor,
        Map<TopicIdPartition, FindBatchResponse> batchCoordinates,
        InklessFetchMetrics metrics
    ) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.keyAlignment = keyAlignment;
        this.cache = cache;
        this.objectFetcher = objectFetcher;
        this.fetchDataExecutor = fetchDataExecutor;
        this.laggingObjectFetcher = laggingObjectFetcher;
        this.laggingFetchDataExecutor = laggingFetchDataExecutor;
        this.laggingConsumerThresholdMs = laggingConsumerThresholdMs;
        this.laggingRateLimiter = laggingRateLimiter;
        this.batchCoordinates = batchCoordinates;
        this.metrics = metrics;
    }

    /**
     * Executes the fetch plan: plans requests, submits to cache, returns futures.
     *
     * @return list of futures that will complete with fetched file extents
     */
    @Override
    public List<CompletableFuture<FileExtent>> get() {
        return TimeUtils.measureDurationMsSupplier(
            time,
            () -> {
                final List<ObjectFetchRequest> requests = planJobs(batchCoordinates);
                return submitAllRequests(requests);
            },
            metrics::fetchPlanFinished
        );
    }

    /**
     * Plans fetch jobs from batch coordinates.
     *
     * @param batchCoordinates map of partition to batch information
     * @return list of planned fetch requests
     */
    List<ObjectFetchRequest> planJobs(final Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        // Group batches by object key and collect full BatchInfo for metadata aggregation
        final Set<Map.Entry<String, List<BatchInfo>>> objectKeysToBatches =
            batchCoordinates.values().stream()
                // Filter out responses with errors
                .filter(findBatch -> findBatch.errors() == Errors.NONE)
                .map(FindBatchResponse::batches)
                .peek(batches -> metrics.recordFetchBatchSize(batches.size()))
                .flatMap(List::stream)
                .collect(Collectors.groupingBy(
                    BatchInfo::objectKey,
                    Collectors.toList()
                ))
                .entrySet();

        metrics.recordFetchObjectsSize(objectKeysToBatches.size());

        // Create fetch requests with aligned byte ranges and aggregated metadata
        return objectKeysToBatches.stream()
            .flatMap(e -> createFetchRequests(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    }

    /**
     * Creates fetch requests for a single object with multiple batches.
     * Aligns byte ranges and aggregates metadata (timestamp for hot/cold path decision).
     */
    private Stream<ObjectFetchRequest> createFetchRequests(
        final String objectKey,
        final List<BatchInfo> batches
    ) {
        if (batches.isEmpty()) {
            return Stream.empty(); // Defensive: shouldn't happen, but handle gracefully
        }

        // Use max timestamp for hot/cold path decision.
        // Objects contain multi-partition data, so if ANY batch is recent, use hot path (cache + priority).
        final long timestamp = batches.stream()
            .mapToLong(b -> b.metadata().batchMaxTimestamp())
            .max()
            .orElse(0L);

        // Extract byte ranges
        final List<ByteRange> byteRanges = batches.stream()
            .map(b -> b.metadata().range())
            .collect(Collectors.toList());

        // Align byte ranges for efficient fetching
        final Set<ByteRange> alignedRanges = keyAlignment.align(byteRanges);

        // Create a fetch request for each aligned range with aggregated metadata
        return alignedRanges.stream()
            .map(byteRange -> new ObjectFetchRequest(
                objectKeyCreator.from(objectKey),
                byteRange,
                timestamp
            ));
    }

    private List<CompletableFuture<FileExtent>> submitAllRequests(final List<ObjectFetchRequest> requests) {
        return requests.stream()
            .map(this::submitSingleRequest)
            .collect(Collectors.toList());
    }

    private CompletableFuture<FileExtent> submitSingleRequest(final ObjectFetchRequest request) {
        final long currentTime = time.milliseconds();
        final long dataAge = currentTime - request.timestamp();

        // If laggingConsumerExecutor is null, treat all requests as recent (feature disabled)
        final boolean laggingFeatureEnabled = laggingFetchDataExecutor != null;
        final boolean isLagging = laggingFeatureEnabled && (dataAge > laggingConsumerThresholdMs);

        if (!isLagging) {
            // Hot path: up-to-date consumers use cache + recentDataExecutor
            metrics.recordRecentDataRequest();
            return cache.computeIfAbsent(
                request.toCacheKey(),
                k -> fetchFileExtent(objectFetcher, request),
                fetchDataExecutor
            );
        } else {
            // Cold path: lagging consumers bypass cache, use dedicated executor with rate limiting.
            // Cache bypass rationale: Objects are multi-partition blobs, caching them would evict hot data
            // and provide little benefit to the lagging consumer. Backpressure via AbortPolicy: queue full
            // → RejectedExecutionException → Kafka error handler → consumer backs off (fetch purgatory).
            metrics.recordLaggingConsumerRequest();
            try {
                return CompletableFuture.supplyAsync(() -> {
                        // Apply rate limiting if configured (rate limit > 0)
                        if (laggingRateLimiter != null) {
                            applyRateLimit(); // InterruptedException here is wrapped in FetchException
                        }
                        return fetchFileExtent(laggingObjectFetcher, request);
                    },
                    laggingFetchDataExecutor
                ).whenComplete((result, throwable) -> {
                    // Track async rejections that occur during task execution:
                    // - RejectedExecutionException: rare async rejection (race condition when executor
                    //   shuts down between submission and execution)
                    // - InterruptedException: interruption during fetchFileExtent execution (e.g., executor
                    //   shutdown). Note: rate limit interruptions are wrapped in FetchException and are
                    //   NOT tracked as rejections (they're fetch operation failures, not capacity issues).
                    if (throwable != null) {
                        final Throwable cause = throwable.getCause();
                        if (cause instanceof RejectedExecutionException
                            || cause instanceof InterruptedException) {
                            metrics.recordLaggingConsumerRejection();
                        }
                        // Note: FetchException (from rate limit interruption) is NOT tracked here as
                        // rejection - it's a fetch operation failure, not a backpressure/capacity issue.
                    }
                });
            } catch (final RejectedExecutionException e) {
                // Sync rejection (queue full at submission) - return failed future instead of propagating exception.
                // This allows allOfFileExtents to handle the failure gracefully, returning empty FileExtent
                // for this partition while other partitions (hot path) can still succeed.
                // Metrics recorded here since the task never executes (vs async rejection tracked in whenComplete).
                metrics.recordLaggingConsumerRejection();
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    // Applies request-based rate limiting by blocking executor thread until token available.
    // Always records wait time (including zero-wait) for accurate latency histogram.
    // Note: If interrupted, the duration is still recorded before the exception is thrown.
    private void applyRateLimit() {
            TimeUtils.measureDurationMs(time, () -> {
                try {
                    laggingRateLimiter.asBlocking().consume(1);
                } catch (final InterruptedException e) {
                    // Rate limit wait was interrupted (typically during shutdown).
                    // Preserve interrupt status for executor framework, but wrap in FetchException
                    // to indicate this is a fetch failure, not a rejection (executor capacity issue).
                    //
                    // Note: This is distinct from executor shutdown interruption caught in whenComplete,
                    // which occurs during fetchFileExtent and IS tracked as rejection. Rate limit
                    // interruption is a fetch operation failure, not a capacity/backpressure issue.
                    //
                    // The duration is still recorded by measureDurationMs even when an exception is thrown.
                    Thread.currentThread().interrupt();
                    throw new FetchException("Rate limit wait interrupted for lagging consumer", e);
                }
            }, metrics::recordRateLimitWaitTime);
    }

    /**
     * Fetches a file extent from remote storage.
     *
     * @param request the fetch request with object key and byte range
     * @return the fetched file extent
     * @throws FileFetchException if remote fetch fails
     */
    private FileExtent fetchFileExtent(final ObjectFetcher fetcher, final ObjectFetchRequest request) {
        try {
            final FileFetchJob job = new FileFetchJob(
                time,
                fetcher,
                request.objectKey(),
                request.byteRange(),
                metrics::fetchFileFinished
            );
            final FileExtent fileExtent = job.call();

            // Record cache entry size for monitoring
            metrics.cacheEntrySize(fileExtent.data().length);

            return fileExtent;
        } catch (final Exception e) {
            throw new FileFetchException(e);
        }
    }

    /**
     * Represents a request to fetch objects from diskless storage.
     * Contains all information needed to fetch and cache a file extent.
     *
     * @param objectKey the storage object key
     * @param byteRange the range of bytes to fetch
     * @param timestamp the maximum timestamp from batches (for hot/cold path decision).
     *                  Using max instead of min because if ANY batch in the object is recent,
     *                  we treat the entire fetch as hot path to prioritize recent data access.
     */
    record ObjectFetchRequest(
        ObjectKey objectKey,
        ByteRange byteRange,
        long timestamp
    ) {
        /**
         * Converts to cache key for deduplication.
         *
         * <p>Note: timestamp is intentionally excluded from cache key.
         * Timestamp determines hot/cold path routing but doesn't affect cache identity -
         * the same object+range from different time periods contains identical data
         * and should hit the same cache entry.
         */
        CacheKey toCacheKey() {
            return new CacheKey()
                .setObject(objectKey.value())
                .setRange(new CacheKey.ByteRange()
                    .setOffset(byteRange.offset())
                    .setLength(byteRange.size()));
        }
    }
}
