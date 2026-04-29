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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.BoundingRangeAlignment;
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
public class FetchPlanner implements Supplier<List<FetchPlanner.FetchRequestWithFuture>> {

    private static final KeyAlignmentStrategy COLD_PATH_ALIGNMENT = new BoundingRangeAlignment();

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
    private final ScheduledExecutorService hedgeScheduler;
    private final long hedgeTtfbThresholdMs;
    private final long hedgeTotalTimeThresholdMs;
    private final ConcurrentHashMap<CompletableFuture<?>, AtomicBoolean> hedgeGuards;
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
        ScheduledExecutorService hedgeScheduler,
        long hedgeTtfbThresholdMs,
        long hedgeTotalTimeThresholdMs,
        ConcurrentHashMap<CompletableFuture<?>, AtomicBoolean> hedgeGuards,
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
        this.hedgeScheduler = hedgeScheduler;
        this.hedgeTtfbThresholdMs = hedgeTtfbThresholdMs;
        this.hedgeTotalTimeThresholdMs = hedgeTotalTimeThresholdMs;
        this.hedgeGuards = hedgeGuards;
        this.batchCoordinates = batchCoordinates;
        this.metrics = metrics;
    }

    /**
     * Executes the fetch plan: plans requests, submits to cache, returns futures.
     *
     * @return list of fetch requests paired with their futures
     */
    @Override
    public List<FetchRequestWithFuture> get() {
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
     * <p>This method filters out responses with errors and groups batches by object key.
     * If all responses have errors or batchCoordinates is empty, this returns an empty list,
     * which is handled gracefully by the caller (no fetch requests are submitted).
     *
     * @param batchCoordinates map of partition to batch information
     * @return list of planned fetch requests (empty if no valid batches found)
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
     * Aggregates metadata and selects range strategy based on data recency:
     * - Hot path (recent data): aligns ranges for cache efficiency
     * - Cold path (lagging consumers): single bounding range to minimize HTTP requests
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
        // Since we check for empty batches above, max() is guaranteed to have a value here.
        final long timestamp = batches.stream()
            .mapToLong(b -> b.metadata().batchMaxTimestamp())
            .max()
            .getAsLong();

        // Extract byte ranges
        final List<ByteRange> byteRanges = batches.stream()
            .map(b -> b.metadata().range())
            .collect(Collectors.toList());

        // Compute the lagging decision once during planning so range strategy and execution path stay consistent.
        final boolean lagging = isLagging(timestamp);

        // Select range strategy based on data recency:
        // - Hot path: align to fixed blocks for cache hit rate
        // - Cold path: single bounding range to minimize HTTP requests (cache is bypassed anyway)
        final Set<ByteRange> fetchRanges = lagging
            ? COLD_PATH_ALIGNMENT.align(byteRanges)
            : keyAlignment.align(byteRanges);

        // Create a fetch request for each range with aggregated metadata
        return fetchRanges.stream()
            .map(byteRange -> new ObjectFetchRequest(
                objectKeyCreator.from(objectKey),
                byteRange,
                timestamp,
                lagging
            ));
    }

    /**
     * Determines if data with the given timestamp should use the cold (lagging) path.
     * This decision is computed once during planning and carried through via {@link ObjectFetchRequest#lagging()}
     * to ensure range strategy (aligned vs bounding) and execution path (cache vs bypass) stay consistent.
     */
    private boolean isLagging(final long timestamp) {
        final boolean laggingFeatureEnabled = laggingFetchDataExecutor != null && laggingObjectFetcher != null;
        if (!laggingFeatureEnabled) {
            return false;
        }
        final long currentTime = time.milliseconds();
        final long dataAge = Math.max(0, currentTime - timestamp);
        return dataAge > laggingConsumerThresholdMs;
    }

    private List<FetchRequestWithFuture> submitAllRequests(final List<ObjectFetchRequest> requests) {
        return requests.stream()
            .map(request -> new FetchRequestWithFuture(request, submitSingleRequest(request)))
            .collect(Collectors.toList());
    }

    private CompletableFuture<FileExtent> submitSingleRequest(final ObjectFetchRequest request) {
        if (!request.lagging()) {
            // Hot path: up-to-date consumers use cache + recentDataExecutor
            metrics.recordRecentDataRequest();
            // Per-caller TTFB signal: set by the load function's TTFB callback when first byte arrives.
            // On a cache dedup (computeIfAbsent returns an existing in-flight future), the load function
            // doesn't run, so this flag stays false — but with per-key hedge dedup (hedgeGuards),
            // at most one hedge fires per primary regardless of how many callers have stale TTFB flags.
            final AtomicBoolean firstByteReceived = new AtomicBoolean(false);
            final CompletableFuture<FileExtent> primary = cache.computeIfAbsent(
                request.toCacheKey(),
                k -> fetchFileExtent(objectFetcher, request, firstByteReceived),
                fetchDataExecutor
            );
            return withHedge(primary, objectFetcher, request, fetchDataExecutor, firstByteReceived);
        } else {
            // Cold path: lagging consumers bypass cache, use dedicated executor with rate limiting.
            // Cache bypass rationale: Objects are multi-partition blobs, caching them would evict hot data
            // and provide little benefit to the lagging consumer. Backpressure via AbortPolicy: queue full
            // → RejectedExecutionException → Kafka error handler → consumer backs off (fetch purgatory).
            metrics.recordLaggingConsumerRequest();
            try {
                final AtomicBoolean firstByteReceived = new AtomicBoolean(false);
                final CompletableFuture<FileExtent> primary = CompletableFuture.supplyAsync(() -> {
                        // Apply rate limiting if configured (rate limit > 0)
                        if (laggingRateLimiter != null) {
                            applyRateLimit(); // InterruptedException here is wrapped in FetchException
                        }
                        return fetchFileExtent(laggingObjectFetcher, request, firstByteReceived);
                    },
                    laggingFetchDataExecutor
                ).whenComplete((result, throwable) -> {
                    // Track async rejections that occur during task execution:
                    // - RejectedExecutionException: rejection when executor is shut down or queue is full.
                    //   This may surface either as the throwable itself or as its cause (e.g., via CompletionException).
                    // Note: InterruptedException and FetchException are treated as fetch operation failures,
                    // not as backpressure/capacity rejections.
                    if (throwable != null) {
                        final Throwable cause = throwable.getCause();
                        final boolean isRejected =
                            throwable instanceof RejectedExecutionException
                                || cause instanceof RejectedExecutionException;
                        if (isRejected) {
                            metrics.recordLaggingConsumerRejection();
                        }
                    }
                });
                return withHedge(primary, laggingObjectFetcher, request, laggingFetchDataExecutor, firstByteReceived);
            } catch (final RejectedExecutionException e) {
                // Sync rejection (executor shut down or queue full at submission) - return failed future
                // instead of propagating exception. This allows allOfFileExtents to handle the failure
                // gracefully, returning empty FileExtent for this partition while other partitions
                // (hot path) can still succeed.
                // Metrics recorded here since the task never executes (vs async rejection tracked in whenComplete).
                metrics.recordLaggingConsumerRejection();
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    // Wraps a primary future with hedging: schedules timer(s) that fire a competing fetch if the
    // primary is too slow. Two triggers: TTFB (stuck connection) and total-time (slow transfer).
    // Timers run on the hedge scheduler; actual fetches run on the data executor.
    //
    // Race resolution: hedge calls primary.complete(value) — CF.complete() is a CAS, first caller wins.
    // This makes hedging transparent to the cache (cache holds a reference to primary).
    //
    // Per-key dedup: hedgeFired is shared across all callers of the same primary (via hedgeGuards map).
    // On cache dedup, N concurrent callers share the same primary and the same guard — at most one
    // hedge fires per primary, preventing hedge storms under high fan-out. The guard is removed
    // when the primary completes.
    //
    // Early exits: returns primary as-is when disabled (null scheduler) or already complete (cache hit).
    // Fast failures propagate immediately — hedging mitigates slow responses, not errors.
    private CompletableFuture<FileExtent> withHedge(
        final CompletableFuture<FileExtent> primary,
        final ObjectFetcher fetcher,
        final ObjectFetchRequest request,
        final ExecutorService executor,
        final AtomicBoolean firstByteReceived
    ) {
        if (hedgeScheduler == null || primary.isDone()) {
            return primary;
        }

        // Per-key dedup: all callers sharing the same primary (via cache dedup) share one guard.
        // At most one hedge fires per primary, regardless of concurrent caller count.
        final AtomicBoolean hedgeFired = hedgeGuards.computeIfAbsent(primary, k -> {
            k.whenComplete((v, e) -> hedgeGuards.remove(k));
            return new AtomicBoolean(false);
        });
        final List<ScheduledFuture<?>> timers = new ArrayList<>(2);

        try {
            // TTFB trigger: fire hedge if first byte hasn't arrived within threshold
            if (hedgeTtfbThresholdMs > 0) {
                timers.add(hedgeScheduler.schedule(() -> {
                    if (!primary.isDone() && !firstByteReceived.get()) {
                        tryFireHedge(hedgeFired, primary, fetcher, request, executor,
                            metrics::recordHedgeTtfbTriggered);
                    }
                }, hedgeTtfbThresholdMs, TimeUnit.MILLISECONDS));
            }

            // Total-time trigger: fire hedge if primary hasn't completed within threshold
            if (hedgeTotalTimeThresholdMs > 0) {
                timers.add(hedgeScheduler.schedule(() -> {
                    if (!primary.isDone()) {
                        tryFireHedge(hedgeFired, primary, fetcher, request, executor,
                            metrics::recordHedgeTotalTimeTriggered);
                    }
                }, hedgeTotalTimeThresholdMs, TimeUnit.MILLISECONDS));
            }
        } catch (final RejectedExecutionException e) {
            // Scheduler shut down — cancel any timer that was already scheduled before the failure,
            // then fall back to primary without hedging.
            timers.forEach(timer -> timer.cancel(false));
            return primary;
        }

        if (timers.isEmpty()) {
            // Both thresholds are 0 — no hedging
            return primary;
        }

        // Cancel timers when primary completes (naturally or via hedge)
        primary.whenComplete((value, error) ->
            timers.forEach(timer -> timer.cancel(false))
        );

        return primary;
    }

    // Fires a single hedge request. Runs on the hedge scheduler thread — must never block.
    // All operations are non-blocking: CAS, meter marks, and supplyAsync (only enqueues a task;
    // actual I/O runs on the data executor). Executor-full → RejectedExecutionException thrown
    // immediately and caught.
    //
    // The losing fetch (primary or hedge) is not cancelled — CF.cancel() doesn't interrupt S3/GCS
    // I/O, so the request runs to completion regardless. The loser's complete() is a no-op.
    private void tryFireHedge(
        final AtomicBoolean hedgeFired,
        final CompletableFuture<FileExtent> primary,
        final ObjectFetcher fetcher,
        final ObjectFetchRequest request,
        final ExecutorService executor,
        final Runnable triggerMetric
    ) {
        // There is a small race between the timer's !primary.isDone() check and this CAS —
        // the primary can complete in between, causing an unnecessary hedge. This is intentionally
        // non-blocking: eliminating the race would require synchronizing with primary completion,
        // adding contention on the hot path. The trade-off is an occasional redundant fetch whose
        // primary.complete() is a no-op. HedgeWonRate remains accurate; HedgeRequestRate may be
        // slightly inflated, which is acceptable for monitoring purposes.
        if (hedgeFired.compareAndSet(false, true)) {
            metrics.recordHedgeRequest();
            triggerMetric.run();
            try {
                final CompletableFuture<FileExtent> hedge = CompletableFuture.supplyAsync(
                    () -> fetchFileExtent(fetcher, request), executor
                );
                hedge.whenComplete((value, error) -> {
                    if (error == null && primary.complete(value)) {
                        metrics.recordHedgeWon();
                    }
                });
            } catch (final RejectedExecutionException ignored) {
                // Executor full — fall back to primary only
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
     * Fetches a file extent from remote storage, signaling when the first byte is received.
     * The {@code firstByteReceived} flag is set by the TTFB callback before the metrics callback,
     * enabling the TTFB hedge trigger to detect stuck connections.
     */
    private FileExtent fetchFileExtent(
        final ObjectFetcher fetcher,
        final ObjectFetchRequest request,
        final AtomicBoolean firstByteReceived
    ) {
        final Consumer<Long> ttfbCallback = ttfbMs -> {
            firstByteReceived.set(true);
            metrics.fetchFirstByteFinished(ttfbMs);
        };
        try {
            final FileFetchJob job = new FileFetchJob(
                time,
                fetcher,
                request.objectKey(),
                request.byteRange(),
                metrics::fetchFileFinished,
                ttfbCallback
            );
            final FileExtent fileExtent = job.call();
            metrics.cacheEntrySize(fileExtent.data().length);
            return fileExtent;
        } catch (final Exception e) {
            throw new FileFetchException(e);
        }
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
                metrics::fetchFileFinished,
                metrics::fetchFirstByteFinished
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
     * @param timestamp the maximum timestamp from batches.
     *                  Using max instead of min because if ANY batch in the object is recent,
     *                  we treat the entire fetch as hot path to prioritize recent data access.
     * @param lagging   pre-computed lagging decision from planning phase, ensuring range strategy
     *                  (aligned vs bounding) and execution path (cache vs bypass) stay consistent.
     */
    record ObjectFetchRequest(
        ObjectKey objectKey,
        ByteRange byteRange,
        long timestamp,
        boolean lagging
    ) {
        /**
         * Converts to cache key for deduplication.
         *
         * <p>Note: timestamp is intentionally excluded from cache key.
         * Timestamp determines hot/cold path routing but doesn't affect cache identity:
         * only the hot path uses the object cache, while the cold path bypasses the cache
         * entirely. For a given object+range, the returned data is identical regardless of
         * timestamp, so using just object+range as the cache key is safe and ensures both
         * hot-path fetches for the same region share a single cache entry.
         */
        CacheKey toCacheKey() {
            return new CacheKey()
                .setObject(objectKey.value())
                .setRange(new CacheKey.ByteRange()
                    .setOffset(byteRange.offset())
                    .setLength(byteRange.size()));
        }
    }

    /**
     * Pairs an ObjectFetchRequest with its corresponding CompletableFuture.
     * This allows preserving request information (object key, range) even when the fetch fails.
     */
    public record FetchRequestWithFuture(ObjectFetchRequest request, CompletableFuture<FileExtent> future) {}
}
