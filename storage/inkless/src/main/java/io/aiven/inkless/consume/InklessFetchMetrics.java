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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.ObjectCache;

@CoverageIgnore
public class InklessFetchMetrics {
    private static final String FETCH_TOTAL_TIME = "FetchTotalTime";
    private static final String FIND_BATCHES_TIME = "FindBatchesTime";
    private static final String FETCH_PLAN_TIME = "FetchPlanTime";
    private static final String CACHE_QUERY_TIME = "CacheQueryTime";
    private static final String CACHE_STORE_TIME = "CacheStoreTime";
    private static final String CACHE_HIT_COUNT = "CacheHitCount";
    private static final String CACHE_MISS_COUNT = "CacheMissCount";
    private static final String CACHE_ENTRY_SIZE = "CacheEntrySize";
    private static final String CACHE_SIZE = "CacheSize";
    private static final String FETCH_FILE_TIME = "FetchFileTime";
    private static final String FETCH_COMPLETION_TIME = "FetchCompletionTime";
    private static final String FETCH_RATE = "FetchRate";
    private static final String FETCH_ERROR_RATE = "FetchErrorRate";
    private static final String FIND_BATCHES_ERROR_RATE = "FindBatchesErrorRate";
    private static final String FILE_FETCH_ERROR_RATE = "FileFetchErrorRate";
    private static final String CACHE_FETCH_ERROR_RATE = "CacheFetchErrorRate";
    private static final String FETCH_PARTITIONS_PER_FETCH_COUNT = "FetchPartitionsPerFetchCount";
    private static final String FETCH_BATCHES_PER_FETCH_COUNT = "FetchBatchesPerPartitionCount";
    private static final String FETCH_OBJECTS_PER_FETCH_COUNT = "FetchObjectsPerFetchCount";
    private static final String RECENT_DATA_REQUEST_RATE = "RecentDataRequestRate";
    private static final String LAGGING_CONSUMER_REQUEST_RATE = "LaggingConsumerRequestRate";
    private static final String LAGGING_CONSUMER_REJECTED_RATE = "LaggingConsumerRejectedRate";
    // Tracks wait time (including zero-wait) for ALL lagging consumer requests when rate limiting is enabled.
    // When rate limiter is disabled (config = 0), LaggingConsumerRequestRate > 0 but this metric rate = 0.
    // Always records wait time to avoid histogram bias - zero-wait cases show when rate limiting is NOT a bottleneck.
    // Use to monitor: rate limiting latency distribution, actual throttling pressure, and limiter effectiveness.
    private static final String LAGGING_RATE_LIMIT_WAIT_TIME = "LaggingRateLimitWaitTime";

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(InklessFetchMetrics.class);
    private final Histogram fetchTimeHistogram;
    private final Histogram findBatchesTimeHistogram;
    private final Histogram fetchPlanTimeHistogram;
    private final Histogram cacheQueryTimeHistogram;
    private final Histogram cacheStoreTimeHistogram;
    private final Histogram cacheEntrySize;
    private final Gauge<Long> cacheSize;
    private final Meter cacheHits;
    private final Meter cacheMisses;
    private final Histogram fetchFileTimeHistogram;
    private final Histogram fetchCompletionTimeHistogram;
    private final Meter fetchRate;
    private final Meter fetchErrorRate;
    private final Meter findBatchesErrorRate;
    private final Meter fileFetchErrorRate;
    private final Meter cacheFetchErrorRate;
    private final Histogram fetchPartitionSizeHistogram;
    private final Histogram fetchBatchesSizeHistogram;
    private final Histogram fetchObjectsSizeHistogram;
    private final Meter recentDataRequestRate;
    private final Meter laggingConsumerRequestRate;
    private final Meter laggingConsumerRejectedRate;
    private final Histogram laggingRateLimitWaitTime;

    public InklessFetchMetrics(final Time time, final ObjectCache cache) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fetchTimeHistogram = metricsGroup.newHistogram(FETCH_TOTAL_TIME, true, Map.of());
        findBatchesTimeHistogram = metricsGroup.newHistogram(FIND_BATCHES_TIME, true, Map.of());
        fetchPlanTimeHistogram = metricsGroup.newHistogram(FETCH_PLAN_TIME, true, Map.of());
        cacheQueryTimeHistogram = metricsGroup.newHistogram(CACHE_QUERY_TIME, true, Map.of());
        cacheStoreTimeHistogram = metricsGroup.newHistogram(CACHE_STORE_TIME, true, Map.of());
        cacheHits = metricsGroup.newMeter(CACHE_HIT_COUNT, "hits", TimeUnit.SECONDS, Map.of());
        cacheMisses = metricsGroup.newMeter(CACHE_MISS_COUNT, "misses", TimeUnit.SECONDS, Map.of());
        fetchFileTimeHistogram = metricsGroup.newHistogram(FETCH_FILE_TIME, true, Map.of());
        fetchCompletionTimeHistogram = metricsGroup.newHistogram(FETCH_COMPLETION_TIME, true, Map.of());
        fetchRate = metricsGroup.newMeter(FETCH_RATE, "fetches", TimeUnit.SECONDS, Map.of());
        fetchErrorRate = metricsGroup.newMeter(FETCH_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        findBatchesErrorRate = metricsGroup.newMeter(FIND_BATCHES_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        fileFetchErrorRate = metricsGroup.newMeter(FILE_FETCH_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        cacheFetchErrorRate = metricsGroup.newMeter(CACHE_FETCH_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        fetchPartitionSizeHistogram = metricsGroup.newHistogram(FETCH_PARTITIONS_PER_FETCH_COUNT, true, Map.of());
        fetchBatchesSizeHistogram = metricsGroup.newHistogram(FETCH_BATCHES_PER_FETCH_COUNT, true, Map.of());
        fetchObjectsSizeHistogram = metricsGroup.newHistogram(FETCH_OBJECTS_PER_FETCH_COUNT, true, Map.of());
        cacheEntrySize = metricsGroup.newHistogram(CACHE_ENTRY_SIZE, true, Map.of());
        cacheSize = metricsGroup.newGauge(CACHE_SIZE, () -> cache.size());
        recentDataRequestRate = metricsGroup.newMeter(RECENT_DATA_REQUEST_RATE, "requests", TimeUnit.SECONDS, Map.of());
        laggingConsumerRequestRate = metricsGroup.newMeter(LAGGING_CONSUMER_REQUEST_RATE, "requests", TimeUnit.SECONDS, Map.of());
        laggingConsumerRejectedRate = metricsGroup.newMeter(LAGGING_CONSUMER_REJECTED_RATE, "rejections", TimeUnit.SECONDS, Map.of());
        laggingRateLimitWaitTime = metricsGroup.newHistogram(LAGGING_RATE_LIMIT_WAIT_TIME, true, Map.of());
    }

    public void fetchCompleted(Instant startAt) {
        final Instant now = TimeUtils.durationMeasurementNow(time);
        fetchTimeHistogram.update(Duration.between(startAt, now).toMillis());
    }

    public void findBatchesFinished(final long durationMs) {
        findBatchesTimeHistogram.update(durationMs);
    }

    public void fetchPlanFinished(final long durationMs) {
        fetchPlanTimeHistogram.update(durationMs);
    }

    public void cacheQueryFinished(final long durationMs) {
        cacheQueryTimeHistogram.update(durationMs);
    }

    public void cacheStoreFinished(final long durationMs) {
        cacheStoreTimeHistogram.update(durationMs);
    }

    public void cacheHit(final boolean hit) {
        if (hit) {
            cacheHits.mark();
        } else {
            cacheMisses.mark();
        }
    }

    public void cacheEntrySize(final int size) {
        cacheEntrySize.update(size);
    }

    public void fetchFileFinished(final long durationMs) {
        fetchFileTimeHistogram.update(durationMs);
    }

    public void fetchCompletionFinished(final long duration) {
        fetchCompletionTimeHistogram.update(duration);
    }

    public void fetchFailed() {
        fetchErrorRate.mark();
    }

    public void findBatchesFailed() {
        findBatchesErrorRate.mark();
    }

    public void fileFetchFailed() {
        fileFetchErrorRate.mark();
    }

    public void cacheFetchFailed() {
        cacheFetchErrorRate.mark();
    }

    public void close() {
        metricsGroup.removeMetric(FETCH_TOTAL_TIME);
        metricsGroup.removeMetric(FETCH_FILE_TIME);
        metricsGroup.removeMetric(FETCH_PLAN_TIME);
        metricsGroup.removeMetric(CACHE_QUERY_TIME);
        metricsGroup.removeMetric(CACHE_STORE_TIME);
        metricsGroup.removeMetric(CACHE_HIT_COUNT);
        metricsGroup.removeMetric(CACHE_MISS_COUNT);
        metricsGroup.removeMetric(CACHE_SIZE);
        metricsGroup.removeMetric(CACHE_ENTRY_SIZE);
        metricsGroup.removeMetric(FIND_BATCHES_TIME);
        metricsGroup.removeMetric(FETCH_COMPLETION_TIME);
        metricsGroup.removeMetric(FETCH_RATE);
        metricsGroup.removeMetric(FETCH_ERROR_RATE);
        metricsGroup.removeMetric(FIND_BATCHES_ERROR_RATE);
        metricsGroup.removeMetric(FILE_FETCH_ERROR_RATE);
        metricsGroup.removeMetric(CACHE_FETCH_ERROR_RATE);
        metricsGroup.removeMetric(FETCH_PARTITIONS_PER_FETCH_COUNT);
        metricsGroup.removeMetric(FETCH_BATCHES_PER_FETCH_COUNT);
        metricsGroup.removeMetric(FETCH_OBJECTS_PER_FETCH_COUNT);
        metricsGroup.removeMetric(RECENT_DATA_REQUEST_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_REQUEST_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_REJECTED_RATE);
        metricsGroup.removeMetric(LAGGING_RATE_LIMIT_WAIT_TIME);
    }

    public void fetchStarted(int partitionSize) {
        fetchRate.mark();
        fetchPartitionSizeHistogram.update(partitionSize);
    }

    public void recordFetchBatchSize(int size) {
        fetchBatchesSizeHistogram.update(size);
    }

    public void recordFetchObjectsSize(int size) {
        fetchObjectsSizeHistogram.update(size);
    }

    /**
     * Records a request that used the hot path (recent data with cache).
     * Metric: RecentDataRequestRate
     */
    public void recordRecentDataRequest() {
        recentDataRequestRate.mark();
    }

    /**
     * Records a request that used the cold path (lagging consumer, bypasses cache).
     * This is recorded for ALL cold path requests, regardless of rate limiting.
     * Metric: LaggingConsumerRequestRate
     *
     * @see #recordRateLimitWaitTime(long) for requests that were actually rate limited
     */
    public void recordLaggingConsumerRequest() {
        laggingConsumerRequestRate.mark();
    }

    /**
     * Records a lagging consumer request that was rejected due to executor unavailability.
     * This includes:
     * - RejectedExecutionException: Queue full (AbortPolicy triggered)
     * - InterruptedException: Executor shutting down or thread interrupted
     *
     * Both cases trigger backpressure: consumer receives error response and backs off via fetch purgatory.
     * Metric: LaggingConsumerRejectedRate
     *
     * High rejection rate indicates:
     * - Sustained lagging consumer load exceeding capacity
     * - May need to increase fetch.lagging.consumer.thread.pool.size
     * - Or increase fetch.lagging.consumer.request.rate.limit
     * - Or consumers are genuinely too far behind (acceptable backpressure)
     * - Or executor is shutting down (transient)
     */
    public void recordLaggingConsumerRejection() {
        laggingConsumerRejectedRate.mark();
    }

    /**
     * Records wait time for lagging consumer requests when rate limiting is enabled.
     * This is recorded for ALL lagging consumer requests processed through the rate limiter,
     * including zero-wait cases (when tokens are immediately available).
     *
     * <p>Recording zero-wait cases is intentional to avoid histogram bias. Zero-wait entries
     * show when rate limiting is NOT a bottleneck, which is valuable monitoring data.
     *
     * <p>Metric: LaggingRateLimitWaitTime (Histogram)
     *
     * <p>Relationship:
     * - When rate limiting is ENABLED: LaggingRateLimitWaitTime.Rate ≈ LaggingConsumerRequestRate
     * - When rate limiting is DISABLED: LaggingRateLimitWaitTime.Rate = 0, LaggingConsumerRequestRate > 0
     *
     * <p>Use this metric to:
     * - Monitor rate limiting latency distribution (including p50, p99, p999)
     * - Identify when rate limiting becomes a bottleneck (high percentiles)
     * - Verify rate limiting is working (rate should match LaggingConsumerRequestRate when enabled)
     * - Detect configuration issues (if rate is 0 but LaggingConsumerRequestRate > 0, rate limiting is disabled)
     *
     * @param waitMs Wait time in milliseconds (can be 0 if token was immediately available)
     */
    public void recordRateLimitWaitTime(long waitMs) {
        laggingRateLimitWaitTime.update(waitMs);
    }
}
