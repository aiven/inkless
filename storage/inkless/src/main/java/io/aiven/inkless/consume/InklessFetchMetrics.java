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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.ObjectCache;

@CoverageIgnore
public class InklessFetchMetrics {
    private static final String GROUP = InklessFetchMetrics.class.getSimpleName();

    private static final String FETCH_TOTAL_TIME = "FetchTotalTime";
    private static final String FETCH_TOTAL_TIME_DOC = "Total time spent processing a fetch request in milliseconds";
    private static final String FIND_BATCHES_TIME = "FindBatchesTime";
    private static final String FIND_BATCHES_TIME_DOC = "Time spent finding batch coordinates in the control plane in milliseconds";
    private static final String FETCH_PLAN_TIME = "FetchPlanTime";
    private static final String FETCH_PLAN_TIME_DOC = "Time spent creating the fetch plan in milliseconds";
    private static final String CACHE_QUERY_TIME = "CacheQueryTime";
    private static final String CACHE_QUERY_TIME_DOC = "Time spent querying the object cache in milliseconds";
    private static final String CACHE_STORE_TIME = "CacheStoreTime";
    private static final String CACHE_STORE_TIME_DOC = "Time spent storing entries in the object cache in milliseconds";
    private static final String CACHE_HIT_COUNT = "CacheHitCount";
    private static final String CACHE_HIT_COUNT_DOC = "Rate of cache hits per second";
    private static final String CACHE_MISS_COUNT = "CacheMissCount";
    private static final String CACHE_MISS_COUNT_DOC = "Rate of cache misses per second";
    private static final String CACHE_ENTRY_SIZE = "CacheEntrySize";
    private static final String CACHE_ENTRY_SIZE_DOC = "Size of individual cache entries in bytes";
    private static final String CACHE_SIZE = "CacheSize";
    private static final String CACHE_SIZE_DOC = "Current number of entries in the object cache";
    private static final String FETCH_FIRST_BYTE_TIME = "FetchFirstByteTime";
    private static final String FETCH_FIRST_BYTE_TIME_DOC = "Time until the first byte is received from storage in milliseconds";
    private static final String FETCH_FILE_TIME = "FetchFileTime";
    private static final String FETCH_FILE_TIME_DOC = "Time spent fetching a file from storage in milliseconds";
    private static final String FETCH_COMPLETION_TIME = "FetchCompletionTime";
    private static final String FETCH_COMPLETION_TIME_DOC = "Time spent completing the fetch response assembly in milliseconds";
    private static final String FETCH_RATE = "FetchRate";
    private static final String FETCH_RATE_DOC = "Rate of fetch requests processed per second";
    private static final String FETCH_ERROR_RATE = "FetchErrorRate";
    private static final String FETCH_ERROR_RATE_DOC = "Rate of failed fetch requests per second";
    private static final String FIND_BATCHES_ERROR_RATE = "FindBatchesErrorRate";
    private static final String FIND_BATCHES_ERROR_RATE_DOC = "Rate of errors when finding batches in the control plane per second";
    private static final String FILE_FETCH_ERROR_RATE = "FileFetchErrorRate";
    private static final String FILE_FETCH_ERROR_RATE_DOC = "Rate of errors when fetching files from storage per second";
    private static final String CACHE_FETCH_ERROR_RATE = "CacheFetchErrorRate";
    private static final String CACHE_FETCH_ERROR_RATE_DOC = "Rate of errors when fetching from the cache per second";
    private static final String FETCH_PARTITIONS_PER_FETCH_COUNT = "FetchPartitionsPerFetchCount";
    private static final String FETCH_PARTITIONS_PER_FETCH_COUNT_DOC = "Number of partitions included in each fetch request";
    private static final String FETCH_BATCHES_PER_FETCH_COUNT = "FetchBatchesPerPartitionCount";
    private static final String FETCH_BATCHES_PER_FETCH_COUNT_DOC = "Number of batches fetched per partition";
    private static final String FETCH_OBJECTS_PER_FETCH_COUNT = "FetchObjectsPerFetchCount";
    private static final String FETCH_OBJECTS_PER_FETCH_COUNT_DOC = "Number of storage objects accessed per fetch request";
    private static final String FETCH_RESPONSE_SIZE = "FetchResponseSize";
    private static final String FETCH_RESPONSE_SIZE_DOC = "Total bytes returned to the caller in each fetch response, recorded for every fetch (0 when no data was served, e.g. a caught-up long-poll or an all-error response).";
    private static final String PARTITION_PARTIAL_FETCH_RATE = "PartitionPartialFetchRate";
    private static final String PARTITION_PARTIAL_FETCH_RATE_DOC = "Rate of partition responses that were truncated mid-batch-list due to a missing extent or validation failure on a trailing batch. Successful prefix is returned to the consumer; the trailing batches are dropped.";
    private static final String PARTITION_STORAGE_ERROR_RATE = "PartitionStorageErrorRate";
    private static final String PARTITION_STORAGE_ERROR_RATE_DOC = "Rate of partition responses returning KAFKA_STORAGE_ERROR from FetchCompleter (missing batch metadata, or missing/incomplete extents so no records were constructable). Data-integrity failures are counted separately under PartitionCorruptRecordRate.";
    private static final String PARTITION_CORRUPT_RECORD_RATE = "PartitionCorruptRecordRate";
    private static final String PARTITION_CORRUPT_RECORD_RATE_DOC = "Rate of partition responses returning CORRUPT_MESSAGE or INVALID_RECORD from FetchCompleter because a batch failed integrity validation (CRC / declared size, or a coalesced-run offset mismatch), with no valid prefix to serve.";
    private static final String RECENT_DATA_REQUEST_RATE = "RecentDataRequestRate";
    private static final String RECENT_DATA_REQUEST_RATE_DOC = "Rate of requests served via the hot path (recent data with cache) per second. "
        + "Under the consolidation metrics group (ConsolidationFetchMetrics) this counts cache-hit peeks that reuse consumer-cached data.";
    private static final String LAGGING_CONSUMER_REQUEST_RATE = "LaggingConsumerRequestRate";
    private static final String LAGGING_CONSUMER_REQUEST_RATE_DOC = "Rate of cold-path requests (bypass the cache) per second. "
        + "Under the consumer metrics group these are lagging-consumer fetches; under the consolidation group "
        + "(ConsolidationFetchMetrics) these are consolidation cold fetches (cache-hit peeks are counted under RecentDataRequestRate).";
    private static final String LAGGING_CONSUMER_REQUEST_REJECTED_RATE = "LaggingConsumerRequestRejectedRate";
    private static final String LAGGING_CONSUMER_REQUEST_REJECTED_RATE_DOC = "Rate of lagging consumer requests rejected due to executor unavailability per second";
    // Tracks wait time (including zero-wait) for ALL lagging consumer requests when rate limiting is enabled.
    // When rate limiter is disabled (config = 0), LaggingConsumerRequestRate > 0 but this metric rate = 0.
    // Always records wait time to avoid histogram bias - zero-wait cases show when rate limiting is NOT a bottleneck.
    // Use to monitor: rate limiting latency distribution, actual throttling pressure, and limiter effectiveness.
    private static final String LAGGING_CONSUMER_RATE_LIMIT_WAIT_TIME = "LaggingConsumerRateLimitWaitTime";
    private static final String LAGGING_CONSUMER_RATE_LIMIT_WAIT_TIME_DOC = "Combined wait time (request-rate plus byte-rate limiters) for rate-limited lagging consumer requests in milliseconds";

    private static final String LAGGING_CONSUMER_REQUEST_RATE_THROTTLED_RATE = "LaggingConsumerRequestRateThrottledRate";
    private static final String LAGGING_CONSUMER_REQUEST_RATE_THROTTLED_RATE_DOC = "Rate of lagging consumer requests that had to wait on the request-rate limiter (cost/QPS protection) per second. "
        + "Together with LaggingConsumerByteRateThrottledRate, attributes which limiter is the binding constraint (the combined wait is in LaggingConsumerRateLimitWaitTime).";
    private static final String LAGGING_CONSUMER_BYTE_RATE_THROTTLED_RATE = "LaggingConsumerByteRateThrottledRate";
    private static final String LAGGING_CONSUMER_BYTE_RATE_THROTTLED_RATE_DOC = "Rate of lagging consumer requests that had to wait on the byte-rate limiter (bandwidth protection) per second. "
        + "Together with LaggingConsumerRequestRateThrottledRate, attributes which limiter is the binding constraint (the combined wait is in LaggingConsumerRateLimitWaitTime).";
    private static final String LAGGING_CONSUMER_BYTE_RATE_OVERSIZED_RATE = "LaggingConsumerByteRateOversizedRate";
    private static final String LAGGING_CONSUMER_BYTE_RATE_OVERSIZED_RATE_DOC = "Rate of lagging consumer fetches whose byte range exceeds the byte-rate limiter capacity (fetch.lagging.consumer.byte.rate.limit) per second. "
        + "Such a fetch cannot be split below its own size, so it is charged a full bucket and allowed through. A non-zero rate indicates the byte-rate limit is set below the produced object size (likely misconfiguration).";

    private static final String HEDGE_REQUEST_RATE = "HedgeRequestRate";
    private static final String HEDGE_REQUEST_RATE_DOC = "Rate of hedged fetch requests issued per second";
    private static final String HEDGE_TTFB_TRIGGERED_RATE = "HedgeTtfbTriggeredRate";
    private static final String HEDGE_TTFB_TRIGGERED_RATE_DOC = "Rate of hedge requests triggered by TTFB timeout per second";
    private static final String HEDGE_TOTAL_TIME_TRIGGERED_RATE = "HedgeTotalTimeTriggeredRate";
    private static final String HEDGE_TOTAL_TIME_TRIGGERED_RATE_DOC = "Rate of hedge requests triggered by total time timeout per second";
    private static final String HEDGE_WON_RATE = "HedgeWonRate";
    private static final String HEDGE_WON_RATE_DOC = "Rate of hedge requests that completed before the original request per second";

    /**
     * This method returns a list of all the metric name templates for the InklessFetchMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(FETCH_TOTAL_TIME, GROUP, FETCH_TOTAL_TIME_DOC),
            new MetricNameTemplate(FIND_BATCHES_TIME, GROUP, FIND_BATCHES_TIME_DOC),
            new MetricNameTemplate(FETCH_PLAN_TIME, GROUP, FETCH_PLAN_TIME_DOC),
            new MetricNameTemplate(CACHE_QUERY_TIME, GROUP, CACHE_QUERY_TIME_DOC),
            new MetricNameTemplate(CACHE_STORE_TIME, GROUP, CACHE_STORE_TIME_DOC),
            new MetricNameTemplate(CACHE_HIT_COUNT, GROUP, CACHE_HIT_COUNT_DOC),
            new MetricNameTemplate(CACHE_MISS_COUNT, GROUP, CACHE_MISS_COUNT_DOC),
            new MetricNameTemplate(CACHE_ENTRY_SIZE, GROUP, CACHE_ENTRY_SIZE_DOC),
            new MetricNameTemplate(CACHE_SIZE, GROUP, CACHE_SIZE_DOC),
            new MetricNameTemplate(FETCH_FIRST_BYTE_TIME, GROUP, FETCH_FIRST_BYTE_TIME_DOC),
            new MetricNameTemplate(FETCH_FILE_TIME, GROUP, FETCH_FILE_TIME_DOC),
            new MetricNameTemplate(FETCH_COMPLETION_TIME, GROUP, FETCH_COMPLETION_TIME_DOC),
            new MetricNameTemplate(FETCH_RATE, GROUP, FETCH_RATE_DOC),
            new MetricNameTemplate(FETCH_ERROR_RATE, GROUP, FETCH_ERROR_RATE_DOC),
            new MetricNameTemplate(FIND_BATCHES_ERROR_RATE, GROUP, FIND_BATCHES_ERROR_RATE_DOC),
            new MetricNameTemplate(FILE_FETCH_ERROR_RATE, GROUP, FILE_FETCH_ERROR_RATE_DOC),
            new MetricNameTemplate(CACHE_FETCH_ERROR_RATE, GROUP, CACHE_FETCH_ERROR_RATE_DOC),
            new MetricNameTemplate(FETCH_PARTITIONS_PER_FETCH_COUNT, GROUP, FETCH_PARTITIONS_PER_FETCH_COUNT_DOC),
            new MetricNameTemplate(FETCH_BATCHES_PER_FETCH_COUNT, GROUP, FETCH_BATCHES_PER_FETCH_COUNT_DOC),
            new MetricNameTemplate(FETCH_OBJECTS_PER_FETCH_COUNT, GROUP, FETCH_OBJECTS_PER_FETCH_COUNT_DOC),
            new MetricNameTemplate(FETCH_RESPONSE_SIZE, GROUP, FETCH_RESPONSE_SIZE_DOC),
            new MetricNameTemplate(PARTITION_PARTIAL_FETCH_RATE, GROUP, PARTITION_PARTIAL_FETCH_RATE_DOC),
            new MetricNameTemplate(PARTITION_STORAGE_ERROR_RATE, GROUP, PARTITION_STORAGE_ERROR_RATE_DOC),
            new MetricNameTemplate(PARTITION_CORRUPT_RECORD_RATE, GROUP, PARTITION_CORRUPT_RECORD_RATE_DOC),
            new MetricNameTemplate(RECENT_DATA_REQUEST_RATE, GROUP, RECENT_DATA_REQUEST_RATE_DOC),
            new MetricNameTemplate(LAGGING_CONSUMER_REQUEST_RATE, GROUP, LAGGING_CONSUMER_REQUEST_RATE_DOC),
            new MetricNameTemplate(LAGGING_CONSUMER_REQUEST_REJECTED_RATE, GROUP, LAGGING_CONSUMER_REQUEST_REJECTED_RATE_DOC),
            new MetricNameTemplate(LAGGING_CONSUMER_RATE_LIMIT_WAIT_TIME, GROUP, LAGGING_CONSUMER_RATE_LIMIT_WAIT_TIME_DOC),
            new MetricNameTemplate(LAGGING_CONSUMER_REQUEST_RATE_THROTTLED_RATE, GROUP, LAGGING_CONSUMER_REQUEST_RATE_THROTTLED_RATE_DOC),
            new MetricNameTemplate(LAGGING_CONSUMER_BYTE_RATE_THROTTLED_RATE, GROUP, LAGGING_CONSUMER_BYTE_RATE_THROTTLED_RATE_DOC),
            new MetricNameTemplate(LAGGING_CONSUMER_BYTE_RATE_OVERSIZED_RATE, GROUP, LAGGING_CONSUMER_BYTE_RATE_OVERSIZED_RATE_DOC),
            new MetricNameTemplate(HEDGE_REQUEST_RATE, GROUP, HEDGE_REQUEST_RATE_DOC),
            new MetricNameTemplate(HEDGE_TTFB_TRIGGERED_RATE, GROUP, HEDGE_TTFB_TRIGGERED_RATE_DOC),
            new MetricNameTemplate(HEDGE_TOTAL_TIME_TRIGGERED_RATE, GROUP, HEDGE_TOTAL_TIME_TRIGGERED_RATE_DOC),
            new MetricNameTemplate(HEDGE_WON_RATE, GROUP, HEDGE_WON_RATE_DOC)
        );
    }

    private final Time time;

    private final KafkaMetricsGroup metricsGroup;
    private final Histogram fetchTimeHistogram;
    private final Histogram findBatchesTimeHistogram;
    private final Histogram fetchPlanTimeHistogram;
    private final Histogram cacheQueryTimeHistogram;
    private final Histogram cacheStoreTimeHistogram;
    private final Histogram cacheEntrySize;
    private final Gauge<Long> cacheSize;
    private final Meter cacheHits;
    private final Meter cacheMisses;
    private final Histogram fetchFirstByteTimeHistogram;
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
    private final Histogram fetchResponseSizeHistogram;
    private final Meter partitionPartialFetchRate;
    private final Meter partitionStorageErrorRate;
    private final Meter partitionCorruptRecordRate;
    private final Meter recentDataRequestRate;
    private final Meter laggingConsumerRequestRate;
    private final Meter laggingConsumerRejectedRate;
    private final Histogram laggingRateLimitWaitTime;
    private final Meter laggingRequestRateThrottledRate;
    private final Meter laggingByteRateThrottledRate;
    private final Meter laggingByteRateOversizedRate;
    private final Meter hedgeRequestRate;
    private final Meter hedgeTtfbTriggeredRate;
    private final Meter hedgeTotalTimeTriggeredRate;
    private final Meter hedgeWonRate;

    public InklessFetchMetrics(final Time time, final ObjectCache cache) {
        this(time, cache, new KafkaMetricsGroup(InklessFetchMetrics.class.getPackageName(), InklessFetchMetrics.class.getSimpleName()));
    }

    public InklessFetchMetrics(final Time time, final ObjectCache cache, final KafkaMetricsGroup metricsGroup) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.metricsGroup = Objects.requireNonNull(metricsGroup, "metricsGroup cannot be null");
        fetchTimeHistogram = metricsGroup.newHistogram(FETCH_TOTAL_TIME, true, Map.of());
        findBatchesTimeHistogram = metricsGroup.newHistogram(FIND_BATCHES_TIME, true, Map.of());
        fetchPlanTimeHistogram = metricsGroup.newHistogram(FETCH_PLAN_TIME, true, Map.of());
        cacheQueryTimeHistogram = metricsGroup.newHistogram(CACHE_QUERY_TIME, true, Map.of());
        cacheStoreTimeHistogram = metricsGroup.newHistogram(CACHE_STORE_TIME, true, Map.of());
        cacheHits = metricsGroup.newMeter(CACHE_HIT_COUNT, "hits", TimeUnit.SECONDS, Map.of());
        cacheMisses = metricsGroup.newMeter(CACHE_MISS_COUNT, "misses", TimeUnit.SECONDS, Map.of());
        fetchFirstByteTimeHistogram = metricsGroup.newHistogram(FETCH_FIRST_BYTE_TIME, true, Map.of());
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
        fetchResponseSizeHistogram = metricsGroup.newHistogram(FETCH_RESPONSE_SIZE, true, Map.of());
        partitionPartialFetchRate = metricsGroup.newMeter(PARTITION_PARTIAL_FETCH_RATE, "partitions", TimeUnit.SECONDS, Map.of());
        partitionStorageErrorRate = metricsGroup.newMeter(PARTITION_STORAGE_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        partitionCorruptRecordRate = metricsGroup.newMeter(PARTITION_CORRUPT_RECORD_RATE, "errors", TimeUnit.SECONDS, Map.of());
        cacheEntrySize = metricsGroup.newHistogram(CACHE_ENTRY_SIZE, true, Map.of());
        cacheSize = metricsGroup.newGauge(CACHE_SIZE, () -> cache.size());
        recentDataRequestRate = metricsGroup.newMeter(RECENT_DATA_REQUEST_RATE, "requests", TimeUnit.SECONDS, Map.of());
        laggingConsumerRequestRate = metricsGroup.newMeter(LAGGING_CONSUMER_REQUEST_RATE, "requests", TimeUnit.SECONDS, Map.of());
        laggingConsumerRejectedRate = metricsGroup.newMeter(LAGGING_CONSUMER_REQUEST_REJECTED_RATE, "rejections", TimeUnit.SECONDS, Map.of());
        laggingRateLimitWaitTime = metricsGroup.newHistogram(LAGGING_CONSUMER_RATE_LIMIT_WAIT_TIME, true, Map.of());
        laggingRequestRateThrottledRate = metricsGroup.newMeter(LAGGING_CONSUMER_REQUEST_RATE_THROTTLED_RATE, "requests", TimeUnit.SECONDS, Map.of());
        laggingByteRateThrottledRate = metricsGroup.newMeter(LAGGING_CONSUMER_BYTE_RATE_THROTTLED_RATE, "requests", TimeUnit.SECONDS, Map.of());
        laggingByteRateOversizedRate = metricsGroup.newMeter(LAGGING_CONSUMER_BYTE_RATE_OVERSIZED_RATE, "requests", TimeUnit.SECONDS, Map.of());
        hedgeRequestRate = metricsGroup.newMeter(HEDGE_REQUEST_RATE, "hedges", TimeUnit.SECONDS, Map.of());
        hedgeTtfbTriggeredRate = metricsGroup.newMeter(HEDGE_TTFB_TRIGGERED_RATE, "hedges", TimeUnit.SECONDS, Map.of());
        hedgeTotalTimeTriggeredRate = metricsGroup.newMeter(HEDGE_TOTAL_TIME_TRIGGERED_RATE, "hedges", TimeUnit.SECONDS, Map.of());
        hedgeWonRate = metricsGroup.newMeter(HEDGE_WON_RATE, "wins", TimeUnit.SECONDS, Map.of());
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

    public void fetchFirstByteFinished(final long durationMs) {
        fetchFirstByteTimeHistogram.update(durationMs);
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
        metricsGroup.removeMetric(FETCH_FIRST_BYTE_TIME);
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
        metricsGroup.removeMetric(FETCH_RESPONSE_SIZE);
        metricsGroup.removeMetric(PARTITION_PARTIAL_FETCH_RATE);
        metricsGroup.removeMetric(PARTITION_STORAGE_ERROR_RATE);
        metricsGroup.removeMetric(PARTITION_CORRUPT_RECORD_RATE);
        metricsGroup.removeMetric(RECENT_DATA_REQUEST_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_REQUEST_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_REQUEST_REJECTED_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_RATE_LIMIT_WAIT_TIME);
        metricsGroup.removeMetric(LAGGING_CONSUMER_REQUEST_RATE_THROTTLED_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_BYTE_RATE_THROTTLED_RATE);
        metricsGroup.removeMetric(LAGGING_CONSUMER_BYTE_RATE_OVERSIZED_RATE);
        metricsGroup.removeMetric(HEDGE_REQUEST_RATE);
        metricsGroup.removeMetric(HEDGE_TTFB_TRIGGERED_RATE);
        metricsGroup.removeMetric(HEDGE_TOTAL_TIME_TRIGGERED_RATE);
        metricsGroup.removeMetric(HEDGE_WON_RATE);
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

    /** Total bytes returned in a single fetch response, summed across partitions. */
    public void recordFetchResponseSize(long bytes) {
        fetchResponseSizeHistogram.update(bytes);
    }

    /**
     * Increments the partial-partition-fetch counter. Call once per partition whose response
     * was truncated mid-batch-list (some leading batches succeeded, a trailing batch was dropped
     * due to a missing extent or a failed validation).
     */
    public void recordPartitionPartialFetch() {
        partitionPartialFetchRate.mark();
    }

    /**
     * Increments the partition-storage-error counter. Call once per partition whose response
     * surfaced KAFKA_STORAGE_ERROR from FetchCompleter (missing metadata, or missing/incomplete
     * extents leaving no constructable records).
     */
    public void recordPartitionStorageError() {
        partitionStorageErrorRate.mark();
    }

    /**
     * Increments the partition-corrupt-record counter. Call once per partition whose response
     * surfaced CORRUPT_MESSAGE / INVALID_RECORD from FetchCompleter because a batch failed integrity
     * validation and there was no valid prefix to serve.
     */
    public void recordPartitionCorruptRecord() {
        partitionCorruptRecordRate.mark();
    }

    /**
     * Records a request that used the hot path (recent data with cache).
     * The consolidation Reader (own metrics group, so a distinct MBean from the consumer Reader)
     * also records here on a cache-hit peek, since reusing consumer-cached data is hot reuse rather
     * than a cold fetch.
     * Metric: RecentDataRequestRate
     */
    public void recordRecentDataRequest() {
        recentDataRequestRate.mark();
    }

    /**
     * Records a request that used the cold path (bypasses the cache).
     * Recorded for ALL cold path requests, regardless of rate limiting.
     * Metric: LaggingConsumerRequestRate
     *
     * @see #recordRateLimitWaitTime(long) for requests that were actually rate limited
     */
    public void recordLaggingConsumerRequest() {
        laggingConsumerRequestRate.mark();
    }

    /**
     * Records a lagging consumer request that was rejected due to executor unavailability.
     * This typically corresponds to:
     * - RejectedExecutionException: Queue full (AbortPolicy triggered)
     *
     * In this case, backpressure is applied: the consumer receives an error response
     * and backs off via fetch purgatory.
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
     * <p>Metric: LaggingConsumerRateLimitWaitTime (Histogram)
     *
     * <p>Relationship:
     * - When rate limiting is ENABLED: LaggingConsumerRateLimitWaitTime.Rate ≈ LaggingConsumerRequestRate
     * - When rate limiting is DISABLED: LaggingConsumerRateLimitWaitTime.Rate = 0, LaggingConsumerRequestRate > 0
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

    /**
     * Records that a lagging consumer request had to wait on the request-rate limiter
     * (cost/QPS protection). Call only when the non-blocking acquire failed and a blocking
     * wait was required, so the meter reflects how often request-rate is the binding constraint.
     * Metric: LaggingConsumerRequestRateThrottledRate
     */
    public void recordRequestRateThrottled() {
        laggingRequestRateThrottledRate.mark();
    }

    /**
     * Records that a lagging consumer request had to wait on the byte-rate limiter
     * (bandwidth protection). Call only when the non-blocking acquire failed and a blocking
     * wait was required, so the meter reflects how often byte-rate is the binding constraint.
     * Metric: LaggingConsumerByteRateThrottledRate
     */
    public void recordByteRateThrottled() {
        laggingByteRateThrottledRate.mark();
    }

    /**
     * Records a lagging consumer fetch whose byte range exceeds the byte-rate limiter capacity.
     * Such a fetch is charged a full bucket and allowed through (it cannot be split below its own
     * size). A non-zero rate indicates fetch.lagging.consumer.byte.rate.limit is set below the
     * produced object size (likely misconfiguration).
     * Metric: LaggingConsumerByteRateOversizedRate
     */
    public void recordByteRateOversized() {
        laggingByteRateOversizedRate.mark();
    }

    public void recordHedgeRequest() {
        hedgeRequestRate.mark();
    }

    public void recordHedgeTtfbTriggered() {
        hedgeTtfbTriggeredRate.mark();
    }

    public void recordHedgeTotalTimeTriggered() {
        hedgeTotalTimeTriggeredRate.mark();
    }

    public void recordHedgeWon() {
        hedgeWonRate.mark();
    }
}
