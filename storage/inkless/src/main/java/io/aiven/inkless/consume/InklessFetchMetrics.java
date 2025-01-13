// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
public class InklessFetchMetrics {
    private static final String FETCH_TOTAL_TIME = "FetchTotalTime";
    private static final String FIND_BATCHES_TIME = "FindBatchesTime";
    private static final String FETCH_PLAN_TIME = "FetchPlanTime";
    private static final String CACHE_QUERY_TIME = "CacheQueryTime";
    private static final String CACHE_STORE_TIME = "CacheStoreTime";
    private static final String CACHE_HIT_COUNT = "CacheHitCount";
    private static final String CACHE_MISS_COUNT = "CacheMissCount";
    private static final String FETCH_FILE_TIME = "FetchFileTime";
    private static final String FETCH_COMPLETION_TIME = "FetchCompletionTime";

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(InklessFetchMetrics.class);
    private final Histogram fetchTimeHistogram;
    private final Histogram findBatchesTimeHistogram;
    private final Histogram fetchPlanTimeHistogram;
    private final Histogram cacheQueryTimeHistogram;
    private final Histogram cacheStoreTimeHistogram;
    private final Meter cacheHits;
    private final Meter cacheMisses;
    private final Histogram fetchFileTimeHistogram;
    private final Histogram fetchCompletionTimeHistogram;

    public InklessFetchMetrics(Time time) {
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

    public void fetchFileFinished(final long durationMs) {
        fetchFileTimeHistogram.update(durationMs);
    }

    public void fetchCompletionFinished(final long duration) {
        fetchCompletionTimeHistogram.update(duration);
    }

    public void close() {
        metricsGroup.removeMetric(FETCH_TOTAL_TIME);
        metricsGroup.removeMetric(FETCH_FILE_TIME);
        metricsGroup.removeMetric(FETCH_PLAN_TIME);
        metricsGroup.removeMetric(FIND_BATCHES_TIME);
        metricsGroup.removeMetric(FETCH_COMPLETION_TIME);
    }
}
