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
