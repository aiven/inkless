// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
public class InklessFetchMetrics {
    private static final String FETCH_TOTAL_TIME = "FetchTotalTime";
    private static final String FIND_BATCHES_TIME = "FindBatchesTime";
    private static final String FETCH_PLAN_TIME = "FetchPlanTime";
    private static final String FETCH_FILE_TIME = "FetchFileTime";
    private static final String FETCH_COMPLETION_TIME = "FetchCompletionTime";

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(InklessFetchMetrics.class);
    private final Histogram fetchTimeHistogram;
    private final Histogram findBatchesTimeHistogram;
    private final Histogram fetchPlanTimeHistogram;
    private final Histogram fetchFileTimeHistogram;
    private final Histogram fetchCompletionTimeHistogram;

    public InklessFetchMetrics(Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fetchTimeHistogram = metricsGroup.newHistogram(FETCH_TOTAL_TIME, true, Map.of());
        findBatchesTimeHistogram = metricsGroup.newHistogram(FIND_BATCHES_TIME, true, Map.of());
        fetchPlanTimeHistogram = metricsGroup.newHistogram(FETCH_PLAN_TIME, true, Map.of());
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
