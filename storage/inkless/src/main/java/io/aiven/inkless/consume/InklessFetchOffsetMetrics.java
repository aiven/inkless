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
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
public class InklessFetchOffsetMetrics {
    private static final String GROUP = InklessFetchOffsetMetrics.class.getSimpleName();

    private static final String FETCH_OFFSET_TOTAL_TIME = "FetchOffsetTotalTime";
    private static final String FETCH_OFFSET_TOTAL_TIME_DOC = "Total time spent processing a fetch offset request in milliseconds";
    private static final String FETCH_OFFSET_RATE = "FetchOffsetRate";
    private static final String FETCH_OFFSET_RATE_DOC = "Rate of fetch offset requests processed per second";
    private static final String FETCH_OFFSET_ERROR_RATE = "FetchOffsetErrorRate";
    private static final String FETCH_OFFSET_ERROR_RATE_DOC = "Rate of failed fetch offset requests per second";

    /**
     * This method returns a list of all the metric name templates for the InklessFetchOffsetMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(FETCH_OFFSET_TOTAL_TIME, GROUP, FETCH_OFFSET_TOTAL_TIME_DOC),
            new MetricNameTemplate(FETCH_OFFSET_RATE, GROUP, FETCH_OFFSET_RATE_DOC),
            new MetricNameTemplate(FETCH_OFFSET_ERROR_RATE, GROUP, FETCH_OFFSET_ERROR_RATE_DOC)
        );
    }

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        InklessFetchOffsetMetrics.class.getPackageName(), InklessFetchOffsetMetrics.class.getSimpleName());

    private final Histogram fetchOffsetTimeHistogram;
    private final Meter fetchOffsetRate;
    private final Meter fetchOffsetErrorRate;

    public InklessFetchOffsetMetrics(Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fetchOffsetTimeHistogram = metricsGroup.newHistogram(FETCH_OFFSET_TOTAL_TIME, true, Map.of());
        fetchOffsetRate = metricsGroup.newMeter(FETCH_OFFSET_RATE, "fetches", java.util.concurrent.TimeUnit.SECONDS, Map.of());
        fetchOffsetErrorRate = metricsGroup.newMeter(FETCH_OFFSET_ERROR_RATE, "errors", java.util.concurrent.TimeUnit.SECONDS, Map.of());
    }

    public void fetchOffsetCompleted(final Instant startTime) {
        fetchOffsetTimeHistogram.update(Duration.between(startTime, TimeUtils.durationMeasurementNow(time)).toMillis());
        fetchOffsetRate.mark();
    }

    public void fetchOffsetFailed() {
        fetchOffsetErrorRate.mark();
    }

    public void close() {
        metricsGroup.removeMetric(FETCH_OFFSET_RATE);
        metricsGroup.removeMetric(FETCH_OFFSET_ERROR_RATE);
        metricsGroup.removeMetric(FETCH_OFFSET_TOTAL_TIME);
    }
}
