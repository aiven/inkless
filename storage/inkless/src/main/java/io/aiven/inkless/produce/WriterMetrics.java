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
package io.aiven.inkless.produce;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
public class WriterMetrics implements Closeable {
    private static final String GROUP = WriterMetrics.class.getSimpleName();

    public static final String REQUEST_RATE = "RequestRate";
    private static final String REQUEST_RATE_DOC = "Rate of produce requests received";
    public static final String ROTATION_RATE = "RotationRate";
    private static final String ROTATION_RATE_DOC = "Rate of file rotations performed";
    public static final String ROTATION_TIME = "RotationTime";
    private static final String ROTATION_TIME_DOC = "Time spent rotating files in milliseconds";

    /**
     * This method returns a list of all the metric name templates for the WriterMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(REQUEST_RATE, GROUP, REQUEST_RATE_DOC),
            new MetricNameTemplate(ROTATION_RATE, GROUP, ROTATION_RATE_DOC),
            new MetricNameTemplate(ROTATION_TIME, GROUP, ROTATION_TIME_DOC)
        );
    }
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        WriterMetrics.class.getPackageName(), WriterMetrics.class.getSimpleName());
    private final Histogram rotationTime;

    final Time time;
    final LongAdder requests = new LongAdder();
    final LongAdder rotations = new LongAdder();
    public WriterMetrics(final Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");

        metricsGroup.newGauge(REQUEST_RATE, requests::intValue);
        metricsGroup.newGauge(ROTATION_RATE, rotations::intValue);
        rotationTime = metricsGroup.newHistogram(ROTATION_TIME, true, Map.of());
    }

    public void requestAdded() {
        requests.increment();
    }

    public void fileRotated(Instant openedAt) {
        final Instant now = TimeUtils.durationMeasurementNow(time);
        rotations.increment();
        rotationTime.update(Duration.between(openedAt, now).toMillis());
    }

    @Override
    public void close() throws IOException {
        metricsGroup.removeMetric(REQUEST_RATE);
        metricsGroup.removeMetric(ROTATION_RATE);
        metricsGroup.removeMetric(ROTATION_TIME);
    }
}
