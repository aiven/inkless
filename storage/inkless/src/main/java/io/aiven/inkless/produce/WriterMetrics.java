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

import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
public class WriterMetrics implements Closeable {
    public static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup(WriterMetrics.class);

    public static final String REQUEST_RATE = "RequestRate";
    public static final String REQUEST_RATE_DESCRIPTION = "The number of write requests per unit time.";

    public static final String ROTATION_RATE = "RotationRate";
    public static final String ROTATION_RATE_DESCRIPTION = "The number of file rotations per unit time.";

    public static final String ROTATION_TIME = "RotationTime";
    public static final String ROTATION_TIME_DESCRIPTION = "The time taken to rotate a file, in milliseconds.";

    private final MetricName requestRateMetricName;
    private final MetricName rotationRateMetricName;
    private final MetricName rotationTimeMetricName;

    private final Histogram rotationTime;

    final Time time;
    final LongAdder requests = new LongAdder();
    final LongAdder rotations = new LongAdder();

    public WriterMetrics(final Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");

        this.requestRateMetricName = METRICS_GROUP.metricName(REQUEST_RATE, Map.of());
        METRICS_GROUP.newGauge(this.requestRateMetricName, requests::intValue);
        this.rotationRateMetricName = METRICS_GROUP.metricName(ROTATION_RATE, Map.of());
        METRICS_GROUP.newGauge(this.rotationRateMetricName, rotations::intValue);
        this.rotationTimeMetricName = METRICS_GROUP.metricName(ROTATION_TIME, Map.of());
        rotationTime = METRICS_GROUP.newHistogram(this.rotationTimeMetricName, true, Map.of());
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
        METRICS_GROUP.removeMetric(REQUEST_RATE);
        METRICS_GROUP.removeMetric(ROTATION_RATE);
        METRICS_GROUP.removeMetric(ROTATION_TIME);
    }

    public List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(
                requestRateMetricName.getName(),
                requestRateMetricName.getGroup(),
                REQUEST_RATE_DESCRIPTION
            ),
            new MetricNameTemplate(
                rotationRateMetricName.getName(),
                rotationRateMetricName.getGroup(),
                ROTATION_RATE_DESCRIPTION
            ),
            new MetricNameTemplate(
                rotationTimeMetricName.getName(),
                rotationTimeMetricName.getGroup(),
                ROTATION_TIME_DESCRIPTION
            )
        );
    }
}