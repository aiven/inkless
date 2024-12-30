// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
public class WriterMetrics implements Closeable {
    public static final String REQUEST_RATE = "RequestRate";
    public static final String ROTATION_RATE = "RotationRate";
    public static final String ROTATION_TIME = "RotationTime";
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(WriterMetrics.class);
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
