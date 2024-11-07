// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import com.groupcdg.pitest.annotations.CoverageIgnore;

/**
 * Inspired by <a href="https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/SensorBuilder.java">SensorBuilder</a>
 */
@CoverageIgnore  // tested on integration level
public class SensorProvider {
    private final Metrics metrics;

    private final Sensor sensor;

    private final boolean preexisting;

    private final Map<String, String> tags;

    public SensorProvider(final Metrics metrics,
                          final String name) {
        this(metrics, name, Collections::emptyMap, Sensor.RecordingLevel.INFO);
    }

    public SensorProvider(final Metrics metrics,
                          final String name,
                          final Sensor.RecordingLevel recordingLevel) {
        this(metrics, name, Collections::emptyMap, recordingLevel);
    }

    public SensorProvider(final Metrics metrics,
                          final String name,
                          final Supplier<Map<String, String>> tagsSupplier) {
        this(metrics, name, tagsSupplier, Sensor.RecordingLevel.INFO);
    }

    public SensorProvider(final Metrics metrics,
                          final String name,
                          final Supplier<Map<String, String>> tagsSupplier,
                          final Sensor.RecordingLevel recordingLevel) {
        this.metrics = metrics;
        final Sensor s = metrics.getSensor(name);

        if (s != null) {
            sensor = s;
            tags = Collections.emptyMap();
            preexisting = true;
        } else {
            sensor = metrics.sensor(name, recordingLevel);
            tags = tagsSupplier.get();
            preexisting = false;
        }
    }

    public SensorProvider with(final MetricNameTemplate name, final MeasurableStat stat) {
        if (!preexisting) {
            sensor.add(metrics.metricInstance(name, tags), stat);
        }

        return this;
    }

    public Sensor get() {
        return sensor;
    }
}
