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
package io.aiven.inkless.common.metrics;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

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
