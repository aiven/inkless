/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class ControlPlaneAvailabilityMetrics implements Closeable {
    private static final String GROUP = ControlPlaneAvailability.class.getSimpleName();

    static final String CONTROL_PLANE_AVAILABILITY = "ControlPlaneAvailability";
    private static final String CONTROL_PLANE_AVAILABILITY_DOC = "Externally reported availability of the diskless "
        + "control plane: 0 = available, 1 = initializing, 2 = offline";
    static final String CONTROL_PLANE_GATED_CALL_RATE = "ControlPlaneGatedCallRate";
    private static final String CONTROL_PLANE_GATED_CALL_RATE_DOC = "Total number of control-plane calls rejected "
        + "without contacting the control plane because it was reported unavailable";

    /**
     * This method returns a list of all the metric name templates for the ControlPlaneAvailabilityMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(CONTROL_PLANE_AVAILABILITY, GROUP, CONTROL_PLANE_AVAILABILITY_DOC),
            new MetricNameTemplate(CONTROL_PLANE_GATED_CALL_RATE, GROUP, CONTROL_PLANE_GATED_CALL_RATE_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        ControlPlaneAvailability.class.getPackageName(), ControlPlaneAvailability.class.getSimpleName());
    private final LongAdder gatedCallRate = new LongAdder();

    public ControlPlaneAvailabilityMetrics(final Supplier<ControlPlaneAvailability.State> state) {
        metricsGroup.newGauge(CONTROL_PLANE_AVAILABILITY, () -> state.get().ordinal());
        metricsGroup.newGauge(CONTROL_PLANE_GATED_CALL_RATE, gatedCallRate::intValue);
    }

    public void recordGatedCall() {
        gatedCallRate.increment();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CONTROL_PLANE_AVAILABILITY);
        metricsGroup.removeMetric(CONTROL_PLANE_GATED_CALL_RATE);
    }
}
