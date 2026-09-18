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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The single {@code ControlPlaneAvailability} gauge is re-registered on every transition rather than
 * read live, since Yammer gauges carry fixed tags. That re-registration needs its own coverage.
 */
class ControlPlaneAvailabilityMetricsTest {
    private KafkaMetricsGroup metricsGroup;
    private ControlPlaneAvailabilityMetrics metrics;

    @BeforeEach
    void setup() {
        metricsGroup = mock(KafkaMetricsGroup.class);
        metrics = new ControlPlaneAvailabilityMetrics(
            ControlPlaneAvailability.State.UNKNOWN, null, metricsGroup);
    }

    @SuppressWarnings("unchecked")
    private int registeredValue(final ControlPlaneAvailability.State state, final String reason) {
        final ArgumentCaptor<Supplier<Integer>> captor = ArgumentCaptor.forClass(Supplier.class);
        verify(metricsGroup).newGauge(eq(ControlPlaneAvailabilityMetrics.CONTROL_PLANE_AVAILABILITY),
            captor.capture(), eq(Map.of(
                ControlPlaneAvailabilityMetrics.STATE_TAG, state.name(),
                ControlPlaneAvailabilityMetrics.REASON_TAG, reason)));
        return captor.getValue().get();
    }

    @Test
    void registersTheInitialStateWithAnEmptyReason() {
        assertThat(registeredValue(ControlPlaneAvailability.State.UNKNOWN, "")).isEqualTo(0);
    }

    @Test
    void updateReRegistersWithTheNewTagsAndValue() {
        metrics.updateAvailability(ControlPlaneAvailability.State.AVAILABLE, null);
        verify(metricsGroup).removeMetric(ControlPlaneAvailabilityMetrics.CONTROL_PLANE_AVAILABILITY, Map.of(
            ControlPlaneAvailabilityMetrics.STATE_TAG, ControlPlaneAvailability.State.UNKNOWN.name(),
            ControlPlaneAvailabilityMetrics.REASON_TAG, ""));
        assertThat(registeredValue(ControlPlaneAvailability.State.AVAILABLE, "")).isEqualTo(1);

        metrics.updateAvailability(ControlPlaneAvailability.State.UNAVAILABLE,
            ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
        verify(metricsGroup).removeMetric(ControlPlaneAvailabilityMetrics.CONTROL_PLANE_AVAILABILITY, Map.of(
            ControlPlaneAvailabilityMetrics.STATE_TAG, ControlPlaneAvailability.State.AVAILABLE.name(),
            ControlPlaneAvailabilityMetrics.REASON_TAG, ""));
        assertThat(registeredValue(ControlPlaneAvailability.State.UNAVAILABLE, "NOT_CONFIGURED")).isEqualTo(0);
    }

    /**
     * The JMX object name is built by iterating the tags without sorting, so their order is part of
     * the metric's identity. An immutable set or map of more than one element iterates in an order
     * salted per JVM, which would rename the MBean between broker restarts and make the generated
     * metrics documentation differ run to run.
     */
    @Test
    void availabilityTagsKeepAFixedOrder() {
        final MetricNameTemplate availability = ControlPlaneAvailabilityMetrics.all().stream()
            .filter(t -> t.name().equals(ControlPlaneAvailabilityMetrics.CONTROL_PLANE_AVAILABILITY))
            .findFirst()
            .orElseThrow();

        assertThat(availability.tags()).containsExactly(
            ControlPlaneAvailabilityMetrics.STATE_TAG, ControlPlaneAvailabilityMetrics.REASON_TAG);
    }

    @Test
    void registeredAvailabilityTagsKeepTheSameOrderAsTheTemplate() {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(metricsGroup).newGauge(eq(ControlPlaneAvailabilityMetrics.CONTROL_PLANE_AVAILABILITY),
            any(), captor.capture());

        assertThat(captor.getValue().keySet()).containsExactly(
            ControlPlaneAvailabilityMetrics.STATE_TAG, ControlPlaneAvailabilityMetrics.REASON_TAG);
    }
}
