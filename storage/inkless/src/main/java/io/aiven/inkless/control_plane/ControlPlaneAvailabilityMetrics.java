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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

public class ControlPlaneAvailabilityMetrics implements Closeable {
    private static final String GROUP = ControlPlaneAvailability.class.getSimpleName();

    static final String CONTROL_PLANE_AVAILABILITY = "ControlPlaneAvailability";
    private static final String CONTROL_PLANE_AVAILABILITY_DOC = "Whether the diskless control plane is available: "
        + "1 = available, 0 = unavailable. The state tag identifies the current state, for example AVAILABLE. The "
        + "reason tag identifies why the control plane is unavailable, for example NOT_CONFIGURED, or an empty "
        + "string when it is not unavailable";
    static final String CONTROL_PLANE_GATED_CALL_RATE = "ControlPlaneGatedCallRate";
    private static final String CONTROL_PLANE_GATED_CALL_RATE_DOC = "Total number of control-plane calls rejected "
        + "without contacting the control plane. The reason tag identifies why, for example NOT_CONFIGURED";
    static final String STATE_TAG = "state";
    static final String REASON_TAG = "reason";

    /**
     * The availability tag names, in the order they appear in the JMX object name.
     *
     * <p>The order is fixed deliberately, and must not come from {@code Set.of}: both the object
     * name and the generated documentation are built by iterating these tags without sorting, and
     * the iteration order of an immutable set of more than one element is salted per JVM. Letting
     * it vary would rename the MBean from one broker restart to the next.
     */
    private static final Set<String> AVAILABILITY_TAGS =
        new LinkedHashSet<>(List.of(STATE_TAG, REASON_TAG));

    /**
     * This method returns a list of all the metric name templates for the ControlPlaneAvailabilityMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(CONTROL_PLANE_AVAILABILITY, GROUP, CONTROL_PLANE_AVAILABILITY_DOC,
                AVAILABILITY_TAGS),
            new MetricNameTemplate(CONTROL_PLANE_GATED_CALL_RATE, GROUP, CONTROL_PLANE_GATED_CALL_RATE_DOC,
                Set.of(REASON_TAG))
        );
    }

    private final KafkaMetricsGroup metricsGroup;
    private final Map<ControlPlaneAvailability.UnavailableReason, LongAdder> gatedCallRate =
        new EnumMap<>(ControlPlaneAvailability.UnavailableReason.class);
    private Map<String, String> availabilityTags;

    public ControlPlaneAvailabilityMetrics(final ControlPlaneAvailability.State initialState,
                                            final ControlPlaneAvailability.UnavailableReason initialReason) {
        this(initialState, initialReason, new KafkaMetricsGroup(
            ControlPlaneAvailability.class.getPackageName(), ControlPlaneAvailability.class.getSimpleName()));
    }

    ControlPlaneAvailabilityMetrics(final ControlPlaneAvailability.State initialState,
                                     final ControlPlaneAvailability.UnavailableReason initialReason,
                                     final KafkaMetricsGroup metricsGroup) {
        this.metricsGroup = metricsGroup;
        registerAvailability(initialState, initialReason);
        for (final ControlPlaneAvailability.UnavailableReason reason
            : ControlPlaneAvailability.UnavailableReason.values()) {
            final LongAdder counter = new LongAdder();
            gatedCallRate.put(reason, counter);
            metricsGroup.newGauge(CONTROL_PLANE_GATED_CALL_RATE, counter::longValue,
                Map.of(REASON_TAG, reason.name()));
        }
    }

    /**
     * Re-registers {@code ControlPlaneAvailability} so its tags match the given state and reason.
     *
     * <p>Yammer gauges carry fixed tags, so reflecting a state or reason change means removing the
     * previous registration and adding a new one rather than mutating one in place.
     */
    public synchronized void updateAvailability(final ControlPlaneAvailability.State state,
                                                  final ControlPlaneAvailability.UnavailableReason reason) {
        metricsGroup.removeMetric(CONTROL_PLANE_AVAILABILITY, availabilityTags);
        registerAvailability(state, reason);
    }

    private void registerAvailability(final ControlPlaneAvailability.State state,
                                       final ControlPlaneAvailability.UnavailableReason reason) {
        // A LinkedHashMap, not Map.of, so the tags land in AVAILABILITY_TAGS order. See there.
        final Map<String, String> tags = new LinkedHashMap<>();
        tags.put(STATE_TAG, state.name());
        tags.put(REASON_TAG, reason == null ? "" : reason.name());
        availabilityTags = tags;
        final int value = state == ControlPlaneAvailability.State.AVAILABLE ? 1 : 0;
        metricsGroup.newGauge(CONTROL_PLANE_AVAILABILITY, () -> value, availabilityTags);
    }

    /** No-op if {@code reason} is {@code null}: nothing has been marked unavailable yet. */
    public void recordGatedCall(final ControlPlaneAvailability.UnavailableReason reason) {
        if (reason != null) {
            gatedCallRate.get(reason).increment();
        }
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CONTROL_PLANE_AVAILABILITY, availabilityTags);
        for (final ControlPlaneAvailability.UnavailableReason reason
            : ControlPlaneAvailability.UnavailableReason.values()) {
            metricsGroup.removeMetric(CONTROL_PLANE_GATED_CALL_RATE, Map.of(REASON_TAG, reason.name()));
        }
    }
}
