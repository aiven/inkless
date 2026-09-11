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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks whether the diskless control plane is reachable, for gating and for metrics.
 *
 * <p>Written only by the {@link AvailabilityGatedControlPlane} that owns it, as a side effect of
 * resolving its delegate. Everyone else reads it.
 */
public class ControlPlaneAvailability implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControlPlaneAvailability.class);

    public enum State {
        /** Nothing has been tried yet, so gating would be premature. */
        UNKNOWN(1),
        AVAILABLE(1),
        UNAVAILABLE(0);

        private final int metricValue;

        State(final int metricValue) {
            this.metricValue = metricValue;
        }

        /** Returns the value the availability gauge reports: 1 when available, 0 otherwise. */
        public int metricValue() {
            return metricValue;
        }
    }

    /** Why the gate is {@link State#UNAVAILABLE}. Meaningless in any other state. */
    public enum UnavailableReason {
        /** No connection string is configured; the delegate factory raised a {@code ConfigException}. */
        NOT_CONFIGURED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.UNKNOWN);
    private volatile UnavailableReason unavailableReason;
    private final ControlPlaneAvailabilityMetrics metrics;

    public ControlPlaneAvailability() {
        this.metrics = new ControlPlaneAvailabilityMetrics(this.state::get);
    }

    public State state() {
        return state.get();
    }

    /** True if diskless work may proceed; false otherwise. An untried control plane reads true. */
    public boolean isAvailable() {
        return state.get() != State.UNAVAILABLE;
    }

    /** Why the gate is unavailable, or {@code null} if it isn't. */
    public UnavailableReason unavailableReason() {
        return unavailableReason;
    }

    public void markAvailable() {
        unavailableReason = null;
        set(State.AVAILABLE);
    }

    public void markUnavailable(final UnavailableReason reason) {
        unavailableReason = Objects.requireNonNull(reason);
        set(State.UNAVAILABLE);
    }

    public void markUnknown() {
        unavailableReason = null;
        set(State.UNKNOWN);
    }

    private void set(final State newState) {
        final State previous = state.getAndSet(newState);
        if (previous != newState) {
            LOGGER.warn("Diskless control plane availability changed from {} to {}", previous, newState);
        }
    }

    /** Records a call rejected without contacting the control plane, tagged by {@link #unavailableReason()}. */
    public void recordGatedCall() {
        metrics.recordGatedCall(unavailableReason);
    }

    @Override
    public void close() {
        metrics.close();
    }
}
