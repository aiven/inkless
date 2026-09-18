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
 * <p>Written only by the {@link ControlPlaneDelegateReconciler} that owns it, as a side effect of
 * building its delegate. Everyone else reads it, most of them through
 * {@link AvailabilityGatedControlPlane#availability()}.
 */
public class ControlPlaneAvailability implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControlPlaneAvailability.class);

    public enum State {
        /**
         * Nothing has been tried yet. Gates the same as {@link #UNAVAILABLE}: a caller that
         * buffered and uploaded against a control plane that turns out to have no delegate ready
         * would hit the same failure on the commit, and again on the follow-up
         * {@link ControlPlane#isSafeToDeleteFile} check, which orphans the uploaded object instead
         * of cleaning it up.
         */
        UNKNOWN,
        AVAILABLE,
        UNAVAILABLE
    }

    /** Why the gate is {@link State#UNAVAILABLE}. Meaningless in any other state. */
    public enum UnavailableReason {
        /**
         * No connection string is configured; the delegate factory raised a
         * {@link ControlPlaneNotConfiguredException}.
         */
        NOT_CONFIGURED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.UNKNOWN);
    private volatile UnavailableReason unavailableReason;
    private final ControlPlaneAvailabilityMetrics metrics;

    public ControlPlaneAvailability() {
        this.metrics = new ControlPlaneAvailabilityMetrics(state.get(), null);
    }

    public State state() {
        return state.get();
    }

    /** True if diskless work may proceed; false otherwise. An untried control plane reads false. */
    public boolean isAvailable() {
        return state.get() == State.AVAILABLE;
    }

    /** Why the gate is unavailable, or {@code null} if it isn't. */
    public UnavailableReason unavailableReason() {
        return unavailableReason;
    }

    public void markAvailable() {
        unavailableReason = null;
        set(State.AVAILABLE);
        metrics.updateAvailability(State.AVAILABLE, null);
    }

    public void markUnavailable(final UnavailableReason reason) {
        unavailableReason = Objects.requireNonNull(reason);
        set(State.UNAVAILABLE);
        metrics.updateAvailability(State.UNAVAILABLE, reason);
    }

    public void markUnknown() {
        unavailableReason = null;
        set(State.UNKNOWN);
        metrics.updateAvailability(State.UNKNOWN, null);
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
