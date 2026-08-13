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
 * Tracks whether the diskless control plane is reachable, for gating, and for metrics.
 *
 * <p>Written only by the {@link ControlPlaneDelegateReconciler} that owns it, as a side effect of
 * building its delegate. Everyone else reads it, most of them through
 * {@link AvailabilityGatedControlPlane#availability()}.
 */
public class ControlPlaneAvailability implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControlPlaneAvailability.class);

    public enum State {
        /**
         * No attempt has happened yet. Callers treat it like {@link #UNAVAILABLE}. A caller that
         * buffered and uploaded against a control plane with no delegate ready would hit the same
         * failure on the commit. It would hit it again on the follow-up
         * {@link ControlPlane#isSafeToDeleteFile} check, so the uploaded object is orphaned.
         */
        UNKNOWN,
        AVAILABLE,
        UNAVAILABLE
    }

    /**
     * Reason a gated call was rejected.
     */
    public enum UnavailableReason {
        /**
         * No connection string is configured; the delegate factory raised a
         * {@link ControlPlaneNotConfiguredException}.
         */
        NOT_CONFIGURED,
        /**
         * The state is {@link State#UNKNOWN}: no delegate has settled for the current generation
         * yet. Causes include an unfinished first build, a reconfiguration that retired the
         * previous delegate and is rebuilding, and a transient failure of the last attempt with a
         * retry pending.
         */
        NOT_READY
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

    /** Reason the gate is unavailable, or {@code null} if it isn't. */
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

    /**
     * Records a call rejected without contacting the control plane, tagged by
     * {@link #unavailableReason()}, or {@link UnavailableReason#NOT_READY} while
     * {@link State#UNKNOWN}, where there is no reason to report yet.
     */
    public void recordGatedCall() {
        metrics.recordGatedCall(unavailableReason == null ? UnavailableReason.NOT_READY : unavailableReason);
    }

    @Override
    public void close() {
        metrics.close();
    }
}
