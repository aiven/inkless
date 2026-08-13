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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.config.ServerConfigs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The availability of the control plane as reported by the deployment's management plane.
 *
 * <p>This is a hint supplied from outside the broker, not something the broker measures. It is
 * trusted verbatim: nothing here probes the control plane and nothing expires the value, so a
 * value of {@link State#OFFLINE} keeps failing diskless work until whoever set it resets it.
 */
public class ControlPlaneAvailability implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControlPlaneAvailability.class);

    public enum State {
        AVAILABLE(ServerConfigs.DISKLESS_CONTROL_PLANE_AVAILABILITY_AVAILABLE),
        INITIALIZING(ServerConfigs.DISKLESS_CONTROL_PLANE_AVAILABILITY_INITIALIZING),
        OFFLINE(ServerConfigs.DISKLESS_CONTROL_PLANE_AVAILABILITY_OFFLINE);

        private final String configValue;

        State(final String configValue) {
            this.configValue = configValue;
        }

        public String configValue() {
            return configValue;
        }

        public static State fromConfig(final String value) {
            for (final State state : values()) {
                if (state.configValue.equals(value)) {
                    return state;
                }
            }
            throw new ConfigException(
                ServerConfigs.DISKLESS_CONTROL_PLANE_AVAILABILITY_CONFIG, value, "Unknown availability value.");
        }
    }

    private final AtomicReference<State> state;
    private final ControlPlaneAvailabilityMetrics metrics;

    public ControlPlaneAvailability(final State initialState) {
        this.state = new AtomicReference<>(initialState);
        this.metrics = new ControlPlaneAvailabilityMetrics(this.state::get);
    }

    public State state() {
        return state.get();
    }

    public boolean isAvailable() {
        return state.get() == State.AVAILABLE;
    }

    public void set(final State newState) {
        final State previous = state.getAndSet(newState);
        if (previous != newState) {
            LOGGER.warn("Diskless control plane availability changed from {} to {}",
                previous.configValue(), newState.configValue());
        }
    }

    public void recordGatedCall() {
        metrics.recordGatedCall();
    }

    @Override
    public void close() {
        metrics.close();
    }
}
