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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlPlaneAvailabilityTest {
    @Test
    void defaultsToTheGivenInitialState() {
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.AVAILABLE);
        assertEquals(ControlPlaneAvailability.State.AVAILABLE, availability.state());
        assertTrue(availability.isAvailable());
    }

    @Test
    void onlyAvailableCountsAsAvailable() {
        assertFalse(new ControlPlaneAvailability(ControlPlaneAvailability.State.INITIALIZING).isAvailable());
        assertFalse(new ControlPlaneAvailability(ControlPlaneAvailability.State.OFFLINE).isAvailable());
    }

    @Test
    void setChangesTheState() {
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.AVAILABLE);
        availability.set(ControlPlaneAvailability.State.OFFLINE);
        assertEquals(ControlPlaneAvailability.State.OFFLINE, availability.state());
        assertFalse(availability.isAvailable());
        availability.set(ControlPlaneAvailability.State.AVAILABLE);
        assertTrue(availability.isAvailable());
    }

    @Test
    void fromConfigParsesEveryValidValue() {
        assertEquals(ControlPlaneAvailability.State.AVAILABLE,
            ControlPlaneAvailability.State.fromConfig("available"));
        assertEquals(ControlPlaneAvailability.State.INITIALIZING,
            ControlPlaneAvailability.State.fromConfig("initializing"));
        assertEquals(ControlPlaneAvailability.State.OFFLINE,
            ControlPlaneAvailability.State.fromConfig("offline"));
    }

    @Test
    void fromConfigRejectsUnknownValue() {
        assertThrows(ConfigException.class, () -> ControlPlaneAvailability.State.fromConfig("powered_off"));
    }

    @Test
    void configValueRoundTrips() {
        for (final ControlPlaneAvailability.State state : ControlPlaneAvailability.State.values()) {
            assertEquals(state, ControlPlaneAvailability.State.fromConfig(state.configValue()));
        }
    }

    @Test
    void onChangeListenerFiresOnTransition() {
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.AVAILABLE);
        final List<ControlPlaneAvailability.State> seen = new ArrayList<>();
        availability.onChange(seen::add);

        availability.set(ControlPlaneAvailability.State.OFFLINE);

        assertEquals(List.of(ControlPlaneAvailability.State.OFFLINE), seen);
    }

    @Test
    void onChangeListenerDoesNotFireWhenStateIsUnchanged() {
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.AVAILABLE);
        final List<ControlPlaneAvailability.State> seen = new ArrayList<>();
        availability.onChange(seen::add);

        availability.set(ControlPlaneAvailability.State.AVAILABLE);

        assertTrue(seen.isEmpty());
    }
}
