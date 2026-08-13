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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlPlaneAvailabilityTest {
    @Test
    void startsUnknownAndReadsAsAvailable() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());
            assertTrue(availability.isAvailable(),
                "an untried control plane must not block diskless work");
            assertNull(availability.unavailableReason());
        }
    }

    @Test
    void markUnavailableBlocksAndRecordsTheReason() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, availability.state());
            assertFalse(availability.isAvailable());
            assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED, availability.unavailableReason());
        }
    }

    @Test
    void markAvailableUnblocksAndClearsTheReason() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            availability.markAvailable();
            assertEquals(ControlPlaneAvailability.State.AVAILABLE, availability.state());
            assertTrue(availability.isAvailable());
            assertNull(availability.unavailableReason());
        }
    }

    @Test
    void markUnknownResetsToOptimisticAndClearsTheReason() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            availability.markUnknown();
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());
            assertTrue(availability.isAvailable());
            assertNull(availability.unavailableReason());
        }
    }

    @Test
    void gaugeReportsOneWhenAvailableAndZeroWhenNot() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            assertEquals(1, availability.state().metricValue());
            availability.markAvailable();
            assertEquals(1, availability.state().metricValue());
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            assertEquals(0, availability.state().metricValue());
        }
    }

    @Test
    void recordGatedCallDoesNotChangeState() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
            availability.recordGatedCall();
            assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, availability.state());
        }
    }

    @Test
    void recordGatedCallBeforeAnyFailureIsANoOp() {
        try (final ControlPlaneAvailability availability = new ControlPlaneAvailability()) {
            availability.recordGatedCall();
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());
        }
    }
}
