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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class AvailabilityGatedControlPlaneTest {
    private final ControlPlane delegate = Mockito.mock(ControlPlane.class);

    private AvailabilityGatedControlPlane gated(final ControlPlaneAvailability.State state) {
        return new AvailabilityGatedControlPlane(delegate, new ControlPlaneAvailability(state));
    }

    @ParameterizedTest
    @EnumSource(value = ControlPlaneAvailability.State.class, names = {"INITIALIZING", "OFFLINE"})
    void gatedStatesRejectWithoutTouchingTheDelegate(final ControlPlaneAvailability.State state) {
        final AvailabilityGatedControlPlane controlPlane = gated(state);

        assertThrows(ControlPlaneException.class, () -> controlPlane.findBatches(List.of(), 1, 1));
        assertThrows(ControlPlaneException.class, () -> controlPlane.createTopicAndPartitions(Set.of()));
        assertThrows(ControlPlaneException.class, () -> controlPlane.deleteTopics(Set.of()));
        assertThrows(ControlPlaneException.class, () -> controlPlane.enforceRetention(List.of(), 1));
        assertThrows(ControlPlaneException.class, () -> controlPlane.getFilesToDelete(Instant.now(), 1));
        assertThrows(ControlPlaneException.class, () -> controlPlane.listOffsets(List.of()));
        assertThrows(ControlPlaneException.class, () -> controlPlane.initDisklessLog(List.of()));
        assertThrows(ControlPlaneException.class, () -> controlPlane.isSafeToDeleteFile("key"));

        verifyNoInteractions(delegate);
    }

    @Test
    void availableStateDelegates() {
        final AvailabilityGatedControlPlane controlPlane = gated(ControlPlaneAvailability.State.AVAILABLE);

        controlPlane.createTopicAndPartitions(Set.of());
        controlPlane.deleteTopics(Set.of());
        controlPlane.listOffsets(List.of());

        verify(delegate).createTopicAndPartitions(Set.of());
        verify(delegate).deleteTopics(Set.of());
        verify(delegate).listOffsets(List.of());
    }

    @ParameterizedTest
    @EnumSource(ControlPlaneAvailability.State.class)
    void lifecycleMethodsAreNeverGated(final ControlPlaneAvailability.State state) throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated(state);

        controlPlane.configure(Map.of());
        controlPlane.close();

        verify(delegate).configure(Map.of());
        verify(delegate).close();
    }
}
