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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AvailabilityGatedControlPlaneTest {
    private final ControlPlane delegate = Mockito.mock(ControlPlane.class);
    private final AtomicInteger factoryInvocations = new AtomicInteger();

    private AvailabilityGatedControlPlane gated(final ControlPlaneAvailability.State state) {
        return gated(new ControlPlaneAvailability(state));
    }

    private AvailabilityGatedControlPlane gated(final ControlPlaneAvailability availability) {
        return new AvailabilityGatedControlPlane(
            () -> {
                factoryInvocations.incrementAndGet();
                return delegate;
            },
            availability);
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
        assertEquals(0, factoryInvocations.get(),
            "the delegate must never be created while the control plane is reported unavailable");
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
        assertEquals(1, factoryInvocations.get(), "the delegate must be created only once and reused");
    }

    @ParameterizedTest
    @EnumSource(ControlPlaneAvailability.State.class)
    void configureIsANoOp(final ControlPlaneAvailability.State state) {
        final AvailabilityGatedControlPlane controlPlane = gated(state);

        controlPlane.configure(Map.of());

        verifyNoInteractions(delegate);
        assertEquals(0, factoryInvocations.get());
    }

    @Test
    void closeIsANoOpWhenTheDelegateWasNeverCreated() throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated(ControlPlaneAvailability.State.OFFLINE);

        controlPlane.close();

        verifyNoInteractions(delegate);
    }

    @Test
    void closeClosesTheDelegateOnceCreated() throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated(ControlPlaneAvailability.State.AVAILABLE);
        controlPlane.deleteTopics(Set.of());

        controlPlane.close();

        verify(delegate).close();
    }

    @Test
    void neverCreatesTheDelegateWhileConstructedUnavailable() {
        gated(ControlPlaneAvailability.State.OFFLINE);
        gated(ControlPlaneAvailability.State.INITIALIZING);

        assertEquals(0, factoryInvocations.get());
    }

    @Test
    void createsTheDelegateLazilyOnceAvailabilityIsRegained() {
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.INITIALIZING);
        final AvailabilityGatedControlPlane controlPlane = gated(availability);

        assertEquals(0, factoryInvocations.get());

        availability.set(ControlPlaneAvailability.State.AVAILABLE);
        controlPlane.deleteTopics(Set.of());

        assertEquals(1, factoryInvocations.get());
        verify(delegate).deleteTopics(Set.of());
    }

    @Test
    void closesTheDelegateWhenBecomingUnavailableAndRecreatesItWhenAvailableAgain() {
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.AVAILABLE);
        final AvailabilityGatedControlPlane controlPlane = gated(availability);
        controlPlane.deleteTopics(Set.of());
        assertEquals(1, factoryInvocations.get());

        availability.set(ControlPlaneAvailability.State.OFFLINE);

        try {
            verify(delegate).close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        assertThrows(ControlPlaneException.class, () -> controlPlane.deleteTopics(Set.of()));
        assertEquals(1, factoryInvocations.get(), "must not recreate the delegate while still unavailable");

        availability.set(ControlPlaneAvailability.State.AVAILABLE);
        controlPlane.deleteTopics(Set.of());

        assertEquals(2, factoryInvocations.get(), "must recreate a fresh delegate once available again");
    }

    @Test
    void becomingUnavailableDoesNotWaitForInFlightCalls() throws Exception {
        final CountDownLatch callStarted = new CountDownLatch(1);
        final CountDownLatch releaseCall = new CountDownLatch(1);
        when(delegate.listOffsets(List.of())).thenAnswer(invocation -> {
            callStarted.countDown();
            assertTrue(releaseCall.await(30, TimeUnit.SECONDS), "the call was never released");
            return List.of();
        });
        final ControlPlaneAvailability availability =
            new ControlPlaneAvailability(ControlPlaneAvailability.State.AVAILABLE);
        final AvailabilityGatedControlPlane controlPlane = gated(availability);

        final Thread caller = new Thread(() -> controlPlane.listOffsets(List.of()), "in-flight-call");
        caller.start();
        assertTrue(callStarted.await(30, TimeUnit.SECONDS), "the call never reached the delegate");

        assertTimeoutPreemptively(Duration.ofSeconds(5),
            () -> availability.set(ControlPlaneAvailability.State.OFFLINE),
            "reporting the control plane unavailable must not block on the in-flight call");
        verify(delegate).close();

        releaseCall.countDown();
        caller.join(TimeUnit.SECONDS.toMillis(30));
    }

    @Test
    void rejectsCallsOnceClosed() throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated(ControlPlaneAvailability.State.AVAILABLE);

        controlPlane.close();

        assertThrows(ControlPlaneException.class, () -> controlPlane.listOffsets(List.of()));
        verifyNoInteractions(delegate);
        assertEquals(0, factoryInvocations.get(), "must not create a delegate after close");
    }
}
