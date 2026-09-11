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
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.aiven.inkless.config.InklessConfig;

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
    private final AtomicReference<RuntimeException> factoryFailure = new AtomicReference<>();

    private static final InklessConfig CONFIG = new InklessConfig(Map.of(
        "control.plane.class", InMemoryControlPlane.class.getCanonicalName()));

    private final Function<InklessConfig, ControlPlane> factory = config -> {
        factoryInvocations.incrementAndGet();
        final RuntimeException failure = factoryFailure.get();
        if (failure != null) {
            throw failure;
        }
        return delegate;
    };

    private AvailabilityGatedControlPlane gated() {
        return new AvailabilityGatedControlPlane(() -> CONFIG, factory);
    }

    @Test
    void createsTheDelegateLazilyAndReusesIt() throws Exception {
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            assertEquals(0, factoryInvocations.get(),
                "construction alone must not create a delegate");

            controlPlane.createTopicAndPartitions(Set.of());
            controlPlane.deleteTopics(Set.of());
            controlPlane.listOffsets(List.of());

            verify(delegate).createTopicAndPartitions(Set.of());
            verify(delegate).deleteTopics(Set.of());
            verify(delegate).listOffsets(List.of());
            assertEquals(1, factoryInvocations.get(), "the delegate must be created once and reused");
            assertEquals(ControlPlaneAvailability.State.AVAILABLE,
                controlPlane.availability().state());
        }
    }

    @Test
    void configExceptionMarksUnavailableAndFailsEveryCall() throws Exception {
        factoryFailure.set(new ConfigException("connection.string", "", "Missing"));
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.findBatches(List.of(), 1, 1));

            assertEquals(ControlPlaneAvailability.State.UNAVAILABLE,
                controlPlane.availability().state());
            assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED,
                controlPlane.availability().unavailableReason());

            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.createTopicAndPartitions(Set.of()));
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.deleteTopics(Set.of()));
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.enforceRetention(List.of(), 1));
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.getFilesToDelete(Instant.now(), 1));
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.listOffsets(List.of()));
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.initDisklessLog(List.of()));
            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.isSafeToDeleteFile("key"));

            verifyNoInteractions(delegate);
            assertEquals(1, factoryInvocations.get(),
                "once known unavailable, later calls must fast-fail without rebuilding the delegate");
        }
    }

    @Test
    void otherFailuresLeaveAvailabilityUnknown() throws Exception {
        factoryFailure.set(new IllegalStateException("connection refused"));
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            assertThrows(IllegalStateException.class, () -> controlPlane.listOffsets(List.of()));

            assertEquals(ControlPlaneAvailability.State.UNKNOWN,
                controlPlane.availability().state(),
                "an unreachable control plane must not be reported as unconfigured");
        }
    }

    @Test
    void invalidateClosesTheDelegateAndRebuildsFromTheNewConfig() throws Exception {
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            controlPlane.listOffsets(List.of());
            assertEquals(1, factoryInvocations.get());

            controlPlane.invalidate();

            verify(delegate).close();
            assertEquals(ControlPlaneAvailability.State.UNKNOWN,
                controlPlane.availability().state());

            controlPlane.listOffsets(List.of());
            assertEquals(2, factoryInvocations.get(), "must rebuild after invalidation");
        }
    }

    @Test
    void takeOutOfServiceClosesTheDelegateAndMarksUnavailableImmediately() throws Exception {
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            controlPlane.listOffsets(List.of());
            assertEquals(1, factoryInvocations.get());

            controlPlane.takeOutOfService(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);

            verify(delegate).close();
            assertEquals(ControlPlaneAvailability.State.UNAVAILABLE,
                controlPlane.availability().state());
            assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED,
                controlPlane.availability().unavailableReason());

            assertThrows(ControlPlaneUnavailableException.class,
                () -> controlPlane.listOffsets(List.of()));
            assertEquals(1, factoryInvocations.get(),
                "a gate taken out of service must fast-fail without rebuilding the delegate");
        }
    }

    @Test
    void invalidateOnUnconfiguredGateIsANoOp() throws Exception {
        factoryFailure.set(new ConfigException("connection.string", "", "Missing"));
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.listOffsets(List.of()));

            controlPlane.invalidate();

            verifyNoInteractions(delegate);
            assertEquals(ControlPlaneAvailability.State.UNKNOWN,
                controlPlane.availability().state());
        }
    }

    @Test
    void configureIsANoOp() throws Exception {
        try (final AvailabilityGatedControlPlane controlPlane = gated()) {
            controlPlane.configure(Map.of());

            verifyNoInteractions(delegate);
            assertEquals(0, factoryInvocations.get());
        }
    }

    @Test
    void closeIsANoOpWhenTheDelegateWasNeverCreated() throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated();

        controlPlane.close();

        verifyNoInteractions(delegate);
    }

    @Test
    void closeClosesTheDelegateOnceCreated() throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated();
        controlPlane.deleteTopics(Set.of());

        controlPlane.close();

        verify(delegate).close();
    }

    @Test
    void rejectsCallsOnceClosed() throws Exception {
        final AvailabilityGatedControlPlane controlPlane = gated();

        controlPlane.close();

        assertThrows(ControlPlaneException.class, () -> controlPlane.listOffsets(List.of()));
        verifyNoInteractions(delegate);
        assertEquals(0, factoryInvocations.get(), "must not create a delegate after close");
    }

    @Test
    void closedTakesPrecedenceOverAnAlreadyUnavailableGate() throws Exception {
        factoryFailure.set(new ConfigException("connection.string", "", "Missing"));
        final AvailabilityGatedControlPlane controlPlane = gated();
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.listOffsets(List.of()));

        controlPlane.close();

        // The fast-fail path for a known-unavailable gate must not shadow "closed": once closed,
        // that's the exception every caller sees, not the older "not configured" one.
        final ControlPlaneException e = assertThrows(ControlPlaneException.class, () -> controlPlane.listOffsets(List.of()));
        assertEquals("Control plane is closed", e.getMessage());
    }

    @Test
    void invalidateDoesNotWaitForInFlightCalls() throws Exception {
        final CountDownLatch callStarted = new CountDownLatch(1);
        final CountDownLatch releaseCall = new CountDownLatch(1);
        when(delegate.listOffsets(List.of())).thenAnswer(invocation -> {
            callStarted.countDown();
            assertTrue(releaseCall.await(30, TimeUnit.SECONDS), "the call was never released");
            return List.of();
        });
        final AvailabilityGatedControlPlane controlPlane = gated();

        final Thread caller = new Thread(() -> controlPlane.listOffsets(List.of()), "in-flight-call");
        caller.start();
        assertTrue(callStarted.await(30, TimeUnit.SECONDS), "the call never reached the delegate");

        assertTimeoutPreemptively(Duration.ofSeconds(5), controlPlane::invalidate,
            "invalidation must not block on an in-flight call");
        verify(delegate).close();

        releaseCall.countDown();
        caller.join(TimeUnit.SECONDS.toMillis(30));
        controlPlane.close();
    }

    @Test
    void concurrentCallsCreateExactlyOneDelegate() throws Exception {
        final int threads = 8;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final AvailabilityGatedControlPlane controlPlane = gated();

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    assertTrue(start.await(30, TimeUnit.SECONDS));
                    controlPlane.listOffsets(List.of());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "concurrent-caller-" + i).start();
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "callers did not finish");

        assertEquals(1, factoryInvocations.get(), "concurrent callers must share one delegate");
        controlPlane.close();
    }
}
