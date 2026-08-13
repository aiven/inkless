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
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Covers the gate as an adapter: that every {@link ControlPlane} method goes through the
 * reconciler, and that the lifecycle calls forward to it. The building, replacing, and closing
 * behind those calls is covered by {@link ControlPlaneDelegateReconcilerTest}.
 */
class AvailabilityGatedControlPlaneTest {
    private static final long AWAIT_SECONDS = 30;

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

    private AvailabilityGatedControlPlane controlPlane;

    private AvailabilityGatedControlPlane gated() {
        controlPlane = new AvailabilityGatedControlPlane(() -> CONFIG, factory, Time.SYSTEM);
        return controlPlane;
    }

    /** Builds a gate whose delegate is ready, so a call reaches the mock instead of fast-failing. */
    private AvailabilityGatedControlPlane ready() {
        gated().start();
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        return controlPlane;
    }

    private void awaitState(final ControlPlaneAvailability.State expected) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .until(() -> controlPlane.availability().state() == expected);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (controlPlane != null) {
            controlPlane.close();
        }
    }

    @Test
    void forwardsEveryCallToTheReadyDelegate() {
        ready();

        controlPlane.commitFile("key", null, 1, 2L, List.of());
        controlPlane.findBatches(List.of(), 1, 1);
        controlPlane.createTopicAndPartitions(Set.of());
        controlPlane.initDisklessLog(List.of());
        controlPlane.repairDisklessLog(List.of());
        controlPlane.deleteRecords(List.of());
        controlPlane.deleteTopics(Set.of());
        controlPlane.purgeDeletedLogs(1);
        controlPlane.enforceRetention(List.of(), 1);
        controlPlane.advanceCrossTierLogStartOffset(List.of());
        controlPlane.getCrossTierLogStart(null);
        controlPlane.getFilesToDelete(Instant.EPOCH, 1);
        controlPlane.deleteFiles(null);
        controlPlane.listOffsets(List.of());
        controlPlane.isSafeToDeleteFile("key");
        controlPlane.getLogInfo(List.of());
        controlPlane.getProducerState(List.of());
        controlPlane.pruneDisklessLogs(List.of());

        verify(delegate).commitFile("key", null, 1, 2L, List.of());
        verify(delegate).findBatches(List.of(), 1, 1);
        verify(delegate).createTopicAndPartitions(Set.of());
        verify(delegate).initDisklessLog(List.of());
        verify(delegate).repairDisklessLog(List.of());
        verify(delegate).deleteRecords(List.of());
        verify(delegate).deleteTopics(Set.of());
        verify(delegate).purgeDeletedLogs(1);
        verify(delegate).enforceRetention(List.of(), 1);
        verify(delegate).advanceCrossTierLogStartOffset(List.of());
        verify(delegate).getCrossTierLogStart(null);
        verify(delegate).getFilesToDelete(Instant.EPOCH, 1);
        verify(delegate).deleteFiles(null);
        verify(delegate).listOffsets(List.of());
        verify(delegate).isSafeToDeleteFile("key");
        verify(delegate).getLogInfo(List.of());
        verify(delegate).getProducerState(List.of());
        verify(delegate).pruneDisklessLogs(List.of());
    }

    @Test
    void everyCallFailsWhileNoControlPlaneIsConfigured() {
        factoryFailure.set(new ConfigException("connection.string", "", "Missing"));
        gated().start();
        awaitState(ControlPlaneAvailability.State.UNAVAILABLE);

        assertThrows(ControlPlaneUnavailableException.class,
            () -> controlPlane.commitFile("key", null, 1, 2L, List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.findBatches(List.of(), 1, 1));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.createTopicAndPartitions(Set.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.initDisklessLog(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.repairDisklessLog(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.deleteRecords(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.deleteTopics(Set.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.purgeDeletedLogs(1));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.enforceRetention(List.of(), 1));
        assertThrows(ControlPlaneUnavailableException.class,
            () -> controlPlane.advanceCrossTierLogStartOffset(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.getCrossTierLogStart(null));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.getFilesToDelete(Instant.EPOCH, 1));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.deleteFiles(null));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.listOffsets(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.isSafeToDeleteFile("key"));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.getLogInfo(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.getProducerState(List.of()));
        assertThrows(ControlPlaneUnavailableException.class, () -> controlPlane.pruneDisklessLogs(List.of()));

        verifyNoInteractions(delegate);
    }

    @Test
    void availabilityReportsTheReconcilersState() {
        ready();
        assertEquals(ControlPlaneAvailability.State.AVAILABLE, controlPlane.availability().state());

        controlPlane.takeOutOfService(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);

        assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, controlPlane.availability().state());
        assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED,
            controlPlane.availability().unavailableReason());
    }

    @Test
    void invalidateRebuildsSoLaterCallsUseTheNewDelegate() {
        ready();
        controlPlane.listOffsets(List.of());

        controlPlane.invalidate();

        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS).until(() -> factoryInvocations.get() == 2);
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        controlPlane.listOffsets(List.of());
        verify(delegate, Mockito.times(2)).listOffsets(List.of());
    }

    @Test
    void invalidateDoesNotWaitForInFlightCalls() throws Exception {
        final CountDownLatch callStarted = new CountDownLatch(1);
        final CountDownLatch releaseCall = new CountDownLatch(1);
        when(delegate.listOffsets(List.of())).thenAnswer(invocation -> {
            callStarted.countDown();
            assertTrue(releaseCall.await(AWAIT_SECONDS, TimeUnit.SECONDS), "the call was never released");
            return List.of();
        });
        ready();

        final Thread caller = new Thread(() -> controlPlane.listOffsets(List.of()), "in-flight-call");
        caller.start();
        assertTrue(callStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS), "the call never reached the delegate");

        assertTimeoutPreemptively(Duration.ofSeconds(5), controlPlane::invalidate,
            "invalidation must not block on an in-flight call");

        releaseCall.countDown();
        caller.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    }

    @Test
    void configureIsANoOp() {
        gated().configure(Map.of());

        verifyNoInteractions(delegate);
        assertEquals(0, factoryInvocations.get());
    }

    @Test
    void closeClosesTheDelegate() throws Exception {
        ready();

        controlPlane.close();

        verify(delegate).close();
    }

    @Test
    void rejectsCallsOnceClosed() throws Exception {
        gated().close();

        assertThrows(ControlPlaneException.class, () -> controlPlane.listOffsets(List.of()));
        verifyNoInteractions(delegate);
        assertEquals(0, factoryInvocations.get(), "must not build a delegate after close");
    }
}
