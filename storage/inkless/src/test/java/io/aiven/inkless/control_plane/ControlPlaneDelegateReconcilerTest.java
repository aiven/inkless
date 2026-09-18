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
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.aiven.inkless.config.InklessConfig;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ControlPlaneDelegateReconcilerTest {
    private static final Duration SHORT = Duration.ofSeconds(5);
    private static final long AWAIT_SECONDS = 30;

    private final ControlPlane delegate = Mockito.mock(ControlPlane.class);
    private final MockTime time = new MockTime();
    private final AtomicInteger factoryInvocations = new AtomicInteger();
    private final AtomicReference<RuntimeException> factoryFailure = new AtomicReference<>();
    /** Counted down every time the factory is entered, so a test can wait for an attempt. */
    private final AtomicReference<CountDownLatch> factoryEntered = new AtomicReference<>(new CountDownLatch(1));
    /** Held closed to keep the factory, and so the reconciler, inside an attempt. */
    private final AtomicReference<CountDownLatch> factoryRelease = new AtomicReference<>();

    private static final InklessConfig CONFIG = new InklessConfig(Map.of(
        "control.plane.class", InMemoryControlPlane.class.getCanonicalName()));

    private final Function<InklessConfig, ControlPlane> factory = config -> {
        factoryInvocations.incrementAndGet();
        factoryEntered.get().countDown();
        final CountDownLatch release = factoryRelease.get();
        if (release != null) {
            awaitUninterruptibly(release);
        }
        final RuntimeException failure = factoryFailure.get();
        if (failure != null) {
            throw failure;
        }
        return delegate;
    };

    /**
     * Waits without honoring an interrupt, the way a real build behaves: a pgjdbc socket read and a
     * Flyway migration both run to completion regardless of {@code shutdownNow}. Tests that park
     * the factory rely on this to stay parked.
     */
    private static void awaitUninterruptibly(final CountDownLatch release) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    assertTrue(release.await(AWAIT_SECONDS, TimeUnit.SECONDS), "the factory was never released");
                    return;
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private ControlPlaneDelegateReconciler reconciler;

    private ControlPlaneDelegateReconciler reconciler() {
        reconciler = new ControlPlaneDelegateReconciler(() -> CONFIG, factory, time);
        return reconciler;
    }

    @AfterEach
    void tearDown() throws Exception {
        // Let go of anything a test parked inside the factory, so close() is never racing a
        // reconciler that cannot make progress.
        final CountDownLatch release = factoryRelease.getAndSet(null);
        if (release != null) {
            release.countDown();
        }
        if (reconciler != null) {
            reconciler.close();
        }
    }

    private void awaitState(final ControlPlaneAvailability.State expected) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .until(() -> reconciler.availability().state() == expected);
    }

    private void awaitFactoryInvocations(final int expected) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .until(() -> factoryInvocations.get() == expected);
    }

    @Test
    void constructionAloneBuildsNothing() {
        reconciler();

        assertEquals(0, factoryInvocations.get());
        verifyNoInteractions(delegate);
        assertEquals(ControlPlaneAvailability.State.UNKNOWN, reconciler.availability().state());
    }

    @Test
    void startBuildsTheDelegateInTheBackgroundAndCurrentHandsItOut() {
        reconciler().start();
        awaitState(ControlPlaneAvailability.State.AVAILABLE);

        assertSame(delegate, reconciler.current());
        assertSame(delegate, reconciler.current());
        assertEquals(1, factoryInvocations.get(), "the delegate must be built once and reused");
    }

    @Test
    void currentBeforeTheDelegateIsReadyFailsAndTriggersTheBuild() {
        reconciler();

        // No start(): the first caller finds nothing ready. It must not build the delegate itself.
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
        verifyNoInteractions(delegate);

        // ...but it must have asked the reconciler to build one, so later callers succeed.
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        assertSame(delegate, reconciler.current());
    }

    @Test
    void awaitFirstBuildReturnsOnceTheDelegateIsReady() throws Exception {
        reconciler().start();

        assertTrue(reconciler.awaitFirstBuild(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS)));
        // The point of waiting: no window in which a caller would fail.
        assertSame(delegate, reconciler.current());
    }

    @Test
    void awaitFirstBuildReturnsWhenTheBuildSettlesUnsuccessfully() throws Exception {
        factoryFailure.set(new ControlPlaneNotConfiguredException("connection.string is missing"));
        reconciler().start();

        // Settling unsuccessfully still counts as finished: an unconfigured or unreachable control
        // plane must not hold up the owner's startup.
        assertTrue(reconciler.awaitFirstBuild(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS)));
        assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, reconciler.availability().state());
    }

    @Test
    void awaitFirstBuildTimesOutWhileTheBuildIsStillRunning() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        assertFalse(reconciler.awaitFirstBuild(200), "must not wait for a build that is not finishing");
    }

    @Test
    void closeReleasesAWaiterOnTheFirstBuild() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        reconciler.close();

        // Shutdown must not leave a startup thread parked waiting for a build that is now abandoned.
        assertTrue(reconciler.awaitFirstBuild(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS)));
    }

    @Test
    void currentNeverWaitsForABuildInFlight() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        // The reconciler is parked inside the factory. A caller must not queue up behind it.
        assertTimeoutPreemptively(SHORT, () -> {
            for (int i = 0; i < 8; i++) {
                assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
            }
        }, "current() must not block on a build in flight");

        factoryRelease.getAndSet(null).countDown();
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        assertEquals(1, factoryInvocations.get(), "the queued-up callers must not each trigger a build");
    }

    @Test
    void notConfiguredSettlesUnavailableAndIsNotRetried() {
        factoryFailure.set(new ControlPlaneNotConfiguredException("connection.string is missing"));
        reconciler().start();
        awaitState(ControlPlaneAvailability.State.UNAVAILABLE);

        assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED,
            reconciler.availability().unavailableReason());
        final ControlPlaneUnavailableException e = assertThrows(ControlPlaneUnavailableException.class,
            () -> reconciler.current());
        assertEquals(ControlPlaneDelegateReconciler.NOT_CONFIGURED_MESSAGE, e.getMessage());
        verifyNoInteractions(delegate);

        // Unconfigured is settled: re-reading the configuration would raise the same exception.
        // Advancing past any backoff must not change that.
        time.sleep(TimeUnit.MINUTES.toMillis(5));
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
        assertEquals(1, factoryInvocations.get(),
            "an unconfigured control plane must not be rebuilt until it is reconfigured");
    }

    @Test
    void otherConfigExceptionIsRetriedRatherThanReportedAsNotConfigured() {
        // A typo in an unrelated key, such as a bad numeric value, must not be conflated with an
        // empty connection string: only ControlPlaneNotConfiguredException means "not configured".
        final ConfigException typo = new ConfigException("max.connections", "abc", "Not a number");
        factoryFailure.set(typo);
        reconciler().start();
        awaitFactoryInvocations(1);

        final ControlPlaneUnavailableException e = assertThrows(ControlPlaneUnavailableException.class,
            () -> reconciler.current());
        assertEquals(ControlPlaneDelegateReconciler.NOT_READY_MESSAGE, e.getMessage());
        assertSame(typo, e.getCause());
        assertEquals(ControlPlaneAvailability.State.UNKNOWN, reconciler.availability().state(),
            "a genuine misconfiguration must not be reported as unconfigured");

        // And it is retried like any other transient failure, not settled until the generation
        // changes.
        time.sleep(TimeUnit.SECONDS.toMillis(1));
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
        awaitFactoryInvocations(2);
    }

    @Test
    void transientFailureLeavesAvailabilityUnknownAndReportsTheCause() {
        final IllegalStateException refused = new IllegalStateException("connection refused");
        factoryFailure.set(refused);
        reconciler().start();
        awaitFactoryInvocations(1);

        final ControlPlaneUnavailableException e = assertThrows(ControlPlaneUnavailableException.class,
            () -> reconciler.current());
        assertEquals(ControlPlaneDelegateReconciler.NOT_READY_MESSAGE, e.getMessage());
        // The caller no longer runs the build, so the factory's failure has to reach it as a cause.
        assertSame(refused, e.getCause());
        assertEquals(ControlPlaneAvailability.State.UNKNOWN, reconciler.availability().state(),
            "an unreachable control plane must not be reported as unconfigured");
    }

    @Test
    void transientFailureIsRetriedOnlyOnceTheBackoffElapses() throws Exception {
        factoryFailure.set(new IllegalStateException("connection refused"));
        reconciler().start();
        awaitFactoryInvocations(1);

        // Inside the backoff window, callers keep failing without re-dialing the database.
        factoryEntered.set(new CountDownLatch(1));
        for (int i = 0; i < 8; i++) {
            assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
        }
        assertFalse(factoryEntered.get().await(200, TimeUnit.MILLISECONDS),
            "a dead control plane must not be re-dialed once per call");
        assertEquals(1, factoryInvocations.get());

        // Past the backoff, the next caller gets one more attempt.
        time.sleep(TimeUnit.SECONDS.toMillis(1));
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the retry never happened");
        awaitFactoryInvocations(2);
    }

    @Test
    void recoversOnceTheControlPlaneComesBack() {
        factoryFailure.set(new IllegalStateException("connection refused"));
        reconciler().start();
        awaitFactoryInvocations(1);
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());

        factoryFailure.set(null);
        time.sleep(TimeUnit.SECONDS.toMillis(1));
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());

        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        assertSame(delegate, reconciler.current());
    }

    @Test
    void recoversOnItsOwnWithoutAnyCallerReachingCurrent() {
        // Produce now fails fast on UNKNOWN instead of calling into the control plane, so nothing
        // here ever calls current(). The reconciler still has to find its own way back.
        factoryFailure.set(new IllegalStateException("connection refused"));
        reconciler().start();
        awaitFactoryInvocations(1);

        factoryFailure.set(null);
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        assertSame(delegate, reconciler.current());
    }

    @Test
    void invalidateRebuildsFromTheNewConfigAndClosesTheRetiredDelegate() throws Exception {
        reconciler().start();
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        assertSame(delegate, reconciler.current());

        reconciler.invalidate();

        assertEquals(ControlPlaneAvailability.State.UNKNOWN, reconciler.availability().state());
        awaitFactoryInvocations(2);
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        // Closing the retired delegate drains connection pools, so it happens on the reconciler
        // thread rather than on the thread that applied the config change.
        verify(delegate).close();
    }

    @Test
    void invalidateRetiresTheDelegateForEveryLaterCallerAtOnce() {
        reconciler().start();
        awaitState(ControlPlaneAvailability.State.AVAILABLE);
        // Park the rebuild so the reconciler cannot reach AVAILABLE again during the assertion.
        factoryRelease.set(new CountDownLatch(1));

        reconciler.invalidate();

        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current(),
            "a retired delegate must not be handed out while the rebuild is pending");
    }

    @Test
    void invalidateDoesNotWaitForABuildInFlight() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        // The old implementation took the same lock for building and teardown, so this is exactly
        // the case where an operator could not take a hung control plane out of service.
        assertTimeoutPreemptively(SHORT, reconciler::invalidate,
            "invalidation must not block on a build in flight");
    }

    @Test
    void takeOutOfServiceMarksUnavailableImmediatelyWithoutWaiting() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        assertTimeoutPreemptively(SHORT,
            () -> reconciler.takeOutOfService(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED),
            "taking the control plane out of service must not block on a build in flight");

        assertEquals(ControlPlaneAvailability.State.UNAVAILABLE, reconciler.availability().state());
        assertEquals(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED,
            reconciler.availability().unavailableReason());
        assertFalse(reconciler.availability().isAvailable(),
            "produce must be rejected before it buffers and uploads");
        assertThrows(ControlPlaneUnavailableException.class, () -> reconciler.current());
    }

    @Test
    void closeIsANoOpWhenTheDelegateWasNeverBuilt() throws Exception {
        reconciler().close();

        verifyNoInteractions(delegate);
    }

    @Test
    void closeClosesTheDelegateOnceBuilt() throws Exception {
        reconciler().start();
        awaitState(ControlPlaneAvailability.State.AVAILABLE);

        reconciler.close();

        verify(delegate).close();
    }

    @Test
    void closeDoesNotWaitForABuildInFlight() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        // A build parked in an uninterruptible socket read must not hold up broker shutdown.
        assertTimeoutPreemptively(SHORT, () -> reconciler.close(),
            "close must not block on a build in flight");
    }

    @Test
    void aDelegateBuiltConcurrentlyWithCloseIsNotLeaked() throws Exception {
        factoryRelease.set(new CountDownLatch(1));
        reconciler().start();
        assertTrue(factoryEntered.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the build never started");

        reconciler.close();
        // Let the build finish after close() has already run. The delegate it produces can never be
        // handed out, so the reconciler has to close it rather than leak it.
        factoryRelease.getAndSet(null).countDown();

        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> verify(delegate).close());
    }

    @Test
    void rejectsCurrentOnceClosed() throws Exception {
        reconciler().close();

        assertThrows(ControlPlaneException.class, () -> reconciler.current());
        verifyNoInteractions(delegate);
        assertEquals(0, factoryInvocations.get(), "must not build a delegate after close");
    }

    @Test
    void closedTakesPrecedenceOverAnAlreadyUnavailableControlPlane() throws Exception {
        factoryFailure.set(new ControlPlaneNotConfiguredException("connection.string is missing"));
        reconciler().start();
        awaitState(ControlPlaneAvailability.State.UNAVAILABLE);

        reconciler.close();

        // The fast-fail path for a known-unavailable control plane must not shadow "closed": once
        // closed, that's the exception every caller sees, not the older "not configured" one.
        final ControlPlaneException e = assertThrows(ControlPlaneException.class, () -> reconciler.current());
        assertEquals(ControlPlaneDelegateReconciler.CLOSED_MESSAGE, e.getMessage());
    }

    @Test
    void concurrentCallersBuildExactlyOneDelegate() throws Exception {
        final int threads = 8;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        reconciler();

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    assertTrue(start.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                    // Whether this one wins the race or fails is not the point; that exactly one
                    // delegate gets built is.
                    reconciler.current();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final ControlPlaneUnavailableException e) {
                    // Expected for callers that arrive before the delegate is ready.
                } finally {
                    done.countDown();
                }
            }, "concurrent-caller-" + i).start();
        }

        start.countDown();
        assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS), "callers did not finish");
        awaitState(ControlPlaneAvailability.State.AVAILABLE);

        assertEquals(1, factoryInvocations.get(), "concurrent callers must share one delegate");
    }
}
