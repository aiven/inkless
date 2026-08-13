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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.config.InklessConfig;

/**
 * Owns building, replacing, and closing the real control-plane delegate, and reports whether one is
 * usable. {@link AvailabilityGatedControlPlane} is the caller-facing gate on top of this.
 *
 * <h2>Why a reconciler</h2>
 *
 * <p>Building the delegate is expensive and, against a sick database, slow without an upper bound:
 * it runs schema migration, waits on a migration advisory lock another broker may hold, and opens
 * several connection pools. None of that may happen on a thread that is serving a request. A fetch
 * thread that blocked on it would still be blocked long after the client gave up, and with a
 * bounded pool a handful of such threads take diskless fetch down for the whole broker.
 *
 * <p>So building is owned by one reconciler thread, and callers never wait for it. The reconciler
 * drives {@code actual} towards {@code desiredGeneration}:
 *
 * <p>{@link #current()} returns a delegate only when {@code actual} carries one for the current
 * generation. Anything else, including a reconfiguration the reconciler has not caught up with,
 * raises {@link ControlPlaneUnavailableException} and asks the reconciler to catch up. It therefore
 * reads two volatiles and takes no lock.
 *
 * <p>The cost of never blocking is that a call arriving before the first delegate is ready fails
 * rather than waiting. {@link #awaitFirstBuild(long)} exists to close that window at startup: the
 * owner starts the build and waits for it before the node serves any request, so the first build is
 * never raced. A rebuild after a reconfiguration is a different matter and is not waited for, so
 * callers must still tolerate the failure, which is retriable.
 *
 * <h2>Reconfiguration</h2>
 *
 * <p>Calls already in flight are not serialized against that teardown: a call that took the
 * delegate just before a reconfiguration runs against one that is being closed underneath it, and
 * fails with whatever that delegate raises. That is the point. Waiting for in-flight calls to drain
 * would block the reconfiguration for as long as the control plane takes to time out, which is
 * unbounded in exactly the situation that prompts the change.
 */
class ControlPlaneDelegateReconciler implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControlPlaneDelegateReconciler.class);

    static final String NOT_CONFIGURED_MESSAGE = "No diskless control plane is configured";
    static final String NOT_READY_MESSAGE = "Diskless control plane is not ready";
    static final String CLOSED_MESSAGE = "Control plane is closed";

    private static final String DELEGATE_NAME = "inkless control plane";
    private static final long RETRY_BACKOFF_INITIAL_MS = 500;
    private static final long RETRY_BACKOFF_MAX_MS = 10_000;

    /**
     * What the reconciler last settled on.
     *
     * @param generation the {@link #desiredGeneration} this was built for
     * @param delegate   the usable delegate, or {@code null} if this generation could not be served
     * @param state      the availability this generation settled at
     * @param failure    what went wrong, or {@code null}; reported as the cause to callers
     */
    private record Actual(long generation,
                          ControlPlane delegate,
                          ControlPlaneAvailability.State state,
                          RuntimeException failure) {
    }

    private final Supplier<InklessConfig> configSupplier;
    private final Function<InklessConfig, ControlPlane> delegateFactory;
    private final Time time;
    private final ControlPlaneAvailability availability = new ControlPlaneAvailability();

    private final AtomicLong desiredGeneration = new AtomicLong();
    /** Written only by the reconciler thread, and by {@link #close()} taking over teardown. */
    private volatile Actual actual = new Actual(-1, null, ControlPlaneAvailability.State.UNKNOWN, null);

    private final ExecutorService reconciler = Executors.newSingleThreadExecutor(
        new InklessThreadFactory("inkless-control-plane-reconciler-", true));
    private final AtomicBoolean reconcileScheduled = new AtomicBoolean();

    private volatile long nextAttemptAtMs;
    private volatile long retryBackoffMs = RETRY_BACKOFF_INITIAL_MS;

    /** Opens when the first build attempt finishes, so the owner can hold off serving requests. */
    private final CountDownLatch firstBuild = new CountDownLatch(1);

    /** Guards publishing a delegate against {@link #close()} taking over teardown. Never on the hot path. */
    private final Object publishLock = new Object();
    private volatile boolean closed = false;

    ControlPlaneDelegateReconciler(final Supplier<InklessConfig> configSupplier,
                                   final Function<InklessConfig, ControlPlane> delegateFactory,
                                   final Time time) {
        this.configSupplier = configSupplier;
        this.delegateFactory = delegateFactory;
        this.time = time;
    }

    ControlPlaneAvailability availability() {
        return availability;
    }

    /**
     * Starts building the delegate in the background, so calls that arrive later find it ready.
     *
     * <p>Optional: a call arriving first triggers the same work. Call this during startup to keep
     * the first diskless request from failing. Returns without waiting, and without opening any
     * connection if no control plane is configured.
     */
    void start() {
        requestReconcile();
    }

    /**
     * Waits for the first build attempt to finish, whatever its outcome. Returns true if it
     * finished, false on timeout.
     *
     * <p>Settling unsuccessfully counts as finished. The point is not to wait for the control plane
     * to become usable, only to keep the owner from serving requests while the very first attempt
     * is still running: a caller that arrives during that window would fail, and some of them, such
     * as diskless topic creation, have nowhere to put a retriable error. Building takes a schema
     * migration and an advisory lock that every other broker is contending for at the same time, so
     * the window is comfortably long enough to hit.
     *
     * <p>The timeout is a backstop, not the expected path. Exceeding it leaves the build running in
     * the background, which is the same state the owner would be in had it never waited.
     */
    boolean awaitFirstBuild(final long timeoutMs) throws InterruptedException {
        return firstBuild.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the delegate for the current generation.
     *
     * <p>Reads two volatiles and takes no lock. Never builds anything: that is the reconciler's
     * job, and this only nudges it.
     *
     * @throws ControlPlaneUnavailableException if no delegate is ready for the current generation
     * @throws ControlPlaneException            if this has been closed
     */
    ControlPlane current() {
        if (closed) {
            throw new ControlPlaneException(CLOSED_MESSAGE);
        }
        final Actual current = actual;
        if (current.generation() == desiredGeneration.get()) {
            final ControlPlane ready = current.delegate();
            if (ready != null) {
                return ready;
            }
            // Settled on this generation without a delegate. A transient failure is worth retrying
            // once the backoff has elapsed; UNAVAILABLE means unconfigured, which stays settled
            // until a reconfiguration increments the generation.
            if (current.state() == ControlPlaneAvailability.State.UNKNOWN
                && time.milliseconds() >= nextAttemptAtMs) {
                requestReconcile();
            }
        } else {
            // A reconfiguration the reconciler has not caught up with yet.
            requestReconcile();
        }
        availability.recordGatedCall();
        throw unavailableException(current);
    }

    /**
     * Retires the current delegate so the reconciler rebuilds one from the current configuration.
     * Marks the availability {@link ControlPlaneAvailability.State#UNKNOWN}, not
     * {@code UNAVAILABLE}, because the new configuration might work. Returns without doing I/O:
     * the reconciler closes the retired delegate on its own thread.
     */
    void invalidate() {
        availability.markUnknown();
        retire();
    }

    /**
     * Retires the current delegate and marks the availability
     * {@link ControlPlaneAvailability.State#UNAVAILABLE} immediately, instead of leaving it
     * {@code UNKNOWN} until the reconciler finds out.
     *
     * <p>Use this when the reconfiguration itself, not just the next call, already knows the control
     * plane is out of service, such as when the connection string is emptied. Returns without doing
     * I/O: the reconciler closes the retired delegate on its own thread.
     */
    void takeOutOfService(final ControlPlaneAvailability.UnavailableReason reason) {
        availability.markUnavailable(reason);
        retire();
    }

    private void retire() {
        desiredGeneration.incrementAndGet();
        // An operator changing the configuration is a reason to try again now, whatever the
        // previous attempt's backoff had decided.
        resetBackoff();
        requestReconcile();
    }

    private ControlPlaneUnavailableException unavailableException(final Actual current) {
        final String message = current.state() == ControlPlaneAvailability.State.UNAVAILABLE
            ? NOT_CONFIGURED_MESSAGE
            : NOT_READY_MESSAGE;
        return current.failure() == null
            ? new ControlPlaneUnavailableException(message)
            : new ControlPlaneUnavailableException(message, current.failure());
    }

    /** Asks the reconciler to run, collapsing concurrent requests into one pass. */
    private void requestReconcile() {
        if (!reconcileScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            reconciler.execute(this::runReconcile);
        } catch (final RejectedExecutionException e) {
            // Shut down: there is nothing left to reconcile towards, and nothing to wait for.
            reconcileScheduled.set(false);
            firstBuild.countDown();
        }
    }

    private void runReconcile() {
        // Cleared before reading the desired generation, so a reconfiguration landing during this
        // pass schedules another one instead of being dropped.
        reconcileScheduled.set(false);
        while (!closed) {
            final long want = desiredGeneration.get();
            final Actual current = actual;
            if (!needsWork(want, current)) {
                return;
            }
            if (!attempt(want, current)) {
                // Failed transiently. Stop here rather than spinning; a later call retries once
                // the backoff has elapsed.
                return;
            }
        }
    }

    private boolean needsWork(final long want, final Actual current) {
        if (current.generation() != want) {
            return true;
        }
        // Settled on this generation, but the last attempt failed transiently, so a retry is due.
        return current.delegate() == null && current.state() == ControlPlaneAvailability.State.UNKNOWN;
    }

    /** Returns true if this generation is now settled, false if it failed transiently. */
    private boolean attempt(final long want, final Actual current) {
        try {
            return attemptOnce(want, current);
        } finally {
            firstBuild.countDown();
        }
    }

    private boolean attemptOnce(final long want, final Actual current) {
        final ControlPlane stale = current.delegate();
        final ControlPlane created;
        try {
            created = delegateFactory.apply(configSupplier.get());
        } catch (final ControlPlaneNotConfiguredException e) {
            // Raised while the delegate parses its configuration, before it opens a socket: no
            // control plane is configured. Settled until the generation changes.
            LOGGER.warn("No diskless control plane is configured", e);
            if (publish(new Actual(want, null, ControlPlaneAvailability.State.UNAVAILABLE, e),
                ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED)) {
                retireStale(stale);
            }
            return true;
        } catch (final RuntimeException e) {
            if (closed) {
                // close() interrupted this build on purpose. Not worth a warning, and there is
                // nothing left to retry towards.
                LOGGER.debug("Abandoned the diskless control plane build: closed while building", e);
                publish(new Actual(want, null, ControlPlaneAvailability.State.UNKNOWN, e), null);
                return true;
            }
            // An unreachable control plane is not an unconfigured one, and neither is any other
            // ConfigException: a delegate implementation only ever raises
            // ControlPlaneNotConfiguredException for that. Retry under a backoff so a typo in an
            // unrelated key stays visible in the logs instead of being reported as "not configured".
            LOGGER.warn("Failed to create the diskless control plane; retrying in {} ms", retryBackoffMs, e);
            if (publish(new Actual(want, null, ControlPlaneAvailability.State.UNKNOWN, e), null)) {
                retireStale(stale);
            }
            backOff();
            return false;
        }
        // Publishing before closing the retired delegate keeps the gap in which nothing can be
        // served as short as possible. The two pools overlap briefly as a result.
        if (publish(new Actual(want, created, ControlPlaneAvailability.State.AVAILABLE, null), null)) {
            retireStale(stale);
        }
        resetBackoff();
        return true;
    }

    /**
     * Makes {@code next} the settled outcome and reflects it in the availability, unless
     * {@link #close()} got there first.
     *
     * <p>Returns true if it was published, which also means the caller now owns closing whatever
     * {@code next} replaced. Returns false if this is already closed, in which case
     * {@link #close()} owns closing the previous delegate and this closes {@code next}'s own, so
     * that a delegate built concurrently with shutdown is not left open. Exactly one of the two
     * closes each delegate.
     */
    private boolean publish(final Actual next, final ControlPlaneAvailability.UnavailableReason reason) {
        synchronized (publishLock) {
            if (closed) {
                Utils.closeQuietly(next.delegate(), DELEGATE_NAME);
                return false;
            }
            actual = next;
        }
        // A reconfiguration may have landed while this generation was being built, and it has
        // already set the availability for where it is heading. Don't overwrite that with an
        // answer about a generation nobody wants any more.
        if (desiredGeneration.get() == next.generation()) {
            switch (next.state()) {
                case AVAILABLE -> availability.markAvailable();
                case UNAVAILABLE -> availability.markUnavailable(reason);
                case UNKNOWN -> availability.markUnknown();
            }
        }
        return true;
    }

    /** Closes a delegate no caller can reach any more. Runs on the reconciler thread. */
    private void retireStale(final ControlPlane stale) {
        if (stale != null) {
            Utils.closeQuietly(stale, DELEGATE_NAME);
        }
    }

    private void backOff() {
        nextAttemptAtMs = time.milliseconds() + retryBackoffMs;
        retryBackoffMs = Math.min(RETRY_BACKOFF_MAX_MS, retryBackoffMs * 2);
    }

    private void resetBackoff() {
        retryBackoffMs = RETRY_BACKOFF_INITIAL_MS;
        nextAttemptAtMs = 0;
    }

    @Override
    public void close() throws IOException {
        final ControlPlane current;
        synchronized (publishLock) {
            closed = true;
            current = actual.delegate();
            actual = new Actual(actual.generation(), null, actual.state(), actual.failure());
        }
        // Interrupt the reconciler without waiting for it. A thread parked in a socket read is not
        // interruptible, and shutdown must not inherit that wait.
        reconciler.shutdownNow();
        // Release anyone still waiting for the first build; it is not going to finish now.
        firstBuild.countDown();
        Utils.closeQuietly(availability, "inkless control plane availability");
        if (current != null) {
            current.close();
        }
    }
}
