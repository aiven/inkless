/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlaneException;

public class JobUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobUtils.class);

    /**
     * Total number of attempts (initial try + retries) for a transient database failure.
     */
    static final int MAX_ATTEMPTS = 3;
    static final long INITIAL_BACKOFF_MS = 50;
    static final long MAX_BACKOFF_MS = 1_000;
    static final int BACKOFF_MULTIPLIER = 2;
    static final double BACKOFF_JITTER = 0.2;

    /**
     * Backoff between retries. Uses jitter (a randomized factor in {@code [1 - jitter, 1 + jitter]})
     * so that many jobs failing against the same struggling standby at once do not retry in lockstep
     * (thundering herd) and re-collide on the same recovery-conflict window.
     */
    private static final ExponentialBackoff BACKOFF =
        new ExponentialBackoff(INITIAL_BACKOFF_MS, BACKOFF_MULTIPLIER, MAX_BACKOFF_MS, BACKOFF_JITTER);

    /**
     * PostgreSQL {@code SQLState}s that are safe to retry because they guarantee the transaction did
     * <em>not</em> commit (the server rolled it back), so replaying the whole transaction cannot
     * double-apply a write.
     *
     * <ul>
     *   <li>{@code 40001} — {@code serialization_failure}. PostgreSQL also raises this
     *       ({@code ERRCODE_T_R_SERIALIZATION_FAILURE}) for hot-standby recovery conflicts, in both
     *       observed forms: the statement cancellation
     *       ("canceling statement due to conflict with recovery") <em>and</em> the connection
     *       termination ("terminating connection due to conflict with recovery"). The latter closes
     *       the JDBC connection, but because {@code readJooqCtx}/{@code writeJooqCtx} are backed by a
     *       connection pool, the retry transparently obtains a fresh connection.</li>
     *   <li>{@code 40P01} — {@code deadlock_detected}. The victim transaction is rolled back.</li>
     * </ul>
     *
     * <p>We deliberately do <b>not</b> retry generic connection-loss states (class {@code 08*},
     * {@code 57P01}) here: a connection dropped <em>during commit</em> leaves the transaction
     * in-doubt, so blindly replaying a write job could apply it twice. The recovery-conflict family
     * that motivated this retry logic is fully covered by the rollback-guaranteed states above.
     */
    private static final Set<String> RETRIABLE_SQL_STATES = Set.of("40001", "40P01");

    public static void run(final Runnable runnable, final Time time, final Consumer<Long> durationCallback) {
        run(() -> {
            runnable.run();
            return null;
        }, time, durationCallback);
    }

    public static <T> T run(final Callable<T> callable, final Time time, final Consumer<Long> durationCallback) {
        try {
            return runWithRetry(callable, time, durationCallback);
        } catch (final Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Execute {@code callable}, retrying with jittered exponential backoff on transient PostgreSQL
     * failures (see {@link #RETRIABLE_SQL_STATES}). On exhaustion, or for any non-retriable failure,
     * the original exception is rethrown so callers observe the unchanged error contract.
     *
     * <p>Only the <em>decisive</em> attempt (the one that succeeds, or the final failure) is reported
     * to {@code durationCallback}. Intermediate retried attempts are timed internally but not
     * forwarded, so failed-attempt durations do not inflate the latency/rate metrics — exactly when
     * an operator relies on them during a conflict storm.
     */
    private static <T> T runWithRetry(final Callable<T> callable, final Time time, final Consumer<Long> durationCallback) throws Exception {
        // measureDurationMs records the just-finished attempt's duration into this holder (via its
        // finally block, so it fires on failure too); we forward it to durationCallback only once,
        // when we stop retrying.
        final long[] lastAttemptDurationMs = {0L};
        final Consumer<Long> captureDuration = d -> lastAttemptDurationMs[0] = d;

        for (int attempt = 1; ; attempt++) {
            try {
                final T result = TimeUtils.measureDurationMs(time, callable, captureDuration);
                durationCallback.accept(lastAttemptDurationMs[0]);
                return result;
            } catch (final Exception e) {
                if (attempt >= MAX_ATTEMPTS || !isRetriable(e)) {
                    // Decisive failure: record its duration once, then propagate unchanged.
                    durationCallback.accept(lastAttemptDurationMs[0]);
                    if (attempt > 1) {
                        LOGGER.warn("Giving up after {} attempts on transient database error", attempt, e);
                    }
                    throw e;
                }
                final long backoffMs = BACKOFF.backoff(attempt - 1);
                LOGGER.warn("Transient database error on attempt {}/{}, retrying in {} ms",
                    attempt, MAX_ATTEMPTS, backoffMs, e);
                time.sleep(backoffMs);
                if (Thread.currentThread().isInterrupted()) {
                    // Interrupted during backoff (e.g. broker shutdown). Utils.sleep restores the
                    // interrupt flag but returns normally, so we must check it explicitly: stop
                    // retrying, keep the flag set, and surface the last error rather than spinning
                    // through the remaining attempts and delaying shutdown.
                    durationCallback.accept(lastAttemptDurationMs[0]);
                    throw e;
                }
            }
        }
    }

    /**
     * Returns {@code true} if any throwable in the cause chain (including suppressed throwables) is a
     * {@link SQLException} whose {@code SQLState} is in {@link #RETRIABLE_SQL_STATES}. The retriable
     * cause is typically wrapped several layers deep (e.g. {@code ControlPlaneException} ->
     * {@code DataAccessException} -> {@code PSQLException}), so the whole tree is scanned.
     */
    static boolean isRetriable(final Throwable throwable) {
        // Identity-based visited set guards against self-referential cause chains.
        return hasRetriableCause(throwable, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static boolean hasRetriableCause(final Throwable throwable, final Set<Throwable> seen) {
        if (throwable == null || !seen.add(throwable)) {
            return false;
        }
        if (throwable instanceof SQLException) {
            final String sqlState = ((SQLException) throwable).getSQLState();
            if (sqlState != null && RETRIABLE_SQL_STATES.contains(sqlState)) {
                return true;
            }
        }
        if (hasRetriableCause(throwable.getCause(), seen)) {
            return true;
        }
        for (final Throwable suppressed : throwable.getSuppressed()) {
            if (hasRetriableCause(suppressed, seen)) {
                return true;
            }
        }
        return false;
    }
}
