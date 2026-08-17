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

    // Jittered so concurrent retries against the same standby do not collide in lockstep.
    private static final ExponentialBackoff BACKOFF =
        new ExponentialBackoff(INITIAL_BACKOFF_MS, BACKOFF_MULTIPLIER, MAX_BACKOFF_MS, BACKOFF_JITTER);

    /**
     * PostgreSQL failures that guarantee transaction rollback and are safe to retry:
     * {@code 40001} for serialization or recovery conflicts, and {@code 40P01} for deadlocks.
     * Connection-loss states are excluded because the commit outcome may be unknown.
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

    private static <T> T runWithRetry(final Callable<T> callable, final Time time, final Consumer<Long> durationCallback) throws Exception {
        // One sample per run(), covering attempts and backoff, so QueryTime matches client-visible
        // latency and QueryRate stays one increment per job.
        return TimeUtils.measureDurationMs(time, () -> {
            for (int attempt = 1; ; attempt++) {
                try {
                    return callable.call();
                } catch (final Exception e) {
                    if (attempt >= MAX_ATTEMPTS || !isRetriable(e)) {
                        if (attempt >= MAX_ATTEMPTS) {
                            LOGGER.warn("Giving up after {} attempts on transient database error", attempt, e);
                        }
                        throw e;
                    }
                    final long backoffMs = BACKOFF.backoff(attempt - 1);
                    LOGGER.debug("Transient database error on attempt {}/{}, retrying in {} ms",
                        attempt, MAX_ATTEMPTS, backoffMs, e);
                    time.sleep(backoffMs);
                    if (Thread.currentThread().isInterrupted()) {
                        // SystemTime.sleep restores the interrupt flag but returns normally, so
                        // check it here or the loop would keep retrying through broker shutdown.
                        throw e;
                    }
                }
            }
        }, durationCallback);
    }

    /**
     * True if any {@link SQLException} in the cause chain, JDBC {@code nextException} chain, or
     * suppressed throwables has a SQLState in {@link #RETRIABLE_SQL_STATES}. Jobs wrap the JDBC
     * failure (e.g. {@code ControlPlaneException} -> {@code DataAccessException} ->
     * {@code PSQLException}), so the argument stays {@link Throwable}.
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
            final SQLException sqlException = (SQLException) throwable;
            final String sqlState = sqlException.getSQLState();
            if (sqlState != null && RETRIABLE_SQL_STATES.contains(sqlState)) {
                return true;
            }
            if (hasRetriableCause(sqlException.getNextException(), seen)) {
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
