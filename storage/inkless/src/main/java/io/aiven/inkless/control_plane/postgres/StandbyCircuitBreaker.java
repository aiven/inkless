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
package io.aiven.inkless.control_plane.postgres;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.aiven.inkless.common.InklessThreadFactory;

/**
 * Tracks standby database reachability for {@link PostgresControlPlane} read traffic.
 *
 * <p>Starts {@code CLOSED}. After {@code failureThreshold} consecutive connectivity or timeout
 * failures on the standby read pool, transitions to {@code OPEN} and evicts idle standby
 * connections so recovery resolves fresh DNS. While {@code OPEN}, a daemon thread probes the
 * standby with {@code SELECT 1} until a probe succeeds.
 *
 * <p>Each {@code CLOSED -> OPEN -> CLOSED} cycle increments a generation counter so slow in-flight
 * calls that started before a recovery can't re-trip the newly closed circuit.
 */
final class StandbyCircuitBreaker implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandbyCircuitBreaker.class);

    // PostgreSQL server-shutdown and startup SQLStates (`57P01` administrator shutdown, `57P02`
    // crash shutdown, `57P03` startup refusal). Query-level transient codes (`40001`, `40P01`)
    // stay excluded because `JobUtils` retries them and they don't indicate an unreachable standby.
    private static final Set<String> STANDBY_UNAVAILABLE_SQL_STATES = Set.of("57P01", "57P02", "57P03");

    private record State(boolean open, int consecutiveFailures, long generation) {
    }

    private final int failureThreshold;
    private final long probeIntervalMs;
    private final BooleanSupplier probeAction;
    private final Runnable evictConnectionsAction;
    private final ScheduledExecutorService probeScheduler;
    private final PostgresControlPlaneMetrics metrics;
    private final BooleanSupplier openSupplier;

    private final Object stateLock = new Object();
    private volatile State state = new State(false, 0, 0L);
    private ScheduledFuture<?> probeFuture;
    private boolean closed;

    StandbyCircuitBreaker(
        final int failureThreshold,
        final long probeIntervalMs,
        final HikariDataSource readDataSource,
        final PostgresControlPlaneMetrics metrics
    ) {
        this(
            failureThreshold,
            probeIntervalMs,
            () -> probeStandby(readDataSource),
            () -> softEvictConnections(readDataSource),
            createProbeScheduler(),
            metrics
        );
    }

    // Visible for testing.
    StandbyCircuitBreaker(
        final int failureThreshold,
        final long probeIntervalMs,
        final BooleanSupplier probeAction,
        final Runnable evictConnectionsAction,
        final ScheduledExecutorService probeScheduler,
        final PostgresControlPlaneMetrics metrics
    ) {
        this.failureThreshold = failureThreshold;
        this.probeIntervalMs = probeIntervalMs;
        this.probeAction = probeAction;
        this.evictConnectionsAction = evictConnectionsAction;
        this.probeScheduler = probeScheduler;
        this.metrics = metrics;
        this.openSupplier = this::isOpen;
        this.metrics.registerStandbyCircuitBreaker(this.openSupplier);
    }

    boolean isOpen() {
        return state.open();
    }

    long generation() {
        return state.generation();
    }

    void recordSuccess(final long callGeneration) {
        // Skip `stateLock` on the normal read path when no failures are pending.
        final State snapshot = state;
        if (snapshot.open() || snapshot.consecutiveFailures() == 0 || snapshot.generation() != callGeneration) {
            return;
        }
        synchronized (stateLock) {
            if (!closed && !state.open() && state.generation() == callGeneration && state.consecutiveFailures() > 0) {
                state = new State(false, 0, callGeneration);
            }
        }
    }

    /**
     * Records a connectivity or timeout failure from a standby call that started at
     * {@code callGeneration}.
     *
     * @return True if this failure tripped the circuit from {@code CLOSED} to {@code OPEN}; false otherwise.
     */
    boolean recordFailure(final long callGeneration, final Throwable cause) {
        final boolean tripped;
        synchronized (stateLock) {
            if (closed || state.open() || state.generation() != callGeneration) {
                return false;
            }
            final int failures = state.consecutiveFailures() + 1;
            if (failures >= failureThreshold) {
                state = new State(true, failures, callGeneration);
                tripped = true;
            } else {
                state = new State(false, failures, callGeneration);
                tripped = false;
            }
        }
        // Run pool eviction outside `stateLock` so HikariCP work doesn't block concurrent callers,
        // then schedule the probe so eviction always completes before the first probe runs.
        if (tripped) {
            LOGGER.warn(
                "Standby circuit breaker tripped open after {} consecutive connectivity/timeout failures; "
                    + "evicting standby connections and routing findBatches to primary",
                failureThreshold, cause);
            try {
                evictConnectionsAction.run();
            } catch (final RuntimeException e) {
                LOGGER.warn("Failed to soft-evict standby pool connections on circuit open", e);
            }
            synchronized (stateLock) {
                if (!closed && state.open() && state.generation() == callGeneration) {
                    scheduleProbeLocked();
                }
            }
        }
        return tripped;
    }

    private void scheduleProbeLocked() {
        if (probeFuture != null) {
            probeFuture.cancel(false);
            probeFuture = null;
        }
        try {
            // Use fixed delay instead of fixed rate so slow connection timeouts don't pile up
            // back-to-back probes.
            probeFuture = probeScheduler.scheduleWithFixedDelay(
                this::runProbe,
                probeIntervalMs,
                probeIntervalMs,
                TimeUnit.MILLISECONDS
            );
        } catch (final RejectedExecutionException e) {
            LOGGER.debug("Standby probe scheduler already shut down", e);
        }
    }

    // Visible for testing.
    void runProbe() {
        if (closed || !state.open()) {
            return;
        }
        final boolean healthy;
        try {
            healthy = probeAction.getAsBoolean();
        } catch (final RuntimeException e) {
            LOGGER.debug("Standby health probe failed", e);
            return;
        }
        if (!healthy) {
            return;
        }
        final boolean recovered;
        synchronized (stateLock) {
            if (closed || !state.open()) {
                return;
            }
            state = new State(false, 0, state.generation() + 1);
            if (probeFuture != null) {
                probeFuture.cancel(false);
                probeFuture = null;
            }
            recovered = true;
        }
        if (recovered) {
            LOGGER.info("Standby health probe succeeded; closing standby circuit breaker and resuming standby reads");
        }
    }

    /**
     * Checks whether any throwable in the cause, JDBC {@code nextException}, or suppressed graph
     * indicates a standby connectivity or timeout failure rather than a query or serialization error.
     *
     * @return True if the failure indicates an unreachable or timed-out standby; false otherwise.
     */
    static boolean isConnectivityOrTimeoutFailure(final Throwable throwable) {
        return hasConnectivityOrTimeoutCause(throwable, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static boolean hasConnectivityOrTimeoutCause(final Throwable throwable, final Set<Throwable> seen) {
        if (throwable == null || !seen.add(throwable)) {
            return false;
        }
        if (throwable instanceof SQLTransientConnectionException
            || throwable instanceof SocketTimeoutException
            || throwable instanceof ConnectException
            || throwable instanceof UnknownHostException) {
            return true;
        }
        if (throwable instanceof SQLException sqlException) {
            final String sqlState = sqlException.getSQLState();
            if (sqlState != null
                && (sqlState.startsWith("08") || STANDBY_UNAVAILABLE_SQL_STATES.contains(sqlState))) {
                return true;
            }
            if (hasConnectivityOrTimeoutCause(sqlException.getNextException(), seen)) {
                return true;
            }
        }
        if (hasConnectivityOrTimeoutCause(throwable.getCause(), seen)) {
            return true;
        }
        for (final Throwable suppressed : throwable.getSuppressed()) {
            if (hasConnectivityOrTimeoutCause(suppressed, seen)) {
                return true;
            }
        }
        return false;
    }

    private static boolean probeStandby(final HikariDataSource readDataSource) {
        try (Connection connection = readDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            // `PostgresControlPlane.dataSourceConfig` disables auto-commit, so `SELECT 1` opens a
            // transaction. Commit it before closing so the connection returns to HikariCP clean.
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
            return true;
        } catch (final Exception e) {
            LOGGER.debug("Standby SELECT 1 probe failed", e);
            return false;
        }
    }

    private static void softEvictConnections(final HikariDataSource readDataSource) {
        final HikariPoolMXBean poolMXBean = readDataSource.getHikariPoolMXBean();
        if (poolMXBean != null) {
            poolMXBean.softEvictConnections();
        }
    }

    private static ScheduledExecutorService createProbeScheduler() {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
            1,
            new InklessThreadFactory("inkless-pg-standby-probe-", true)
        );
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public void close() {
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            state = new State(false, 0, state.generation() + 1);
            if (probeFuture != null) {
                probeFuture.cancel(true);
                probeFuture = null;
            }
        }
        probeScheduler.shutdownNow();
        metrics.unregisterStandbyCircuitBreaker(openSupplier);
    }
}
