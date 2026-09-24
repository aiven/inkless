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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;

/**
 * Connection pool metrics shared by every {@code PostgresControlPlane} in the process.
 *
 * <p>A rebuilt control plane opens new pools under the same pool names, so the metric names don't
 * change across generations. Yammer's registry keeps whichever metric registered a name first, so
 * each pool name gets its metrics registered once, here, and they are never removed. The gauges
 * read whichever pool currently holds the name.
 */
public final class PostgresConnectionPoolMetrics {
    private static final PostgresConnectionPoolMetrics INSTANCE = new PostgresConnectionPoolMetrics();

    private static final String GROUP = PostgresConnectionPoolMetrics.class.getSimpleName();

    public static final String ACTIVE_CONNECTIONS_COUNT = "ActiveConnectionsCount";
    private static final String ACTIVE_CONNECTIONS_COUNT_DOC = "Number of currently active connections in the pool";
    public static final String TOTAL_CONNECTIONS_COUNT = "TotalConnectionsCount";
    private static final String TOTAL_CONNECTIONS_COUNT_DOC = "Total number of connections in the pool";
    public static final String IDLE_CONNECTIONS_COUNT = "IdleConnectionsCount";
    private static final String IDLE_CONNECTIONS_COUNT_DOC = "Number of idle connections in the pool";
    public static final String MAX_CONNECTIONS_COUNT = "MaxConnectionsCount";
    private static final String MAX_CONNECTIONS_COUNT_DOC = "Maximum number of connections allowed in the pool";
    public static final String MIN_CONNECTIONS_COUNT = "MinConnectionsCount";
    private static final String MIN_CONNECTIONS_COUNT_DOC = "Minimum number of connections maintained in the pool";
    public static final String PENDING_THREADS_COUNT = "PendingThreadsCount";
    private static final String PENDING_THREADS_COUNT_DOC = "Number of threads waiting for a connection from the pool";
    public static final String CONNECTION_ACQUIRED_NANOS = "ConnectionAcquiredNanos";
    private static final String CONNECTION_ACQUIRED_NANOS_DOC = "Time spent acquiring connections from the pool in nanoseconds";
    public static final String CONNECTION_USAGE_MILLIS = "ConnectionUsageMillis";
    private static final String CONNECTION_USAGE_MILLIS_DOC = "Time connections are held before being returned to the pool in milliseconds";
    public static final String CONNECTION_TIMEOUT_COUNT = "ConnectionTimeoutCount";
    private static final String CONNECTION_TIMEOUT_COUNT_DOC = "Rate of connection acquisition timeouts per second";

    /**
     * This method returns a list of all the metric name templates for the PostgresConnectionPoolMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        final Set<String> tags = Set.of("pool");
        return List.of(
            new MetricNameTemplate(ACTIVE_CONNECTIONS_COUNT, GROUP, ACTIVE_CONNECTIONS_COUNT_DOC, tags),
            new MetricNameTemplate(TOTAL_CONNECTIONS_COUNT, GROUP, TOTAL_CONNECTIONS_COUNT_DOC, tags),
            new MetricNameTemplate(IDLE_CONNECTIONS_COUNT, GROUP, IDLE_CONNECTIONS_COUNT_DOC, tags),
            new MetricNameTemplate(MAX_CONNECTIONS_COUNT, GROUP, MAX_CONNECTIONS_COUNT_DOC, tags),
            new MetricNameTemplate(MIN_CONNECTIONS_COUNT, GROUP, MIN_CONNECTIONS_COUNT_DOC, tags),
            new MetricNameTemplate(PENDING_THREADS_COUNT, GROUP, PENDING_THREADS_COUNT_DOC, tags),
            new MetricNameTemplate(CONNECTION_ACQUIRED_NANOS, GROUP, CONNECTION_ACQUIRED_NANOS_DOC, tags),
            new MetricNameTemplate(CONNECTION_USAGE_MILLIS, GROUP, CONNECTION_USAGE_MILLIS_DOC, tags),
            new MetricNameTemplate(CONNECTION_TIMEOUT_COUNT, GROUP, CONNECTION_TIMEOUT_COUNT_DOC, tags)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        PostgresConnectionPoolMetrics.class.getPackageName(), PostgresConnectionPoolMetrics.class.getSimpleName());
    private final ConcurrentMap<String, PoolMetrics> pools = new ConcurrentHashMap<>();

    private PostgresConnectionPoolMetrics() {
    }

    public static PostgresConnectionPoolMetrics instance() {
        return INSTANCE;
    }

    /**
     * Returns the tracker HikariCP uses for a pool, and makes the gauges for {@code poolName} read
     * that pool's {@code poolStats}.
     */
    public IMetricsTracker tracker(final String poolName, final PoolStats poolStats) {
        final PoolMetrics pool = pools.computeIfAbsent(poolName, PoolMetrics::new);
        pool.current.set(poolStats);
        return new Tracker(pool, poolStats);
    }

    private final class PoolMetrics {
        /** The newest open pool with this name, or null once it closes. */
        private final AtomicReference<PoolStats> current = new AtomicReference<>();
        private final Meter connectionTimeoutMeter;
        private final Meter connectionUsageMeter;
        private final Meter connectionAcquireMeter;

        private PoolMetrics(final String poolName) {
            final var tags = Map.of("pool", poolName);
            connectionTimeoutMeter = metricsGroup.newMeter(CONNECTION_TIMEOUT_COUNT, "connection timeouts", TimeUnit.SECONDS, tags);
            connectionUsageMeter = metricsGroup.newMeter(CONNECTION_USAGE_MILLIS, "connection usage", TimeUnit.MILLISECONDS, tags);
            connectionAcquireMeter = metricsGroup.newMeter(CONNECTION_ACQUIRED_NANOS, "connection acquires", TimeUnit.NANOSECONDS, tags);

            newGauge(TOTAL_CONNECTIONS_COUNT, tags, PoolStats::getTotalConnections);
            newGauge(IDLE_CONNECTIONS_COUNT, tags, PoolStats::getIdleConnections);
            newGauge(ACTIVE_CONNECTIONS_COUNT, tags, PoolStats::getActiveConnections);
            newGauge(PENDING_THREADS_COUNT, tags, PoolStats::getPendingThreads);
            newGauge(MAX_CONNECTIONS_COUNT, tags, PoolStats::getMaxConnections);
            newGauge(MIN_CONNECTIONS_COUNT, tags, PoolStats::getMinConnections);
        }

        private void newGauge(final String name, final Map<String, String> tags, final ToIntFunction<PoolStats> stat) {
            metricsGroup.newGauge(name, () -> {
                final PoolStats stats = current.get();
                return stats == null ? 0 : stat.applyAsInt(stats);
            }, tags);
        }
    }

    private record Tracker(PoolMetrics pool, PoolStats poolStats) implements IMetricsTracker {
        @Override
        public void recordConnectionAcquiredNanos(final long elapsedAcquiredNanos) {
            pool.connectionAcquireMeter.mark(elapsedAcquiredNanos);
        }

        @Override
        public void recordConnectionUsageMillis(final long elapsedBorrowedMillis) {
            pool.connectionUsageMeter.mark(elapsedBorrowedMillis);
        }

        @Override
        public void recordConnectionTimeout() {
            pool.connectionTimeoutMeter.mark();
        }

        /**
         * Detaches the gauges from this pool, unless a newer pool with the same name already took
         * them over. The old pool can close after the new one opens.
         */
        @Override
        public void close() {
            pool.current.compareAndSet(poolStats, null);
        }
    }
}
