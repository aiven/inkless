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
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class PostgresConnectionPoolMetrics implements IMetricsTracker {
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

    private final KafkaMetricsGroup metrics;

    /**
     * What this instance currently owns, so {@link #close()} can tell its own registrations apart
     * from a newer pool's. Unlike the pool metrics themselves, {@code pool} is a fixed tag value,
     * not unique per {@link com.zaxxer.hikari.HikariDataSource}: a reconfiguration opens a new pool
     * under the same name while the old one may still be closing. Populated by {@link #registerGauge}
     * and {@link #registerMeter}.
     */
    private final Map<MetricName, Metric> owned = new LinkedHashMap<>();

    private final Meter connectionTimeoutMeter;
    private final Meter connectionUsageMeter;
    private final Meter connectionAcquireMeter;

    public PostgresConnectionPoolMetrics(final KafkaMetricsGroup metrics, final String poolName, final PoolStats poolStats) {
        this.metrics = metrics;
        final var tags = Map.of("pool", poolName);

        this.connectionTimeoutMeter = registerMeter(metrics.metricName(CONNECTION_TIMEOUT_COUNT, tags),
            "connection timeouts", TimeUnit.SECONDS);
        this.connectionUsageMeter = registerMeter(metrics.metricName(CONNECTION_USAGE_MILLIS, tags),
            "connection usage", TimeUnit.MILLISECONDS);
        this.connectionAcquireMeter = registerMeter(metrics.metricName(CONNECTION_ACQUIRED_NANOS, tags),
            "connection acquires", TimeUnit.NANOSECONDS);

        registerGauge(metrics.metricName(TOTAL_CONNECTIONS_COUNT, tags), poolStats::getTotalConnections);
        registerGauge(metrics.metricName(IDLE_CONNECTIONS_COUNT, tags), poolStats::getIdleConnections);
        registerGauge(metrics.metricName(ACTIVE_CONNECTIONS_COUNT, tags), poolStats::getActiveConnections);
        registerGauge(metrics.metricName(PENDING_THREADS_COUNT, tags), poolStats::getPendingThreads);
        registerGauge(metrics.metricName(MAX_CONNECTIONS_COUNT, tags), poolStats::getMaxConnections);
        registerGauge(metrics.metricName(MIN_CONNECTIONS_COUNT, tags), poolStats::getMinConnections);
    }

    /**
     * Registers a fresh gauge under {@code name}, evicting whatever a previous pool sharing that
     * name left behind. Without the eviction, Yammer's registry would hand back the previous pool's
     * gauge instead of registering this one's, and this pool's numbers would never surface.
     */
    private <T> Gauge<T> registerGauge(final MetricName name, final Supplier<T> supplier) {
        metrics.removeMetric(name);
        final Gauge<T> gauge = metrics.newGauge(name, supplier);
        owned.put(name, gauge);
        return gauge;
    }

    /** Same eviction as {@link #registerGauge}, for a {@link Meter}. */
    private Meter registerMeter(final MetricName name, final String eventType, final TimeUnit unit) {
        metrics.removeMetric(name);
        final Meter meter = metrics.newMeter(name, eventType, unit);
        owned.put(name, meter);
        return meter;
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        connectionAcquireMeter.mark(elapsedAcquiredNanos);
    }

    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        connectionUsageMeter.mark(elapsedBorrowedMillis);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutMeter.mark();
    }

    /**
     * Removes only the registrations this instance still owns.
     *
     * <p>Because {@code pool} is a fixed tag, a newer pool's {@link #registerGauge}/
     * {@link #registerMeter} may already have evicted this instance's registration and put its own
     * under the same name, while this instance is still closing. Removing unconditionally would
     * delete that newer pool's metric instead of this one's, silently blanking it with no error.
     * The identity check tells the two apart: it only removes a name if the registry still holds the
     * exact object this instance registered.
     */
    @Override
    public void close() {
        final Map<MetricName, Metric> registered = KafkaYammerMetrics.defaultRegistry().allMetrics();
        owned.forEach((name, mine) -> {
            if (registered.get(name) == mine) {
                metrics.removeMetric(name);
            }
        });
    }
}
