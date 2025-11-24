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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PostgresConnectionPoolMetrics implements IMetricsTracker {
    public static final String ACTIVE_CONNECTIONS_COUNT = "ActiveConnectionsCount";
    public static final String TOTAL_CONNECTIONS_COUNT = "TotalConnectionsCount";
    public static final String IDLE_CONNECTIONS_COUNT = "IdleConnectionsCount";
    public static final String MAX_CONNECTIONS_COUNT = "MaxConnectionsCount";
    public static final String MIN_CONNECTIONS_COUNT = "MinConnectionsCount";
    public static final String PENDING_THREADS_COUNT = "PendingThreadsCount";
    public static final String CONNECTION_ACQUIRED_NANOS = "ConnectionAcquiredNanos";
    public static final String CONNECTION_USAGE_MILLIS = "ConnectionUsageMillis";
    public static final String CONNECTION_TIMEOUT_COUNT = "ConnectionTimeoutCount";

    private final MetricName activeConnectionsCountMetricName;
    private final MetricName totalConnectionsCountMetricName;
    private final MetricName idleConnectionsCountMetricName;
    private final MetricName maxConnectionsCountMetricName;
    private final MetricName minConnectionsCountMetricName;
    private final MetricName pendingThreadsCountMetricName;
    private final MetricName connectionAcquiredNanosMetricName;
    private final MetricName connectionUsageMillisMetricName;
    private final MetricName connectionTimeoutCountMetricName;

    final KafkaMetricsGroup metrics;

    private final Meter connectionTimeoutMeter;
    private final Meter connectionUsageMeter;
    private final Meter connectionAcquireMeter;

    public PostgresConnectionPoolMetrics(final KafkaMetricsGroup metrics, final String poolName, final PoolStats poolStats) {
        this.metrics = metrics;
        final var tags = Map.of("pool", poolName);
        connectionTimeoutCountMetricName = metrics.metricName(CONNECTION_TIMEOUT_COUNT, tags);
        this.connectionTimeoutMeter = metrics.newMeter(connectionTimeoutCountMetricName, "connection timeouts", TimeUnit.SECONDS);
        connectionUsageMillisMetricName = metrics.metricName(CONNECTION_USAGE_MILLIS, tags);
        this.connectionUsageMeter = metrics.newMeter(connectionUsageMillisMetricName, "connection usage", TimeUnit.MILLISECONDS);
        connectionAcquiredNanosMetricName = metrics.metricName(CONNECTION_ACQUIRED_NANOS, tags);
        this.connectionAcquireMeter = metrics.newMeter(connectionAcquiredNanosMetricName, "connection acquires", TimeUnit.NANOSECONDS);

        totalConnectionsCountMetricName = metrics.metricName(TOTAL_CONNECTIONS_COUNT, tags);
        metrics.newGauge(totalConnectionsCountMetricName, poolStats::getTotalConnections);
        idleConnectionsCountMetricName = metrics.metricName(IDLE_CONNECTIONS_COUNT, tags);
        metrics.newGauge(idleConnectionsCountMetricName, poolStats::getIdleConnections);
        activeConnectionsCountMetricName = metrics.metricName(ACTIVE_CONNECTIONS_COUNT, tags);
        metrics.newGauge(activeConnectionsCountMetricName, poolStats::getActiveConnections);
        pendingThreadsCountMetricName = metrics.metricName(PENDING_THREADS_COUNT, tags);
        metrics.newGauge(pendingThreadsCountMetricName, poolStats::getPendingThreads);
        maxConnectionsCountMetricName = metrics.metricName(MAX_CONNECTIONS_COUNT, tags);
        metrics.newGauge(maxConnectionsCountMetricName, poolStats::getMaxConnections);
        minConnectionsCountMetricName = metrics.metricName(MIN_CONNECTIONS_COUNT, tags);
        metrics.newGauge(minConnectionsCountMetricName, poolStats::getMinConnections);
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

    @Override
    public void close() {
        metrics.removeMetric(connectionTimeoutCountMetricName);
        metrics.removeMetric(connectionAcquiredNanosMetricName);
        metrics.removeMetric(connectionUsageMillisMetricName);
        metrics.removeMetric(totalConnectionsCountMetricName);
        metrics.removeMetric(idleConnectionsCountMetricName);
        metrics.removeMetric(activeConnectionsCountMetricName);
        metrics.removeMetric(pendingThreadsCountMetricName);
        metrics.removeMetric(maxConnectionsCountMetricName);
        metrics.removeMetric(minConnectionsCountMetricName);
    }
}
