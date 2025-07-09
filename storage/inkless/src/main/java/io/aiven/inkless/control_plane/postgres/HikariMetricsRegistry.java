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

public class HikariMetricsRegistry {
    public static final String METRIC_CONTEXT = "io.aiven.inkless.control_plane.postgres.connection_pool";

    public static final String ACTIVE_CONNECTIONS_COUNT = "active-connections-count";
    public static final String TOTAL_CONNECTIONS_COUNT = "total-connections-count";
    public static final String IDLE_CONNECTIONS_COUNT = "idle-connections-count";
    public static final String MAX_CONNECTIONS_COUNT = "max-connections-count";
    public static final String MIN_CONNECTIONS_COUNT = "min-connections-count";
    public static final String PENDING_THREADS_COUNT = "pending-threads-count";
    public static final String CONNECTION_ACQUIRED_NANOS = "connection-acquired-nanos";
    public static final String CONNECTION_ACQUIRED_NANOS_MAX = CONNECTION_ACQUIRED_NANOS + "-max";
    public static final String CONNECTION_ACQUIRED_NANOS_AVG = CONNECTION_ACQUIRED_NANOS + "-avg";
    public static final String CONNECTION_USAGE_MILLIS = "connection-usage-millis";
    public static final String CONNECTION_USAGE_MILLIS_MAX = CONNECTION_USAGE_MILLIS + "-max";
    public static final String CONNECTION_USAGE_MILLIS_AVG = CONNECTION_USAGE_MILLIS + "-avg";
    public static final String CONNECTION_TIMEOUT_COUNT = "connection-timeout-count";

    public final MetricNameTemplate activeConnectionsCountMetricName;
    public final MetricNameTemplate totalConnectionsCountMetricName;
    public final MetricNameTemplate idleConnectionsCountMetricName;
    public final MetricNameTemplate maxConnectionsCountMetricName;
    public final MetricNameTemplate minConnectionsCountMetricName;
    public final MetricNameTemplate pendingThreadsCountMetricName;
    public final MetricNameTemplate connectionAcquiredNanosAvgMetricName;
    public final MetricNameTemplate connectionAcquiredNanosMaxMetricName;
    public final MetricNameTemplate connectionUsageMillisAvgMetricName;
    public final MetricNameTemplate connectionUsageMillisMaxMetricName;
    public final MetricNameTemplate connectionTimeoutCountMetricName;

    public HikariMetricsRegistry(String poolName) {
        activeConnectionsCountMetricName = new MetricNameTemplate(
            ACTIVE_CONNECTIONS_COUNT,
            poolName,
            "Number of active connections in the pool"
        );
        totalConnectionsCountMetricName = new MetricNameTemplate(
            TOTAL_CONNECTIONS_COUNT,
            poolName,
            "Total number of connections created in the pool"
        );
        idleConnectionsCountMetricName = new MetricNameTemplate(
            IDLE_CONNECTIONS_COUNT,
            poolName,
            "Number of idle connections in the pool"
        );
        maxConnectionsCountMetricName = new MetricNameTemplate(
            MAX_CONNECTIONS_COUNT,
            poolName,
            "Maximum number of connections allowed in the pool"
        );
        minConnectionsCountMetricName = new MetricNameTemplate(
            MIN_CONNECTIONS_COUNT,
            poolName,
            "Minimum number of connections maintained in the pool"
        );
        pendingThreadsCountMetricName = new MetricNameTemplate(
            PENDING_THREADS_COUNT,
            poolName,
            "Number of threads waiting for a connection from the pool"
        );
        connectionAcquiredNanosAvgMetricName = new MetricNameTemplate(
            CONNECTION_ACQUIRED_NANOS_AVG,
            poolName,
            "Average time spent acquiring connections in nanoseconds"
        );
        connectionAcquiredNanosMaxMetricName = new MetricNameTemplate(
            CONNECTION_ACQUIRED_NANOS_MAX,
            poolName,
            "Maximum time spent acquiring a connection in nanoseconds"
        );
        connectionUsageMillisAvgMetricName = new MetricNameTemplate(
            CONNECTION_USAGE_MILLIS_AVG,
            poolName,
            "Average time spent using connections in milliseconds"
        );
        connectionUsageMillisMaxMetricName = new MetricNameTemplate(
            CONNECTION_USAGE_MILLIS_MAX,
            poolName,
            "Maximum time spent using a connection in milliseconds"
        );
        connectionTimeoutCountMetricName = new MetricNameTemplate(
            CONNECTION_TIMEOUT_COUNT,
            poolName,
            "Number of times a connection acquisition timed out"
        );
    }
}
