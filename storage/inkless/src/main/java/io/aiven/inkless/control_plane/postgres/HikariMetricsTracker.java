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
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import io.aiven.inkless.common.metrics.MeasurableValue;
import io.aiven.inkless.common.metrics.SensorProvider;

import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.ACTIVE_CONNECTIONS_COUNT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.CONNECTION_ACQUIRED_NANOS;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.CONNECTION_TIMEOUT_COUNT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.CONNECTION_USAGE_MILLIS;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.IDLE_CONNECTIONS_COUNT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.MAX_CONNECTIONS_COUNT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.METRIC_CONTEXT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.MIN_CONNECTIONS_COUNT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.PENDING_THREADS_COUNT;
import static io.aiven.inkless.control_plane.postgres.HikariMetricsRegistry.TOTAL_CONNECTIONS_COUNT;

public class HikariMetricsTracker implements IMetricsTracker {
    private final Metrics metrics;
    final HikariMetricsRegistry metricsRegistry;

    private final LongAdder connectionTimeoutCount = new LongAdder();

    private Sensor activeConnectionsCountSensor;
    private Sensor totalConnectionsCountSensor;
    private Sensor idleConnectionsCountSensor;
    private Sensor maxConnectionsCountSensor;
    private Sensor minConnectionsCountSensor;
    private Sensor pendingThreadsCountSensor;
    private Sensor connectionTimeoutCountSensor;

    private HikariMetricsTracker(final String poolName) {
        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );
        this.metricsRegistry = new HikariMetricsRegistry(poolName);
    }

    public static HikariMetricsTracker create(final String poolName, final PoolStats poolStats) {
        final var tracker = new HikariMetricsTracker(poolName);

        tracker.activeConnectionsCountSensor = tracker.registerSensor(tracker.metricsRegistry.activeConnectionsCountMetricName, ACTIVE_CONNECTIONS_COUNT, () -> (long) poolStats.getActiveConnections());
        tracker.totalConnectionsCountSensor = tracker.registerSensor(tracker.metricsRegistry.totalConnectionsCountMetricName, TOTAL_CONNECTIONS_COUNT, () -> (long) poolStats.getTotalConnections());
        tracker.idleConnectionsCountSensor = tracker.registerSensor(tracker.metricsRegistry.idleConnectionsCountMetricName, IDLE_CONNECTIONS_COUNT, () -> (long) poolStats.getIdleConnections());
        tracker.maxConnectionsCountSensor = tracker.registerSensor(tracker.metricsRegistry.maxConnectionsCountMetricName, MAX_CONNECTIONS_COUNT, () -> (long) poolStats.getMaxConnections());
        tracker.minConnectionsCountSensor = tracker.registerSensor(tracker.metricsRegistry.minConnectionsCountMetricName, MIN_CONNECTIONS_COUNT, () -> (long) poolStats.getMinConnections());
        tracker.pendingThreadsCountSensor = tracker.registerSensor(tracker.metricsRegistry.pendingThreadsCountMetricName, PENDING_THREADS_COUNT, () -> (long) poolStats.getPendingThreads());
        tracker.connectionTimeoutCountSensor = tracker.registerSensor(tracker.metricsRegistry.connectionTimeoutCountMetricName, CONNECTION_TIMEOUT_COUNT, tracker.connectionTimeoutCount::sum);

        return tracker;
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        new SensorProvider(metrics, CONNECTION_ACQUIRED_NANOS)
            .with(metricsRegistry.connectionAcquiredNanosAvgMetricName, new Avg())
            .with(metricsRegistry.connectionAcquiredNanosMaxMetricName, new Max())
            .get()
            .record(elapsedAcquiredNanos);
    }

    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        new SensorProvider(metrics, CONNECTION_USAGE_MILLIS)
            .with(metricsRegistry.connectionUsageMillisAvgMetricName, new Avg())
            .with(metricsRegistry.connectionUsageMillisMaxMetricName, new Max())
            .get()
            .record(elapsedBorrowedMillis);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutCount.increment();
    }

    @Override
    public void close() {
        metrics.close();
    }

    Sensor registerSensor(final MetricNameTemplate metricName, final String sensorName, final Supplier<Long> supplier) {
        return new SensorProvider(metrics, sensorName)
            .with(metricName, new MeasurableValue(supplier))
            .get();
    }

    @Override
    public String toString() {
        return "HikariMetricsTracker{" +
            "metrics=" + metrics +
            ", metricsRegistry=" + metricsRegistry +
            ", connectionTimeoutCount=" + connectionTimeoutCount +
            ", activeConnectionsCountSensor=" + activeConnectionsCountSensor +
            ", totalConnectionsCountSensor=" + totalConnectionsCountSensor +
            ", idleConnectionsCountSensor=" + idleConnectionsCountSensor +
            ", maxConnectionsCountSensor=" + maxConnectionsCountSensor +
            ", minConnectionsCountSensor=" + minConnectionsCountSensor +
            ", pendingThreadsCountSensor=" + pendingThreadsCountSensor +
            ", connectionTimeoutCountSensor=" + connectionTimeoutCountSensor +
            '}';
    }
}
