package io.aiven.inkless.control_plane.postgres;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;
import io.aiven.inkless.common.metrics.SensorProvider;
import io.aiven.inkless.common.metrics.MeasurableValue;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

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

    private final Sensor activeConnectionsCountSensor;
    private final Sensor totalConnectionsCountSensor;
    private final Sensor idleConnectionsCountSensor;
    private final Sensor maxConnectionsCountSensor;
    private final Sensor minConnectionsCountSensor;
    private final Sensor pendingThreadsCountSensor;
    private final Sensor connectionTimeoutCountSensor;


    public HikariMetricsTracker(String poolName, PoolStats poolStats) {
        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );

        metricsRegistry = new HikariMetricsRegistry(poolName);
        activeConnectionsCountSensor = registerSensor(metricsRegistry.activeConnectionsCountMetricName, ACTIVE_CONNECTIONS_COUNT, () -> (long) poolStats.getActiveConnections());
        totalConnectionsCountSensor = registerSensor(metricsRegistry.totalConnectionsCountMetricName, TOTAL_CONNECTIONS_COUNT, () -> (long) poolStats.getTotalConnections());
        idleConnectionsCountSensor = registerSensor(metricsRegistry.idleConnectionsCountMetricName, IDLE_CONNECTIONS_COUNT, () -> (long) poolStats.getIdleConnections());
        maxConnectionsCountSensor = registerSensor(metricsRegistry.maxConnectionsCountMetricName, MAX_CONNECTIONS_COUNT, () -> (long) poolStats.getMaxConnections());
        minConnectionsCountSensor = registerSensor(metricsRegistry.minConnectionsCountMetricName, MIN_CONNECTIONS_COUNT, () -> (long) poolStats.getMinConnections());
        pendingThreadsCountSensor = registerSensor(metricsRegistry.pendingThreadsCountMetricName, PENDING_THREADS_COUNT, () -> (long) poolStats.getPendingThreads());

        connectionTimeoutCountSensor = registerSensor(metricsRegistry.connectionTimeoutCountMetricName, CONNECTION_TIMEOUT_COUNT, connectionTimeoutCount::sum);
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
        // Implement any cleanup logic if necessary
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
