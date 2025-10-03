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
package io.aiven.inkless.common.metrics;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static io.aiven.inkless.common.metrics.ThreadPoolMonitorMetricsRegistry.ACTIVE_THREADS;
import static io.aiven.inkless.common.metrics.ThreadPoolMonitorMetricsRegistry.METRIC_CONFIG;
import static io.aiven.inkless.common.metrics.ThreadPoolMonitorMetricsRegistry.PARALLELISM;
import static io.aiven.inkless.common.metrics.ThreadPoolMonitorMetricsRegistry.POOL_SIZE;
import static io.aiven.inkless.common.metrics.ThreadPoolMonitorMetricsRegistry.QUEUED_TASK_COUNT;

public class ThreadPoolMonitor implements Closeable {
    private final Metrics metrics;
    private final String groupName;
    private final Sensor activeThreadsSensor;
    private final Sensor poolSizeSensor;
    private final Sensor parallelismSensor;
    private final Sensor queuedTaskCountSensor;

    // only thread-pool executor is supported
    public ThreadPoolMonitor(final String groupName, final ExecutorService pool) {
        if (!(pool instanceof ThreadPoolExecutor threadPoolExecutor)) {
            throw new UnsupportedOperationException("Only ThreadPoolExecutor is supported");
        }

        this.groupName = groupName;

        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONFIG)
        );
        final var metricsRegistry = new ThreadPoolMonitorMetricsRegistry(groupName);

        // ThreadPoolExecutor monitoring
        this.activeThreadsSensor = new SensorProvider(metrics, ACTIVE_THREADS)
            .with(metricsRegistry.activeThreadsTotalMetricName, new MeasurableValue(() -> (long) threadPoolExecutor.getActiveCount()))
            .get();

        this.poolSizeSensor = new SensorProvider(metrics, POOL_SIZE)
            .with(metricsRegistry.poolSizeTotalMetricName, new MeasurableValue(() -> (long) threadPoolExecutor.getPoolSize()))
            .get();

        this.parallelismSensor = new SensorProvider(metrics, PARALLELISM)
            .with(metricsRegistry.parallelismTotalMetricName, new MeasurableValue(() -> (long) threadPoolExecutor.getCorePoolSize()))
            .get();

        this.queuedTaskCountSensor = new SensorProvider(metrics, QUEUED_TASK_COUNT)
            .with(metricsRegistry.queuedTaskCountTotalMetricName, new MeasurableValue(() -> (long) threadPoolExecutor.getQueue().size()))
            .get();
    }

    @Override
    public void close() {
        metrics.close();
    }

    @Override
    public String toString() {
        return "ThreadPoolMonitor{" +
               "groupName='" + groupName + '\'' +
               ", activeThreadsSensor=" + activeThreadsSensor +
               ", poolSizeSensor=" + poolSizeSensor +
               ", parallelismSensor=" + parallelismSensor +
               ", queuedTaskCountSensor=" + queuedTaskCountSensor +
               '}';
    }
}
