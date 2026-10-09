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
package io.aiven.inkless.remote_log_metadata.postgres;

import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Max;

import com.zaxxer.hikari.HikariPoolMXBean;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

class PostgresRemoteLogMetadataMetrics {
    private final Sensor addSegmentSensor;
    private final Sensor updateSegmentSensor;
    private final Sensor putPartitionDeleteSensor;
    private final Sensor lookupSegmentSensor;
    private final Sensor nextTxnSegmentSensor;
    private final Sensor highestOffsetSensor;
    private final Sensor listSegmentsSensor;
    private final Sensor remoteLogSizeSensor;

    private final LongAdder transitionConflicts = new LongAdder();
    private final LongAdder operationErrors = new LongAdder();

    PostgresRemoteLogMetadataMetrics(final PluginMetrics pluginMetrics,
                                      final IntSupplier activePartitions,
                                      final Supplier<HikariPoolMXBean> pool) {
        addSegmentSensor = latencySensor(pluginMetrics, "AddSegment", "Add a remote log segment");
        updateSegmentSensor = latencySensor(pluginMetrics, "UpdateSegment", "Update a remote log segment");
        putPartitionDeleteSensor = latencySensor(pluginMetrics, "PutPartitionDelete", "Put a remote partition delete");
        lookupSegmentSensor = latencySensor(pluginMetrics, "LookupSegment", "Look up a remote log segment");
        nextTxnSegmentSensor = latencySensor(pluginMetrics, "NextTxnSegment", "Find the next segment with a transaction index");
        highestOffsetSensor = latencySensor(pluginMetrics, "HighestOffset", "Look up the highest offset for an epoch");
        listSegmentsSensor = latencySensor(pluginMetrics, "ListSegments", "List remote log segments");
        remoteLogSizeSensor = latencySensor(pluginMetrics, "RemoteLogSize", "Compute the remote log size for an epoch");

        gauge(pluginMetrics, "TransitionConflicts",
            "Mutations dropped as invalid state transitions",
            (config, now) -> (double) transitionConflicts.sum());
        gauge(pluginMetrics, "OperationErrors",
            "Operations that failed with an unexpected error",
            (config, now) -> (double) operationErrors.sum());
        gauge(pluginMetrics, "ActivePartitions",
            "Partitions currently registered through leadership notifications",
            (config, now) -> (double) activePartitions.getAsInt());
        gauge(pluginMetrics, "PoolActiveConnections",
            "Currently active connections in the RLMM pool",
            (config, now) -> (double) poolInt(pool, HikariPoolMXBean::getActiveConnections));
        gauge(pluginMetrics, "PoolIdleConnections",
            "Currently idle connections in the RLMM pool",
            (config, now) -> (double) poolInt(pool, HikariPoolMXBean::getIdleConnections));
        gauge(pluginMetrics, "PoolTotalConnections",
            "Total connections in the RLMM pool",
            (config, now) -> (double) poolInt(pool, HikariPoolMXBean::getTotalConnections));
        gauge(pluginMetrics, "PoolPendingThreads",
            "Threads waiting for a connection from the RLMM pool",
            (config, now) -> (double) poolInt(pool, HikariPoolMXBean::getThreadsAwaitingConnection));
    }

    void recordAddSegment(final long durationMs) {
        addSegmentSensor.record(durationMs);
    }

    void recordUpdateSegment(final long durationMs) {
        updateSegmentSensor.record(durationMs);
    }

    void recordPutPartitionDelete(final long durationMs) {
        putPartitionDeleteSensor.record(durationMs);
    }

    void recordLookupSegment(final long durationMs) {
        lookupSegmentSensor.record(durationMs);
    }

    void recordNextTxnSegment(final long durationMs) {
        nextTxnSegmentSensor.record(durationMs);
    }

    void recordHighestOffset(final long durationMs) {
        highestOffsetSensor.record(durationMs);
    }

    void recordListSegments(final long durationMs) {
        listSegmentsSensor.record(durationMs);
    }

    void recordRemoteLogSize(final long durationMs) {
        remoteLogSizeSensor.record(durationMs);
    }

    void recordTransitionConflict() {
        transitionConflicts.increment();
    }

    void recordError() {
        operationErrors.increment();
    }

    private static Sensor latencySensor(final PluginMetrics pluginMetrics, final String name, final String what) {
        final Sensor sensor = pluginMetrics.addSensor(name);
        sensor.add(pluginMetrics.metricName(name + "AvgMs", what + ": average latency in milliseconds", new LinkedHashMap<>()),
            new Avg());
        sensor.add(pluginMetrics.metricName(name + "MaxMs", what + ": maximum latency in milliseconds", new LinkedHashMap<>()),
            new Max());
        sensor.add(pluginMetrics.metricName(name + "Count", what + ": total operations", new LinkedHashMap<>()),
            new CumulativeCount());
        return sensor;
    }

    private static void gauge(final PluginMetrics pluginMetrics,
                              final String name,
                              final String description,
                              final MetricValueProvider<Double> provider) {
        pluginMetrics.addMetric(pluginMetrics.metricName(name, description, new LinkedHashMap<>()), provider);
    }

    private static int poolInt(final Supplier<HikariPoolMXBean> pool,
                               final java.util.function.ToIntFunction<HikariPoolMXBean> getter) {
        final HikariPoolMXBean bean = pool.get();
        return bean == null ? 0 : getter.applyAsInt(bean);
    }
}
