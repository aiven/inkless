/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
package io.aiven.inkless.delete;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class RetentionEnforcerMetrics implements Closeable {
    private static final String GROUP = RetentionEnforcer.class.getSimpleName();

    static final String RETENTION_ENFORCEMENT_TOTAL_TIME = "RetentionEnforcementTotalTime";
    private static final String RETENTION_ENFORCEMENT_TOTAL_TIME_DOC = "Total time spent on a retention enforcement cycle in milliseconds";
    static final String RETENTION_ENFORCEMENT_RATE = "RetentionEnforcementRate";
    private static final String RETENTION_ENFORCEMENT_RATE_DOC = "Total number of retention enforcement cycles started";
    static final String RETENTION_ENFORCEMENT_TOTAL_BATCHES_DELETED = "RetentionEnforcementTotalBatchesDeleted";
    private static final String RETENTION_ENFORCEMENT_TOTAL_BATCHES_DELETED_DOC = "Total number of batches deleted by retention enforcement";
    static final String RETENTION_ENFORCEMENT_TOTAL_BYTES_DELETED = "RetentionEnforcementTotalBytesDeleted";
    private static final String RETENTION_ENFORCEMENT_TOTAL_BYTES_DELETED_DOC = "Total number of bytes deleted by retention enforcement";
    static final String RETENTION_ENFORCEMENT_ERROR_RATE = "RetentionEnforcementErrorRate";
    private static final String RETENTION_ENFORCEMENT_ERROR_RATE_DOC = "Total number of retention enforcement errors";

    /**
     * This method returns a list of all the metric name templates for the RetentionEnforcerMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(RETENTION_ENFORCEMENT_TOTAL_TIME, GROUP, RETENTION_ENFORCEMENT_TOTAL_TIME_DOC),
            new MetricNameTemplate(RETENTION_ENFORCEMENT_RATE, GROUP, RETENTION_ENFORCEMENT_RATE_DOC),
            new MetricNameTemplate(RETENTION_ENFORCEMENT_TOTAL_BATCHES_DELETED, GROUP, RETENTION_ENFORCEMENT_TOTAL_BATCHES_DELETED_DOC),
            new MetricNameTemplate(RETENTION_ENFORCEMENT_TOTAL_BYTES_DELETED, GROUP, RETENTION_ENFORCEMENT_TOTAL_BYTES_DELETED_DOC),
            new MetricNameTemplate(RETENTION_ENFORCEMENT_ERROR_RATE, GROUP, RETENTION_ENFORCEMENT_ERROR_RATE_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        RetentionEnforcer.class.getPackageName(), RetentionEnforcer.class.getSimpleName());
    private final Histogram retentionEnforcementTotalTime;
    private final LongAdder retentionEnforcementRate = new LongAdder();
    private final LongAdder retentionEnforcementTotalBatchesDeleted = new LongAdder();
    private final LongAdder retentionEnforcementTotalBytesDeleted = new LongAdder();
    private final LongAdder retentionEnforcementErrorRate = new LongAdder();

    public RetentionEnforcerMetrics() {
        retentionEnforcementTotalTime = metricsGroup.newHistogram(RETENTION_ENFORCEMENT_TOTAL_TIME, true, Map.of());
        metricsGroup.newGauge(RETENTION_ENFORCEMENT_RATE, retentionEnforcementRate::intValue);
        metricsGroup.newGauge(RETENTION_ENFORCEMENT_TOTAL_BATCHES_DELETED, retentionEnforcementTotalBatchesDeleted::intValue);
        metricsGroup.newGauge(RETENTION_ENFORCEMENT_TOTAL_BYTES_DELETED, retentionEnforcementTotalBytesDeleted::intValue);
        metricsGroup.newGauge(RETENTION_ENFORCEMENT_ERROR_RATE, retentionEnforcementErrorRate::intValue);
    }

    public void recordRetentionEnforcementStarted() {
        retentionEnforcementRate.increment();
    }

    public void recordRetentionEnforcementFinishedSuccessfully(final long durationMs,
                                                               final long totalBatchesDeleted,
                                                               final long totalBytesDeleted) {
        retentionEnforcementTotalTime.update(durationMs);
        retentionEnforcementTotalBatchesDeleted.add(totalBatchesDeleted);
        retentionEnforcementTotalBytesDeleted.add(totalBytesDeleted);
    }

    public void recordRetentionEnforcementFinishedWithError() {
        retentionEnforcementErrorRate.increment();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(RETENTION_ENFORCEMENT_TOTAL_TIME);
        metricsGroup.removeMetric(RETENTION_ENFORCEMENT_RATE);
        metricsGroup.removeMetric(RETENTION_ENFORCEMENT_TOTAL_BATCHES_DELETED);
        metricsGroup.removeMetric(RETENTION_ENFORCEMENT_TOTAL_BYTES_DELETED);
        metricsGroup.removeMetric(RETENTION_ENFORCEMENT_ERROR_RATE);
    }
}
