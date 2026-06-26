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

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * JMX metrics for the write path that pushes the leader's cross-tier (remote) log start offset to
 * the control plane (see {@link CrossTierLogStartReporter}).
 */
public final class CrossTierLogStartReporterMetrics implements Closeable {
    private static final String GROUP = CrossTierLogStartReporter.class.getSimpleName();

    static final String PARTITIONS_REPORTED = "PartitionsReported";
    private static final String PARTITIONS_REPORTED_DOC =
        "Total number of partition cross-tier log start offsets accepted by the control plane";
    static final String REPORT_ERRORS = "ReportErrors";
    private static final String REPORT_ERRORS_DOC =
        "Total number of errors while reporting cross-tier log start offsets to the control plane";
    static final String PENDING_PARTITIONS = "PendingPartitions";
    private static final String PENDING_PARTITIONS_DOC =
        "Current number of partitions with a buffered cross-tier log start offset awaiting the next flush";

    /**
     * This method returns a list of all the metric name templates for the CrossTierLogStartReporterMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(PARTITIONS_REPORTED, GROUP, PARTITIONS_REPORTED_DOC),
            new MetricNameTemplate(REPORT_ERRORS, GROUP, REPORT_ERRORS_DOC),
            new MetricNameTemplate(PENDING_PARTITIONS, GROUP, PENDING_PARTITIONS_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        CrossTierLogStartReporter.class.getPackageName(), CrossTierLogStartReporter.class.getSimpleName());
    private final LongAdder partitionsReported = new LongAdder();
    private final LongAdder reportErrors = new LongAdder();

    public CrossTierLogStartReporterMetrics(final Supplier<Integer> pendingSupplier) {
        metricsGroup.newGauge(PARTITIONS_REPORTED, partitionsReported::intValue);
        metricsGroup.newGauge(REPORT_ERRORS, reportErrors::intValue);
        metricsGroup.newGauge(PENDING_PARTITIONS, pendingSupplier::get);
    }

    public void recordPartitionReported() {
        partitionsReported.increment();
    }

    public void recordReportError() {
        reportErrors.increment();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(PARTITIONS_REPORTED);
        metricsGroup.removeMetric(REPORT_ERRORS);
        metricsGroup.removeMetric(PENDING_PARTITIONS);
    }
}
