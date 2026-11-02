/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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

public class ConsolidatedDisklessLogPrunerMetrics implements Closeable {
    private static final String GROUP = "ConsolidatedDisklessLogPruner";

    static final String CONSOLIDATION_PRUNE_TOTAL_TIME = "ConsolidationPruneTotalTime";
    private static final String CONSOLIDATION_PRUNE_TOTAL_TIME_DOC =
        "Total time spent on a consolidation prune run in milliseconds";
    static final String CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED = "ConsolidationPruneTotalBatchesDeleted";
    private static final String CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED_DOC =
        "Total number of batches deleted by consolidation prune";
    static final String CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED = "ConsolidationPruneTotalBytesDeleted";
    private static final String CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED_DOC =
        "Total number of bytes deleted by consolidation prune";
    static final String CONSOLIDATION_PRUNE_ERROR_RATE = "ConsolidationPruneErrorRate";
    private static final String CONSOLIDATION_PRUNE_ERROR_RATE_DOC =
        "Total number of consolidation prune errors";

    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(
                CONSOLIDATION_PRUNE_TOTAL_TIME,
                GROUP,
                CONSOLIDATION_PRUNE_TOTAL_TIME_DOC
            ),
            new MetricNameTemplate(
                CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED,
                GROUP,
                CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED_DOC
            ),
            new MetricNameTemplate(
                CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED,
                GROUP,
                CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED_DOC
            ),
            new MetricNameTemplate(
                CONSOLIDATION_PRUNE_ERROR_RATE,
                GROUP,
                CONSOLIDATION_PRUNE_ERROR_RATE_DOC
            )
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        ConsolidatedDisklessLogPrunerMetrics.class.getPackageName(), GROUP);
    private final Histogram consolidationPruneTotalTime;
    private final LongAdder consolidationPruneTotalBatchesDeleted = new LongAdder();
    private final LongAdder consolidationPruneTotalBytesDeleted = new LongAdder();
    private final LongAdder consolidationPruneErrorRate = new LongAdder();

    public ConsolidatedDisklessLogPrunerMetrics() {
        consolidationPruneTotalTime = metricsGroup.newHistogram(CONSOLIDATION_PRUNE_TOTAL_TIME, true, Map.of());
        metricsGroup.newGauge(
            CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED,
            consolidationPruneTotalBatchesDeleted::sum
        );
        metricsGroup.newGauge(
            CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED,
            consolidationPruneTotalBytesDeleted::sum
        );
        metricsGroup.newGauge(
            CONSOLIDATION_PRUNE_ERROR_RATE,
            consolidationPruneErrorRate::sum
        );
    }

    public void recordConsolidationPruneFinishedSuccessfully(final long durationMs,
                                                             final long totalBatchesDeleted,
                                                             final long totalBytesDeleted) {
        consolidationPruneTotalTime.update(durationMs);
        consolidationPruneTotalBatchesDeleted.add(totalBatchesDeleted);
        consolidationPruneTotalBytesDeleted.add(totalBytesDeleted);
    }

    public void recordConsolidationPruneFinishedWithError() {
        consolidationPruneErrorRate.increment();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(CONSOLIDATION_PRUNE_TOTAL_TIME);
        metricsGroup.removeMetric(CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED);
        metricsGroup.removeMetric(CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED);
        metricsGroup.removeMetric(CONSOLIDATION_PRUNE_ERROR_RATE);
    }
}
