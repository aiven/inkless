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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.aiven.inkless.delete.ConsolidatedDisklessLogPrunerMetrics.CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED;
import static io.aiven.inkless.delete.ConsolidatedDisklessLogPrunerMetrics.CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED;
import static io.aiven.inkless.delete.ConsolidatedDisklessLogPrunerMetrics.CONSOLIDATION_PRUNE_TOTAL_TIME;
import static org.assertj.core.api.Assertions.assertThat;

class ConsolidatedDisklessLogPrunerMetricsTest {

    @Test
    void accumulatesTotalsAcrossRuns() {
        final ConsolidatedDisklessLogPrunerMetrics metrics = new ConsolidatedDisklessLogPrunerMetrics();
        try {
            metrics.recordConsolidationPruneFinishedSuccessfully(10L, 5L, 70L);
            metrics.recordConsolidationPruneFinishedSuccessfully(30L, 5L, 70L);

            assertThat(gaugeValue(CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED)).isEqualTo(10L);
            assertThat(gaugeValue(CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED)).isEqualTo(140L);
            final Histogram totalTime = (Histogram) registered(CONSOLIDATION_PRUNE_TOTAL_TIME);
            assertThat(totalTime.count()).isEqualTo(2L);
            assertThat(totalTime.max()).isEqualTo(30.0);
        } finally {
            metrics.close();
        }
    }

    @Test
    void closeRemovesTheMetrics() {
        final ConsolidatedDisklessLogPrunerMetrics metrics = new ConsolidatedDisklessLogPrunerMetrics();
        assertThat(registered(CONSOLIDATION_PRUNE_TOTAL_TIME)).isNotNull();

        metrics.close();

        assertThat(registered(CONSOLIDATION_PRUNE_TOTAL_TIME)).isNull();
        assertThat(registered(CONSOLIDATION_PRUNE_TOTAL_BATCHES_DELETED)).isNull();
        assertThat(registered(CONSOLIDATION_PRUNE_TOTAL_BYTES_DELETED)).isNull();
    }

    private static Metric registered(final String name) {
        final KafkaMetricsGroup group = new KafkaMetricsGroup(
            ConsolidatedDisklessLogPrunerMetrics.class.getPackageName(), "ConsolidatedDisklessLogPruner");
        return KafkaYammerMetrics.defaultRegistry().allMetrics().get(group.metricName(name, Map.of()));
    }

    private static Object gaugeValue(final String name) {
        return ((Gauge<?>) registered(name)).value();
    }
}
