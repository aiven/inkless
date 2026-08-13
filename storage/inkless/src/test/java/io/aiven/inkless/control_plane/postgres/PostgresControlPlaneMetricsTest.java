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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresControlPlaneMetricsTest {
    // Shared by every test in the JVM, so assertions compare before and after instead of expecting
    // absolute values.
    private final PostgresControlPlaneMetrics metrics = PostgresControlPlaneMetrics.instance();

    @Test
    void instanceIsShared() {
        assertThat(PostgresControlPlaneMetrics.instance()).isSameAs(metrics);
    }

    @Test
    void lastSuccessfulQueryAgeMs_isMinusOneBeforeFirstQuery() {
        assertThat(PostgresControlPlaneMetrics.ageMs(-1L, 5_000L)).isEqualTo(-1L);
    }

    @Test
    void lastSuccessfulQueryAgeMs_isTimeSinceLastQuery() {
        assertThat(PostgresControlPlaneMetrics.ageMs(1_000L, 5_000L)).isEqualTo(4_000L);
    }

    @Test
    void recordUpdatesTheRegisteredMetrics() {
        final int rateBefore = (int) gaugeValue("CommitFileQueryRate");
        final long countBefore = ((Histogram) registered("CommitFileQueryTime")).count();
        final long startMs = System.currentTimeMillis();

        metrics.onCommitFileCompleted(50L);

        assertThat(gaugeValue("CommitFileQueryRate")).isEqualTo(rateBefore + 1);
        assertThat(((Histogram) registered("CommitFileQueryTime")).count()).isEqualTo(countBefore + 1);
        assertThat(metrics.commitFileMetrics.lastSuccessfulQueryTimeMs.get())
            .isBetween(startMs, System.currentTimeMillis());
        assertThat((long) gaugeValue("CommitFileLastSuccessfulQueryAgeMs")).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void recordIsIndependentPerQueryType() {
        final int findBatchesBefore = (int) gaugeValue("FindBatchesQueryRate");

        metrics.onCommitFileCompleted(50L);

        assertThat(gaugeValue("FindBatchesQueryRate")).isEqualTo(findBatchesBefore);
    }

    private static Metric registered(final String name) {
        final KafkaMetricsGroup group = new KafkaMetricsGroup(
            PostgresControlPlane.class.getPackageName(), PostgresControlPlane.class.getSimpleName());
        return KafkaYammerMetrics.defaultRegistry().allMetrics().get(group.metricName(name, Map.of()));
    }

    private static Object gaugeValue(final String name) {
        return ((Gauge<?>) registered(name)).value();
    }
}
