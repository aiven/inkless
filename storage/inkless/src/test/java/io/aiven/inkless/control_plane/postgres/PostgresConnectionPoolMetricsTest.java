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
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresConnectionPoolMetricsTest {
    private final PostgresConnectionPoolMetrics metrics = PostgresConnectionPoolMetrics.instance();

    @Test
    void instanceIsShared() {
        assertThat(PostgresConnectionPoolMetrics.instance()).isSameAs(metrics);
    }

    @Test
    void gaugesReadTheNewestPoolWithTheName(final TestInfo testInfo) {
        final String pool = poolName(testInfo);
        metrics.tracker(pool, stats(3));
        metrics.tracker(pool, stats(7));

        assertThat(gaugeValue(pool, PostgresConnectionPoolMetrics.TOTAL_CONNECTIONS_COUNT)).isEqualTo(7);
    }

    @Test
    void closingAnOlderPoolKeepsTheNewerPoolsGauges(final TestInfo testInfo) {
        final String pool = poolName(testInfo);
        final IMetricsTracker older = metrics.tracker(pool, stats(3));
        metrics.tracker(pool, stats(7));

        older.close();

        assertThat(gaugeValue(pool, PostgresConnectionPoolMetrics.TOTAL_CONNECTIONS_COUNT)).isEqualTo(7);
    }

    @Test
    void closingTheCurrentPoolZeroesItsGauges(final TestInfo testInfo) {
        final String pool = poolName(testInfo);
        final IMetricsTracker tracker = metrics.tracker(pool, stats(3));

        tracker.close();

        assertThat(gaugeValue(pool, PostgresConnectionPoolMetrics.TOTAL_CONNECTIONS_COUNT)).isEqualTo(0);
    }

    @Test
    void metersAccumulateAcrossPoolsWithTheName(final TestInfo testInfo) {
        final String pool = poolName(testInfo);
        final IMetricsTracker older = metrics.tracker(pool, stats(3));
        older.recordConnectionTimeout();
        older.close();

        metrics.tracker(pool, stats(7)).recordConnectionTimeout();

        assertThat(((Meter) registered(pool, PostgresConnectionPoolMetrics.CONNECTION_TIMEOUT_COUNT)).count())
            .isEqualTo(2);
    }

    private static String poolName(final TestInfo testInfo) {
        return "test-" + testInfo.getTestMethod().orElseThrow().getName();
    }

    private static PoolStats stats(final int total) {
        return new PoolStats(0) {
            @Override
            protected void update() {
                totalConnections = total;
            }
        };
    }

    private static Metric registered(final String pool, final String name) {
        final KafkaMetricsGroup group = new KafkaMetricsGroup(
            PostgresConnectionPoolMetrics.class.getPackageName(), PostgresConnectionPoolMetrics.class.getSimpleName());
        return KafkaYammerMetrics.defaultRegistry().allMetrics().get(group.metricName(name, Map.of("pool", pool)));
    }

    private static Object gaugeValue(final String pool, final String name) {
        return ((Gauge<?>) registered(pool, name)).value();
    }
}
