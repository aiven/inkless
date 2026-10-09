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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP0;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.assertCauseIs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitFailureCause;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitSuccess;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.rlmmConfigs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.startedSegment;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.updateFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresRlmmMetricsTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private PostgresRemoteLogMetadataManager rlmm;
    private TestPluginMetrics pluginMetrics;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        rlmm = new PostgresRemoteLogMetadataManager();
        rlmm.configure(rlmmConfigs(pgContainer));
        rlmm.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
        pluginMetrics = new TestPluginMetrics();
        rlmm.withPluginMetrics(pluginMetrics);
    }

    @AfterEach
    void tearDown() throws Exception {
        rlmm.close();
        pgContainer.tearDown();
    }

    @Test
    void operationLatenciesRecorded() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
        rlmm.remoteLogSegmentMetadata(TP0, 0, 50L);
        rlmm.highestOffsetForEpoch(TP0, 0);
        rlmm.listRemoteLogSegments(TP0);
        rlmm.remoteLogSize(TP0, 0);
        rlmm.nextSegmentWithTxnIndex(TP0, 0, 50L);

        assertEquals(1.0, metricValue("AddSegmentCount"));
        assertEquals(1.0, metricValue("UpdateSegmentCount"));
        assertEquals(1.0, metricValue("LookupSegmentCount"));
        assertEquals(1.0, metricValue("HighestOffsetCount"));
        assertEquals(1.0, metricValue("ListSegmentsCount"));
        assertEquals(1.0, metricValue("RemoteLogSizeCount"));
        assertEquals(1.0, metricValue("NextTxnSegmentCount"));
        assertTrue(metricValue("AddSegmentMaxMs") >= 0.0);
    }

    @Test
    void transitionConflictsCounted() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));

        // Finishing over a delete-started segment is an invalid transition and a no-op.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
        assertEquals(1.0, metricValue("TransitionConflicts"));
        assertEquals(0.0, metricValue("OperationErrors"));
    }

    @Test
    void operationErrorsCountedExceptNotFound() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));

        // A programming error counts. A missing resource is routine and does not.
        final RemoteLogSegmentMetadata different = startedSegment(started.remoteLogSegmentId(),
            0L, 200L, Map.of(0, 0L), false, Optional.empty());
        assertCauseIs(awaitFailureCause(rlmm.addRemoteLogSegmentMetadata(different)),
            IllegalArgumentException.class);
        assertEquals(1.0, metricValue("OperationErrors"));

        // A missing resource is routine and does not count.
        final RemoteLogSegmentMetadataUpdate unknown = updateFor(
            new RemoteLogSegmentId(TP0, Uuid.randomUuid()), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        assertCauseIs(awaitFailureCause(rlmm.updateRemoteLogSegmentMetadata(unknown)),
            RemoteResourceNotFoundException.class);
        assertEquals(1.0, metricValue("OperationErrors"));
    }

    @Test
    void gaugesExposed() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));

        assertEquals(1.0, metricValue("ActivePartitions"));
        assertTrue(metricValue("PoolTotalConnections") >= 1.0);
        assertTrue(metricValue("PoolIdleConnections") >= 0.0);
        assertTrue(metricValue("PoolActiveConnections") >= 0.0);
        assertTrue(metricValue("PoolPendingThreads") >= 0.0);
    }

    private double metricValue(final String name) {
        return (Double) pluginMetrics.metrics.metrics()
            .get(pluginMetrics.metricName(name, "", new LinkedHashMap<>()))
            .metricValue();
    }

    private static final class TestPluginMetrics implements PluginMetrics {
        private final Metrics metrics = new Metrics();

        @Override
        public MetricName metricName(final String name, final String description,
                                     final LinkedHashMap<String, String> tags) {
            return metrics.metricName(name, "pg-rlmm-test", description, tags);
        }

        @Override
        public void addMetric(final MetricName metricName, final MetricValueProvider<?> metricValueProvider) {
            metrics.addMetric(metricName, metricValueProvider);
        }

        @Override
        public void removeMetric(final MetricName metricName) {
            metrics.removeMetric(metricName);
        }

        @Override
        public Sensor addSensor(final String name) {
            return metrics.sensor(name);
        }

        @Override
        public void removeSensor(final String name) {
            metrics.removeSensor(name);
        }
    }
}
