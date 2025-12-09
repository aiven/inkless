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
package io.aiven.inkless.common;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;

class SharedStateTest {

    @Test void testStorageMetricName() {
        // create a shared state instance
        final MockTime time = new MockTime();
        final SharedState sharedState = SharedState.initialize(
            time,
            1,
            new InklessConfig(Map.of()),
            new NoopMetadataView(),
            new InMemoryControlPlane(time),
            new BrokerTopicStats(),
            () -> null
        );
        // Ensure that the JMX reporter is registered
        assertThat(sharedState.storageMetrics.reporters()).hasSize(1);
        final MetricsReporter metricsReporter = sharedState.storageMetrics.reporters().get(0);
        assertThat(metricsReporter).isInstanceOf(JmxReporter.class);
        final JmxReporter jmxReporter = (JmxReporter) metricsReporter;

        // Ensure the proper metric name is built and registered
        final MetricName metricName = new MetricName("test-metric", "test-group", "desc", Map.of());
        sharedState.storageMetrics.addMetric(metricName, (config, now) -> 42.0);
        final KafkaMetric metric = sharedState.storageMetrics.metric(metricName);
        jmxReporter.init(List.of(metric));
        jmxReporter.containsMbean("io.aiven.inkless.storage:type=test-group,name=test-metric");
    }

    private static class NoopMetadataView implements MetadataView {
        @Override
        public Map<String, Object> getDefaultConfig() {
            return Map.of();
        }

        @Override
        public Iterable<Node> getAliveBrokerNodes(ListenerName listenerName) {
            return null;
        }

        @Override
        public Integer getBrokerCount() {
            return 0;
        }

        @Override
        public Uuid getTopicId(String topicName) {
            return null;
        }

        @Override
        public boolean isDisklessTopic(String topicName) {
            return false;
        }

        @Override
        public Properties getTopicConfig(String topicName) {
            return null;
        }

        @Override
        public Set<TopicIdPartition> getDisklessTopicPartitions() {
            return Set.of();
        }
    }
}