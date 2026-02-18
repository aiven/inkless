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
 * GNU Affero General Public License for the details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.metrics;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.LogMetricNames;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class InklessLogMetricsTest {

    private static final String TOPIC = "diskless-metrics-test-topic";
    private static final Uuid TOPIC_ID = new Uuid(100L, 200L);
    private static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);
    private static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID, 1, TOPIC);

    @Mock
    private ControlPlane controlPlane;
    @Mock
    private MetadataView metadataView;

    private InklessLogMetrics metrics;

    @BeforeEach
    void setUp() {
        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of());
    }

    @AfterEach
    void tearDown() {
        if (metrics != null) {
            try {
                metrics.close();
            } catch (Exception ignored) {
                // cleanup
            }
        }
        removeDisklessLogMetricsFromRegistry();
    }

    private static void removeDisklessLogMetricsFromRegistry() {
        List<MetricName> toRemove = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(n -> "kafka.log".equals(n.getGroup()) && "Log".equals(n.getType())
                && (LogMetricNames.LOG_START_OFFSET.equals(n.getName())
                    || LogMetricNames.LOG_END_OFFSET.equals(n.getName())
                    || LogMetricNames.SIZE.equals(n.getName())))
            .filter(n -> n.getMBeanName().contains("topic=" + TOPIC))
            .collect(Collectors.toList());
        toRemove.forEach(KafkaYammerMetrics.defaultRegistry()::removeMetric);
    }

    private static long gaugeValue(String metricName, String topic, int partition) {
        String suffix = ",name=" + metricName + ",topic=" + topic + ",partition=" + partition;
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getMBeanName().endsWith(suffix))
            .findFirst()
            .map(e -> ((Number) ((Gauge<?>) e.getValue()).value()).longValue())
            .orElse(-999L);
    }

    @Test
    void registersGaugesAndReturnsValuesFromGetLogInfo() {
        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0, T0P1));
        when(controlPlane.getLogInfo(anyList())).thenReturn(List.of(
            GetLogInfoResponse.success(0L, 10L, 100L),
            GetLogInfoResponse.success(1L, 11L, 200L)
        ));

        metrics = new InklessLogMetrics(controlPlane, metadataView);

        assertThat(gaugeValue(LogMetricNames.LOG_START_OFFSET, TOPIC, 0)).isEqualTo(0L);
        assertThat(gaugeValue(LogMetricNames.LOG_END_OFFSET, TOPIC, 0)).isEqualTo(10L);
        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 0)).isEqualTo(100L);
        assertThat(gaugeValue(LogMetricNames.LOG_START_OFFSET, TOPIC, 1)).isEqualTo(1L);
        assertThat(gaugeValue(LogMetricNames.LOG_END_OFFSET, TOPIC, 1)).isEqualTo(11L);
        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 1)).isEqualTo(200L);
    }

    @Test
    void removesGaugesWhenPartitionDisappears() {
        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0, T0P1));
        when(controlPlane.getLogInfo(anyList())).thenReturn(List.of(
            GetLogInfoResponse.success(0L, 10L, 100L),
            GetLogInfoResponse.success(1L, 11L, 200L)
        ));

        metrics = new InklessLogMetrics(controlPlane, metadataView);
        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 1)).isEqualTo(200L);

        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0));
        when(controlPlane.getLogInfo(anyList())).thenReturn(List.of(GetLogInfoResponse.success(0L, 10L, 100L)));
        metrics.run();

        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 0)).isEqualTo(100L);
        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 1)).isEqualTo(-999L);
    }

    @Test
    void closeRemovesAllGauges() throws Exception {
        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0));
        when(controlPlane.getLogInfo(anyList())).thenReturn(List.of(GetLogInfoResponse.success(0L, 5L, 50L)));

        metrics = new InklessLogMetrics(controlPlane, metadataView);
        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 0)).isEqualTo(50L);

        metrics.close();
        metrics = null;

        assertThat(gaugeValue(LogMetricNames.SIZE, TOPIC, 0)).isEqualTo(-999L);
    }
}
