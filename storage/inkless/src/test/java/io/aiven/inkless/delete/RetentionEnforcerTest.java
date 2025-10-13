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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.MetadataView;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class RetentionEnforcerTest {
    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 0);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_1 = new Uuid(0, 1);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID_2 = new Uuid(0, 2);
    static final TopicIdPartition T2P0 = new TopicIdPartition(TOPIC_ID_2, 0, TOPIC_2);

    Time time = new MockTime();
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    RetentionEnforcementScheduler retentionEnforcementScheduler;

    @Captor
    ArgumentCaptor<List<EnforceRetentionRequest>> requestCaptor;

    @Nested
    class RetentionSettings {
        @Test
        void fullDefault() {
            when(retentionEnforcementScheduler.getReadyPartitions()).thenReturn(List.of(T0P0));
            when(metadataView.getDefaultConfig()).thenReturn(Map.of());
            when(metadataView.getTopicConfig(any())).thenReturn(new Properties());

            final var enforcer = new RetentionEnforcer(time, metadataView, controlPlane, retentionEnforcementScheduler, 0);
            enforcer.run();

            verify(controlPlane).enforceRetention(requestCaptor.capture(), eq(0));
            assertThat(requestCaptor.getValue()).map(EnforceRetentionRequest::retentionBytes).containsExactly(-1L);
            assertThat(requestCaptor.getValue()).map(EnforceRetentionRequest::retentionMs).containsExactly(604800000L);
        }

        @Test
        void logConfig() {
            when(retentionEnforcementScheduler.getReadyPartitions()).thenReturn(List.of(T0P0));
            when(metadataView.getDefaultConfig()).thenReturn(Map.of(
                RETENTION_BYTES_CONFIG, "123",
                RETENTION_MS_CONFIG, "567"
            ));
            when(metadataView.getTopicConfig(any())).thenReturn(new Properties());

            final var enforcer = new RetentionEnforcer(time, metadataView, controlPlane, retentionEnforcementScheduler, 0);
            enforcer.run();

            verify(controlPlane).enforceRetention(requestCaptor.capture(), eq(0));
            assertThat(requestCaptor.getValue()).map(EnforceRetentionRequest::retentionBytes).containsExactly(123L);
            assertThat(requestCaptor.getValue()).map(EnforceRetentionRequest::retentionMs).containsExactly(567L);
        }

        @Test
        void definedForTopic() {
            when(retentionEnforcementScheduler.getReadyPartitions()).thenReturn(List.of(T0P0));
            when(metadataView.getDefaultConfig()).thenReturn(Map.of(
                RETENTION_BYTES_CONFIG, "123",
                RETENTION_MS_CONFIG, "567"
            ));
            final Properties topicConfig = new Properties();
            topicConfig.put(RETENTION_BYTES_CONFIG, "123000");
            topicConfig.put(RETENTION_MS_CONFIG, "567000");
            when(metadataView.getTopicConfig(any())).thenReturn(topicConfig);

            final var enforcer = new RetentionEnforcer(time, metadataView, controlPlane, retentionEnforcementScheduler, 0);
            enforcer.run();

            verify(controlPlane).enforceRetention(requestCaptor.capture(), eq(0));
            assertThat(requestCaptor.getValue()).map(EnforceRetentionRequest::retentionBytes).containsExactly(123000L);
            assertThat(requestCaptor.getValue()).map(EnforceRetentionRequest::retentionMs).containsExactly(567000L);
        }
    }

    @Test
    void onlyDeleteTopics() {
        when(retentionEnforcementScheduler.getReadyPartitions()).thenReturn(List.of(T0P0, T1P0, T2P0));
        when(metadataView.getDefaultConfig()).thenReturn(Map.of(
            RETENTION_BYTES_CONFIG, "123",
            RETENTION_MS_CONFIG, "567"
        ));

        final var t0Config = new Properties();
        t0Config.put(CLEANUP_POLICY_CONFIG, "compact");
        when(metadataView.getTopicConfig(eq(TOPIC_0))).thenReturn(t0Config);
        final var t1Config = new Properties();
        t1Config.put(CLEANUP_POLICY_CONFIG, "delete,compact");
        when(metadataView.getTopicConfig(eq(TOPIC_1))).thenReturn(t1Config);
        final var t2Config = new Properties();
        t2Config.put(CLEANUP_POLICY_CONFIG, "delete");
        when(metadataView.getTopicConfig(eq(TOPIC_2))).thenReturn(t2Config);

        final var enforcer = new RetentionEnforcer(time, metadataView, controlPlane, retentionEnforcementScheduler, 0);
        enforcer.run();

        verify(controlPlane).enforceRetention(requestCaptor.capture(), eq(0));
        assertThat(requestCaptor.getValue())
            .map(EnforceRetentionRequest::topicId)
            .containsExactly(TOPIC_ID_1, TOPIC_ID_2);
    }
}
