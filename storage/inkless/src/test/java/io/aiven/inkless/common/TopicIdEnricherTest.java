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
package io.aiven.inkless.common;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;

import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicIdEnricherTest {
    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 10);
    static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, 0);
    static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, 1);
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_1 = new Uuid(11, 11);
    static final TopicPartition T1P0 = new TopicPartition(TOPIC_1, 0);
    static final TopicPartition T1P1 = new TopicPartition(TOPIC_1, 1);

    @Mock
    MetadataView metadataView;

    @Test
    void allFound() throws TopicIdEnricher.TopicIdNotFoundException {
        when(metadataView.getTopicId(TOPIC_0)).thenReturn(TOPIC_ID_0);
        when(metadataView.getTopicId(TOPIC_1)).thenReturn(TOPIC_ID_1);

        final Map<TopicIdPartition, String> result = TopicIdEnricher.enrich(metadataView, Map.of(
            T0P0, "a",
            T0P1, "b",
            T1P0, "c",
            T1P1, "d"
        ));
        assertThat(result).isEqualTo(Map.of(
            new TopicIdPartition(TOPIC_ID_0, T0P0), "a",
            new TopicIdPartition(TOPIC_ID_0, T0P1), "b",
            new TopicIdPartition(TOPIC_ID_1, T1P0), "c",
            new TopicIdPartition(TOPIC_ID_1, T1P1), "d"
        ));
    }

    @Test
    void someNotFound() {
        // This instead of the normal thenReturn to not depend on the map key iteration order
        // (and not trigger the strict mock checker).
        when(metadataView.getTopicId(anyString())).thenAnswer(invocation -> {
            final String topicName = invocation.getArgument(0, String.class);
            if (topicName.equals(TOPIC_1)) {
                return TOPIC_ID_1;
            } else {
                return Uuid.ZERO_UUID;
            }
        });

        assertThatThrownBy(() -> TopicIdEnricher.enrich(metadataView, Map.of(
            T0P0, "a",
            T0P1, "b",
            T1P0, "c",
            T1P1, "d"
        ))).isInstanceOf(TopicIdEnricher.TopicIdNotFoundException.class)
            .extracting("topicName")
            .isEqualTo(TOPIC_0);
    }
}
