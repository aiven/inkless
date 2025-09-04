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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Set;

import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicTypeCounterTest {
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
    void empty() {
        final TopicTypeCounter counter = new TopicTypeCounter(metadataView);
        final TopicTypeCounter.Result result = counter.count(Set.of());
        assertThat(result).isEqualTo(new TopicTypeCounter.Result(0, 0));
    }

    @Test
    void onlyInkless() {
        when(metadataView.isDisklessTopic(TOPIC_0)).thenReturn(true);
        when(metadataView.isDisklessTopic(TOPIC_1)).thenReturn(true);

        final TopicTypeCounter counter = new TopicTypeCounter(metadataView);
        final TopicTypeCounter.Result result = counter.count(Set.of(T0P0, T0P1, T1P0, T1P1));
        assertThat(result).isEqualTo(new TopicTypeCounter.Result(4, 0));
    }

    @Test
    void onlyClassic() {
        when(metadataView.isDisklessTopic(TOPIC_0)).thenReturn(false);
        when(metadataView.isDisklessTopic(TOPIC_1)).thenReturn(false);

        final TopicTypeCounter counter = new TopicTypeCounter(metadataView);
        final TopicTypeCounter.Result result = counter.count(Set.of(T0P0, T0P1, T1P0, T1P1));
        assertThat(result).isEqualTo(new TopicTypeCounter.Result(0, 4));
    }

    @Test
    void mix() {
        when(metadataView.isDisklessTopic(TOPIC_0)).thenReturn(false);
        when(metadataView.isDisklessTopic(TOPIC_1)).thenReturn(true);

        final TopicTypeCounter counter = new TopicTypeCounter(metadataView);
        final TopicTypeCounter.Result result = counter.count(Set.of(T0P0, T0P1, T1P0, T1P1));
        assertThat(result).isEqualTo(new TopicTypeCounter.Result(2, 2));
    }

    @Test
    void resultBothTypesPresent() {
        assertThat(new TopicTypeCounter.Result(2, 2).bothTypesPresent())
            .isTrue();
        assertThat(new TopicTypeCounter.Result(0, 2).bothTypesPresent())
            .isFalse();
        assertThat(new TopicTypeCounter.Result(2, 0).bothTypesPresent())
            .isFalse();
    }

    @Test
    void noInkless() {
        assertThat(new TopicTypeCounter.Result(2, 2).noDiskless())
            .isFalse();
        assertThat(new TopicTypeCounter.Result(0, 2).noDiskless())
            .isTrue();
        assertThat(new TopicTypeCounter.Result(2, 0).noDiskless())
            .isFalse();
    }
}