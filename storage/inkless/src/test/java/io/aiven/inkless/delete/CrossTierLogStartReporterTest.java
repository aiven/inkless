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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;

import io.aiven.inkless.cache.CrossTierLogStartCache;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetRequest;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class CrossTierLogStartReporterTest {

    private static final String TOPIC = "t";
    private static final Uuid TOPIC_ID = new Uuid(1, 2);
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicIdPartition TIDP0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);

    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    CrossTierLogStartCache cache;

    CrossTierLogStartReporter reporter;

    @BeforeEach
    void setUp() {
        reporter = new CrossTierLogStartReporter(metadataView, controlPlane, cache);
    }

    private void asConsolidatingDiskless() {
        when(metadataView.isConsolidatingDisklessTopic(TOPIC)).thenReturn(true);
        when(metadataView.getTopicId(TOPIC)).thenReturn(TOPIC_ID);
    }

    @Test
    void ignoresNonConsolidatingTopics() {
        when(metadataView.isConsolidatingDisklessTopic(TOPIC)).thenReturn(false);
        reporter.enqueue(TP0, 50);
        assertThat(reporter.pendingView()).isEmpty();
    }

    @Test
    void ignoresNegativeOffset() {
        reporter.enqueue(TP0, -1);
        assertThat(reporter.pendingView()).isEmpty();
        verifyNoInteractions(metadataView);
    }

    @Test
    void ignoresUnknownTopicId() {
        when(metadataView.isConsolidatingDisklessTopic(TOPIC)).thenReturn(true);
        when(metadataView.getTopicId(TOPIC)).thenReturn(Uuid.ZERO_UUID);
        reporter.enqueue(TP0, 50);
        assertThat(reporter.pendingView()).isEmpty();
    }

    @Test
    void enqueueKeepsHighestPending() {
        asConsolidatingDiskless();
        reporter.enqueue(TP0, 50);
        reporter.enqueue(TP0, 30);
        reporter.enqueue(TP0, 70);
        assertThat(reporter.pendingView()).containsExactly(java.util.Map.entry(TIDP0, 70L));
    }

    @Test
    void flushReportsAndWritesThroughCache() {
        asConsolidatingDiskless();
        when(controlPlane.advanceCrossTierLogStartOffset(any()))
            .thenReturn(List.of(AdvanceCrossTierLogStartOffsetResponse.success(50)));

        reporter.enqueue(TP0, 50);
        reporter.run();

        verify(controlPlane).advanceCrossTierLogStartOffset(
            List.of(new AdvanceCrossTierLogStartOffsetRequest(TOPIC_ID, 0, 50)));
        verify(cache).put(TIDP0, 50L);
        assertThat(reporter.pendingView()).isEmpty();
    }

    @Test
    void flushDropsNonAdvancingReports() {
        asConsolidatingDiskless();
        when(controlPlane.advanceCrossTierLogStartOffset(any()))
            .thenReturn(List.of(AdvanceCrossTierLogStartOffsetResponse.success(50)));

        reporter.enqueue(TP0, 50);
        reporter.run();

        // A subsequent lower-or-equal observation is not re-reported.
        reporter.enqueue(TP0, 40);
        assertThat(reporter.pendingView()).isEmpty();

        // A higher observation is reported again.
        when(controlPlane.advanceCrossTierLogStartOffset(any()))
            .thenReturn(List.of(AdvanceCrossTierLogStartOffsetResponse.success(60)));
        reporter.enqueue(TP0, 60);
        reporter.run();
        verify(controlPlane).advanceCrossTierLogStartOffset(
            List.of(new AdvanceCrossTierLogStartOffsetRequest(TOPIC_ID, 0, 60)));
        verify(cache).put(TIDP0, 60L);
    }

    @Test
    void flushReBuffersOnControlPlaneFailure() {
        asConsolidatingDiskless();
        when(controlPlane.advanceCrossTierLogStartOffset(any()))
            .thenThrow(new RuntimeException("boom"));

        reporter.enqueue(TP0, 50);
        // run() swallows the exception thrown by flush().
        reporter.run();

        // The update is re-buffered for the next flush.
        assertThat(reporter.pendingView()).containsExactly(java.util.Map.entry(TIDP0, 50L));
        verify(cache, never()).put(any(), anyLong());
    }

    @Test
    void flushHandlesUnknownTopicOrPartition() {
        asConsolidatingDiskless();
        when(controlPlane.advanceCrossTierLogStartOffset(any()))
            .thenReturn(List.of(AdvanceCrossTierLogStartOffsetResponse.unknownTopicOrPartition()));

        reporter.enqueue(TP0, 50);
        reporter.run();

        verify(cache, never()).put(any(), anyLong());
        assertThat(reporter.pendingView()).isEmpty();
    }
}
