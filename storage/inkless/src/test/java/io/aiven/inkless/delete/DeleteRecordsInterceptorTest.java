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
package io.aiven.inkless.delete;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

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
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.test_utils.SynchronousExecutor;

import static org.apache.kafka.common.requests.DeleteRecordsResponse.INVALID_LOW_WATERMARK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class DeleteRecordsInterceptorTest {
    static final int BROKER_ID = 11;

    static final Supplier<LogConfig> DEFAULT_TOPIC_CONFIGS = () -> new LogConfig(Map.of());

    Time time = new MockTime();
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    Consumer<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback;
    @Mock
    BrokerTopicStats brokerTopicStats;

    @Captor
    ArgumentCaptor<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> resultCaptor;
    @Captor
    ArgumentCaptor<List<DeleteRecordsRequest>> deleteRecordsCaptor;

    private SharedState getSharedState() {
        return SharedState.initialize(
            time,
            BROKER_ID,
            new InklessConfig(Map.of()),
            metadataView,
            controlPlane,
            brokerTopicStats,
            DEFAULT_TOPIC_CONFIGS
        );
    }

    @Test
    public void mixingDisklessAndClassicTopicsIsNotAllowed() throws Exception {
        when(metadataView.isDisklessTopic(eq("diskless"))).thenReturn(true);
        when(metadataView.isDisklessTopic(eq("non_diskless"))).thenReturn(false);
        try (final SharedState sharedState = getSharedState()) {
            final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(sharedState);

            final Map<TopicPartition, Long> entriesPerPartition = Map.of(
                new TopicPartition("diskless", 0),
                1234L,
                new TopicPartition("non_diskless", 0),
                4567L
            );

            final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
            assertThat(result).isTrue();

            verify(responseCallback).accept(resultCaptor.capture());
            assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
                new TopicPartition("diskless", 0),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK),
                new TopicPartition("non_diskless", 0),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK)
            ));
            verify(controlPlane, never()).deleteRecords(any());
        }
    }

    @Test
    public void notInterceptDeletingRecordsFromClassicTopics() throws Exception {
        when(metadataView.isDisklessTopic(eq("non_diskless"))).thenReturn(false);
        try(final SharedState sharedState = getSharedState()) {
            final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(sharedState);

            final Map<TopicPartition, Long> entriesPerPartition = Map.of(
                new TopicPartition("non_diskless", 0), 4567L
            );

            final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
            assertThat(result).isFalse();
            verify(responseCallback, never()).accept(any());
            verify(controlPlane, never()).deleteRecords(any());
        }
    }

    @Test
    public void interceptDeletingRecordsFromDisklessTopics() throws Exception {
        final Uuid topicId = new Uuid(1, 2);
        when(metadataView.isDisklessTopic(eq("diskless"))).thenReturn(true);
        when(metadataView.getTopicId(eq("diskless"))).thenReturn(topicId);

        when(controlPlane.deleteRecords(anyList())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final List<DeleteRecordsRequest> argument =
                (List<DeleteRecordsRequest>)invocation.getArgument(0, List.class);
            if (argument.get(0).topicIdPartition().partition() == 0) {
                return List.of(
                    new DeleteRecordsResponse(Errors.NONE, 123L),
                    new DeleteRecordsResponse(Errors.KAFKA_STORAGE_ERROR, INVALID_LOW_WATERMARK)
                );
            } else {
                return List.of(
                    new DeleteRecordsResponse(Errors.KAFKA_STORAGE_ERROR, INVALID_LOW_WATERMARK),
                    new DeleteRecordsResponse(Errors.NONE, 123L)
                );
            }
        });

        try(final SharedState sharedState = getSharedState()) {
            final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(sharedState, new SynchronousExecutor());

            final TopicPartition tp0 = new TopicPartition("diskless", 0);
            final TopicPartition tp1 = new TopicPartition("diskless", 1);
            final Map<TopicPartition, Long> entriesPerPartition = Map.of(
                tp0, 4567L,
                tp1, 999L
            );

            final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
            assertThat(result).isTrue();
            verify(controlPlane).deleteRecords(deleteRecordsCaptor.capture());
            assertThat(deleteRecordsCaptor.getValue()).containsExactlyInAnyOrder(
                new DeleteRecordsRequest(new TopicIdPartition(topicId, tp0), 4567L),
                new DeleteRecordsRequest(new TopicIdPartition(topicId, tp1), 999L)
            );

            verify(responseCallback).accept(resultCaptor.capture());
            assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
                new TopicPartition("diskless", 0),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code())
                    .setLowWatermark(123L),
                new TopicPartition("diskless", 1),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(1)
                    .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK)
            ));
        }
    }

    @Test
    public void controlPlaneException() throws Exception {
        final Uuid topicId = new Uuid(1, 2);
        when(metadataView.isDisklessTopic(eq("diskless"))).thenReturn(true);
        when(metadataView.getTopicId(eq("diskless"))).thenReturn(topicId);

        when(controlPlane.deleteRecords(anyList())).thenThrow(new RuntimeException("test"));

        try (final SharedState sharedState = getSharedState()) {
            final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(sharedState, new SynchronousExecutor());

            final TopicPartition topicPartition = new TopicPartition("diskless", 1);
            final Map<TopicPartition, Long> entriesPerPartition = Map.of(
                topicPartition, 4567L
            );

            final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
            assertThat(result).isTrue();
            verify(controlPlane).deleteRecords(eq(List.of(
                new DeleteRecordsRequest(new TopicIdPartition(topicId, topicPartition), 4567L)
            )));

            verify(responseCallback).accept(resultCaptor.capture());
            assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
                new TopicPartition("diskless", 1),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(1)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK)
            ));
        }
    }

    @Test
    public void topicIdNotFound() throws Exception {
        when(metadataView.isDisklessTopic(eq("diskless1"))).thenReturn(true);
        when(metadataView.isDisklessTopic(eq("diskless2"))).thenReturn(true);
        // This instead of the normal thenReturn to not depend on the map key iteration order
        // (and not trigger the strict mock checker).
        when(metadataView.getTopicId(anyString())).thenAnswer(invocation -> {
            final String topicName = invocation.getArgument(0, String.class);
            if (topicName.equals("diskless2")) {
                return Uuid.ZERO_UUID;
            } else {
                return new Uuid(1, 2);
            }
        });

        try (final SharedState sharedState = getSharedState()) {
            final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(sharedState, new SynchronousExecutor());

            final TopicPartition topicPartition1 = new TopicPartition("diskless1", 1);
            final TopicPartition topicPartition2 = new TopicPartition("diskless2", 2);
            final Map<TopicPartition, Long> entriesPerPartition = Map.of(
                topicPartition1, 4567L,
                topicPartition2, 8590L
            );

            final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
            assertThat(result).isTrue();
            verify(controlPlane, never()).deleteRecords(anyList());

            verify(responseCallback).accept(resultCaptor.capture());
            assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
                new TopicPartition("diskless1", 1),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(1)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK),
                new TopicPartition("diskless2", 2),
                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(2)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK)
            ));
        }
    }
}
