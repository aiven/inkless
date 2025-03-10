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

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;
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
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();

    static final Supplier<LogConfig> DEFAULT_TOPIC_CONFIGS = () -> new LogConfig(Map.of());

    Time time = new MockTime();
    @Mock
    InklessConfig inklessConfig;
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    StorageBackend storageBackend;
    @Mock
    Consumer<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback;
    @Mock
    BrokerTopicStats brokerTopicStats;

    @Captor
    ArgumentCaptor<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> resultCaptor;
    @Captor
    ArgumentCaptor<List<DeleteRecordsRequest>> deleteRecordsCaptor;

    @Test
    public void mixingInklessAndClassicTopicsIsNotAllowed() {
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS));

        final Map<TopicPartition, Long> entriesPerPartition = Map.of(
            new TopicPartition("inkless", 0),
            1234L,
            new TopicPartition("non_inkless", 0),
            4567L
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
            new TopicPartition("inkless", 0),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(0)
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setLowWatermark(INVALID_LOW_WATERMARK),
            new TopicPartition("non_inkless", 0),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(0)
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setLowWatermark(INVALID_LOW_WATERMARK)
        ));
        verify(controlPlane, never()).deleteRecords(any());
    }

    @Test
    public void notInterceptDeletingRecordsFromClassicTopics() {
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS));

        final Map<TopicPartition, Long> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0), 4567L
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();
        verify(responseCallback, never()).accept(any());
        verify(controlPlane, never()).deleteRecords(any());
    }

    @Test
    public void interceptDeletingRecordsFromInklessTopics() {
        final Uuid topicId = new Uuid(1, 2);
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.getTopicId(eq("inkless"))).thenReturn(topicId);

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

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS),
            new SynchronousExecutor());

        final TopicPartition tp0 = new TopicPartition("inkless", 0);
        final TopicPartition tp1 = new TopicPartition("inkless", 1);
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
            new TopicPartition("inkless", 0),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code())
                .setLowWatermark(123L),
            new TopicPartition("inkless", 1),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(1)
                .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code())
                .setLowWatermark(INVALID_LOW_WATERMARK)
        ));
    }

    @Test
    public void controlPlaneException() {
        final Uuid topicId = new Uuid(1, 2);
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.getTopicId(eq("inkless"))).thenReturn(topicId);

        when(controlPlane.deleteRecords(anyList())).thenThrow(new RuntimeException("test"));

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS),
            new SynchronousExecutor());

        final TopicPartition topicPartition = new TopicPartition("inkless", 1);
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
            new TopicPartition("inkless", 1),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(1)
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                .setLowWatermark(INVALID_LOW_WATERMARK)
        ));
    }

    @Test
    public void topicIdNotFound() {
        when(metadataView.isInklessTopic(eq("inkless1"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("inkless2"))).thenReturn(true);
        // This instead of the normal thenReturn to not depend on the map key iteration order
        // (and not trigger the strict mock checker).
        when(metadataView.getTopicId(anyString())).thenAnswer(invocation -> {
            final String topicName = invocation.getArgument(0, String.class);
            if (topicName.equals("inkless2")) {
                return Uuid.ZERO_UUID;
            } else {
                return new Uuid(1, 2);
            }
        });

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS),
            new SynchronousExecutor());

        final TopicPartition topicPartition1 = new TopicPartition("inkless1", 1);
        final TopicPartition topicPartition2 = new TopicPartition("inkless2", 2);
        final Map<TopicPartition, Long> entriesPerPartition = Map.of(
            topicPartition1, 4567L,
            topicPartition2, 8590L
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();
        verify(controlPlane, never()).deleteRecords(anyList());

        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
            new TopicPartition("inkless1", 1),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(1)
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                .setLowWatermark(INVALID_LOW_WATERMARK),
            new TopicPartition("inkless2", 2),
            new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setPartitionIndex(2)
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                .setLowWatermark(INVALID_LOW_WATERMARK)
        ));
    }
}
