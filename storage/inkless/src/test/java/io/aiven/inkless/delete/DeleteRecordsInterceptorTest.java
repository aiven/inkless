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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

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
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    Consumer<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback;
    @Mock
    ExecutorService executorService;

    @Captor
    ArgumentCaptor<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> resultCaptor;
    @Captor
    ArgumentCaptor<List<DeleteRecordsRequest>> deleteRecordsCaptor;

    @Test
    public void mixingDisklessAndClassicTopicsIsNotAllowed() throws Exception {
        when(metadataView.isDisklessTopic(eq("diskless"))).thenReturn(true);
        when(metadataView.isDisklessTopic(eq("non_diskless"))).thenReturn(false);

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(controlPlane, metadataView, new SynchronousExecutor());

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
        interceptor.close();
    }

    @Test
    public void notInterceptDeletingRecordsFromClassicTopics() throws Exception {
        when(metadataView.isDisklessTopic(eq("non_diskless"))).thenReturn(false);

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(controlPlane, metadataView, new SynchronousExecutor());

        final Map<TopicPartition, Long> entriesPerPartition = Map.of(
            new TopicPartition("non_diskless", 0), 4567L
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();
        verify(responseCallback, never()).accept(any());
        verify(controlPlane, never()).deleteRecords(any());
        interceptor.close();
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


        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(controlPlane, metadataView, new SynchronousExecutor());

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
        interceptor.close();
    }

    @Test
    public void controlPlaneException() throws Exception {
        final Uuid topicId = new Uuid(1, 2);
        when(metadataView.isDisklessTopic(eq("diskless"))).thenReturn(true);
        when(metadataView.getTopicId(eq("diskless"))).thenReturn(topicId);

        when(controlPlane.deleteRecords(anyList())).thenThrow(new RuntimeException("test"));

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(controlPlane, metadataView, new SynchronousExecutor());

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
        interceptor.close();
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

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(controlPlane, metadataView, new SynchronousExecutor());

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
        interceptor.close();
    }

    @Test
    public void closeShutdownsExecutorService() throws IOException, InterruptedException {
        when(executorService.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).thenReturn(true);

        final DeleteRecordsInterceptor interceptor = new DeleteRecordsInterceptor(controlPlane, metadataView, executorService);

        interceptor.close();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }
}
