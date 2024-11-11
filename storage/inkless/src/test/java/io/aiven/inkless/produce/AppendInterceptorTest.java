// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import io.aiven.inkless.control_plane.MetadataView;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AppendInterceptorTest {
    @Test
    public void mixingInklessAndClassicTopicsIsNotAllowed() {
        final MetadataView metadataView = mock(MetadataView.class);
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(metadataView);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes())),
            new TopicPartition("non_inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()))
        );
        final var responseCallback = Mockito.<Consumer<Map<TopicPartition, PartitionResponse>>>mock();

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

        @SuppressWarnings("unchecked") final ArgumentCaptor<Map<TopicPartition, PartitionResponse>> resultCaptor =
            ArgumentCaptor.forClass(Map.class);
        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
            new TopicPartition("inkless", 0),
            new PartitionResponse(Errors.INVALID_REQUEST),
            new TopicPartition("non_inkless", 0),
            new PartitionResponse(Errors.INVALID_REQUEST)
        ));
    }
}