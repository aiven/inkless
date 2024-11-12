// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import io.aiven.inkless.control_plane.MetadataView;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AppendInterceptorTest {
    private static final MemoryRecords RECORDS_WITH_PRODUCER_ID = MemoryRecords.withRecords(
        (byte) 2,
        0L,
        Compression.NONE,
        TimestampType.CREATE_TIME,
        123L,
        (short) 0,
        0,
        0,
        false,
        new SimpleRecord(0, "hello".getBytes())
    );
    private static final MemoryRecords RECORDS_WITHOUT_PRODUCER_ID = MemoryRecords.withRecords(
        (byte) 2,
        0L,
        Compression.NONE,
        TimestampType.CREATE_TIME,
        123L,
        (short) 0,
        0,
        0,
        false,
        new SimpleRecord(0, "hello".getBytes())
    );

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

    @Test
    public void notInterceptProducingToClassicTopics() {
        final MetadataView metadataView = mock(MetadataView.class);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(metadataView);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()))
        );
        final var responseCallback = Mockito.<Consumer<Map<TopicPartition, PartitionResponse>>>mock();

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();
        verify(responseCallback, never()).accept(any());
    }

    @Test
    public void rejectIdempotentProduceForInklessTopics() {
        final MetadataView metadataView = mock(MetadataView.class);
        when(metadataView.isInklessTopic(eq("inkless1"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("inkless2"))).thenReturn(true);
        final AppendInterceptor interceptor = new AppendInterceptor(metadataView);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless1", 0),
            RECORDS_WITHOUT_PRODUCER_ID,
            new TopicPartition("inkless2", 0),
            RECORDS_WITH_PRODUCER_ID
        );
        final var responseCallback = Mockito.<Consumer<Map<TopicPartition, PartitionResponse>>>mock();

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

        @SuppressWarnings("unchecked") final ArgumentCaptor<Map<TopicPartition, PartitionResponse>> resultCaptor =
            ArgumentCaptor.forClass(Map.class);
        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue()).isEqualTo(Map.of(
            new TopicPartition("inkless1", 0),
            new PartitionResponse(Errors.INVALID_REQUEST),
            new TopicPartition("inkless2", 0),
            new PartitionResponse(Errors.INVALID_REQUEST)
        ));
    }

    @Test
    public void acceptIdempotentProduceForNonInklessTopics() {
        final MetadataView metadataView = mock(MetadataView.class);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(metadataView);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            RECORDS_WITH_PRODUCER_ID
        );
        final var responseCallback = Mockito.<Consumer<Map<TopicPartition, PartitionResponse>>>mock();

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();

        verify(responseCallback, never()).accept(any());
    }
}
