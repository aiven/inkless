// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AppendInterceptorTest {
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
    Consumer<Map<TopicPartition, PartitionResponse>> responseCallback;

    @Captor
    ArgumentCaptor<Map<TopicPartition, PartitionResponse>> resultCaptor;

    @BeforeEach
    void setup() {
        when(inklessConfig.commitInterval()).thenReturn(Duration.ofMillis(1));
        when(inklessConfig.produceBufferMaxBytes()).thenReturn(1);
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ofMillis(1));
    }

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
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(new SharedState(time, inklessConfig, metadataView, controlPlane, storageBackend));

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes())),
            new TopicPartition("non_inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()))
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

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
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(new SharedState(time, inklessConfig, metadataView, controlPlane, storageBackend));

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()))
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();
        verify(responseCallback, never()).accept(any());
    }

    @Test
    public void rejectIdempotentProduceForInklessTopics() {
        when(metadataView.isInklessTopic(eq("inkless1"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("inkless2"))).thenReturn(true);
        final AppendInterceptor interceptor = new AppendInterceptor(new SharedState(time, inklessConfig, metadataView, controlPlane, storageBackend));

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless1", 0),
            RECORDS_WITHOUT_PRODUCER_ID,
            new TopicPartition("inkless2", 0),
            RECORDS_WITH_PRODUCER_ID
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

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
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(new SharedState(time, inklessConfig, metadataView, controlPlane, storageBackend));

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            RECORDS_WITH_PRODUCER_ID
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();

        verify(responseCallback, never()).accept(any());
    }
}
