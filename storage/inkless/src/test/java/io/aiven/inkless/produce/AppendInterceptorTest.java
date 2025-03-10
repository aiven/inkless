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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AppendInterceptorTest {
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
    Consumer<Map<TopicPartition, PartitionResponse>> responseCallback;
    @Mock
    Writer writer;
    @Mock
    BrokerTopicStats brokerTopicStats;

    @Captor
    ArgumentCaptor<Map<TopicPartition, PartitionResponse>> resultCaptor;

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
    private static final MemoryRecords TRANSACTIONAL_RECORDS = MemoryRecords.withTransactionalRecords(
        Compression.NONE,
        123,
        (short) 0,
        0,
        new SimpleRecord(0, "hello".getBytes())
    );
    private static final MemoryRecords RECORDS_WITHOUT_PRODUCER_ID = MemoryRecords.withRecords(
        (byte) 2,
        0L,
        Compression.NONE,
        TimestampType.CREATE_TIME,
        -1L,
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
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

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
        verify(writer, never()).write(any(), anyMap());
    }

    @Test
    public void notInterceptProducingToClassicTopics() {
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()))
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();
        verify(responseCallback, never()).accept(any());
        verify(writer, never()).write(any(), anyMap());
    }

    @Test
    public void acceptIdempotentProduceForNonInklessTopics() {
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            RECORDS_WITH_PRODUCER_ID
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();

        verify(responseCallback, never()).accept(any());
        verify(writer, never()).write(any(), anyMap());
    }

    @Test
    public void rejectTransactionalProduceForInklessTopics() {
        when(metadataView.isInklessTopic(eq("inkless1"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("inkless2"))).thenReturn(true);
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless1", 0),
            RECORDS_WITHOUT_PRODUCER_ID,
            new TopicPartition("inkless2", 0),
            TRANSACTIONAL_RECORDS
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
        verify(writer, never()).write(any(), anyMap());
    }

    @Test
    public void acceptTransactionalProduceForNonInklessTopics() {
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("non_inkless", 0),
            RECORDS_WITH_PRODUCER_ID
        );

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();

        verify(responseCallback, never()).accept(any());
        verify(writer, never()).write(any(), anyMap());
    }

    @Test
    public void emptyRequests() {
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of();

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isFalse();

        verify(responseCallback, never()).accept(any());
        verify(writer, never()).write(any(), anyMap());
    }

    @Test
    public void acceptNonIdempotentNotTransactionalProduceForInklessTopics() {
        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless", 0),
            RECORDS_WITHOUT_PRODUCER_ID
        );

        final var writeResult = Map.of(
            new TopicPartition("inkless", 0), new PartitionResponse(Errors.NONE)
        );
        when(writer.write(any(), anyMap())).thenReturn(
            CompletableFuture.completedFuture(writeResult)
        );

        when(metadataView.getTopicId(eq("inkless"))).thenReturn(new Uuid(123, 456));
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.getTopicConfig(any())).thenReturn(new Properties());
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

        verify(responseCallback).accept(eq(writeResult));
    }

    @Test
    public void writeFutureFailed() {
        final Map<TopicPartition, MemoryRecords> entriesPerPartition = Map.of(
            new TopicPartition("inkless", 0),
            RECORDS_WITHOUT_PRODUCER_ID
        );

        final Exception exception = new Exception("test");
        when(writer.write(any(), anyMap())).thenReturn(
            CompletableFuture.failedFuture(exception)
        );

        when(metadataView.getTopicId(eq("inkless"))).thenReturn(new Uuid(123, 456));
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.getTopicConfig(any())).thenReturn(new Properties());
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final boolean result = interceptor.intercept(entriesPerPartition, responseCallback);
        assertThat(result).isTrue();

        final var expectedResult = Map.of(
            new TopicPartition("inkless", 0), new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR)
        );
        verify(responseCallback).accept(eq(expectedResult));
    }

    @Test
    public void close() throws IOException {
        final AppendInterceptor interceptor = new AppendInterceptor(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        interceptor.close();

        verify(writer).close();
    }
}
