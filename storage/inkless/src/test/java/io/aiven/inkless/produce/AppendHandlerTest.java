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

import org.apache.kafka.common.TopicIdPartition;
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
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AppendHandlerTest {
    static final int BROKER_ID = 11;
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();

    static final Supplier<LogConfig> DEFAULT_TOPIC_CONFIGS = () -> new LogConfig(Map.of());

    Time time = new MockTime();
    RequestLocal requestLocal = RequestLocal.noCaching();
    @Mock
    InklessConfig inklessConfig;
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    Writer writer;
    @Mock
    BrokerTopicStats brokerTopicStats;

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
    public void rejectTransactionalProduce() throws Exception {
        final AppendHandler interceptor = new AppendHandler(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), 0, "inkless1");
        final TopicIdPartition topicIdPartition2 = new TopicIdPartition(Uuid.randomUuid(), 0, "inkless2");
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition = Map.of(
            topicIdPartition1, RECORDS_WITHOUT_PRODUCER_ID,
            topicIdPartition2, TRANSACTIONAL_RECORDS
        );

        final var result = interceptor.handle(entriesPerPartition, requestLocal).get();
        assertThat(result).isEqualTo(
            Map.of(
                topicIdPartition1, new PartitionResponse(Errors.INVALID_REQUEST),
                topicIdPartition2, new PartitionResponse(Errors.INVALID_REQUEST)
            )
        );
        verify(writer, never()).write(any(), anyMap(), any());
    }

    @Test
    public void emptyRequests() throws Exception {
        final AppendHandler interceptor = new AppendHandler(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition = Map.of();

        final var result = interceptor.handle(entriesPerPartition, requestLocal);
        assert(result.get().isEmpty());

        verify(writer, never()).write(any(), anyMap(), any());
    }

    @Test
    public void acceptNotTransactionalProduceForInklessTopics() throws Exception {
        final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("diskless", 0));
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition = Map.of(
            topicIdPartition, RECORDS_WITHOUT_PRODUCER_ID
        );

        final var writeResult = Map.of(
            topicIdPartition, new PartitionResponse(Errors.NONE)
        );
        when(writer.write(any(), anyMap(), any())).thenReturn(
            CompletableFuture.completedFuture(writeResult)
        );

        when(metadataView.getTopicConfig(any())).thenReturn(new Properties());
        final AppendHandler interceptor = new AppendHandler(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        final var result = interceptor.handle(entriesPerPartition, requestLocal).get();
        assertThat(result).isEqualTo(writeResult);
    }

    @Test
    public void writeFutureFailed() {
        final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless");
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition = Map.of(
            topicIdPartition,
            RECORDS_WITHOUT_PRODUCER_ID
        );

        final Exception exception = new Exception("test");
        when(writer.write(any(), anyMap(), any())).thenReturn(
            CompletableFuture.failedFuture(exception)
        );

        when(metadataView.getTopicConfig(any())).thenReturn(new Properties());
        final AppendHandler interceptor = new AppendHandler(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        assertThatThrownBy(() -> interceptor.handle(entriesPerPartition, requestLocal).get()).hasCause(exception);
    }

    @Test
    public void close() throws IOException {
        final AppendHandler interceptor = new AppendHandler(
            new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane,
                OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, DEFAULT_TOPIC_CONFIGS), writer);

        interceptor.close();

        verify(writer).close();
    }
}
