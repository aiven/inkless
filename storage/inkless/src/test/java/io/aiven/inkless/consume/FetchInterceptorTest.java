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
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchInterceptorTest {
    private static final int BROKER_ID = 11;
    private static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();

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
    Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback;
    @Mock
    Consumer<Void> delayCallback;
    @Mock
    BrokerTopicStats brokerTopicStats;
    @Mock
    Reader reader;
    @Mock
    Supplier<LogConfig> defaultTopicConfigs;

    @Captor
    ArgumentCaptor<Map<TopicIdPartition, FetchPartitionData>> resultCaptor;

    private SharedState sharedState;
    private final short fetchVersion = ApiMessageType.FETCH.highestSupportedVersion(true);
    private final Uuid inklessUuid = Uuid.randomUuid();
    private final Uuid classicUuid = Uuid.randomUuid();

    @BeforeEach
    public void setup() {
        sharedState = new SharedState(time, BROKER_ID, inklessConfig, metadataView, controlPlane, storageBackend,
            OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, brokerTopicStats, defaultTopicConfigs);
    }

    @Test
    public void mixingInklessAndClassicTopicsIsNotAllowed() {
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        try (FetchInterceptor interceptor = new FetchInterceptor(sharedState, reader)) {

            final FetchParams params = new FetchParams(fetchVersion,
                    -1, -1, -1, -1, -1,
                    FetchIsolation.LOG_END, Optional.empty());

            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                    new TopicIdPartition(inklessUuid, 0, "inkless"),
                    new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty()),
                    new TopicIdPartition(classicUuid, 0, "non_inkless"),
                    new FetchRequest.PartitionData(classicUuid, 0, 0, 1024, Optional.empty())
            );

            final boolean result = interceptor.intercept(params, fetchInfos, responseCallback, delayCallback);
            assertThat(result).isTrue();
        }

        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue())
                .isNotNull()
                .containsKeys(
                        new TopicIdPartition(inklessUuid, 0, "inkless"),
                        new TopicIdPartition(classicUuid, 0, "non_inkless")
                )
                .values()
                .allMatch(fetchPartitionData -> fetchPartitionData.error == Errors.INVALID_REQUEST);
    }

    @Test
    public void notInterceptProducingToClassicTopics() {
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        try (FetchInterceptor interceptor = new FetchInterceptor(sharedState, reader)) {

            final FetchParams params = new FetchParams(fetchVersion,
                    -1, -1, -1, -1, -1,
                    FetchIsolation.LOG_END, Optional.empty());

            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                    new TopicIdPartition(classicUuid, 0, "non_inkless"),
                    new FetchRequest.PartitionData(classicUuid, 0, 0, 1024, Optional.empty())
            );

            final boolean result = interceptor.intercept(params, fetchInfos, responseCallback, delayCallback);
            assertThat(result).isFalse();
            verify(responseCallback, never()).accept(any());
        }
    }

    @Test
    public void readerFutureFailed() {
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(reader.fetch(any(), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
        try (FetchInterceptor interceptor = new FetchInterceptor(sharedState, reader)) {

            final FetchParams params = new FetchParams(fetchVersion,
                    -1, -1, -1, -1, -1,
                    FetchIsolation.LOG_END, Optional.empty());

            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                    new TopicIdPartition(inklessUuid, 0, "inkless"),
                    new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty())
            );

            final boolean result = interceptor.intercept(params, fetchInfos, responseCallback, delayCallback);
            assertThat(result).isTrue();
        }
        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue())
                .isNotNull()
                .containsKeys(
                        new TopicIdPartition(inklessUuid, 0, "inkless")
                )
                .values()
                .allMatch(fetchPartitionData -> fetchPartitionData.error == Errors.UNKNOWN_SERVER_ERROR);
    }

    @Test
    public void readerFutureSuccess() {
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        final Map<TopicIdPartition, FetchPartitionData> value = Map.of(
                new TopicIdPartition(inklessUuid, 0, "inkless"),
                new FetchPartitionData(
                        Errors.NONE,
                        -1,
                        -1,
                        MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("message".getBytes())),
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false
                )
        );
        when(reader.fetch(any(), any())).thenReturn(CompletableFuture.completedFuture(value));
        try (FetchInterceptor interceptor = new FetchInterceptor(sharedState, reader)) {

            final FetchParams params = new FetchParams(fetchVersion,
                    -1, -1, -1, -1, -1,
                    FetchIsolation.LOG_END, Optional.empty());

            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                    new TopicIdPartition(inklessUuid, 0, "inkless"),
                    new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty())
            );

            final boolean result = interceptor.intercept(params, fetchInfos, responseCallback, delayCallback);
            assertThat(result).isTrue();
        }
        verify(responseCallback).accept(resultCaptor.capture());
        verify(delayCallback, never()).accept(any());
        assertThat(resultCaptor.getValue()).isSameAs(value);
    }

    @Test
    public void readerFutureSuccessEmpty() {
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        final Map<TopicIdPartition, FetchPartitionData> value = Map.of(
            new TopicIdPartition(inklessUuid, 0, "inkless"),
            new FetchPartitionData(
                Errors.NONE,
                -1,
                -1,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
            )
        );
        when(reader.fetch(any(), any())).thenReturn(CompletableFuture.completedFuture(value));
        try (FetchInterceptor interceptor = new FetchInterceptor(sharedState, reader)) {

            final FetchParams params = new FetchParams(fetchVersion,
                -1, -1, -1, -1, -1,
                FetchIsolation.LOG_END, Optional.empty());

            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                new TopicIdPartition(inklessUuid, 0, "inkless"),
                new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty())
            );

            final boolean result = interceptor.intercept(params, fetchInfos, responseCallback, delayCallback);
            assertThat(result).isTrue();
        }
        verify(responseCallback, never()).accept(any());
        verify(delayCallback).accept(null);
    }
}
