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
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchHandlerTest {

    @Mock
    Reader reader;

    private final short fetchVersion = ApiMessageType.FETCH.highestSupportedVersion(true);
    private final Uuid inklessUuid = Uuid.randomUuid();
    private final TopicIdPartition topicIdPartition = new TopicIdPartition(inklessUuid, 0, "diskless");


    @Test
    public void readerFutureFailed() throws Exception {
        when(reader.fetch(any(), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
        try (FetchHandler handler = new FetchHandler(reader)) {
            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                    topicIdPartition,
                    new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty())
            );

            final var result = handler.handle(Collections.emptyMap(), fetchInfos).get();

            assertThat(result).hasSize(1);
            assertThat(result.get(topicIdPartition)).satisfies(data -> {
                assertThat(data.error).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
                assertThat(data.highWatermark).isEqualTo(-1L);
                assertThat(data.logStartOffset).isEqualTo(-1L);
                assertThat(data.records).isSameAs(MemoryRecords.EMPTY);
            });
        }
    }

    @Test
    public void readerFutureSuccess() throws Exception {
        final Map<TopicIdPartition, FetchPartitionData> value = Map.of(
                topicIdPartition,
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
        try (FetchHandler handler = new FetchHandler(reader)) {
            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                    topicIdPartition,
                    new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty())
            );

              final var result = handler.handle(Collections.emptyMap(), fetchInfos).get();

              assertThat(result).hasSize(1);
              assertThat(result.get(topicIdPartition)).satisfies(data -> {
                assertThat(data.error).isEqualTo(Errors.NONE);
                assertThat(StreamSupport.stream(data.records.records().spliterator(), false).count()).isEqualTo(1);
              });
        }
    }

    @Test
    public void readerFutureSuccessEmpty() throws Exception {
        final Map<TopicIdPartition, FetchPartitionData> value = Map.of(
            topicIdPartition,
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
        try (FetchHandler handler = new FetchHandler(reader)) {

            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                topicIdPartition,
                new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty())
            );

            final var result = handler.handle(Collections.emptyMap(), fetchInfos).get();

            assertThat(result).hasSize(1);
            assertThat(result.get(topicIdPartition)).satisfies(data -> {
                assertThat(data.error).isEqualTo(Errors.NONE);
                assertThat(StreamSupport.stream(data.records.records().spliterator(), false).count()).isEqualTo(0);
            });

        }
    }

    @Test
    public void emptyRequest() throws Exception {
        try (FetchHandler handler = new FetchHandler(reader)) {
            final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of();

            final var result = handler.handle(Collections.emptyMap(), fetchInfos).get();

            assertThat(result).hasSize(0);
        }
    }
}
