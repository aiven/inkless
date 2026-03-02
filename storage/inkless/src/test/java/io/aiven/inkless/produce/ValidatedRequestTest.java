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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ValidatedRequest} and {@link ValidatedRequest.ValidatedBatch}.
 */
class ValidatedRequestTest {
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final String TOPIC_0 = "topic0";
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);

    // Use the UNKNOWN_LOG_APPEND_INFO constant for testing
    private static final LogAppendInfo APPEND_INFO = LogAppendInfo.UNKNOWN_LOG_APPEND_INFO;

    @Test
    void validatedBatchConstructorValidation() {
        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]));

        assertThatThrownBy(() -> new ValidatedRequest.ValidatedBatch(null, records, APPEND_INFO))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("topicIdPartition");

        assertThatThrownBy(() -> new ValidatedRequest.ValidatedBatch(T0P0, null, APPEND_INFO))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("validatedRecords");

        assertThatThrownBy(() -> new ValidatedRequest.ValidatedBatch(T0P0, records, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("appendInfo");
    }

    @Test
    void validatedRequestConstructorValidation() {
        final Map<TopicIdPartition, MemoryRecords> originalRecords = Map.of();
        final Map<TopicIdPartition, ValidatedRequest.ValidatedBatch> validatedBatches = Map.of();
        final Map<TopicIdPartition, PartitionResponse> invalidBatches = Map.of();
        final CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture = new CompletableFuture<>();

        assertThatThrownBy(() -> new ValidatedRequest(null, validatedBatches, invalidBatches, resultFuture))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("originalRecords");

        assertThatThrownBy(() -> new ValidatedRequest(originalRecords, null, invalidBatches, resultFuture))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("validatedBatches");

        assertThatThrownBy(() -> new ValidatedRequest(originalRecords, validatedBatches, null, resultFuture))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("invalidBatches");

        assertThatThrownBy(() -> new ValidatedRequest(originalRecords, validatedBatches, invalidBatches, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("resultFuture");
    }

    @Test
    void validatedSizeEmpty() {
        final ValidatedRequest request = new ValidatedRequest(
            Map.of(),
            Map.of(),
            Map.of(),
            new CompletableFuture<>()
        );

        assertThat(request.validatedSize()).isZero();
    }

    @Test
    void validatedSizeWithBatches() {
        final MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]));
        final MemoryRecords records2 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[20]));

        final ValidatedRequest request = new ValidatedRequest(
            Map.of(T0P0, records1, T0P1, records2),
            Map.of(
                T0P0, new ValidatedRequest.ValidatedBatch(T0P0, records1, APPEND_INFO),
                T0P1, new ValidatedRequest.ValidatedBatch(T0P1, records2, APPEND_INFO)
            ),
            Map.of(),
            new CompletableFuture<>()
        );

        // Size should be sum of both record sizes
        assertThat(request.validatedSize()).isEqualTo(records1.sizeInBytes() + records2.sizeInBytes());
    }

    @Test
    void hasNoValidBatchesTrue() {
        final ValidatedRequest request = new ValidatedRequest(
            Map.of(),
            Map.of(),  // No valid batches
            Map.of(T0P0, new PartitionResponse(Errors.INVALID_RECORD)),  // Only invalid
            new CompletableFuture<>()
        );

        assertThat(request.hasNoValidBatches()).isTrue();
    }

    @Test
    void hasNoValidBatchesFalse() {
        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]));

        final ValidatedRequest request = new ValidatedRequest(
            Map.of(T0P0, records),
            Map.of(T0P0, new ValidatedRequest.ValidatedBatch(T0P0, records, APPEND_INFO)),
            Map.of(),
            new CompletableFuture<>()
        );

        assertThat(request.hasNoValidBatches()).isFalse();
    }

    @Test
    void accessors() {
        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]));
        final Map<TopicIdPartition, MemoryRecords> originalRecords = Map.of(T0P0, records);
        final Map<TopicIdPartition, ValidatedRequest.ValidatedBatch> validatedBatches = Map.of(
            T0P0, new ValidatedRequest.ValidatedBatch(T0P0, records, APPEND_INFO)
        );
        final Map<TopicIdPartition, PartitionResponse> invalidBatches = Map.of(
            T0P1, new PartitionResponse(Errors.INVALID_RECORD)
        );
        final CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture = new CompletableFuture<>();

        final ValidatedRequest request = new ValidatedRequest(
            originalRecords,
            validatedBatches,
            invalidBatches,
            resultFuture
        );

        assertThat(request.originalRecords()).isEqualTo(originalRecords);
        assertThat(request.validatedBatches()).isEqualTo(validatedBatches);
        assertThat(request.invalidBatches()).isEqualTo(invalidBatches);
        assertThat(request.resultFuture()).isSameAs(resultFuture);
    }
}
