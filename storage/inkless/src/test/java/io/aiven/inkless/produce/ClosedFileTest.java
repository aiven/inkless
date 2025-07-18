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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClosedFileTest {
    public static final TopicPartition T0P0 = new TopicPartition("t0", 0);
    static final TopicIdPartition TID0P0 = new TopicIdPartition(Uuid.randomUuid(), T0P0);

    @Test
    void originalRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, null, Map.of(), List.of(), Map.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("originalRequests cannot be null");
    }

    @Test
    void awaitingFuturesByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), null, List.of(), Map.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("awaitingFuturesByRequest cannot be null");
    }

    @Test
    void invalidResponseByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), null, new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("invalidResponseByRequest cannot be null");
    }

    @Test
    void commitBatchRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), null, Map.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitBatchRequests cannot be null");
    }

    @Test
    void requestsWithDifferentLengths() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(1, Map.of()), Map.of(), List.of(), Map.of(), new byte[1]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("originalRequests and awaitingFuturesByRequest must be of same size");
    }

    @Test
    void dataNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), Map.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
    }

    @Test
    void dataRequestMismatch() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), Map.of(), new byte[10]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("data must be empty if commitBatchRequests is empty");
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH,
            Map.of(1, Map.of(new TopicIdPartition(Uuid.randomUuid(), T0P0), MemoryRecords.EMPTY)), // different topic ID
            Map.of(1, new CompletableFuture<>()),
            List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            Map.of(),
            new byte[0])
        )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No corresponding valid or invalid response found for partition t0-0 in request 1");
    }

    @Test
    void size() {
        final int size = new ClosedFile(Instant.EPOCH,
            Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
            Map.of(1, new CompletableFuture<>()),
            List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            Map.of(),
            new byte[10]).size();
        assertThat(size).isEqualTo(10);
    }

    @Test
    void originalRequestWithValidAndInvalidCollections() {
        assertThatThrownBy(() ->
            new ClosedFile(Instant.EPOCH,
                Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
                Map.of(1, Map.of(TID0P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR))),
                new byte[10]).size())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Partition t0-0 in request 1 found in both valid and invalid collections");
    }

    @Test
    void originalRequestNotFoundOnValidOrInvalidCollections() {
        assertThatThrownBy(() ->
            new ClosedFile(Instant.EPOCH,
                Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                List.of(), // no commit request
                Map.of(2, Map.of(TID0P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR))), // invalid with different request ID
                new byte[10]).size())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No corresponding valid or invalid response found for partition t0-0 in request 1");
    }

    @Test
    void moreOutputPartitionsThanOriginalRequest() {
        assertThatThrownBy(() ->
            new ClosedFile(Instant.EPOCH,
                Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
                Map.of(1, Map.of(new TopicIdPartition(Uuid.randomUuid(), 1, "t0"), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR))), // another partition
                new byte[10]).size())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Total number of valid and invalid responses doesn't match original requests for request id 1");
    }

    @Test
    void allowEmpty() {
        final int size = new ClosedFile(null,
            Map.of(),
            Map.of(),
            List.of(),
            Map.of(),
            new byte[0]).size();
        assertThat(size).isEqualTo(0);
    }

    @Test
    void nullStartWithRequests() {
        assertThatThrownBy(() ->
            new ClosedFile(
                null,
                Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
                Map.of(),
                new byte[10]).size()
        )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("start time cannot be null if there are requests processed");
    }
}
