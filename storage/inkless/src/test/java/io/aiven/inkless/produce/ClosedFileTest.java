// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
    void startNull() {
        assertThatThrownBy(() -> new ClosedFile(null, Map.of(), Map.of(), Map.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("start cannot be null");
    }

    @Test
    void originalRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, null, Map.of(), Map.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("originalRequests cannot be null");
    }

    @Test
    void awaitingFuturesByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), null, Map.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("awaitingFuturesByRequest cannot be null");
    }

    @Test
    void invalidResponseByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), null, List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("invalidResponseByRequest cannot be null");
    }

    @Test
    void commitBatchRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), Map.of(), null, new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitBatchRequests cannot be null");
    }

    @Test
    void requestsWithDifferentLengths() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(1, Map.of()), Map.of(), Map.of(), List.of(), new byte[1]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("originalRequests and awaitingFuturesByRequest must be of same size");
    }

    @Test
    void dataNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), Map.of(), List.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
    }

    @Test
    void dataRequestMismatch() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), Map.of(), List.of(), new byte[10]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("data must be empty if commitBatchRequests is empty");
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH,
            Map.of(1, Map.of(new TopicIdPartition(Uuid.randomUuid(), T0P0), MemoryRecords.EMPTY)),
            Map.of(1, new CompletableFuture<>()),
            Map.of(),
            List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            new byte[0]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("data must be empty if commitBatchRequests is empty");
    }

    @Test
    void size() {
        final int size = new ClosedFile(Instant.EPOCH,
            Map.of(1, Map.of(new TopicIdPartition(Uuid.randomUuid(), T0P0), MemoryRecords.EMPTY)),
            Map.of(1, new CompletableFuture<>()), Map.of(),
            List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            new byte[10]).size();
        assertThat(size).isEqualTo(10);
    }

    @Test
    void moreCommitRequestsThanOriginalRequest() {
        // 1 valid, 1 invalid > 1 original
        assertThatThrownBy(() ->
            new ClosedFile(Instant.EPOCH,
                Map.of(1, Map.of(new TopicIdPartition(Uuid.randomUuid(), T0P0), MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                Map.of(1, Map.of(new TopicPartition("t0", 0), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR))),
                List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
                new byte[10]).size())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("commitBatchRequests and invalidResponseByRequest must match the originalRequests size");
    }

    @Test
    void mismatchCommitRequestsWithOriginalRequestsPerId() {
        // valid with id 1, invalid with id 2 = 1 original with id 1
        assertThatThrownBy(() ->
            new ClosedFile(Instant.EPOCH,
                Map.of(1, Map.of(new TopicIdPartition(Uuid.randomUuid(), T0P0), MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                Map.of(2, Map.of(new TopicPartition("t0", 0), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR))),
                List.of(),
                new byte[10]).size())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("commitBatchRequests and invalidResponseByRequest must match the originalRequests size for request id 1");
    }
}
