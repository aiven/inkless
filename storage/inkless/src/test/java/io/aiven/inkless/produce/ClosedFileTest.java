// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchRequestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClosedFileTest {

    static final TopicPartition TOPIC_PARTITION = new TopicPartition("t", 0);
    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), TOPIC_PARTITION);

    @Test
    void startNull() {
        assertThatThrownBy(() -> new ClosedFile(null, Map.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("start cannot be null");
    }

    @Test
    void awaitingFuturesByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, null, List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("allFuturesByRequest cannot be null");
    }

    @Test
    void commitBatchRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), null, new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitBatchRequestContexts cannot be null");
    }

    @Test
    void dataNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), List.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
    }

    @Test
    void dataRequestMismatch() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), List.of(), new byte[10]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("data must be empty if commitBatchRequestContexts is empty");
        assertThatThrownBy(() ->
            new ClosedFile(
                Instant.EPOCH,
                Map.of(1, Map.of(TOPIC_PARTITION, new CompletableFuture<>())),
                List.of(new CommitBatchRequestContext(1, TOPIC_PARTITION, CommitBatchRequest.of(TOPIC_ID_PARTITION, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME))),
                new byte[0])
        ).isInstanceOf(IllegalArgumentException.class).hasMessage("data must be empty if commitBatchRequestContexts is empty");
    }

    @Test
    void size() {
        final int size = new ClosedFile(
            Instant.EPOCH,
            Map.of(1, Map.of(TOPIC_PARTITION, new CompletableFuture<>())),
            List.of(new CommitBatchRequestContext(1, TOPIC_PARTITION, CommitBatchRequest.of(TOPIC_ID_PARTITION, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME))),
            new byte[10]
        ).size();
        assertThat(size).isEqualTo(10);
    }

    @Test
    void requestIdNotFound() {
        assertThatThrownBy(() -> new ClosedFile(
            Instant.EPOCH,
            Map.of(1, Map.of(TOPIC_PARTITION, new CompletableFuture<>())),
            List.of(new CommitBatchRequestContext(2, TOPIC_PARTITION, CommitBatchRequest.of(TOPIC_ID_PARTITION, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME))),
            new byte[0]
        )).isInstanceOf(IllegalArgumentException.class).hasMessage("Request ID 2 not found in validated futures");
    }

    @Test
    void topicPartitionNotFound() {
        assertThatThrownBy(() -> new ClosedFile(
            Instant.EPOCH,
            Map.of(1, Map.of(TOPIC_PARTITION, new CompletableFuture<>())),
            List.of(new CommitBatchRequestContext(1, new TopicPartition("t", 1), CommitBatchRequest.of(TOPIC_ID_PARTITION, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME))),
            new byte[0]
        )).isInstanceOf(IllegalArgumentException.class).hasMessage("Topic partition t-1 not found in validated futures for request ID 1");
    }

    @Test
    void futureAlreadyCompleted() {
        final CompletableFuture<ProduceResponse.PartitionResponse> future = new CompletableFuture<>();
        future.complete(new ProduceResponse.PartitionResponse(Errors.NONE));
        assertThatThrownBy(() -> new ClosedFile(
            Instant.EPOCH,
            Map.of(1, Map.of(TOPIC_PARTITION, future)),
            List.of(new CommitBatchRequestContext(1, TOPIC_PARTITION, CommitBatchRequest.of(TOPIC_ID_PARTITION, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME))),
            new byte[0]
        )).isInstanceOf(IllegalStateException.class).hasMessage("Future for topic partition t-0 in request ID 1 is already completed");
    }
}
