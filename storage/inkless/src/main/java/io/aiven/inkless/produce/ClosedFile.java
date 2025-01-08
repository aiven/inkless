// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.control_plane.CommitBatchRequest;

record ClosedFile(Instant start,
                  Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests,
                  Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest,
                  List<CommitBatchRequest> commitBatchRequests,
                  List<Integer> requestIds,
                  byte[] data) {
    ClosedFile {
        Objects.requireNonNull(start, "start cannot be null");
        Objects.requireNonNull(originalRequests, "originalRequests cannot be null");
        Objects.requireNonNull(awaitingFuturesByRequest, "awaitingFuturesByRequest cannot be null");
        Objects.requireNonNull(commitBatchRequests, "commitBatchRequests cannot be null");
        Objects.requireNonNull(requestIds, "requestIds cannot be null");

        if (originalRequests.size() != awaitingFuturesByRequest.size()) {
            throw new IllegalArgumentException(
                "originalRequests and awaitingFuturesByRequest must be of same size");
        }

        if (commitBatchRequests.size() != requestIds.size()) {
            throw new IllegalArgumentException(
                "commitBatchRequests and requestIds must be of same size");
        }

        Objects.requireNonNull(data, "data cannot be null");
    }

    int size() {
        return data.length;
    }
}
