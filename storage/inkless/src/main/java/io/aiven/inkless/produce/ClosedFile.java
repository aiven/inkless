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
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.CommitBatchRequest;

record ClosedFile(Instant start,
                  Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests,
                  // Keeps pending and completed request futures to build the final response per request
                  Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest,
                  List<CommitBatchRequest> commitBatchRequests,
                  List<Integer> requestIds,
                  byte[] data) {
    ClosedFile {
        Objects.requireNonNull(start, "start cannot be null");
        Objects.requireNonNull(originalRequests, "originalRequests cannot be null");
        Objects.requireNonNull(allFuturesByRequest, "allFuturesByRequest cannot be null");
        Objects.requireNonNull(commitBatchRequests, "commitBatchRequests cannot be null");
        Objects.requireNonNull(requestIds, "requestIds cannot be null");

        if (originalRequests.size() != allFuturesByRequest.size()) {
            throw new IllegalArgumentException(
                "originalRequests and allFuturesByRequest must be of same size");
        }

        if (commitBatchRequests.size() != requestIds.size()) {
            throw new IllegalArgumentException(
                "commitBatchRequests and requestIds must be of same size");
        }

        Objects.requireNonNull(data, "data cannot be null");

        if (commitBatchRequests.isEmpty() != (data.length == 0)) {
            throw new IllegalArgumentException("data must be empty if commitBatchRequests is empty");
        }
    }


    Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> validatedFutures() {
        // filter out already completed futures that may have failed validation
        return allFuturesByRequest.entrySet()
            .stream()
            .map(entry ->
                Map.entry(entry.getKey(), entry.getValue().entrySet()
                    .stream()
                    .filter(inner -> !inner.getValue().isDone())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    int size() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }
}
