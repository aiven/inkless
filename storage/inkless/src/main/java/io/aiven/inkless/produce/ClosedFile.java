// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.control_plane.CommitBatchRequestContext;

final class ClosedFile {
    private final Instant start;
    // Keeps pending and completed request futures to build the final response per request
    private final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest;
    private final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> validatedFutures;
    // List is required to maintain the order of incoming requests and define the response order
    private final List<CommitBatchRequestContext> commitBatchRequestContexts;
    private final byte[] data;

    ClosedFile(
        Instant start,
        Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest,
        List<CommitBatchRequestContext> commitBatchRequestContexts,
        byte[] data
    ) {
        Objects.requireNonNull(start, "start cannot be null");
        Objects.requireNonNull(allFuturesByRequest, "allFuturesByRequest cannot be null");
        Objects.requireNonNull(commitBatchRequestContexts, "commitBatchRequestContexts cannot be null");

        // Ensure that requests to be processed have a proper pending future to be completed
        final var validatedFutures = new HashMap<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>>();
        for (final var request: commitBatchRequestContexts) {
            final var futuresPerRequest = allFuturesByRequest.get(request.requestId());
            if (futuresPerRequest == null) {
                throw new IllegalArgumentException("Request ID %s not found in validated futures".formatted(request.requestId()));
            }
            final var future = futuresPerRequest.get(request.topicPartition());
            if (future == null) {
                throw new IllegalArgumentException("Topic partition %s not found in validated futures for request ID %s".formatted(request.topicPartition(), request.requestId()));
            }
            if (future.isDone()) {
                throw new IllegalStateException("Future for topic partition %s in request ID %s is already completed".formatted(request.topicPartition(), request.requestId()));
            } else {
                validatedFutures.computeIfAbsent(request.requestId(), k -> new HashMap<>())
                    .put(request.topicPartition(), future);
            }
        }

        Objects.requireNonNull(data, "data cannot be null");

        if (commitBatchRequestContexts.isEmpty() != (data.length == 0)) {
            throw new IllegalArgumentException("data must be empty if commitBatchRequestContexts is empty");
        }
        this.start = start;
        this.allFuturesByRequest = allFuturesByRequest;
        this.validatedFutures = validatedFutures;
        this.commitBatchRequestContexts = commitBatchRequestContexts;
        this.data = data;
    }

    public Instant start() {
        return start;
    }

    public Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest() {
        return allFuturesByRequest;
    }

    public List<CommitBatchRequestContext> commitBatchRequestContexts() {
        return commitBatchRequestContexts;
    }

    public byte[] data() {
        return data;
    }

    public Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> validatedFutures() {
        return validatedFutures;
    }

    int size() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ClosedFile) obj;
        return Objects.equals(this.start, that.start) &&
            Objects.equals(this.allFuturesByRequest, that.allFuturesByRequest) &&
            Objects.equals(this.commitBatchRequestContexts, that.commitBatchRequestContexts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, allFuturesByRequest, commitBatchRequestContexts);
    }

    @Override
    public String toString() {
        return "ClosedFile[" +
            "start=" + start + ", " +
            "allFuturesByRequest=" + allFuturesByRequest + ", " +
            "commitBatchRequestContexts=" + commitBatchRequestContexts + ", " +
            "data=(length:" + data.length + ")]";
    }

}
