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

final class ClosedFile {
    private final Instant start;
    private final Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests;
    private final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest;
    private final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> validatedFutures;
    private final List<CommitBatchRequest> commitBatchRequests;
    private final List<Integer> requestIds;
    private final byte[] data;

    ClosedFile(
        Instant start,
        Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests,
        Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest,
        List<CommitBatchRequest> commitBatchRequests,
        List<Integer> requestIds,
        byte[] data
    ) {
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
        this.start = start;
        this.originalRequests = originalRequests;
        this.allFuturesByRequest = allFuturesByRequest;
        this.validatedFutures = buildValidatedFutures(allFuturesByRequest);
        this.commitBatchRequests = commitBatchRequests;
        this.requestIds = requestIds;
        this.data = data;
    }

    private static Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> buildValidatedFutures(
        Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest
    ) {
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

    public Instant start() {
        return start;
    }

    public Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests() {
        return originalRequests;
    }

    public Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest() {
        return allFuturesByRequest;
    }

    public List<CommitBatchRequest> commitBatchRequests() {
        return commitBatchRequests;
    }

    public List<Integer> requestIds() {
        return requestIds;
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
            Objects.equals(this.originalRequests, that.originalRequests) &&
            Objects.equals(this.allFuturesByRequest, that.allFuturesByRequest) &&
            Objects.equals(this.commitBatchRequests, that.commitBatchRequests) &&
            Objects.equals(this.requestIds, that.requestIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, originalRequests, allFuturesByRequest, commitBatchRequests, requestIds);
    }

    @Override
    public String toString() {
        return "ClosedFile[" +
            "start=" + start + ", " +
            "originalRequests=" + originalRequests + ", " +
            "allFuturesByRequest=" + allFuturesByRequest + ", " +
            "commitBatchRequests=" + commitBatchRequests + ", " +
            "requestIds=" + requestIds + ", " +
            "data=(length:" + data.length + ")]";
    }

}
