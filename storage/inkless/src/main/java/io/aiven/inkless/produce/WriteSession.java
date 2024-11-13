// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import io.aiven.inkless.control_plane.CommitBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A write session.
 *
 * <p>This class brings together the batch buffer, the associated add requests,
 * and the logic to finish the promised futures based on the control plane commit result.
 *
 * <p>This class is not thread-safe and is supposed to be protected with a lock at the call site.
 */
class WriteSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteSession.class);

    private final int maxSize;

    private int requestId = 0;
    private final BatchBuffer buffer = new BatchBuffer();
    private final Map<Integer, Map<TopicPartition, MemoryRecords>> originalRequests = new HashMap<>();
    private final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest =
        new HashMap<>();
    private BatchBufferCloseResult closeResult = null;

    WriteSession(final int maxSize) {
        this.maxSize = maxSize;
    }

    CompletableFuture<Map<TopicPartition, PartitionResponse>> add(
        final Map<TopicPartition, MemoryRecords> entriesPerPartition
    ) {
        if (isClosed()) {
            LOGGER.error("Attempt to add after closing");
            throw new IllegalStateException("Attempt to add after closing");
        }

        originalRequests.put(requestId, entriesPerPartition);

        for (final var entry : entriesPerPartition.entrySet()) {
            for (final var batch : entry.getValue().batches()) {
                buffer.addBatch(entry.getKey(), batch, requestId);
            }
        }

        final CompletableFuture<Map<TopicPartition, PartitionResponse>> result = new CompletableFuture<>();
        awaitingFuturesByRequest.put(requestId, result);

        requestId += 1;

        return result;
    }

    boolean canAddMore() {
        return buffer.totalSize() < maxSize;
    }

    BatchBufferCloseResult closeAndPrepareForCommit() {
        if (isClosed()) {
            LOGGER.error("Attempt to close after closing");
            throw new IllegalStateException("Attempt to close after closing");
        }

        closeResult = buffer.close();
        return closeResult;
    }

    void finishCommit(final List<CommitBatchResponse> commitBatchResponses,
                      final Throwable error) {
        if (!isClosed()) {
            LOGGER.error("Attempt to finish commit before closing");
            throw new IllegalStateException("Attempt to finish commit before closing");
        }

        if (error == null) {
            finishCommitSuccessfully(commitBatchResponses);
        } else {
            finishCommitWithError(error);
        }
    }

    private void finishCommitSuccessfully(final List<CommitBatchResponse> commitBatchResponses) {
        LOGGER.debug("Committed successfully");
        final Map<Integer, Map<TopicPartition, PartitionResponse>> resultsPerRequest = new HashMap<>();
        for (int i = 0; i < commitBatchResponses.size(); i++) {
            final int requestId = closeResult.requestIds().get(i);
            final var result = resultsPerRequest.computeIfAbsent(requestId, ignore -> new HashMap<>());

            final var commitBatchRequest = closeResult.commitBatchRequests().get(i);
            final var commitBatchResponse = commitBatchResponses.get(i);
            // TODO correct append time and start offset
            result.put(commitBatchRequest.topicPartition(), new PartitionResponse(
                commitBatchResponse.errors(), commitBatchResponse.assignedOffset(), -1, -1
            ));
        }

        for (final var entry : awaitingFuturesByRequest.entrySet()) {
            final var result = resultsPerRequest.get(entry.getKey());
            entry.getValue().complete(result);
        }
    }

    private void finishCommitWithError(final Throwable error) {
        LOGGER.error("Commit failed", error);
        for (final var entry : awaitingFuturesByRequest.entrySet()) {
            final var originalRequest = originalRequests.get(entry.getKey());
            final var result = originalRequest.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    ignore -> new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")));
            entry.getValue().complete(result);
        }
    }

    private boolean isClosed() {
        return closeResult != null;
    }
}
