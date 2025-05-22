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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.CommitBatchRequest;

/**
 * A closed file that contains all the information needed to commit the file to the control plane.
 *
 * <p>Original requests must match the commit batch requests and invalid responses to ensure that all batches are accounted for.
 * <p>Awaiting futures by request must match the original requests, so a future is hold per request.
 *
 * @param start The time when the file was closed.
 * @param originalRequests The original requests that were sent to the broker, by request id.
 * @param awaitingFuturesByRequest The futures that are awaiting a response from the broker, by request id.
 * @param commitBatchRequests The commit batch requests that will be sent to the remote storage.
 * @param invalidResponseByRequest The invalid responses after validating the request that will not be sent to the remote storage, by request id.
 * @param data concatenated data of all the valid batches.
 */
record ClosedFile(Instant start,
                  Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests,
                  Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest,
                  List<CommitBatchRequest> commitBatchRequests,
                  Map<Integer, Map<TopicPartition, PartitionResponse>> invalidResponseByRequest,
                  ByteBuffer data,
                  int totalSize) {
    ClosedFile {
        Objects.requireNonNull(originalRequests, "originalRequests cannot be null");
        Objects.requireNonNull(awaitingFuturesByRequest, "awaitingFuturesByRequest cannot be null");
        Objects.requireNonNull(commitBatchRequests, "commitBatchRequests cannot be null");
        Objects.requireNonNull(invalidResponseByRequest, "invalidResponseByRequest cannot be null");
        Objects.requireNonNull(data, "data cannot be null");

        if (!originalRequests.isEmpty() && start == null) {
            throw new IllegalArgumentException("start time cannot be null if there are requests processed");
        }

        if (totalSize < 0) {
            throw new IllegalArgumentException("totalSize cannot be negative");
        }

        // Validate request maps have matching sizes
        if (originalRequests.size() != awaitingFuturesByRequest.size()) {
            throw new IllegalArgumentException(
                "originalRequests and awaitingFuturesByRequest must be of same size");
        }

        // Map commit requests by requestId for faster lookup
        final var validCommitRequestsById = commitBatchRequests.stream()
            .collect(Collectors.groupingBy(CommitBatchRequest::requestId));

        // Validate each requestId separately
        for (final var entry : originalRequests.entrySet()) {
            final var requestId = entry.getKey();
            final var originalTopicPartitions = entry.getValue().keySet();

            // Get valid requests for this requestId
            final var validCommitRequests = validCommitRequestsById.getOrDefault(requestId, List.of())
                .stream()
                .map(CommitBatchRequest::topicIdPartition)
                .collect(Collectors.toSet());

            // Get invalid responses for this requestId
            final var invalidResponses = invalidResponseByRequest.getOrDefault(requestId, Map.of()).keySet();

            // For each original partition, verify it exists in either valid or invalid collections
            for (final var originalPartition : originalTopicPartitions) {
                final var topicPartition = originalPartition.topicPartition();
                boolean foundInValid = validCommitRequests.contains(originalPartition);
                boolean foundInInvalid = invalidResponses.contains(topicPartition);

                if (!foundInValid && !foundInInvalid) {
                    throw new IllegalArgumentException(
                        "No corresponding valid or invalid response found for partition " +
                            topicPartition + " in request " + requestId);
                }

                if (foundInValid && foundInInvalid) {
                    throw new IllegalArgumentException(
                        "Partition " + topicPartition + " in request " + requestId +
                            " found in both valid and invalid collections");
                }
            }

            // Verify no extra entries in valid or invalid collections
            if (originalTopicPartitions.size() != validCommitRequests.size() + invalidResponses.size()) {
                throw new IllegalArgumentException(
                    "Total number of valid and invalid responses doesn't match original requests " +
                        "for request id " + requestId);
            }
        }

        // Validate data consistency
        if (commitBatchRequests.isEmpty() != (totalSize == 0)) {
            throw new IllegalArgumentException("data must be empty if commitBatchRequests is empty");
        }
    }

    public boolean isEmpty() {
        return totalSize == 0;
    }
}
