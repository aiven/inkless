/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
                  byte[] data) {
    ClosedFile {
        Objects.requireNonNull(start, "start cannot be null");
        Objects.requireNonNull(originalRequests, "originalRequests cannot be null");
        Objects.requireNonNull(awaitingFuturesByRequest, "awaitingFuturesByRequest cannot be null");
        Objects.requireNonNull(commitBatchRequests, "commitBatchRequests cannot be null");

        if (originalRequests.size() != awaitingFuturesByRequest.size()) {
            throw new IllegalArgumentException(
                "originalRequests and awaitingFuturesByRequest must be of same size");
        }

        Objects.requireNonNull(data, "data cannot be null");

        if (commitBatchRequests.isEmpty() != (data.length == 0)) {
            throw new IllegalArgumentException("data must be empty if commitBatchRequests is empty");
        }
    }

    int size() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }
}
