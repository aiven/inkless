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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Holds the result of validation stage, to be passed to the buffer writer stage.
 *
 * <p>This class is used to transfer validated batches between the validation workers
 * and the single buffer writer thread in the pipelined Writer architecture.
 *
 * <p>Thread-safety: This class is immutable except for the resultFuture which is
 * set once during construction and completed once by the buffer writer.
 */
class ValidatedRequest {

    /**
     * Holds a validated batch ready for buffering.
     */
    record ValidatedBatch(
        TopicIdPartition topicIdPartition,
        MemoryRecords validatedRecords,
        LogAppendInfo appendInfo
    ) {
        ValidatedBatch {
            Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
            Objects.requireNonNull(validatedRecords, "validatedRecords cannot be null");
            Objects.requireNonNull(appendInfo, "appendInfo cannot be null");
        }
    }

    private final Map<TopicIdPartition, MemoryRecords> originalRecords;
    private final Map<TopicIdPartition, ValidatedBatch> validatedBatches;
    private final Map<TopicIdPartition, PartitionResponse> invalidBatches;
    private final CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture;
    private final boolean rotationTrigger;

    ValidatedRequest(
        Map<TopicIdPartition, MemoryRecords> originalRecords,
        Map<TopicIdPartition, ValidatedBatch> validatedBatches,
        Map<TopicIdPartition, PartitionResponse> invalidBatches,
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture
    ) {
        this(originalRecords, validatedBatches, invalidBatches, resultFuture, false);
    }

    private ValidatedRequest(
        Map<TopicIdPartition, MemoryRecords> originalRecords,
        Map<TopicIdPartition, ValidatedBatch> validatedBatches,
        Map<TopicIdPartition, PartitionResponse> invalidBatches,
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture,
        boolean rotationTrigger
    ) {
        this.originalRecords = Objects.requireNonNull(originalRecords, "originalRecords cannot be null");
        this.validatedBatches = Objects.requireNonNull(validatedBatches, "validatedBatches cannot be null");
        this.invalidBatches = Objects.requireNonNull(invalidBatches, "invalidBatches cannot be null");
        this.resultFuture = Objects.requireNonNull(resultFuture, "resultFuture cannot be null");
        this.rotationTrigger = rotationTrigger;
    }

    /**
     * Creates a special rotation trigger request.
     * This is used to signal the buffer writer to rotate the active file.
     */
    static ValidatedRequest rotationTrigger() {
        return new ValidatedRequest(
            Map.of(), Map.of(), Map.of(),
            CompletableFuture.completedFuture(Map.of()),
            true
        );
    }

    Map<TopicIdPartition, MemoryRecords> originalRecords() {
        return originalRecords;
    }

    Map<TopicIdPartition, ValidatedBatch> validatedBatches() {
        return validatedBatches;
    }

    Map<TopicIdPartition, PartitionResponse> invalidBatches() {
        return invalidBatches;
    }

    CompletableFuture<Map<TopicIdPartition, PartitionResponse>> resultFuture() {
        return resultFuture;
    }

    /**
     * Returns true if this is a rotation trigger signal, not a real request.
     */
    boolean isRotationTrigger() {
        return rotationTrigger;
    }

    /**
     * Returns the total size in bytes of all validated batches.
     */
    int validatedSize() {
        return validatedBatches.values().stream()
            .mapToInt(b -> b.validatedRecords().sizeInBytes())
            .sum();
    }

    /**
     * Returns true if there are no validated batches (all were invalid).
     */
    boolean hasNoValidBatches() {
        return validatedBatches.isEmpty();
    }
}
