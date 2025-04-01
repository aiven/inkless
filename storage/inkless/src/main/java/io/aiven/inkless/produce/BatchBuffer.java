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
import org.apache.kafka.common.record.MutableRecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import io.aiven.inkless.control_plane.CommitBatchRequest;

class BatchBuffer {
    private final List<BatchHolder> batches = new ArrayList<>();

    private int totalSize = 0;
    private boolean closed = false;

    void addBatch(final TopicIdPartition topicPartition,
                  final MutableRecordBatch batch,
                  final int requestId) {
        Objects.requireNonNull(topicPartition, "topicPartition cannot be null");
        Objects.requireNonNull(batch, "batch cannot be null");

        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        final BatchHolder batchHolder = new BatchHolder(topicPartition, batch, requestId);
        batches.add(batchHolder);
        totalSize += batch.sizeInBytes();
    }

    CloseResult close() {
        int totalSize = totalSize();

        // Group together by topic-partition.
        // The sort is stable so the relative order of batches of the same partition won't change.
        batches.sort(
            Comparator.comparing((BatchHolder b) -> b.topicIdPartition().topicId())
                .thenComparing(b -> b.topicIdPartition().partition())
        );

        final ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);

        final List<CommitBatchRequest> commitBatchRequests = new ArrayList<>();
        for (final BatchHolder batchHolder : batches) {
            commitBatchRequests.add(
                CommitBatchRequest.of(
                    batchHolder.requestId,
                    batchHolder.topicIdPartition(),
                    byteBuffer.position(),
                    batchHolder.batch
                )
            );
            batchHolder.batch.writeTo(byteBuffer);
        }

        closed = true;
        return new CloseResult(commitBatchRequests, byteBuffer.array());
    }

    int totalSize() {
        return totalSize;
    }

    private record BatchHolder(TopicIdPartition topicIdPartition,
                               MutableRecordBatch batch,
                               int requestId) {
    }

    /**
     * The result of closing a batch buffer.
     *
     * @param commitBatchRequests commit batch requests matching in order the batches in {@code data}.
     */
    record CloseResult(List<CommitBatchRequest> commitBatchRequests,
                       byte[] data) {
    }
}
