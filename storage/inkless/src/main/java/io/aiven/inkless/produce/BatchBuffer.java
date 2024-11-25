// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

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

    void addBatch(final TopicPartition topicPartition, final RecordBatch batch, final int requestId) {
        Objects.requireNonNull(topicPartition, "topicPartition cannot be null");
        Objects.requireNonNull(batch, "batch cannot be null");

        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        batches.add(new BatchHolder(topicPartition, batch, requestId));
        totalSize += batch.sizeInBytes();
    }

    CloseResult close() {
        int totalSize = totalSize();

        // Group together by topic-partition.
        // The sort is stable so the relative order of batches of the same partition won't change.
        batches.sort(
            Comparator.comparing((BatchHolder b) -> b.topicPartition().topic())
                .thenComparing(b -> b.topicPartition().partition())
        );

        final ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);

        final List<CommitBatchRequest> commitBatchRequests = new ArrayList<>();
        final List<Integer> requestIds = new ArrayList<>();
        for (final BatchHolder batchHolder : batches) {
            final int offset = byteBuffer.position();
            commitBatchRequests.add(new CommitBatchRequest(
                batchHolder.topicPartition(), offset, batchHolder.batch.sizeInBytes(), batchHolder.numberOfRecords()
            ));
            requestIds.add(batchHolder.requestId);
            batchHolder.batch.writeTo(byteBuffer);
        }

        closed = true;
        return new CloseResult(commitBatchRequests, requestIds, byteBuffer.array());
    }

    int totalSize() {
        return totalSize;
    }

    private record BatchHolder(TopicPartition topicPartition,
                               RecordBatch batch,
                               int requestId) {
        long numberOfRecords() {
            return batch.nextOffset() - batch.baseOffset();
        }
    }

    /**
     * The result of closing a batch buffer.
     *
     * @param commitBatchRequests commit batch requests matching in order the batches in {@code data}.
     * @param requestIds          produce request IDs matching in order the batches in {@code data}.
     */
    record CloseResult(List<CommitBatchRequest> commitBatchRequests,
                       List<Integer> requestIds,
                       byte[] data) {
    }
}
