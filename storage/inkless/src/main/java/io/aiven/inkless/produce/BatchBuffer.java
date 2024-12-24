// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;

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

    void addBatch(final TopicIdPartition topicPartition, final TimestampType messageTimestampType,
                  final MutableRecordBatch batch, final int requestId) {
        Objects.requireNonNull(topicPartition, "topicPartition cannot be null");
        Objects.requireNonNull(messageTimestampType, "timestampType cannot be null");
        Objects.requireNonNull(batch, "batch cannot be null");

        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        final BatchHolder batchHolder = new BatchHolder(topicPartition, batch, messageTimestampType, requestId);
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
        final List<Integer> requestIds = new ArrayList<>();
        for (final BatchHolder batchHolder : batches) {
            final int byteOffset = byteBuffer.position();

            commitBatchRequests.add(batchHolder.commitBatchRequest(byteOffset));
            requestIds.add(batchHolder.requestId);
            batchHolder.batch.writeTo(byteBuffer);
        }

        closed = true;
        return new CloseResult(commitBatchRequests, requestIds, byteBuffer.array());
    }

    int totalSize() {
        return totalSize;
    }

    private record BatchHolder(TopicIdPartition topicIdPartition,
                               MutableRecordBatch batch,
                               TimestampType timestampType,
                               int requestId) {
        CommitBatchRequest commitBatchRequest(int byteOffset) {
            final CommitBatchRequest request;
            if (batch.hasProducerId()) {
                request = CommitBatchRequest.idempotent(
                    topicIdPartition(),
                    byteOffset,
                    batch.sizeInBytes(),
                    batch.baseOffset(),
                    batch.lastOffset(),
                    batch.maxTimestamp(),
                    timestampType,
                    batch.producerId(),
                    batch.producerEpoch(),
                    batch.baseSequence(),
                    batch.lastSequence()
                );
            } else {
                request = CommitBatchRequest.of(
                    topicIdPartition(),
                    byteOffset,
                    batch.sizeInBytes(),
                    batch.baseOffset(),
                    batch.lastOffset(),
                    batch.maxTimestamp(),
                    timestampType
                );
            }
            return request;
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
