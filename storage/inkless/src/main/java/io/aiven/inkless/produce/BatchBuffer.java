// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MutableRecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.aiven.inkless.control_plane.CommitBatchRequest;

class BatchBuffer {
    private final List<BatchHolder> batches = new ArrayList<>();
    private final Map<Integer, Map<TopicPartition, Map<Long, BatchHolder>>> requestTopicPartitionToBatch = new HashMap<>();

    private int totalSize = 0;
    private boolean closed = false;

    void addBatch(final TopicPartition topicPartition, final MutableRecordBatch batch, final int requestId) {
        Objects.requireNonNull(topicPartition, "topicPartition cannot be null");
        Objects.requireNonNull(batch, "batch cannot be null");

        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        final BatchHolder batchHolder = new BatchHolder(topicPartition, batch, requestId);
        batches.add(batchHolder);
        final Map<Long, BatchHolder> offsetBatchHolderMap = requestTopicPartitionToBatch
            .computeIfAbsent(requestId, r -> new HashMap<>())
            .computeIfAbsent(topicPartition, tp -> new HashMap<>());
        // We assume that the base offset is unique per topic-partition and request.
        if (offsetBatchHolderMap.containsKey(batch.baseOffset())) {
            throw new IllegalStateException("Duplicate base offset " + batch.baseOffset()
                + " for topic-partition " + topicPartition
                + " and request " + requestId);
        }
        offsetBatchHolderMap.putIfAbsent(batch.baseOffset(), batchHolder);

        totalSize += batch.sizeInBytes();
    }

    CloseResult close(Map<Integer, Map<TopicPartition, DuplicatedBatchMetadata>> duplicatedBatches) {
        int totalSize = totalSize();

        for (var requestEntry : duplicatedBatches.entrySet()) {
            for (var batchEntry : requestEntry.getValue().entrySet()) {
                final BatchHolder batchHolder = requestTopicPartitionToBatch
                    .get(requestEntry.getKey())
                    .get(batchEntry.getKey())
                    .get(batchEntry.getValue().originalBaseOffset());
                batches.remove(batchHolder);
                totalSize -= batchHolder.batch.sizeInBytes();
            }
        }

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
            final int byteOffset = byteBuffer.position();

            final CommitBatchRequest request;
            if (batchHolder.batch.hasProducerId()) {
                request = CommitBatchRequest.ofIdempotent(
                    batchHolder.topicPartition(),
                    byteOffset,
                    batchHolder.batch.sizeInBytes(),
                    batchHolder.batch.baseOffset(),
                    batchHolder.batch.lastOffset(),
                    batchHolder.batch.maxTimestamp(),
                    batchHolder.batch.producerId(),
                    batchHolder.batch.producerEpoch(),
                    batchHolder.batch.lastSequence()
                );
            } else {
                request = CommitBatchRequest.of(
                    batchHolder.topicPartition(),
                    byteOffset,
                    batchHolder.batch.sizeInBytes(),
                    batchHolder.batch.baseOffset(),
                    batchHolder.batch.lastOffset()
                );
            }
            commitBatchRequests.add(request);
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
                               MutableRecordBatch batch,
                               int requestId) {
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
