// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import io.aiven.inkless.control_plane.CommitBatchRequest;

class BatchBuffer {
    private final AtomicReference<List<BatchHolder>> batches = new AtomicReference<>(new ArrayList<>());

    void addBatch(final TopicPartition topicPartition, final RecordBatch batch) {
        batches.getAndUpdate(list -> {
            list.add(new BatchHolder(topicPartition, batch));
            return list;
        });
    }

    CloseResult close() {
        final List<BatchHolder> currentBatchHolders = this.batches.getAndUpdate(ignore -> new ArrayList<>());
        // Group together by topic-partition.
        // The sort is stable so the relative order of batches of the same partition won't change.
        currentBatchHolders.sort(
            Comparator.comparing((BatchHolder b) -> b.topicPartition().topic())
                .thenComparing(b -> b.topicPartition().partition())
        );

        final int totalSize = currentBatchHolders.stream().mapToInt(b -> b.batch().sizeInBytes()).sum();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);

        final List<CommitBatchRequest> commitBatchRequests = new ArrayList<>();
        for (final BatchHolder batchHolder : currentBatchHolders) {
            final int offset = byteBuffer.position();
            commitBatchRequests.add(new CommitBatchRequest(
                batchHolder.topicPartition(), offset, batchHolder.batch.sizeInBytes(), batchHolder.numberOfRecords()
            ));
            batchHolder.batch.writeTo(byteBuffer);
        }
        return new CloseResult(commitBatchRequests, byteBuffer.array());
    }

    record CloseResult(List<CommitBatchRequest> commitBatchRequests,
                       byte[] data) {}

    record BatchHolder(TopicPartition topicPartition,
                       RecordBatch batch) {
        long numberOfRecords() {
            return batch.nextOffset() - batch.baseOffset();
        }
    }
}
