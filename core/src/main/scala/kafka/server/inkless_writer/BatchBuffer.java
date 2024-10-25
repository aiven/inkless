package kafka.server.inkless_writer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import kafka.server.inkless_common.CommitFileRequest;

class BatchBuffer {
    private final List<BatchInfo> batches = new ArrayList<>();

    void addBatch(final TopicPartition topicPartition, final RecordBatch batch, final long requestTracker) {
        batches.add(new BatchInfo(topicPartition, batch, requestTracker));
    }

    public CloseResult close() {
        // stable sort
        batches.sort(Comparator
            .comparing((BatchInfo b) -> b.topicPartition.topic())
            .thenComparingInt(b -> b.topicPartition.partition())
        );

        int totalBytes = 0;
        for (final BatchInfo batchInfo : batches) {
            totalBytes += batchInfo.batch.sizeInBytes();
        }
        final ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);

        final List<CommitFileRequest.Batch> batchRequests = new ArrayList<>();
        for (final BatchInfo batchInfo : batches) {
            batchRequests.add(new CommitFileRequest.Batch(
                batchInfo.topicPartition, byteBuffer.position(), batchInfo.batch.sizeInBytes(),
                batchInfo.numberOfRecords(), batchInfo.requestTracker
            ));
            batchInfo.batch.writeTo(byteBuffer);
        }
        return new CloseResult(batchRequests, byteBuffer.array());
    }

    public static class CloseResult {
        public final List<CommitFileRequest.Batch> batches;
        public final byte[] data;

        CloseResult(final List<CommitFileRequest.Batch> batches, final byte[] data) {
            this.batches = batches;
            this.data = data;
        }
    }

    private static class BatchInfo {
        final TopicPartition topicPartition;
        final RecordBatch batch;
        final long requestTracker;

        private BatchInfo(final TopicPartition topicPartition, final RecordBatch batch,
                          final long requestTracker) {
            this.topicPartition = topicPartition;
            this.batch = batch;
            this.requestTracker = requestTracker;
        }

        long numberOfRecords() {
            return batch.nextOffset() - batch.baseOffset();
        }
    }
}
