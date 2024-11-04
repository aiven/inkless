package io.aiven.inkless.common;

import java.util.List;

import org.apache.kafka.common.TopicPartition;

public class CommitFileRequest {
    public final String filePath;
    public final List<Batch> batches;

    public CommitFileRequest(final String filePath, final List<Batch> batches) {
        this.filePath = filePath;
        this.batches = batches;
    }

    public static class Batch {
        public final TopicPartition topicPartition;
        public final int byteOffset;
        public final int sizeInBytes;
        public final long numberOfRecords;
        public final long requestTracker;  // TODO shouldn't be here, find another way to propagate it

        public Batch(final TopicPartition topicPartition,
                     final int byteOffset, final int sizeInBytes,
                     final long numberOfRecords, final long requestTracker) {
            this.topicPartition = topicPartition;
            this.byteOffset = byteOffset;
            this.sizeInBytes = sizeInBytes;
            this.numberOfRecords = numberOfRecords;
            this.requestTracker = requestTracker;
        }

        @Override
        public String toString() {
            return "Batch["
                + "topicPartition=" + this.topicPartition
                + ", byteOffset=" + this.byteOffset
                + ", sizeInBytes=" + this.sizeInBytes
                + ", numberOfRecords=" + this.numberOfRecords
                + ", requestTracker=" + this.requestTracker
                + "]";
        }
    }
}
