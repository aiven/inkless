package io.aiven.cps.common;

import java.util.List;

public class CommitFileRequest {
    public final String fileName;
    public final List<Batch> batches;

    public CommitFileRequest(final String fileName, List<Batch> batches) {
        this.fileName = fileName;
        this.batches = batches;
    }

    public static class Batch {
        public final String topic;
        public final int partition;
        public final int byteOffset;
        public final int byteSize;
        public final int numberOfRecords;

        public Batch(final String topic, final int partition,
                     final int byteOffset, final int byteSize,
                     final int numberOfRecords) {
            this.topic = topic;
            this.partition = partition;
            this.byteOffset = byteOffset;
            this.byteSize = byteSize;
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public String toString() {
            return "Batch["
                + "topic=" + this.topic
                + ", partition=" + this.partition
                + ", byteOffset=" + this.byteOffset
                + ", byteSize=" + this.byteSize
                + ", numberOfRecords=" + this.numberOfRecords
                + "]";
        }
    }
}
