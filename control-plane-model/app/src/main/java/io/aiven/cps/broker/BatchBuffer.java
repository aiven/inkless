package io.aiven.cps.broker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.aiven.cps.common.CommitFileRequest;

public class BatchBuffer {
    private final String fileName;
    private final List<Batch> batches = new ArrayList<>();

    public BatchBuffer(final String fileName) {
        this.fileName = fileName;
    }

    public void addBatch(final String topic, final int partition, final int numberOfRecords, final byte[] data) {
        batches.add(new Batch(topic, partition, numberOfRecords, data));
    }

    public CloseResult close() throws IOException {
        // stable sort
        batches.sort(Comparator
            .comparing((Batch b) -> b.topic)
            .thenComparingInt(b -> b.partition)
        );

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final List<CommitFileRequest.Batch> batchRequests = new ArrayList<>();
            for (final Batch batch : batches) {
                batchRequests.add(new CommitFileRequest.Batch(
                    batch.topic, batch.partition, out.size(), batch.data.length, batch.numberOfRecords
                ));
                out.writeBytes(batch.data);
            }
            final CommitFileRequest commitFileRequest = new CommitFileRequest(fileName, batchRequests);
            return new CloseResult(commitFileRequest, out.toByteArray());
        }
    }

    private static class Batch {
        final String topic;
        final int partition;
        final int numberOfRecords;
        final byte[] data;

        Batch(final String topic, final int partition, final int numberOfRecords, final byte[] data) {
            this.topic = topic;
            this.partition = partition;
            this.numberOfRecords = numberOfRecords;
            this.data = data;
        }
    }

    public static class CloseResult {
        public final CommitFileRequest commitFileRequest;
        public final byte[] data;

        CloseResult(final CommitFileRequest commitFileRequest, final byte[] data) {
            this.commitFileRequest = commitFileRequest;
            this.data = data;
        }
    }
}
