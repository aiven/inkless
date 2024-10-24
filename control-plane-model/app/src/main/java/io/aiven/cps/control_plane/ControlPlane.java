package io.aiven.cps.control_plane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import io.aiven.cps.common.CommitFileRequest;
import io.aiven.cps.common.CommitFileResponse;
import io.aiven.cps.common.FindBatchRequest;
import io.aiven.cps.common.FindBatchResponse;

public class ControlPlane {
    private final HashMap<TopicPartition, TreeMap<Long, Batch>> batchCoordinates = new HashMap<>();

    public CommitFileResponse commitFile(final CommitFileRequest request) {
        final List<Long> assignedOffsets = new ArrayList<>();

        for (final CommitFileRequest.Batch batch : request.batches) {
            final TopicPartition tp = new TopicPartition(batch.topic, batch.partition);

            TreeMap<Long, Batch> tpCoordinates = batchCoordinates.computeIfAbsent(tp, ignore -> new TreeMap<>());
            final long assignedOffset;
            if (tpCoordinates.isEmpty()) {
                assignedOffset = 0;
            } else {
                assignedOffset = tpCoordinates.lastKey() + tpCoordinates.lastEntry().getValue().numberOfRecords;
            }
            tpCoordinates.put(
                assignedOffset, new Batch(request.fileName, batch.byteOffset, batch.byteSize, batch.numberOfRecords)
            );
            assignedOffsets.add(assignedOffset);
        }

        return new CommitFileResponse(assignedOffsets);
    }

    public FindBatchResponse findBatch(final FindBatchRequest request) {
        TreeMap<Long, Batch> tpCoordinates = batchCoordinates.get(new TopicPartition(request.topic, request.partition));
        if (tpCoordinates == null) {
            return null;
        }

        final Map.Entry<Long, Batch> entry = tpCoordinates.floorEntry(request.kafkaOffset);
        if (entry == null) {
            return null;
        }

        final long offset = entry.getKey();
        final Batch batch = entry.getValue();
        if (request.kafkaOffset >= offset + batch.numberOfRecords) {
            return null;
        }

        return new FindBatchResponse(
            batch.fileName,
            batch.byteOffset,
            batch.byteSize,
            offset,
            batch.numberOfRecords
        );
    }

    private static class TopicPartition {
        final String topic;
        final int partition;

        TopicPartition(final String topic, final int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicPartition that = (TopicPartition) o;
            return partition == that.partition && Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }

        @Override
        public String toString() {
            return String.format("%s-%d", this.topic, this.partition);
        }
    }

    private static class Batch {
        final String fileName;
        final int byteOffset;
        final int byteSize;
        final int numberOfRecords;

        private Batch(final String fileName,
                      final int byteOffset, final int byteSize,
                      final int numberOfRecords) {
            this.fileName = fileName;
            this.byteOffset = byteOffset;
            this.byteSize = byteSize;
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public String toString() {
            return "Batch["
                + "fileName=" + this.fileName
                + ", byteOffset=" + this.byteOffset
                + ", byteSize=" + this.byteSize
                + ", numberOfRecords=" + this.numberOfRecords
                + "]";
        }
    }
}
