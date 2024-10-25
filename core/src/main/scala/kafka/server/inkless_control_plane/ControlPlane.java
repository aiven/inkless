package kafka.server.inkless_control_plane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.kafka.common.TopicPartition;

import kafka.server.inkless_common.CommitFileRequest;
import kafka.server.inkless_common.CommitFileResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlPlane {
    private static final Logger logger = LoggerFactory.getLogger(ControlPlane.class);

    private final HashMap<TopicPartition, TreeMap<Long, Batch>> batchCoordinates = new HashMap<>();

    public synchronized CommitFileResponse commitFile(final CommitFileRequest request) {
        logger.error("Committing file");

        final List<Long> assignedOffsets = new ArrayList<>();

        for (final CommitFileRequest.Batch batch : request.batches) {
            TreeMap<Long, Batch> tpCoordinates = batchCoordinates.computeIfAbsent(batch.topicPartition, ignore -> new TreeMap<>());
            final long assignedOffset;
            if (tpCoordinates.isEmpty()) {
                assignedOffset = 0;
            } else {
                assignedOffset = tpCoordinates.lastKey() + tpCoordinates.lastEntry().getValue().numberOfRecords;
            }
            tpCoordinates.put(
                assignedOffset, new Batch(request.filePath, batch.byteOffset, batch.sizeInBytes, batch.numberOfRecords)
            );
            assignedOffsets.add(assignedOffset);
        }

        return new CommitFileResponse(assignedOffsets);
    }

    private static class Batch {
        final String filePath;
        final int byteOffset;
        final int byteSize;
        final long numberOfRecords;

        private Batch(final String filePath,
                      final int byteOffset, final int byteSize,
                      final long numberOfRecords) {
            this.filePath = filePath;
            this.byteOffset = byteOffset;
            this.byteSize = byteSize;
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public String toString() {
            return "Batch["
                + "filePath=" + this.filePath
                + ", byteOffset=" + this.byteOffset
                + ", byteSize=" + this.byteSize
                + ", numberOfRecords=" + this.numberOfRecords
                + "]";
        }
    }
}
