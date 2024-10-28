package kafka.server.inkless_control_plane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.TopicPartition;

import kafka.server.inkless_common.CommitFileRequest;
import kafka.server.inkless_common.CommitFileResponse;
import kafka.server.inkless_common.FindBatchResponse;
import kafka.server.inkless_common.FindBatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlPlane {
    private static final Logger logger = LoggerFactory.getLogger(ControlPlane.class);

    private final HashMap<TopicPartition, TreeMap<Long, Batch>> batchCoordinates = new HashMap<>();
    private final HashMap<TopicPartition, Long> highWatermarks = new HashMap<>();

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

            highWatermarks.put(batch.topicPartition, assignedOffset + batch.numberOfRecords);
        }

        return new CommitFileResponse(assignedOffsets);
    }

    public synchronized FindBatchResponse findBatch(final FindBatchRequest request) {
        TreeMap<Long, Batch> tpCoordinates = batchCoordinates.get(request.topicPartition);
        if (tpCoordinates == null) {
            return null;
        }
        final Long highWatermark = highWatermarks.get(request.topicPartition);
        if (highWatermark == null) {
            // TODO handle
            throw new RuntimeException("highWatermark is null");
        }

        final Map.Entry<Long, Batch> entry = tpCoordinates.floorEntry(request.kafkaOffset);
        if (entry == null) {
            return new FindBatchResponse(null, highWatermark);
        }

        final long offset = entry.getKey();
        final Batch batch = entry.getValue();
        if (request.kafkaOffset >= offset + batch.numberOfRecords) {
            return new FindBatchResponse(null, highWatermark);
        }

        return new FindBatchResponse(
            new FindBatchResponse.BatchInfo(
                batch.filePath,
                batch.byteOffset,
                batch.byteSize,
                offset,
                batch.numberOfRecords
            ),
            highWatermark
        );
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
