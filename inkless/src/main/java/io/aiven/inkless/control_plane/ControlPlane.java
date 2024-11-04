package io.aiven.inkless.control_plane;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.TopicPartition;

import io.aiven.inkless.common.BatchCoordinates;
import io.aiven.inkless.common.BatchInfo;
import io.aiven.inkless.common.CommitFileRequest;
import io.aiven.inkless.common.CommitFileResponse;
import io.aiven.inkless.common.FindBatchRequest;
import io.aiven.inkless.common.FindBatchResponse;

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

    public synchronized Map<TopicPartition, FindBatchResponse> findBatchesForFetch(
        final List<FindBatchRequest> findBatchRequests,
        final boolean minOneMessage,
        final int fetchMaxBytes
    ) {
        int totalSizeRemaining = fetchMaxBytes;
        final HashMap<TopicPartition, Integer> sizePerPartitionRemaining = new HashMap<>();

        final Map<TopicPartition, FindBatchResponse> result = new HashMap<>();
        final Map<TopicPartition, Long> currentBatches = new HashMap<>();
        final Deque<TopicPartition> topicPartitionsToIterate = new ArrayDeque<>();  // LIFO

        for (final FindBatchRequest findBatchRequest : findBatchRequests) {
            final Long highWatermark = highWatermarks.get(findBatchRequest.topicPartition);
            if (highWatermark == null) {
                // TODO handle
                throw new RuntimeException("highWatermark is null");
            }

            result.put(findBatchRequest.topicPartition, new FindBatchResponse(new ArrayList<>(), highWatermark));
            sizePerPartitionRemaining.put(findBatchRequest.topicPartition, findBatchRequest.maxPartitionFetchBytes);

            final TreeMap<Long, Batch> tpCoordinates = batchCoordinates.get(findBatchRequest.topicPartition);
            if (tpCoordinates == null) {
                continue;
            }

            final Long batchOffset = tpCoordinates.floorKey(findBatchRequest.kafkaOffset);
            if (batchOffset == null) {
                continue;
            }

            currentBatches.put(findBatchRequest.topicPartition, batchOffset);
            topicPartitionsToIterate.addLast(findBatchRequest.topicPartition);
        }

        // TODO this isn't properly tested and should be rewritten in SQL anyway
        while (!topicPartitionsToIterate.isEmpty()) {
            final TopicPartition tp = topicPartitionsToIterate.removeFirst();

            // These two shouldn't be null as we did precautions above.
            final Long batchKey = currentBatches.get(tp);
            final TreeMap<Long, Batch> coordinates = batchCoordinates.get(tp);
            final Batch batch = coordinates.get(batchKey);

            final boolean canAdd =
                (totalSizeRemaining >= batch.byteSize && sizePerPartitionRemaining.get(tp) >= batch.byteSize)
                    || (minOneMessage && result.get(tp).batches.isEmpty());
            if (canAdd) {
                totalSizeRemaining -= batch.byteSize;
                sizePerPartitionRemaining.put(tp, sizePerPartitionRemaining.get(tp) - batch.byteSize);

                final BatchInfo batchInfo = new BatchInfo(
                    new BatchCoordinates(tp, batchKey),
                    batch.filePath,
                    batch.byteOffset,
                    batch.byteSize,
                    batch.numberOfRecords
                );
                result.get(tp).batches.add(batchInfo);

                // Do we have the next batch?
                final Long nextBatchKey = coordinates.higherKey(batchKey);
                if (nextBatchKey != null) {
                    currentBatches.put(tp, nextBatchKey);
                    // If we're here, we want to consider this partition again.
                    topicPartitionsToIterate.addLast(tp);
                }
            }
        }

        return result;
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
