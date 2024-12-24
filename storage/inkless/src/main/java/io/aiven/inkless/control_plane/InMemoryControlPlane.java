// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;

public class InMemoryControlPlane extends AbstractControlPlane {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryControlPlane.class);

    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    private final Map<String, FileInfo> files = new HashMap<>();
    private final List<FileToDeleteInternal> filesToDelete = new ArrayList<>();
    private final HashMap<TopicIdPartition, TreeMap<Long, BatchInfoInternal>> batches = new HashMap<>();
    private final HashMap<TopicIdPartition, TreeMap<Long, LatestProducerState>> producers = new HashMap<>();

    public InMemoryControlPlane(final Time time) {
        super(time);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // Do nothing.
        super.configure(configs);
    }

    @Override
    public synchronized void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        for (final CreateTopicAndPartitionsRequest request : requests) {
            for (int partition = 0; partition < request.numPartitions(); partition++) {
                final TopicIdPartition topicIdPartition = new TopicIdPartition(
                    request.topicId(), partition, request.topicName());

                LOGGER.info("Creating {}", topicIdPartition);
                logs.putIfAbsent(topicIdPartition, new LogInfo());
                batches.putIfAbsent(topicIdPartition, new TreeMap<>());
            }
        }
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests
    ) {
        final long now = time.milliseconds();
        final FileInfo fileInfo = new FileInfo(objectKey, uploaderBrokerId, fileSize);
        files.put(objectKey, fileInfo);
        return requests
            .map(request -> commitFileForValidRequest(now, fileInfo, request))
            .iterator();
    }

    private CommitBatchResponse commitFileForValidRequest(final long now,
                                                          final FileInfo fileInfo,
                                                          final CommitBatchRequest request) {
        final TopicIdPartition topicIdPartition = request.topicIdPartition();
        final LogInfo logInfo = logs.get(topicIdPartition);
        final TreeMap<Long, BatchInfoInternal> coordinates = this.batches.get(topicIdPartition);
        // This can't really happen as non-existing partitions should be filtered out earlier.
        if (logInfo == null || coordinates == null) {
            LOGGER.warn("Unexpected non-existing partition {}", topicIdPartition);
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        // Update the producer state
        if (request.hasProducerId()) {
            final LatestProducerState latestProducerState = producers
                .computeIfAbsent(topicIdPartition, k -> new TreeMap<>())
                .computeIfAbsent(request.producerId(), k -> LatestProducerState.empty(request));

            if (latestProducerState.epoch > request.producerEpoch()) {
                LOGGER.warn("Producer request with epoch {} is less than the latest epoch {}. Rejecting request",
                    request.producerEpoch(), latestProducerState.epoch);
                return CommitBatchResponse.invalidProducerEpoch();
            }

            if (latestProducerState.lastEntries.isEmpty()) {
                if (request.baseSequence() != 0) {
                    LOGGER.warn("Producer request with base sequence {} is not 0. Rejecting request", request.baseSequence());
                    return CommitBatchResponse.sequenceOutOfOrder(request);
                }
            } else {
                final int lastSeq = latestProducerState.lastEntries.getLast().lastSeq;
                if (request.baseSequence() - 1 != lastSeq || (lastSeq == Integer.MAX_VALUE && request.baseSequence() != 0)) {
                    LOGGER.warn("Producer request with base sequence {} is not the next sequence after the last sequence {}. Rejecting request",
                        request.baseSequence(), lastSeq);
                    return CommitBatchResponse.sequenceOutOfOrder(request);
                }
            }

            final LatestProducerState current;
            if (latestProducerState.epoch < request.producerEpoch()) {
                current = LatestProducerState.empty(request);
            } else {
                current = latestProducerState;
            }
            current.addElement(request.batchMetadata());

            producers.get(topicIdPartition).put(request.producerId(), current);
        }

        final long firstOffset = logInfo.highWatermark;
        final long lastOffset = firstOffset + request.offsetDelta();
        logInfo.highWatermark = lastOffset + 1;
        final BatchInfo batchInfo = new BatchInfo(
            fileInfo.objectKey,
            request.byteOffset(),
            request.size(),
            firstOffset,
            request.baseOffset(),
            request.lastOffset(),
            now,
            request.batchMaxTimestamp(),
            request.messageTimestampType(),
            request.producerId(),
            request.producerEpoch(),
            request.baseSequence(),
            request.lastSequence()
        );
        coordinates.put(lastOffset, new BatchInfoInternal(batchInfo, fileInfo));

        return CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset, request);
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final boolean minOneMessage,
        final int fetchMaxBytes
    ) {
        return requests
            .map(request -> findBatchesForExistingPartition(request, minOneMessage, fetchMaxBytes))
            .iterator();
    }

    private FindBatchResponse findBatchesForExistingPartition(final FindBatchRequest request,
                                                              final boolean minOneMessage,
                                                              final int fetchMaxBytes) {
        final LogInfo logInfo = logs.get(request.topicIdPartition());
        final TreeMap<Long, BatchInfoInternal> coordinates = batches.get(request.topicIdPartition());
        // This can't really happen as non-existing partitions should be filtered out earlier.
        if (logInfo == null || coordinates == null) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < 0) {
            LOGGER.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
            return FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset, logInfo.highWatermark);
        }

        // if offset requests is > end offset return out-of-range exception, otherwise return empty batch.
        // Similar to {@link LocalLog#read() L490}
        if (request.offset() > logInfo.highWatermark) {
            return FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset, logInfo.highWatermark);
        }

        List<BatchInfo> batches = new ArrayList<>();
        long totalSize = 0;
        for (Long batchOffset : coordinates.navigableKeySet().tailSet(request.offset())) {
            BatchInfo batch = coordinates.get(batchOffset).batchInfo();
            batches.add(batch);
            totalSize += batch.size();
            if (totalSize > fetchMaxBytes) {
                break;
            }
        }
        return FindBatchResponse.success(batches, logInfo.logStartOffset, logInfo.highWatermark);
    }

    @Override
    public synchronized void deleteTopics(final Set<Uuid> topicIds) {
        // There may be some non-Inkless topics there, but they should be no-op.

        final List<TopicIdPartition> partitionsToDelete = logs.keySet().stream()
            .filter(tidp -> topicIds.contains(tidp.topicId()))
            .toList();
        for (final TopicIdPartition topicIdPartition : partitionsToDelete) {
            LOGGER.info("Deleting {}", topicIdPartition);
            logs.remove(topicIdPartition);
            final TreeMap<Long, BatchInfoInternal> coordinates = batches.remove(topicIdPartition);
            if (coordinates == null) {
                continue;
            }

            for (final var entry : coordinates.entrySet()) {
                final BatchInfoInternal batchInfoInternal = entry.getValue();
                final FileInfo fileInfo = batchInfoInternal.fileInfo;
                fileInfo.deleteBatch(batchInfoInternal.batchInfo);
                if (fileInfo.allDeleted()) {
                    files.remove(fileInfo.objectKey);
                    filesToDelete.add(new FileToDeleteInternal(fileInfo, TimeUtils.now(time)));
                }
            }
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        return filesToDelete.stream()
            .map(f -> new FileToDelete(f.fileInfo().objectKey, f.markedForDeletionAt()))
            .toList();
    }

    @Override
    public List<FindProducerStateResponse> findProducerStates(final List<FindProducerStateRequest> findProducerStateRequests) {
        return findProducerStateRequests.stream()
            .map(this::findProducerState)
            .toList();
    }

    private FindProducerStateResponse findProducerState(final FindProducerStateRequest request) {
        ProducerStateEntry entry = ProducerStateEntry.empty(request.producerId());
        entry.maybeUpdateProducerEpoch(request.producerEpoch());

        final TreeMap<Long, LatestProducerState> producerIdsState = producers.get(request.topicIdPartition());
        if (producerIdsState == null) {
            return FindProducerStateResponse.empty(request);
        }

        final LatestProducerState latestProducerState = producerIdsState.get(request.producerId());
        if (latestProducerState == null) {
            return FindProducerStateResponse.empty(request);
        }

        final Instant minTimestamp = producerStateCache.minimumTimestamp();
        final boolean isNewEpoch = latestProducerState.epoch != request.producerEpoch();
        final boolean isOldProducer = latestProducerState.timestamp < minTimestamp.toEpochMilli();
        if (isNewEpoch || isOldProducer) {
            // Return new state to let commit handle the update
            return FindProducerStateResponse.empty(request);
        }

        // Return existing state when epoch matches and time is valid
        return FindProducerStateResponse.of(request, latestProducerState.lastEntries());
    }

    @Override
    public long logStartOffset(final TopicIdPartition topicIdPartition) {
        return logs.get(topicIdPartition).logStartOffset;
    }

    @Override
    public void close() throws IOException {
        // Do nothing.
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
    }

    private static class FileInfo {
        final String objectKey;
        final int uploaderBrokerId;
        final long fileSize;
        long usedSize;

        private FileInfo(final String objectKey,
                         final int uploaderBrokerId,
                         final long fileSize) {
            this.objectKey = objectKey;
            this.uploaderBrokerId = uploaderBrokerId;
            this.fileSize = fileSize;
            this.usedSize = fileSize;
        }

        private void deleteBatch(final BatchInfo batchInfo) {
            final long newUsedSize = usedSize - batchInfo.size();
            if (newUsedSize < 0) {
                throw new IllegalStateException("newUsedSize < 0: " + newUsedSize);
            }
            this.usedSize = newUsedSize;
        }

        private boolean allDeleted() {
            return this.usedSize == 0;
        }
    }

    private record FileToDeleteInternal(FileInfo fileInfo,
                                        Instant markedForDeletionAt) {
    }

    private record BatchInfoInternal(BatchInfo batchInfo,
                                     FileInfo fileInfo) {
    }
    
    private record LatestProducerState(short epoch, long timestamp, LinkedList<BatchMetadata> lastEntries) {
        static LatestProducerState empty(CommitBatchRequest request) {
            return new LatestProducerState(request.producerEpoch(), request.batchMaxTimestamp(), new LinkedList<>());
        }

        public void addElement(BatchMetadata element) {
            // Keep the last 5 entries
            while (lastEntries.size() >= 5) {
                lastEntries.removeFirst();
            }
            lastEntries.addLast(element);
        }
    }
}
