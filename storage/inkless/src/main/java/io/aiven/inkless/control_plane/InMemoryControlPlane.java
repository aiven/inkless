// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

    public InMemoryControlPlane(final Time time,
                                final MetadataView metadataView) {
        super(time, metadataView);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // Do nothing.
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
    protected Iterator<CommitBatchResponse> commitFileForExistingPartitions(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests
    ) {
        final long now = time.milliseconds();
        final FileInfo fileInfo = new FileInfo(objectKey, uploaderBrokerId, fileSize);
        files.put(objectKey, fileInfo);
        return requests
            .map(request -> commitFileForExistingPartition(now, fileInfo, request))
            .iterator();
    }

    private CommitBatchResponse commitFileForExistingPartition(final long now,
                                                               final FileInfo fileInfo,
                                                               final CommitBatchRequest request) {
        final String topicName = request.topicPartition().topic();
        final Uuid topicId = metadataView.getTopicId(topicName);

        final TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, request.topicPartition());
        final LogInfo logInfo = logs.get(topicIdPartition);
        final TreeMap<Long, BatchInfoInternal> coordinates = this.batches.get(topicIdPartition);
        // This can't really happen as non-existing partitions should be filtered out earlier.
        if (logInfo == null || coordinates == null) {
            LOGGER.warn("Unexpected non-existing partition {}", topicIdPartition);
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        final long firstOffset = logInfo.highWatermark;
        logInfo.highWatermark += request.numberOfRecords();
        final long lastOffset = logInfo.highWatermark - 1;
        final BatchInfo batchInfo = new BatchInfo(
            fileInfo.objectKey,
            request.byteOffset(),
            request.size(),
            firstOffset,
            request.numberOfRecords(),
            request.messageTimestampType(),
            now,
            request.batchMaxTimestamp()
        );
        coordinates.put(lastOffset, new BatchInfoInternal(batchInfo, fileInfo));
        return CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset);
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
}
