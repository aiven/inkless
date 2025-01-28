// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;

public class InMemoryControlPlane extends AbstractControlPlane {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryControlPlane.class);

    private final AtomicLong fileIdCounter = new AtomicLong(0);
    private final AtomicLong batchIdCounter = new AtomicLong(0);
    private final AtomicLong fileMergeWorkItemIdCounter = new AtomicLong(0);
    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    // LinkedHashMap to preserve the insertion order, to select files for merging in order.
    private final LinkedHashMap<String, FileInfo> files = new LinkedHashMap<>();
    private final Map<String, FileToDeleteInternal> filesToDelete = new HashMap<>();
    // Maps the last offset to the batch id.
    private final HashMap<TopicIdPartition, TreeMap<Long, Long>> batchCoordinates = new HashMap<>();
    private final HashMap<Long, BatchInfoInternal> batches = new HashMap<>();
    // The key is the file ID.
    private final Map<Long, List<Long>> fileToBatch = new HashMap<>();
    // The key is the ID.
    private final Map<Long, FileMergeWorkItem> fileMergeWorkItems = new HashMap<>();

    private InMemoryControlPlaneConfig controlPlaneConfig;

    public InMemoryControlPlane(final Time time) {
        super(time);
    }

    @Override
    public synchronized void configure(final Map<String, ?> configs) {
        controlPlaneConfig = new InMemoryControlPlaneConfig(configs);
    }

    @Override
    public synchronized void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        for (final CreateTopicAndPartitionsRequest request : requests) {
            for (int partition = 0; partition < request.numPartitions(); partition++) {
                final TopicIdPartition topicIdPartition = new TopicIdPartition(
                    request.topicId(), partition, request.topicName());

                LOGGER.info("Creating {}", topicIdPartition);
                logs.putIfAbsent(topicIdPartition, new LogInfo());
                batchCoordinates.putIfAbsent(topicIdPartition, new TreeMap<>());
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
        final FileInfo fileInfo = new FileInfo(fileIdCounter.incrementAndGet(), objectKey, FileReason.PRODUCE, uploaderBrokerId, fileSize);
        files.put(objectKey, fileInfo);
        return requests
            .map(request -> commitFileForExistingPartition(now, fileInfo, request))
            .iterator();
    }

    private CommitBatchResponse commitFileForExistingPartition(final long now,
                                                               final FileInfo fileInfo,
                                                               final CommitBatchRequest request) {
        final TopicIdPartition topicIdPartition = request.topicIdPartition();
        final LogInfo logInfo = logs.get(topicIdPartition);
        final TreeMap<Long, Long> coordinates = batchCoordinates.get(topicIdPartition);
        // This can't really happen as non-existing partitions should be filtered out earlier.
        if (logInfo == null || coordinates == null) {
            LOGGER.warn("Unexpected non-existing partition {}", topicIdPartition);
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        final long firstOffset = logInfo.highWatermark;
        final long lastOffset = firstOffset + request.offsetDelta();
        logInfo.highWatermark = lastOffset + 1;
        final BatchInfo batchInfo = new BatchInfo(
            batchIdCounter.incrementAndGet(),
            fileInfo.objectKey,
            new BatchMetadata(
                topicIdPartition,
                request.byteOffset(),
                request.size(),
                firstOffset,
                lastOffset,
                now,
                request.batchMaxTimestamp(),
                request.messageTimestampType()
            )
        );
        putBatchAndFiles(batchInfo, fileInfo);
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
        final TreeMap<Long, Long> coordinates = batchCoordinates.get(request.topicIdPartition());
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

        List<BatchInfo> found = new ArrayList<>();
        long totalSize = 0;
        for (Long batchOffset : coordinates.navigableKeySet().tailSet(request.offset())) {
            BatchInfo batch = batches.get(coordinates.get(batchOffset)).batchInfo();
            found.add(batch);
            totalSize += batch.metadata().size();
            if (totalSize > fetchMaxBytes) {
                break;
            }
        }
        return FindBatchResponse.success(found, logInfo.logStartOffset, logInfo.highWatermark);
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        return requests.stream()
            .map(this::deleteRecordsForPartition)
            .toList();
    }

    private DeleteRecordsResponse deleteRecordsForPartition(final DeleteRecordsRequest request) {
        final LogInfo logInfo = logs.get(request.topicIdPartition());
        final TreeMap<Long, Long> coordinates = batchCoordinates.get(request.topicIdPartition());
        // This can't really happen as non-existing partitions should be filtered out earlier.
        if (logInfo == null || coordinates == null) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return DeleteRecordsResponse.unknownTopicOrPartition();
        }

        final long convertedOffset = request.offset() == org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK
            ? logInfo.highWatermark
            : request.offset();
        if (convertedOffset < 0 || convertedOffset > logInfo.highWatermark) {
            return DeleteRecordsResponse.offsetOutOfRange();
        }
        if (convertedOffset > logInfo.logStartOffset) {
            logInfo.logStartOffset = convertedOffset;
        }

        // coordinates.firstKey() is last offset in the batch
        while (!coordinates.isEmpty() && coordinates.firstKey() < logInfo.logStartOffset) {
            final Long batchId = coordinates.remove(coordinates.firstKey());
            removeBatchAndMaybeFiles(batchId);
        }
        return (DeleteRecordsResponse.success(logInfo.logStartOffset));
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
            final TreeMap<Long, Long> coordinates = batchCoordinates.remove(topicIdPartition);
            if (coordinates == null) {
                continue;
            }

            for (final var entry : coordinates.entrySet()) {
                final Long batchId = entry.getValue();
                removeBatchAndMaybeFiles(batchId);
            }
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        return filesToDelete.values().stream()
            .map(f -> new FileToDelete(f.fileInfo().objectKey, f.markedForDeletionAt()))
            .toList();
    }

    @Override
    public synchronized void deleteFiles(DeleteFilesRequest request) {
        for (final String objectKey : request.objectKeyPaths()) {
            filesToDelete.remove(objectKey);
            files.remove(objectKey);
        }
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
        return requests
                .map(request -> listOffset(request, logs))
                .iterator();
    }

    private ListOffsetsResponse listOffset(ListOffsetsRequest request, Map<TopicIdPartition, LogInfo> data) {
        LogInfo logInfo = data.get(request.topicIdPartition());

        if (logInfo == null) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return ListOffsetsResponse.unknownTopicOrPartition(request.topicIdPartition());
        }

        long timestamp = request.timestamp();
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.logStartOffset);
        } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.logStartOffset);
        }
        LOGGER.error("listOffset request for timestamp {} in {} unsupported", timestamp, request.topicIdPartition());
        return new ListOffsetsResponse(Errors.UNKNOWN_SERVER_ERROR, request.topicIdPartition(), -1, -1);
    }

    @Override
    public synchronized FileMergeWorkItem getFileMergeWorkItem() {
        final Instant now = TimeUtils.now(time);

        // Before looking into the locked files, clear the merge work items older than the lock period.
        for (final var entry : fileMergeWorkItems.entrySet()) {
            final var id = entry.getKey();
            final var workItem = entry.getValue();
            final Instant expiresAt = workItem.createdAt().plus(controlPlaneConfig.fileMergeLockPeriod());
            if (now.isAfter(expiresAt) || expiresAt.equals(now)) {
                fileMergeWorkItems.remove(id);
            }
        }

        // Find the locked files, i.e. the files that are already a part of some file merge work item.
        final Set<Long> lockedFiles = fileMergeWorkItems.values().stream()
            .flatMap(wi -> wi.files().stream())
            .map(FileMergeWorkItem.File::fileId)
            .collect(Collectors.toSet());

        long totalMergeableSize = 0;
        final List<FileMergeWorkItem.File> mergeableFiles = new ArrayList<>();
        // This iterates in the insertion order.
        for (final var entry : files.entrySet()) {
            if (totalMergeableSize >= controlPlaneConfig.fileMergeSizeThresholdBytes()) {
                break;
            }

            final FileInfo fileInfo = entry.getValue();
            // This file is already in some merge work item -- skip.
            if (lockedFiles.contains(fileInfo.fileId)) {
                continue;
            }

            // This file is already the result of a merging operation -- skip.
            if (fileInfo.fileReason == FileReason.MERGE) {
                continue;
            }

            mergeableFiles.add(new FileMergeWorkItem.File(
                fileInfo.fileId,
                fileInfo.objectKey,
                fileToBatch.get(fileInfo.fileId)
                    .stream()
                    .map(batches::get)
                    .map(BatchInfoInternal::batchInfo)
                    .toList()
            ));
            totalMergeableSize += fileInfo.fileSize;
        }

        // Have we found enough data to merge?
        if (totalMergeableSize < controlPlaneConfig.fileMergeSizeThresholdBytes()) {
            return null;
        } else {
            final long id = fileMergeWorkItemIdCounter.incrementAndGet();
            final FileMergeWorkItem workItem = new FileMergeWorkItem(id, now, mergeableFiles);
            fileMergeWorkItems.put(id, workItem);
            return workItem;
        }
    }

    @Override
    public synchronized void commitFileMergeWorkItem(final long workItemId,
                                                     final String objectKey,
                                                     final int uploaderBrokerId,
                                                     final long fileSize,
                                                     final List<MergedFileBatch> mergedFileBatches) {
        final FileMergeWorkItem workItem = fileMergeWorkItems.get(workItemId);
        if (workItem == null) {
            throw new FileMergeWorkItemNotExist(workItemId);
        }

        final Set<Long> workItemFileIds = workItem.files().stream()
            .map(FileMergeWorkItem.File::fileId)
            .collect(Collectors.toSet());

        // Before we start doing modifications, verify we can finish them without errors.
        for (final MergedFileBatch mergedFileBatch : mergedFileBatches) {
            // We don't support compaction or concatenation yet, so the only correct number of parent batches is 1.
            if (mergedFileBatch.parentBatches().size() != 1) {
                throw new ControlPlaneException(
                    String.format("Invalid parent batch count %d in %s",
                        mergedFileBatch.parentBatches().size(),
                        mergedFileBatch
                    )
                );
            }

            // Check the parent batches: if they exist, they must be part of this work item (through their files).
            final Set<Long> parentBatches = new HashSet<>(mergedFileBatch.parentBatches());
            final TreeMap<Long, Long> coordinates = batchCoordinates.get(mergedFileBatch.metadata().topicIdPartition());
            if (coordinates != null) {
                final var parentBatchesFound = parentBatches.stream()
                    .filter(batches::containsKey)
                    .map(batches::get)
                    .toList();
                for (final var parentBatch : parentBatchesFound) {
                    if (!workItemFileIds.contains(parentBatch.fileInfo.fileId)) {
                        throw new ControlPlaneException(
                            String.format("Batch %d is not part of work item in: %s",
                                parentBatch.batchInfo.batchId(), mergedFileBatch));
                    }
                }
            }
        }

        // Commit after all the checks.
        fileMergeWorkItems.remove(workItemId);

        // Delete the old file and insert the new ones.
        final Set<Long> currentFilesToDelete = this.filesToDelete.values().stream()
            .map(fd -> fd.fileInfo().fileId)
            .collect(Collectors.toSet());
        for (final var oldFile : workItem.files()) {
            // A file may be already deleted.
            if (!currentFilesToDelete.contains(oldFile.fileId())) {
                final FileInfo oldFileInfo = this.files.remove(oldFile.objectKey());
                // It may be also already physically deleted, without any trace in `files` or `filesToDelete`.
                if (oldFileInfo != null) {
                    filesToDelete.put(oldFileInfo.objectKey, new FileToDeleteInternal(oldFileInfo, TimeUtils.now(time)));
                }
            }
        }
        final FileInfo mergedFile = new FileInfo(fileIdCounter.incrementAndGet(), objectKey, FileReason.MERGE, uploaderBrokerId, fileSize);

        // Delete the old batches and insert the new one.
        for (final MergedFileBatch batch : mergedFileBatches) {
            final TreeMap<Long, Long> coordinates = batchCoordinates.get(batch.metadata().topicIdPartition());
            // Probably the partition was deleted -- skip the new batch (exclude it from the file too).
            if (coordinates == null) {
                continue;
            }

            // We now support only a single parent batch now.
            final Set<Long> parentBatches = new HashSet<>(batch.parentBatches());
            final Optional<Map.Entry<Long, Long>> parentBatchFound = coordinates.entrySet().stream()
                .filter(kv -> parentBatches.contains(kv.getValue()))
                .findFirst();
            // Probably the parent batch was deleted -- skip the new batch (exclude it from the file too).
            if (parentBatchFound.isEmpty()) {
                continue;
            }

            for (Long oldBatchId: parentBatches) {
                final BatchInfoInternal oldBatchInfoInternal = removeBatchAndMaybeFiles(oldBatchId);
                final BatchInfo batchInfo = oldBatchInfoInternal.batchInfo;
                batchCoordinates.get(batchInfo.metadata().topicIdPartition()).remove(batchInfo.metadata().lastOffset());
            }

            final long batchId = batchIdCounter.incrementAndGet();
            final BatchInfo batchInfo = new BatchInfo(batchId, objectKey, batch.metadata());
            putBatchAndFiles(batchInfo, mergedFile);
        }

        final List<Long> batchesOnFile = fileToBatch.getOrDefault(mergedFile.fileId, List.of());
        if (batchesOnFile.isEmpty()) {
            // It may happen that the new file is absolutely empty after taking into account all the deleted batches.
            // In this case, delete it as well.
            filesToDelete.put(mergedFile.objectKey, new FileToDeleteInternal(mergedFile, TimeUtils.now(time)));
        } else {
            this.files.put(objectKey, mergedFile);
        }
    }

    @Override
    public synchronized void releaseFileMergeWorkItem(final long workItemId) {
        final FileMergeWorkItem workItem = fileMergeWorkItems.remove(workItemId);
        if (workItem == null) {
            throw new FileMergeWorkItemNotExist(workItemId);
        }
    }

    @Override
    public void close() throws IOException {
        // Do nothing.
    }

    private void putBatchAndFiles(final BatchInfo batchInfo, final FileInfo fileInfo) {
        batchCoordinates.get(batchInfo.metadata().topicIdPartition()).put(batchInfo.metadata().lastOffset(), batchInfo.batchId());
        batches.put(batchInfo.batchId(), new BatchInfoInternal(batchInfo, fileInfo));
        fileToBatch.computeIfAbsent(fileInfo.fileId, k -> new ArrayList<>()).add(batchInfo.batchId());
    }

    private BatchInfoInternal removeBatchAndMaybeFiles(Long batchId) {
        final BatchInfoInternal batchInfoInternal = batches.remove(batchId);
        final FileInfo fileInfo = batchInfoInternal.fileInfo;
        final List<Long> batches = fileToBatch.get(fileInfo.fileId);
        batches.remove(batchId);
        if (batches.isEmpty()) {
            files.remove(fileInfo.objectKey);
            filesToDelete.put(fileInfo.objectKey, new FileToDeleteInternal(fileInfo, TimeUtils.now(time)));
            fileToBatch.remove(fileInfo.fileId);
        }
        return batchInfoInternal;
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
    }

    private static class FileInfo {
        final long fileId;
        final String objectKey;
        final FileReason fileReason;
        final int uploaderBrokerId;
        final long fileSize;

        private FileInfo(final long fileId,
                         final String objectKey,
                         final FileReason fileReason,
                         final int uploaderBrokerId,
                         final long fileSize) {
            this.fileId = fileId;
            this.objectKey = objectKey;
            this.fileReason = fileReason;
            this.uploaderBrokerId = uploaderBrokerId;
            this.fileSize = fileSize;
        }
    }

    private record FileToDeleteInternal(FileInfo fileInfo,
                                        Instant markedForDeletionAt) {
    }

    private record BatchInfoInternal(BatchInfo batchInfo,
                                     FileInfo fileInfo) {
    }
}
