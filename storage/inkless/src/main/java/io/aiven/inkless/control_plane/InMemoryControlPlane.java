/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.apache.kafka.storage.internals.log.ProducerStateEntry.NUM_BATCHES_TO_RETAIN;

// TODO: in-memory control plane is using synchronous operations. It could be improved by using finer-grained locks if needed later.
public class InMemoryControlPlane extends AbstractControlPlane {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryControlPlane.class);

    private final AtomicLong fileIdCounter = new AtomicLong(0);
    private final AtomicLong batchIdCounter = new AtomicLong(0);
    private final AtomicLong fileMergeWorkItemIdCounter = new AtomicLong(0);
    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    // LinkedHashMap to preserve the insertion order, to select files for merging in order.
    // The key is the object key.
    private final LinkedHashMap<String, FileInfo> files = new LinkedHashMap<>();
    // The key is the partition. The inner map key is the last offset in the batch.
    private final HashMap<TopicIdPartition, TreeMap<Long, BatchInfoInternal>> batches = new HashMap<>();
    // The key is the ID.
    private final Map<Long, FileMergeWorkItem> fileMergeWorkItems = new HashMap<>();
    private final HashMap<TopicIdPartition, TreeMap<Long, LatestProducerState>> producers = new HashMap<>();

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
                batches.putIfAbsent(topicIdPartition, new TreeMap<>());
            }
        }
    }

    @Override
    public synchronized void initLogDisklessStartOffset(final Set<InitLogDisklessStartOffsetRequest> requests) {
        for (final InitLogDisklessStartOffsetRequest request : requests) {
            final TopicIdPartition topicIdPartition = new TopicIdPartition(
                request.topicId(), request.partition(), request.topicName());

            // Only create if not already exists
            if (!logs.containsKey(topicIdPartition)) {
                LOGGER.info("Initializing {} with logStartOffset {}, disklessStartOffset {}",
                    topicIdPartition, request.logStartOffset(), request.disklessStartOffset());
                final LogInfo logInfo = new LogInfo();
                logInfo.logStartOffset = request.logStartOffset();
                logInfo.highWatermark = request.disklessStartOffset();
                logInfo.disklessStartOffset = request.disklessStartOffset();
                logs.put(topicIdPartition, logInfo);
                batches.putIfAbsent(topicIdPartition, new TreeMap<>());
            }
        }
    }

    @Override
    protected synchronized Iterator<CommitBatchResponse> commitFileForValidRequests(
            final String objectKey,
            final ObjectFormat format,
            final int uploaderBrokerId,
            final long fileSize,
            final Stream<CommitBatchRequest> requests
    ) {
        if (files.containsKey(objectKey)) {
            throw new ControlPlaneException("Error committing file");
        }

        try {
            final long now = time.milliseconds();
            final FileInfo fileInfo = FileInfo.createUploaded(fileIdCounter.incrementAndGet(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, FileReason.PRODUCE, uploaderBrokerId, fileSize);
            final List<CommitBatchResponse> responses = requests.map(request -> commitFileForValidRequest(now, fileInfo, request)).toList();

            files.put(objectKey, fileInfo);
            if (fileInfo.allBatchesDeleted()) {
                fileInfo.markDeleted(TimeUtils.now(time));
            }
            return responses.iterator();
        } catch (final RuntimeException e) {
            throw new ControlPlaneException("Error when committing requests", e);
        }
    }

    private CommitBatchResponse commitFileForValidRequest(
        final long now,
        final FileInfo fileInfo,
        final CommitBatchRequest request
    ) {
        final TopicIdPartition topicIdPartition = request.topicIdPartition();
        final LogInfo logInfo = logs.get(topicIdPartition);
        final TreeMap<Long, BatchInfoInternal> coordinates = this.batches.get(topicIdPartition);
        // This can't really happen as non-existing partitions should be filtered out earlier.
        if (logInfo == null || coordinates == null) {
            LOGGER.warn("Unexpected non-existing partition {}", topicIdPartition);
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        final long firstOffset = logInfo.highWatermark;

        // Update the producer state
        if (request.hasProducerId()) {
            final LatestProducerState latestProducerState = producers
                .computeIfAbsent(topicIdPartition, k -> new TreeMap<>())
                .computeIfAbsent(request.producerId(), k -> LatestProducerState.empty(request.producerEpoch()));

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
                final Optional<ProducerStateItem> first = latestProducerState.lastEntries.stream()
                    .filter(e -> e.baseSequence() == request.baseSequence() && e.lastSequence() == request.lastSequence())
                    .findFirst();
                if (first.isPresent()) {
                    LOGGER.warn("Producer request with base sequence {} and last sequence {} is a duplicate. Rejecting request",
                        request.baseSequence(), request.lastSequence());
                    final ProducerStateItem batchMetadata = first.get();
                    return CommitBatchResponse.ofDuplicate(batchMetadata.assignedOffset(), batchMetadata.batchMaxTimestamp(), logInfo.logStartOffset);
                }

                final int lastSeq = latestProducerState.lastEntries.getLast().lastSequence();
                if (request.baseSequence() - 1 != lastSeq || (lastSeq == Integer.MAX_VALUE && request.baseSequence() != 0)) {
                    LOGGER.warn("Producer request with base sequence {} is not the next sequence after the last sequence {}. Rejecting request",
                        request.baseSequence(), lastSeq);
                    return CommitBatchResponse.sequenceOutOfOrder(request);
                }
            }

            final LatestProducerState current;
            if (latestProducerState.epoch < request.producerEpoch()) {
                current = LatestProducerState.empty(request.producerEpoch());
            } else {
                current = latestProducerState;
            }
            current.addElement(request.baseSequence(), request.lastSequence(), firstOffset, request.batchMaxTimestamp());

            producers.get(topicIdPartition).put(request.producerId(), current);
        }

        final long lastOffset = firstOffset + request.offsetDelta();
        logInfo.highWatermark = lastOffset + 1;
        logInfo.byteSize += request.size();
        final BatchInfo batchInfo = new BatchInfo(
            batchIdCounter.incrementAndGet(),
            fileInfo.objectKey,
                new BatchMetadata(
                request.magic(),
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
        coordinates.put(lastOffset, new BatchInfoInternal(batchInfo, fileInfo));

        fileInfo.addBatch(batchInfo);

        return CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset, fileInfo.objectKey, request);
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes,
        // ignored for in-memory implementation
        final int maxBatchesPerPartition
    ) {
        return requests
            .map(request -> findBatchesForExistingPartition(request, fetchMaxBytes))
            .iterator();
    }

    private synchronized FindBatchResponse findBatchesForExistingPartition(
        final FindBatchRequest request,
        final int fetchMaxBytes
    ) {
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
            totalSize += batch.metadata().byteSize();
            if (totalSize > fetchMaxBytes) {
                break;
            }
        }
        return FindBatchResponse.success(batches, logInfo.logStartOffset, logInfo.highWatermark);
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        return requests.stream()
            .map(this::deleteRecordsForPartition)
            .toList();
    }

    private DeleteRecordsResponse deleteRecordsForPartition(final DeleteRecordsRequest request) {
        final LogInfo logInfo = logs.get(request.topicIdPartition());
        final TreeMap<Long, BatchInfoInternal> coordinates = this.batches.get(request.topicIdPartition());
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
            final BatchInfoInternal batchInfoInternal = coordinates.remove(coordinates.firstKey());
            batchInfoInternal.fileInfo().deleteBatch(batchInfoInternal.batchInfo, TimeUtils.now(time));
            logInfo.byteSize -= batchInfoInternal.batchInfo().metadata().byteSize();
            assert logInfo.byteSize >= 0;
        }
        return (DeleteRecordsResponse.success(logInfo.logStartOffset));
    }

    @Override
    public synchronized void deleteTopics(final Set<Uuid> topicIds) {
        // There may be some non-diskless topics there, but they should be no-op.

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
                batchInfoInternal.fileInfo().deleteBatch(batchInfoInternal.batchInfo, TimeUtils.now(time));
            }
        }
    }

    @Override
    public synchronized List<EnforceRetentionResponse> enforceRetention(final List<EnforceRetentionRequest> requests, final int maxBatchesPerRequest) {
        final Instant now = TimeUtils.now(time);

        final List<EnforceRetentionResponse> responses = new ArrayList<>();
        for (final EnforceRetentionRequest request : requests) {
            final TopicIdPartition tidp = findTopicIdPartition(request.topicId(), request.partition());
            final LogInfo logInfo;
            // The key is the last offset in the batch.
            final TreeMap<Long, BatchInfoInternal> coordinates;
            if (tidp == null
                || (logInfo = logs.get(tidp)) == null
                || (coordinates = batches.get(tidp)) == null
            ) {
                responses.add(EnforceRetentionResponse.unknownTopicOrPartition());
                continue;
            }

            final Set<Long> toDelete = new HashSet<>();
            int batchesDeleted = 0;
            long bytesDeleted = 0;

            // Enforce the size retention.
            if (request.retentionBytes() >= 0
                // Does it even make sense to start iterating?
                && logInfo.byteSize > request.retentionBytes()) {
                long accumulatedSize = 0;
                // Note the reverse order.
                for (final BatchInfoInternal batch : coordinates.descendingMap().values()) {
                    accumulatedSize += batch.batchInfo().metadata().byteSize();
                    if (accumulatedSize > request.retentionBytes()) {
                        toDelete.add(batch.batchInfo().metadata().lastOffset());
                    }
                }
            }

            // Enforce the time retention.
            if (request.retentionMs() >= 0) {
                final long lastRetainedTimestamp = now.toEpochMilli() - request.retentionMs();
                // Select batches for deletion only while we see them breaching the retention threshold, but no further.
                coordinates.values().stream()
                    .takeWhile(b -> b.batchInfo().metadata().timestamp() < lastRetainedTimestamp)
                    .map(b -> b.batchInfo().metadata().lastOffset())
                    .forEach(toDelete::add);
            }

            List<Long> sortedKeysToDelete = new ArrayList<>(toDelete);
            // We must delete in the offset order.
            Collections.sort(sortedKeysToDelete);

            for (final long key : sortedKeysToDelete) {
                // Enforce max batches per request
                if (maxBatchesPerRequest > 0 && batchesDeleted >= maxBatchesPerRequest) {
                    break;
                }
                final BatchInfoInternal removed = coordinates.remove(key);
                removed.fileInfo().deleteBatch(removed.batchInfo(), now);
                batchesDeleted += 1;
                bytesDeleted += removed.batchInfo().metadata().byteSize();
            }

            logInfo.byteSize -= bytesDeleted;
            if (coordinates.isEmpty()) {
                logInfo.logStartOffset = logInfo.highWatermark;
                if (logInfo.byteSize != 0) {
                    throw new RuntimeException(String.format("Log size expected to be 0, but it's %d", logInfo.byteSize));
                }
            } else {
                logInfo.logStartOffset = coordinates.firstEntry().getValue().batchInfo().metadata().baseOffset();
            }
            responses.add(EnforceRetentionResponse.success(batchesDeleted, bytesDeleted, logInfo.logStartOffset));
        }
        return responses;
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        return files.values().stream()
            .filter(f -> f.fileState == FileState.DELETING)
            .map(f -> new FileToDelete(f.objectKey, f.markedForDeletionAt))
            .toList();
    }

    @Override
    public synchronized void deleteFiles(DeleteFilesRequest request) {
        for (final String objectKey : request.objectKeyPaths()) {
            files.remove(objectKey);
        }
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
        return requests
                .map(request -> listOffset(request))
                .iterator();
    }

    private ListOffsetsResponse listOffset(ListOffsetsRequest request) {
        final LogInfo logInfo = logs.get(request.topicIdPartition());

        if (logInfo == null) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return ListOffsetsResponse.unknownTopicOrPartition(request.topicIdPartition());
        }

        final long timestamp = request.timestamp();
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP || timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, logInfo.logStartOffset);
        } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, logInfo.highWatermark);
        } else if (timestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
            long maxTimestamp = NO_TIMESTAMP;
            long maxTimestampOffset = -1;
            for (final var entry : batches.get(request.topicIdPartition()).entrySet()) {
                final BatchInfo batchInfo = entry.getValue().batchInfo();
                final long batchTimestamp = batchInfo.metadata().timestamp();
                if (batchTimestamp > maxTimestamp) {
                    maxTimestamp = batchTimestamp;
                    maxTimestampOffset = entry.getKey();
                }
            }
            return ListOffsetsResponse.success(request.topicIdPartition(), maxTimestamp, maxTimestampOffset);
        } else if (timestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, -1);
        } else if (timestamp >= 0) {
            for (final var entry : batches.get(request.topicIdPartition()).entrySet()) {
                final BatchMetadata batchMetadata = entry.getValue().batchInfo().metadata();
                final long batchTimestamp = batchMetadata.timestamp();
                if (batchTimestamp >= timestamp) {
                    return ListOffsetsResponse.success(request.topicIdPartition(), batchTimestamp,
                        Math.max(logInfo.logStartOffset, batchMetadata.baseOffset()));
                }
            }
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, -1);
        } else {
            LOGGER.error("listOffset request for timestamp {} in {} unsupported", timestamp, request.topicIdPartition());
            return ListOffsetsResponse.unknownServerError(request.topicIdPartition());
        }
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
            // This file is deleted.
            if (fileInfo.fileState == FileState.DELETING) {
                continue;
            }
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
                fileInfo.format,
                fileInfo.fileSize,
                batchesFromFileToMerge(fileInfo)
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

    private List<BatchInfo> batchesFromFileToMerge(final FileInfo fileInfo) {
        final List<BatchInfo> result = new ArrayList<>();

        for (final var coordinatesEntry : this.batches.entrySet()) {
            for (final var batchInfoInternal : coordinatesEntry.getValue().values()) {
                if (batchInfoInternal.fileInfo == fileInfo) {
                    final BatchInfo batchInfo = batchInfoInternal.batchInfo;
                    result.add(batchInfo);
                }
            }
        }

        return result;
    }

    @Override
    public synchronized void commitFileMergeWorkItem(final long workItemId,
                                                     final String objectKey,
                                                     final ObjectFormat format,
                                                     final int uploaderBrokerId,
                                                     final long fileSize,
                                                     final List<MergedFileBatch> batches) {
        final FileMergeWorkItem workItem = fileMergeWorkItems.get(workItemId);
        if (workItem == null) {
            // Do not delete the file here, it may be a retry of a successful commit.
            // Only delete the file if a failure condition is found.

            throw new FileMergeWorkItemNotExist(workItemId);
        }

        final Set<Long> workItemFileIds = workItem.files().stream()
            .map(FileMergeWorkItem.File::fileId)
            .collect(Collectors.toSet());

        // Before we start doing modifications, verify we can finish them without errors.
        for (final MergedFileBatch mergedFileBatch : batches) {
            // We don't support compaction or concatenation yet, so the only correct number of parent batches is 1.
            if (mergedFileBatch.parentBatches().size() != 1) {
                createEmptyDeletingFile(objectKey, uploaderBrokerId);

                throw new ControlPlaneException(
                    String.format("Invalid parent batch count %d in %s",
                        mergedFileBatch.parentBatches().size(),
                        mergedFileBatch
                    )
                );
            }

            // Check the parent batches: if they exist, they must be part of this work item (through their files).
            final Set<Long> parentBatches = new HashSet<>(mergedFileBatch.parentBatches());
            final TreeMap<Long, BatchInfoInternal> coordinates = this.batches.get(mergedFileBatch.metadata().topicIdPartition());
            if (coordinates != null) {
                final var parentBatchesFound = coordinates.values().stream()
                    .filter(b -> parentBatches.contains(b.batchInfo.batchId()))
                    .toList();
                for (final var parentBatch : parentBatchesFound) {
                    if (!workItemFileIds.contains(parentBatch.fileInfo.fileId)) {
                        createEmptyDeletingFile(objectKey, uploaderBrokerId);

                        throw new ControlPlaneException(
                            String.format("Batch %d is not part of work item in %s",
                                parentBatch.batchInfo.batchId(), mergedFileBatch));
                    }
                }
            }
        }

        // Commit after all the checks.
        fileMergeWorkItems.remove(workItemId);

        // Delete the old files and insert the new one.
        for (final var oldFile : workItem.files()) {
            final FileInfo oldFileInfo = this.files.get(oldFile.objectKey());
            if (oldFileInfo != null && oldFileInfo.fileState == FileState.UPLOADED) {
                oldFileInfo.markDeleted(TimeUtils.now(time));
            }
        }
        final FileInfo mergedFile = FileInfo.createUploaded(fileIdCounter.incrementAndGet(), objectKey, format, FileReason.MERGE, uploaderBrokerId, fileSize);
        this.files.put(objectKey, mergedFile);

        // Delete the old batches and insert the new one.
        for (final MergedFileBatch batch : batches) {
            final TreeMap<Long, BatchInfoInternal> coordinates = this.batches.get(batch.metadata().topicIdPartition());
            // Probably the partition was deleted -- skip the new batch (exclude it from the file too).
            if (coordinates == null) {
                continue;
            }

            // We now support only a single parent batch now.
            final Set<Long> parentBatches = new HashSet<>(batch.parentBatches());
            final Optional<Map.Entry<Long, BatchInfoInternal>> parentBatchFound = coordinates.entrySet().stream()
                .filter(kv -> parentBatches.contains(kv.getValue().batchInfo.batchId()))
                .findFirst();
            // Probably the parent batch was deleted -- skip the new batch (exclude it from the file too).
            if (parentBatchFound.isEmpty()) {
                continue;
            }
            coordinates.remove(parentBatchFound.get().getKey());

            final BatchInfo batchInfo = new BatchInfo(batchIdCounter.incrementAndGet(), objectKey, batch.metadata());
            coordinates.put(batch.metadata().lastOffset(), new BatchInfoInternal(batchInfo, mergedFile));
            mergedFile.addBatch(batchInfo);
        }

        // It may happen that the new file is absolutely empty after taking into account all the deleted batches.
        // In this case, delete it as well.
        if (mergedFile.allBatchesDeleted()) {
            mergedFile.markDeleted(TimeUtils.now(time));
        }
    }

    private void createEmptyDeletingFile(final String objectKey, final int uploaderBrokerId) {
        final FileInfo fileInfo = FileInfo.createDeleting(fileIdCounter.incrementAndGet(), objectKey, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, FileReason.MERGE, uploaderBrokerId, 0, TimeUtils.now(time));
        this.files.put(objectKey, fileInfo);
    }

    @Override
    public synchronized void releaseFileMergeWorkItem(final long workItemId) {
        final FileMergeWorkItem workItem = fileMergeWorkItems.remove(workItemId);
        if (workItem == null) {
            throw new FileMergeWorkItemNotExist(workItemId);
        }
    }

    @Override
    public boolean isSafeToDeleteFile(String objectKeyPath) {
        return !files.containsKey(objectKeyPath);
    }

    @Override
    public synchronized List<GetDisklessLogResponse> getDisklessLog(final List<GetDisklessLogRequest> requests) {
        final List<GetDisklessLogResponse> result = new ArrayList<>();
        for (final GetDisklessLogRequest request : requests) {
            final TopicIdPartition tidp = findTopicIdPartition(request.topicId(), request.partition());
            final LogInfo logInfo;
            if (tidp == null || (logInfo = logs.get(tidp)) == null) {
                result.add(GetDisklessLogResponse.unknownTopicOrPartition(request.topicId(), request.partition()));
            } else {
                result.add(GetDisklessLogResponse.success(
                    request.topicId(),
                    request.partition(),
                    logInfo.logStartOffset,
                    logInfo.highWatermark,
                    logInfo.disklessStartOffset
                ));
            }
        }
        return result;
    }

    @Override
    public synchronized List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        final List<GetLogInfoResponse> result = new ArrayList<>();
        for (final GetLogInfoRequest request : requests) {
            final TopicIdPartition tidp = findTopicIdPartition(request.topicId(), request.partition());
            final LogInfo logInfo;
            if (tidp == null || (logInfo = logs.get(tidp)) == null) {
                result.add(GetLogInfoResponse.unknownTopicOrPartition());
            } else {
                result.add(GetLogInfoResponse.success(
                    logInfo.logStartOffset,
                    logInfo.highWatermark,
                    logInfo.byteSize
                ));
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        // Do nothing.
    }

    private TopicIdPartition findTopicIdPartition(final Uuid topicId, final int partition) {
        return logs.keySet()
            .stream().filter(tidp -> topicId.equals(tidp.topicId()) && partition == tidp.partition())
            .findFirst()
            .orElse(null);
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
        long byteSize = 0;
        Long disklessStartOffset = null;
    }

    private static class FileInfo {
        final long fileId;
        final String objectKey;
        final ObjectFormat format;
        final FileReason fileReason;
        FileState fileState;
        final int uploaderBrokerId;
        Instant markedForDeletionAt;
        final long fileSize;
        final List<BatchInfo> batches = new ArrayList<>();

        private FileInfo(final long fileId,
                         final String objectKey,
                         final ObjectFormat format,
                         final FileReason fileReason,
                         final FileState fileState,
                         final int uploaderBrokerId,
                         final Instant markedForDeletionAt,
                         final long fileSize) {
            this.fileId = fileId;
            this.objectKey = objectKey;
            this.format = format;
            this.fileReason = fileReason;
            this.fileState = fileState;
            this.uploaderBrokerId = uploaderBrokerId;
            this.markedForDeletionAt = markedForDeletionAt;
            this.fileSize = fileSize;
        }

        private static FileInfo createUploaded(final long fileId,
                                               final String objectKey,
                                               final ObjectFormat format,
                                               final FileReason fileReason,
                                               final int uploaderBrokerId,
                                               final long fileSize) {
            return new FileInfo(fileId, objectKey, format, fileReason, FileState.UPLOADED, uploaderBrokerId, null, fileSize);
        }

        private static FileInfo createDeleting(final long fileId,
                                               final String objectKey,
                                               final ObjectFormat format,
                                               final FileReason fileReason,
                                               final int uploaderBrokerId,
                                               final long fileSize,
                                               final Instant now) {
            return new FileInfo(fileId, objectKey, format, fileReason, FileState.DELETING, uploaderBrokerId, now, fileSize);
        }

        public void addBatch(final BatchInfo batchInfo) {
            this.batches.add(batchInfo);
        }

        private void deleteBatch(final BatchInfo batchInfo, final Instant now) {
            this.batches.remove(batchInfo);
            if (allBatchesDeleted()) {
                markDeleted(now);
            }
        }

        private boolean allBatchesDeleted() {
            return this.batches.isEmpty();
        }

        private void markDeleted(final Instant now) {
            this.fileState = FileState.DELETING;
            this.markedForDeletionAt = now;
        }
    }

    private record BatchInfoInternal(BatchInfo batchInfo,
                                     FileInfo fileInfo) {
    }

    private record ProducerStateItem(int baseSequence,
                                     int lastSequence,
                                     long assignedOffset,
                                     long batchMaxTimestamp) {
    }

    private record LatestProducerState(short epoch, LinkedList<ProducerStateItem> lastEntries) {
        static LatestProducerState empty(final short epoch) {
            return new LatestProducerState(epoch, new LinkedList<>());
        }

        public void addElement(final int baseSequence,
                               final int lastSequence,
                               final long assignedOffset,
                               final long batchMaxTimestamp) {
            // Keep the last 5 entries
            while (lastEntries.size() >= NUM_BATCHES_TO_RETAIN) {
                lastEntries.removeFirst();
            }
            lastEntries.addLast(new ProducerStateItem(baseSequence, lastSequence, assignedOffset, batchMaxTimestamp));
        }
    }
}
