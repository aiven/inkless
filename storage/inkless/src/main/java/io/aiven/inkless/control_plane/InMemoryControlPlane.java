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
    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    // LinkedHashMap to preserve the insertion order, to select files for merging in order.
    // The key is the object key.
    private final LinkedHashMap<String, FileInfo> files = new LinkedHashMap<>();
    // The key is the partition. The inner map key is the last offset in the batch.
    private final HashMap<TopicIdPartition, TreeMap<Long, BatchInfoInternal>> batches = new HashMap<>();
    // The key is the ID.
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
    public synchronized List<InitDisklessLogResponse> initDisklessLog(final List<InitDisklessLogRequest> requests) {
        final List<InitDisklessLogResponse> responses = new ArrayList<>();
        for (final InitDisklessLogRequest request : requests) {
            final TopicIdPartition topicIdPartition = new TopicIdPartition(
                request.topicId(), request.partition(), request.topicName());

            final LogInfo existingLog = logs.get(topicIdPartition);
            if (existingLog != null) {
                responses.add(InitDisklessLogResponse.alreadyInitialized());
                continue;
            }

            final LogInfo logInfo = new LogInfo();
            logInfo.logStartOffset = request.logStartOffset();
            logInfo.highWatermark = request.disklessStartOffset();
            logInfo.disklessStartOffset = request.disklessStartOffset();
            logs.put(topicIdPartition, logInfo);
            batches.putIfAbsent(topicIdPartition, new TreeMap<>());

            if (request.producerStates() != null) {
                final TreeMap<Long, LatestProducerState> partitionProducers =
                    producers.computeIfAbsent(topicIdPartition, k -> new TreeMap<>());
                for (final InitDisklessLogProducerState ps : request.producerStates()) {
                    partitionProducers
                        .computeIfAbsent(ps.producerId(), k -> LatestProducerState.empty(ps.producerEpoch()))
                        .addElement(ps.baseSequence(), ps.lastSequence(), ps.assignedOffset(), ps.batchMaxTimestamp());
                }
            }

            responses.add(InitDisklessLogResponse.success());
        }
        return responses;
    }

    @Override
    public synchronized List<RepairDisklessLogResponse> repairDisklessLog(final List<RepairDisklessLogRequest> requests) {
        final List<RepairDisklessLogResponse> responses = new ArrayList<>();
        for (final RepairDisklessLogRequest request : requests) {
            final TopicIdPartition topicIdPartition = new TopicIdPartition(
                request.topicId(), request.partition(), request.topicName());

            final LogInfo existingLog = logs.get(topicIdPartition);
            if (existingLog != null) {
                existingLog.disklessStartOffset = request.disklessStartOffset();
                responses.add(new RepairDisklessLogResponse(true));
            } else {
                responses.add(new RepairDisklessLogResponse(false));
            }
        }
        return responses;
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

        // Populate only when unset: appends add newer batches at the head, so a populated log's oldest
        // batch is unchanged. Covers empty->non-empty and lazily backfills a still-null log.
        if (logInfo.earliestBatchTimestamp == null) {
            logInfo.earliestBatchTimestamp = earliestBatchTimestamp(coordinates);
        }

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

        // A fetch below the log start offset is out of range: the requested records have been
        // deleted/retained away. Returning the first batch at or after the offset (via the tailSet
        // below) would silently skip the gap [offset, logStartOffset) instead of signalling it.
        if (request.offset() < logInfo.logStartOffset) {
            LOGGER.debug("Offset {} below log start offset {} for {}",
                request.offset(), logInfo.logStartOffset, request.topicIdPartition());
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
        final boolean advanced = convertedOffset > logInfo.logStartOffset;
        if (advanced) {
            logInfo.logStartOffset = convertedOffset;
        }

        // coordinates.firstKey() is last offset in the batch
        while (!coordinates.isEmpty() && coordinates.firstKey() < logInfo.logStartOffset) {
            final BatchInfoInternal batchInfoInternal = coordinates.remove(coordinates.firstKey());
            batchInfoInternal.fileInfo().deleteBatch(batchInfoInternal.batchInfo, TimeUtils.now(time));
            logInfo.byteSize -= batchInfoInternal.batchInfo().metadata().byteSize();
            assert logInfo.byteSize >= 0;
        }
        // Recompute only on advance (null when the log emptied); a no-op delete leaves it untouched.
        if (advanced) {
            logInfo.earliestBatchTimestamp = earliestBatchTimestamp(coordinates);
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

            // Short-circuit (mirrors enforce_retention_v2): if the oldest retained batch survives every
            // enabled policy, nothing is deletable, so skip the O(depth) selection scans below. This is a
            // pure optimization - both scans would select nothing in this case (the size scan starts only
            // when byteSize exceeds the limit; the time takeWhile stops at the first non-breaching batch),
            // so we still fall through to the unchanged tail that reports log_start_offset. A null
            // earliestBatchTimestamp means "unknown", so we cannot prove time-safety and run the scans.
            final boolean sizeSafe = request.retentionBytes() < 0 || logInfo.byteSize <= request.retentionBytes();
            final boolean timeSafe = request.retentionMs() < 0
                || (logInfo.earliestBatchTimestamp != null
                    && logInfo.earliestBatchTimestamp >= now.toEpochMilli() - request.retentionMs());
            final boolean nothingDeletable = sizeSafe && timeSafe;

            // Enforce the size retention.
            if (!nothingDeletable
                && request.retentionBytes() >= 0
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
            if (!nothingDeletable && request.retentionMs() >= 0) {
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
            // Deletion advances log_start, so recompute when something was removed (null when emptied).
            if (batchesDeleted > 0) {
                logInfo.earliestBatchTimestamp = earliestBatchTimestamp(coordinates);
            }
            responses.add(EnforceRetentionResponse.success(batchesDeleted, bytesDeleted, logInfo.logStartOffset));
        }
        return responses;
    }

    @Override
    public synchronized List<AdvanceCrossTierLogStartOffsetResponse> advanceCrossTierLogStartOffset(final List<AdvanceCrossTierLogStartOffsetRequest> requests) {
        final List<AdvanceCrossTierLogStartOffsetResponse> responses = new ArrayList<>();
        for (final AdvanceCrossTierLogStartOffsetRequest request : requests) {
            final TopicIdPartition tidp = findTopicIdPartition(request.topicId(), request.partition());
            final LogInfo logInfo;
            if (tidp == null || (logInfo = logs.get(tidp)) == null) {
                responses.add(AdvanceCrossTierLogStartOffsetResponse.unknownTopicOrPartition());
                continue;
            }
            // Forward-only update.
            if (logInfo.remoteLogStartOffset < 0 || request.remoteLogStartOffset() > logInfo.remoteLogStartOffset) {
                logInfo.remoteLogStartOffset = request.remoteLogStartOffset();
            }
            responses.add(AdvanceCrossTierLogStartOffsetResponse.success(logInfo.remoteLogStartOffset));
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
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            // Prefer the cross-tier (remote) start when known; fall back to log_start_offset otherwise.
            final long earliest = logInfo.remoteLogStartOffset >= 0 ? logInfo.remoteLogStartOffset : logInfo.logStartOffset;
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, earliest);
        } else if (timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
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
    public boolean isSafeToDeleteFile(String objectKeyPath) {
        return !files.containsKey(objectKeyPath);
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
                    logInfo.disklessStartOffset,
                    logInfo.byteSize
                ));
            }
        }
        return result;
    }

    @Override
    public synchronized List<GetProducerStateResponse> getProducerState(final List<GetProducerStateRequest> requests) {
        final List<GetProducerStateResponse> result = new ArrayList<>();
        for (final GetProducerStateRequest request : requests) {
            final TopicIdPartition tidp = findTopicIdPartition(request.topicId(), request.partition());
            if (tidp == null || logs.get(tidp) == null) {
                result.add(GetProducerStateResponse.unknownTopicOrPartition());
            } else {
                final TreeMap<Long, LatestProducerState> partitionProducers = producers.get(tidp);
                if (partitionProducers == null || partitionProducers.isEmpty()) {
                    result.add(GetProducerStateResponse.success(List.of()));
                } else {
                    final List<GetProducerStateResponse.ProducerStateEntry> entries = new ArrayList<>();
                    for (final var entry : partitionProducers.entrySet()) {
                        final long producerId = entry.getKey();
                        final LatestProducerState state = entry.getValue();
                        for (final ProducerStateItem item : state.lastEntries()) {
                            entries.add(new GetProducerStateResponse.ProducerStateEntry(
                                producerId,
                                state.epoch(),
                                item.baseSequence(),
                                item.lastSequence(),
                                item.assignedOffset(),
                                item.batchMaxTimestamp()
                            ));
                        }
                    }
                    result.add(GetProducerStateResponse.success(entries));
                }
            }
        }
        return result;
    }

    @Override
    public synchronized List<PruneDisklessLogsResponse> pruneDisklessLogs(final List<PruneDisklessLogsRequest> pruneDisklessLogsRequests) {
        if (pruneDisklessLogsRequests.isEmpty()) {
            return List.of();
        }
        final Instant now = TimeUtils.now(time);
        final List<PruneDisklessLogsResponse> responses = new ArrayList<>();
        for (final PruneDisklessLogsRequest request : pruneDisklessLogsRequests) {
            responses.add(pruneDisklessLogsForPartition(request, now));
        }
        return responses;
    }

    private PruneDisklessLogsResponse pruneDisklessLogsForPartition(final PruneDisklessLogsRequest request, final Instant now) {
        final TopicIdPartition requestTip = request.topicIdPartition();
        final TopicIdPartition tidp = findTopicIdPartition(requestTip.topicId(), requestTip.partition());
        final LogInfo logInfo;
        final TreeMap<Long, BatchInfoInternal> coordinates;
        if (tidp == null
            || (logInfo = logs.get(tidp)) == null
            || (coordinates = batches.get(tidp)) == null) {
            return new PruneDisklessLogsResponse(requestTip, -1, PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION);
        }

        final long highestRemote = request.highestRemoteOffset();
        final long logStartBefore = logInfo.logStartOffset;
        final long highWatermark = logInfo.highWatermark;

        final List<Long> keysToRemove = new ArrayList<>();
        for (final long lastOffsetKey : coordinates.keySet()) {
            if (lastOffsetKey <= highestRemote) {
                keysToRemove.add(lastOffsetKey);
            }
        }
        Collections.sort(keysToRemove);
        for (final long key : keysToRemove) {
            final BatchInfoInternal removed = coordinates.remove(key);
            if (removed == null) {
                continue;
            }
            removed.fileInfo().deleteBatch(removed.batchInfo(), now);
            logInfo.byteSize -= removed.batchInfo().metadata().byteSize();
            if (logInfo.byteSize < 0) {
                throw new IllegalStateException("byteSize underflow for " + tidp);
            }
        }

        final long newLogStart;
        if (coordinates.isEmpty()) {
            newLogStart = Math.min(highWatermark, Math.max(highestRemote + 1, logStartBefore));
        } else {
            final long firstRemainingBaseOffset = coordinates.firstEntry().getValue().batchInfo().metadata().baseOffset();
            newLogStart = Math.max(logStartBefore, firstRemainingBaseOffset);
        }
        logInfo.logStartOffset = newLogStart;

        // Cross-tier prune removes the oldest batches, so recompute when it deleted something
        // (null when the log emptied); no removal leaves it untouched.
        if (!keysToRemove.isEmpty()) {
            logInfo.earliestBatchTimestamp = earliestBatchTimestamp(coordinates);
        }

        return new PruneDisklessLogsResponse(requestTip, newLogStart, PruneDisklessLogsError.NONE);
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

    // Effective timestamp of the oldest retained batch (smallest last-offset key == batch at logStartOffset),
    // or null when the log is empty. Matches batch_timestamp() / logs.earliest_batch_timestamp semantics.
    private static Long earliestBatchTimestamp(final TreeMap<Long, BatchInfoInternal> coordinates) {
        if (coordinates.isEmpty()) {
            return null;
        }
        return coordinates.firstEntry().getValue().batchInfo().metadata().timestamp();
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
        long byteSize = 0;
        long disklessStartOffset = 0;
        // -1 means "no remote tier tracked yet"; mirrors the NULL sentinel of logs.remote_log_start_offset.
        long remoteLogStartOffset = -1;
        // Effective timestamp of the oldest retained batch (the one at logStartOffset), mirroring
        // logs.earliest_batch_timestamp. null means "unknown, must scan" (same as the NULL sentinel in SQL).
        // Maintained purely for parity with the Postgres control plane so the enforceRetention short-circuit
        // behaves identically across backends.
        Long earliestBatchTimestamp = null;
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
