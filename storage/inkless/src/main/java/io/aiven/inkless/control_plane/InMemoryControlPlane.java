// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.TopicsDelta;

import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class InMemoryControlPlane extends AbstractControlPlane {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryControlPlane.class);

    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    private final List<FileInfo> files = new ArrayList<>();
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
    @DoNotMutate
    public synchronized void onTopicMetadataChanges(final TopicsDelta topicsDelta) {
        // Delete.
        final Set<TopicIdPartition> tidpsToDelete = logs.keySet().stream()
                .filter(tidp -> metadataView.isInklessTopic(tidp.topic()))
                .filter(tidp -> topicsDelta.deletedTopicIds().contains(tidp.topicId()))
                .collect(Collectors.toSet());
        for (final TopicIdPartition topicIdPartition : tidpsToDelete) {
            logger.info("Deleting {}", topicIdPartition);
            logs.remove(topicIdPartition);
            batches.remove(topicIdPartition);
        }

        // Create.
        for (final var changedTopic : topicsDelta.changedTopics().entrySet()) {
            final String topicName = changedTopic.getValue().name();

            if (!metadataView.isInklessTopic(topicName)) {
                continue;
            }

            for (final var entry : changedTopic.getValue().newPartitions().entrySet()) {
                final TopicIdPartition topicIdPartition = new TopicIdPartition(
                        changedTopic.getKey(), entry.getKey(), topicName);

                logger.info("Creating {}", topicIdPartition);
                logs.put(topicIdPartition, new LogInfo());
                batches.put(topicIdPartition, new TreeMap<>());
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
        files.add(fileInfo);
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
        if (logInfo == null || coordinates == null) {
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        final long firstOffset = logInfo.highWatermark;
        logInfo.highWatermark += request.numberOfRecords();
        final long lastOffset = logInfo.highWatermark - 1;
        final BatchInfo batchInfo = new BatchInfo(
            fileInfo.objectKey(),
            request.byteOffset(),
            request.size(),
            firstOffset,
            request.numberOfRecords(),
            metadataView.getTopicConfig(topicName).messageTimestampType,
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
        if (logInfo == null || coordinates == null) {
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < 0) {
            logger.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
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
    public void close() throws IOException {
        // Do nothing.
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
    }

    private record FileInfo(String objectKey,
                            int uploaderBrokerId,
                            long fileSize) {
    }

    private record BatchInfoInternal(BatchInfo batchInfo,
                                     FileInfo fileInfo) {
    }
}
