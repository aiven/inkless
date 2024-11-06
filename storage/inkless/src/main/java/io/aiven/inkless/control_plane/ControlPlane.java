// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import io.aiven.inkless.common.ObjectKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlPlane {
    private static final Logger logger = LoggerFactory.getLogger(ControlPlane.class);

    private final MetadataView metadataView;

    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    private final HashMap<TopicIdPartition, TreeMap<Long, BatchInfo>> batches = new HashMap<>();

    public ControlPlane(final MetadataView metadataView) {
        this.metadataView = metadataView;
    }

    public synchronized List<CommitBatchResponse> commitFile(final ObjectKey objectKey,
                                                             final List<CommitBatchRequest> batches) {
        final List<CommitBatchResponse> responses = new ArrayList<>();

        for (final CommitBatchRequest request : batches) {
            final String topicName = request.topicPartition().topic();
            final Uuid topicId = metadataView.getTopicId(topicName);
            final Set<TopicPartition> partitions = metadataView.getTopicPartitions(topicName);
            if (topicId == Uuid.ZERO_UUID
                || !partitions.contains(request.topicPartition())) {
                responses.add(CommitBatchResponse.unknownTopicOrPartition());
            } else {
                final TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, request.topicPartition());
                final LogInfo logInfo = logs.computeIfAbsent(topicIdPartition, ignore -> new LogInfo());
                final long assignedOffset = logInfo.highWatermark;
                logInfo.highWatermark += request.numberOfRecords();
                final BatchInfo batchToStore = new BatchInfo(objectKey, request.byteOffset(), request.size(), request.numberOfRecords());
                this.batches
                    .computeIfAbsent(topicIdPartition, ignore -> new TreeMap<>())
                    .put(assignedOffset, batchToStore);
                responses.add(CommitBatchResponse.success(assignedOffset));
            }
        }

        return responses;
    }

    public synchronized List<FindBatchResponse> findBatches(final List<FindBatchRequest> findBatchRequests,
                                                           final boolean minOneMessage,
                                                           final int fetchMaxBytes) {
        // TODO return more batches per request

        final List<FindBatchResponse> result = new ArrayList<>();

        for (final FindBatchRequest request : findBatchRequests) {
            final String topicName = request.topicIdPartition().topic();
            final Uuid topicId = metadataView.getTopicId(topicName);
            final Set<TopicPartition> partitions = metadataView.getTopicPartitions(topicName);
            if (!topicId.equals(request.topicIdPartition().topicId())
                || !partitions.contains(request.topicIdPartition().topicPartition())) {
                result.add(FindBatchResponse.unknownTopicOrPartition());
            } else {
                final LogInfo logInfo = logs.computeIfAbsent(request.topicIdPartition(), ignore -> new LogInfo());
                if (request.offset() >= logInfo.highWatermark) {
                    result.add(FindBatchResponse.offsetOutOfRange());
                } else {
                    final TreeMap<Long, BatchInfo> coordinates = this.batches.get(request.topicIdPartition());
                    if (coordinates != null) {
                        final var entry = coordinates.floorEntry(request.offset());
                        result.add(FindBatchResponse.success(List.of(entry.getValue()), logInfo.highWatermark));
                    } else {
                        logger.error("Batch coordinates not found for {}: high watermark={}, requested offset={}",
                            request.topicIdPartition(),
                            logInfo.highWatermark,
                            request.offset());
                        result.add(FindBatchResponse.unknownServerError());
                    }
                }
            }
        }

        return result;
    }

    private static class LogInfo {
        long highWatermark = 0;
    }
}
