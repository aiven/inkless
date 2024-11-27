// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.aiven.inkless.common.ObjectKey;

public class InMemoryControlPlane implements ControlPlane {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryControlPlane.class);

    private final Time time;
    private final MetadataView metadataView;

    private final Map<TopicIdPartition, LogInfo> logs = new HashMap<>();
    private final HashMap<TopicIdPartition, TreeMap<Long, BatchInfo>> batches = new HashMap<>();

    public InMemoryControlPlane(final Time time, final MetadataView metadataView) {
        this.time = time;
        this.metadataView = metadataView;
    }

    @Override
    public synchronized List<CommitBatchResponse> commitFile(final ObjectKey objectKey,
                                                             final List<CommitBatchRequest> batches) {
        // Real-life batches cannot be empty, even if they have 0 records
        // Checking this just as an assertion.
        for (final CommitBatchRequest batch : batches) {
            if (batch.size() == 0) {
                throw new IllegalArgumentException("Batches with size 0 are not allowed");
            }
        }

        final List<CommitBatchResponse> responses = new ArrayList<>();
        final long now = time.milliseconds();

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
                final long firstOffset = logInfo.highWatermark;
                logInfo.highWatermark += request.numberOfRecords();
                final long lastOffset = logInfo.highWatermark - 1;
                final BatchInfo batchToStore = new BatchInfo(
                    objectKey,
                    request.byteOffset(),
                    request.size(),
                    firstOffset,
                    request.numberOfRecords(),
                    metadataView.getTopicConfig(topicName).messageTimestampType,
                    now
                );
                this.batches
                    .computeIfAbsent(topicIdPartition, ignore -> new TreeMap<>())
                    .put(lastOffset, batchToStore);
                responses.add(CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset));
            }
        }

        return responses;
    }

    @Override
    public synchronized List<FindBatchResponse> findBatches(final List<FindBatchRequest> findBatchRequests,
                                                           final boolean minOneMessage,
                                                           final int fetchMaxBytes) {
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
                if (request.offset() < 0) {
                    logger.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
                    result.add(FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset, logInfo.highWatermark));
                } else {
                    if (request.offset() >= logInfo.highWatermark) {
                        result.add(FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset, logInfo.highWatermark));
                    } else {
                        final TreeMap<Long, BatchInfo> coordinates = this.batches.get(request.topicIdPartition());
                        if (coordinates != null) {
                            List<BatchInfo> batches = new ArrayList<>();
                            long totalSize = 0;
                            for (Long batchOffset : coordinates.navigableKeySet().tailSet(request.offset())) {
                                BatchInfo batch = coordinates.get(batchOffset);
                                batches.add(batch);
                                totalSize += batch.size();
                                if (totalSize > fetchMaxBytes) {
                                    break;
                                }
                            }
                            result.add(FindBatchResponse.success(
                                batches, logInfo.logStartOffset, logInfo.highWatermark));
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
        }

        return result;
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
    }
}
