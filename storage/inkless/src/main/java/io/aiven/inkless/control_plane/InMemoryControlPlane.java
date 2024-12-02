// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.TopicsDelta;

import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
        metadataView.subscribeToTopicMetadataChanges(this);
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
                .filter(tidp -> topicsDelta.deletedTopicIds().contains(tidp.topicId()))
                .collect(Collectors.toSet());
        for (final TopicIdPartition topicIdPartition : tidpsToDelete) {
            logger.info("Deleting {}", topicIdPartition);
            logs.remove(topicIdPartition);
            batches.remove(topicIdPartition);
        }

        // Create.
        for (final var changedTopic : topicsDelta.changedTopics().entrySet()) {
            for (final var entry : changedTopic.getValue().newPartitions().entrySet()) {
                final TopicIdPartition topicIdPartition = new TopicIdPartition(
                        changedTopic.getKey(), entry.getKey(), changedTopic.getValue().name());

                logger.info("Creating {}", topicIdPartition);
                logs.put(topicIdPartition, new LogInfo());
                batches.put(topicIdPartition, new TreeMap<>());
            }
        }
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

        final long now = time.milliseconds();

        final SplitMapper<CommitBatchRequest, CommitBatchResponse> splitMapper = new SplitMapper<>(
            batches, this::partitionExistsInMetadataForCommitBatchRequest
        );

        // Right away set answer for partitions not present in the metadata.
        splitMapper.setFalseOut(
            splitMapper.getFalseIn().map(r -> CommitBatchResponse.unknownTopicOrPartition()).iterator()
        );

        // Process those partitions that are present in the metadata.
        splitMapper.setTrueOut(
            splitMapper.getTrueIn().map(request -> commitFileForExistingPartition(now, objectKey, request)).iterator()
        );

        return splitMapper.getOut();
    }

    private boolean partitionExistsInMetadataForCommitBatchRequest(final CommitBatchRequest request) {
        final String topicName = request.topicPartition().topic();
        final Uuid topicId = metadataView.getTopicId(topicName);
        final Set<TopicPartition> partitions = metadataView.getTopicPartitions(topicName);
        return topicId != Uuid.ZERO_UUID
            && partitions.contains(request.topicPartition());
    }

    private CommitBatchResponse commitFileForExistingPartition(final long now,
                                                               final ObjectKey objectKey,
                                                               final CommitBatchRequest request) {
        final String topicName = request.topicPartition().topic();
        final Uuid topicId = metadataView.getTopicId(topicName);

        final TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, request.topicPartition());
        final LogInfo logInfo = logs.get(topicIdPartition);
        final TreeMap<Long, BatchInfo> coordinates = this.batches.get(topicIdPartition);
        if (logInfo == null || coordinates == null) {
            return CommitBatchResponse.unknownTopicOrPartition();
        }

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
        coordinates.put(lastOffset, batchToStore);
        return CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset);
    }

    @Override
    public synchronized List<FindBatchResponse> findBatches(final List<FindBatchRequest> findBatchRequests,
                                                            final boolean minOneMessage,
                                                            final int fetchMaxBytes) {
        final SplitMapper<FindBatchRequest, FindBatchResponse> splitMapper = new SplitMapper<>(
            findBatchRequests, this::partitionExistsInMetadataForFindBatchRequest
        );

        // Right away set answer for partitions not present in the metadata.
        splitMapper.setFalseOut(
            splitMapper.getFalseIn().map(r -> FindBatchResponse.unknownTopicOrPartition()).iterator()
        );

        // Process those partitions that are present in the metadata.
        splitMapper.setTrueOut(
            splitMapper.getTrueIn()
                .map(request -> findBatchesForExistingPartition(request, minOneMessage, fetchMaxBytes)).iterator()
        );

        return splitMapper.getOut();
    }

    private FindBatchResponse findBatchesForExistingPartition(final FindBatchRequest request,
                                                              final boolean minOneMessage,
                                                              final int fetchMaxBytes) {
        final LogInfo logInfo = logs.get(request.topicIdPartition());
        final TreeMap<Long, BatchInfo> coordinates = this.batches.get(request.topicIdPartition());
        if (logInfo == null || coordinates == null) {
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < 0) {
            logger.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
            return FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset, logInfo.highWatermark);
        }

        if (request.offset() >= logInfo.highWatermark) {
            return FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset, logInfo.highWatermark);
        }

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
        return FindBatchResponse.success(batches, logInfo.logStartOffset, logInfo.highWatermark);
    }

    private boolean partitionExistsInMetadataForFindBatchRequest(final FindBatchRequest request) {
        final String topicName = request.topicIdPartition().topic();
        final Uuid topicId = metadataView.getTopicId(topicName);
        final Set<TopicPartition> partitions = metadataView.getTopicPartitions(topicName);
        return topicId.equals(request.topicIdPartition().topicId())
            && partitions.contains(request.topicIdPartition().topicPartition());
    }

    private static class LogInfo {
        long logStartOffset = 0;
        long highWatermark = 0;
    }
}
