// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;


public abstract class AbstractControlPlane implements ControlPlane {
    protected final Time time;
    protected final MetadataView metadataView;

    public AbstractControlPlane(final Time time,
                                final MetadataView metadataView) {
        this.time = time;
        this.metadataView = metadataView;
    }

    @Override
    public synchronized List<CommitBatchResponse> commitFile(final String objectKey,
                                                             final int uploaderBrokerId,
                                                             final long fileSize,
                                                             final List<CommitBatchRequest> batches) {
        // Real-life batches cannot be empty, even if they have 0 records
        // Checking this just as an assertion.
        for (final CommitBatchRequest batch : batches) {
            if (batch.size() == 0) {
                throw new IllegalArgumentException("Batches with size 0 are not allowed");
            }
        }

        final SplitMapper<CommitBatchRequest, CommitBatchResponse> splitMapper = new SplitMapper<>(
            batches, this::partitionExistsInMetadataForCommitBatchRequest
        );

        // Right away set answer for partitions not present in the metadata.
        splitMapper.setFalseOut(
            splitMapper.getFalseIn().map(r -> CommitBatchResponse.unknownTopicOrPartition()).iterator()
        );

        // Process those partitions that are present in the metadata.
        splitMapper.setTrueOut(commitFileForExistingPartitions(objectKey, uploaderBrokerId, fileSize, splitMapper.getTrueIn()));

        return splitMapper.getOut();
    }

    private boolean partitionExistsInMetadataForCommitBatchRequest(final CommitBatchRequest request) {
        final String topicName = request.topicPartition().topic();
        final Uuid topicId = metadataView.getTopicId(topicName);
        final Set<TopicPartition> partitions = metadataView.getTopicPartitions(topicName);
        return topicId != Uuid.ZERO_UUID
            && partitions.contains(request.topicPartition());
    }

    protected abstract Iterator<CommitBatchResponse> commitFileForExistingPartitions(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests
    );

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
        splitMapper.setTrueOut(findBatchesForExistingPartitions(splitMapper.getTrueIn(), minOneMessage, fetchMaxBytes));

        return splitMapper.getOut();
    }

    protected abstract Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final boolean minOneMessage,
        final int fetchMaxBytes);

    private boolean partitionExistsInMetadataForFindBatchRequest(final FindBatchRequest request) {
        final String topicName = request.topicIdPartition().topic();
        final Uuid topicId = metadataView.getTopicId(topicName);
        final Set<TopicPartition> partitions = metadataView.getTopicPartitions(topicName);
        return topicId.equals(request.topicIdPartition().topicId())
            && partitions.contains(request.topicIdPartition().topicPartition());
    }
}
