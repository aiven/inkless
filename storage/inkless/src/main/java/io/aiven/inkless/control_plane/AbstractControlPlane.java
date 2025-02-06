// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractControlPlane implements ControlPlane {
    protected final Time time;

    public AbstractControlPlane(final Time time) {
        this.time = time;
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

        final SplitMapper<CommitBatchRequest, CommitBatchResponse> splitMapper = new SplitMapper<>(batches, this::isValidRequest);

        // Right away set answer for invalid requests
        splitMapper.setFalseOut(
            splitMapper.getFalseIn()
                .map(this::responseOnInvalidRequest)
                .iterator()
        );

        // Process those valid ones
        splitMapper.setTrueOut(commitFileForValidRequests(objectKey, uploaderBrokerId, fileSize, splitMapper.getTrueIn()));

        return splitMapper.getOut();
    }

    private boolean isValidRequest(final CommitBatchRequest request) {
        return request.topicIdPartition().topicId() != Uuid.ZERO_UUID;
    }

    private CommitBatchResponse responseOnInvalidRequest(CommitBatchRequest r) {
        // Invalid requests: unknown topic/partition
        return CommitBatchResponse.unknownTopicOrPartition();
    }

    protected abstract Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests
    );

    @Override
    public synchronized List<FindBatchResponse> findBatches(final List<FindBatchRequest> findBatchRequests,
                                                            final int fetchMaxBytes) {
        final SplitMapper<FindBatchRequest, FindBatchResponse> splitMapper = new SplitMapper<>(
            findBatchRequests, findBatchRequest -> true
        );

        // Right away set answer for partitions not present in the metadata.
        splitMapper.setFalseOut(
            splitMapper.getFalseIn().map(r -> FindBatchResponse.unknownTopicOrPartition()).iterator()
        );

        // Process those partitions that are present in the metadata.
        splitMapper.setTrueOut(findBatchesForExistingPartitions(splitMapper.getTrueIn(), fetchMaxBytes));

        return splitMapper.getOut();
    }

    protected abstract Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes);

    @Override
    public synchronized List<ListOffsetsResponse> listOffsets(final List<ListOffsetsRequest> listOffsetsRequests) {
        final SplitMapper<ListOffsetsRequest, ListOffsetsResponse> splitMapper = new SplitMapper<>(
                listOffsetsRequests, findBatchRequest -> true
        );

        // Right away set answer for partitions not present in the metadata.
        splitMapper.setFalseOut(
                splitMapper.getFalseIn().map(r -> ListOffsetsResponse.unknownTopicOrPartition(r.topicIdPartition())).iterator()
        );

        // Process those partitions that are present in the metadata.
        splitMapper.setTrueOut(listOffsetsForExistingPartitions(splitMapper.getTrueIn()));

        return splitMapper.getOut();
    }

    protected abstract Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(
            Stream<ListOffsetsRequest> trueIn);
}
