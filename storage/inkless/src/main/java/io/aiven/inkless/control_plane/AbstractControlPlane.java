// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.config.InklessConfig;

public abstract class AbstractControlPlane implements ControlPlane {
    protected final Time time;

    protected ProducerStateCache producerStateCache;

    public AbstractControlPlane(final Time time) {
        this.time = time;
    }

    public void configure(final Map<String, ?> configs) {
        InklessConfig config = new InklessConfig(configs);
        this.producerStateCache = new ProducerStateCache(time, config.producerStateManagerConfig());
    }

    protected abstract List<FindProducerStateResponse> findProducerStates(List<FindProducerStateRequest> findProducerStateRequests);

    protected abstract long logStartOffset(TopicIdPartition topicIdPartition);

    @Override
    public synchronized List<CommitBatchResponse> commitFile(final String objectKey,
                                                             final int uploaderBrokerId,
                                                             final long fileSize,
                                                             final List<CommitBatchRequest> batches) {
        refreshProducersIfNeeded(batches);

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

        final List<CommitBatchResponse> out = splitMapper.getOut();
        responsePostProcessing(out);
        return out;
    }

    private boolean isValidRequest(final CommitBatchRequest request) {
        final boolean knownPartition = request.topicIdPartition().topicId() != Uuid.ZERO_UUID;
        return knownPartition && producerStateCache.hasDuplicate(request).isEmpty();
    }

    private CommitBatchResponse responseOnInvalidRequest(CommitBatchRequest r) {
        // Invalid requests: either unknown topic/partition or duplicate
        return producerStateCache.hasDuplicate(r)
            .map(batchMetadata -> CommitBatchResponse.ofDuplicate(batchMetadata, logStartOffset(r.topicIdPartition())))
            .orElseGet(CommitBatchResponse::unknownTopicOrPartition);
    }

    private void refreshProducersIfNeeded(List<CommitBatchRequest> batches) {
        // If the producerId is active on the current state, then there is no need to fetch the state from the control-plane
        // ProducerId to broker is stable until the producer refreshes its metadata and starts writing to a new broker
        // only then refresh the state from the control plane
        var findProducerStateRequests = batches.stream()
            .peek(r -> producerStateCache.initializeProducerStateManagerIfNeeded(r.topicIdPartition().topicPartition()))
            .filter(CommitBatchRequest::hasProducerId)
            .filter(r -> producerStateCache.isUnknownProducerId(r.topicIdPartition().topicPartition(), r.producerId()))
            .map(r -> new FindProducerStateRequest(r.topicIdPartition(), r.producerId(), r.producerEpoch()))
            .collect(Collectors.toList());

        if (findProducerStateRequests.isEmpty()) {
            return;
        }

        refreshProducerStateCache(findProducerStateRequests);
    }

    private void responsePostProcessing(final List<CommitBatchResponse> out) {
        // Post-processing:
        // update local producer state if idempotent request is successful and refresh local producer state if needed

        final List<FindProducerStateRequest> outOfOrderSequenceRequests = new ArrayList<>();
        for (final CommitBatchResponse response : out) {
            if (response.isOk() && response.request().hasProducerId()) {
                producerStateCache.update(response);
            } else if (response.errors() == Errors.OUT_OF_ORDER_SEQUENCE_NUMBER) {
                final var req = response.request();
                final var findProducerStateRequest = new FindProducerStateRequest(req.topicIdPartition(), req.producerId(), req.producerEpoch());
                outOfOrderSequenceRequests.add(findProducerStateRequest);
            }
        }

        // out of order sequence requests need to be refreshed from the control plane in case other brokers have seen the producerId/epoch
        if (!outOfOrderSequenceRequests.isEmpty()) {
            refreshProducerStateCache(outOfOrderSequenceRequests);
        }
    }

    private void refreshProducerStateCache(final List<FindProducerStateRequest> findProducerStateRequests) {
        findProducerStates(findProducerStateRequests).forEach(producerStateCache::loadEntry);
    }

    protected abstract Iterator<CommitBatchResponse> commitFileForValidRequests(
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
            findBatchRequests, findBatchRequest -> true
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
}
