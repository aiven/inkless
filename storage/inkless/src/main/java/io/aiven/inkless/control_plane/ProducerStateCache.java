// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Supporting control-plane component to manage the state of producers for all inkless partitions on a broker.
 * State is managed in memory (no snapshots) and sourced from the control-plane.
 */
public class ProducerStateCache {
    private final Time time;
    private final ProducerStateManagerConfig producerStateManagerConfig;

    private final Map<TopicPartition, ProducerStateManager> partitionProducers;

    @CoverageIgnore
    public ProducerStateCache(final Time time, final ProducerStateManagerConfig producerStateManagerConfig) {
        this.time = time;
        this.producerStateManagerConfig = producerStateManagerConfig;

        this.partitionProducers = new HashMap<>();
    }

    public void update(final CommitBatchResponse response) {
        Objects.requireNonNull(response, "response cannot be null");
        final var request = Objects.requireNonNull(response.request(), "request cannot be null");
        if (!request.hasProducerId()) {
            return;
        }

        final var producerStateManager = producerStateManager(request.topicIdPartition().topicPartition());

        // Preparation and update happen on the same step as
        // Control Plane implementations do the validations happening on preparation.
        // if there is any inconsistency, it will be thrown here.
        final var info = producerStateManager.prepareUpdate(request.producerId(), AppendOrigin.CLIENT);
        info.appendDataBatch(
            request.producerEpoch(),
            request.baseSequence(), request.lastSequence(),
            request.batchMaxTimestamp(),
            new LogOffsetMetadata(response.assignedOffset()),
            response.assignedOffset() + request.offsetDelta(),
            false
        );
        producerStateManager.update(info);
    }

    public Optional<BatchMetadata> hasDuplicate(final CommitBatchRequest request) {
        if (!request.hasProducerId()) {
            return Optional.empty();
        }

        return producerStateManager(request.topicIdPartition().topicPartition())
            .lastEntry(request.producerId())
            .flatMap(entry ->
                entry.findDuplicateBatch(
                    request.producerEpoch(),
                    request.baseSequence(),
                    request.lastSequence()
                ));
    }

    // package-private for testing
    ProducerStateManager producerStateManager(final TopicPartition topicPartition) {
        // ensure Manager is initialized at this point using #initializeProducerStateManagerIfNeeded
        return Objects.requireNonNull(partitionProducers.get(topicPartition), "producerStateManager cannot be null");
    }

    public void initializeProducerStateManagerIfNeeded(final TopicPartition tp) {
        partitionProducers.computeIfAbsent(tp, this::buildProducerStateManager);
    }

    private ProducerStateManager buildProducerStateManager(final TopicPartition tp) {
        try {
            // can obviate the need for a file as it is not used (no snapshots)
            final File psm = new File("/tmp/unknown");
            final int maxTransactionTimeoutMs = -1;  // as we don't support transactions yet
            return new ProducerStateManager(tp, psm, maxTransactionTimeoutMs, producerStateManagerConfig, time);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isUnknownProducerId(final TopicPartition topicPartition, final long producerId) {
        return !producerStateManager(topicPartition).activeProducers().containsKey(producerId);
    }

    /**
     * Minimum time for a producerId to be considered valid based on configuration
     */
    public Instant minimumTimestamp() {
        final long now = time.milliseconds();
        if (now < producerStateManagerConfig.producerIdExpirationMs()) return Instant.ofEpochMilli(0);
        return Instant.ofEpochMilli(now - producerStateManagerConfig.producerIdExpirationMs());
    }

    public void loadEntry(FindProducerStateResponse response) {
        producerStateManager(response.topicPartition()).loadProducerEntry(response.entry());
    }
}
