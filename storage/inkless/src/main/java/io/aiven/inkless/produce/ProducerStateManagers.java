// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Keeps track of producer state for each topic partition at a broker level.
 */
public class ProducerStateManagers {
    final Time time;
    final ProducerStateManagerConfig config;
    // topicPartition -> producerId -> producerStateEntry
    // Accessible for testing
    final HashMap<TopicPartition, ProducerStateManager> producerStateManagers = new HashMap<>();

    public ProducerStateManagers(Time time, ProducerStateManagerConfig config) {
        this.time = time;
        this.config = config;
    }
    // TODO: remove producers based on retention, topic changes, etc.

    /**
     * Validate duplicate batches from a set of requests
     *
     * @param requestsWithProducerId map of request id to map of topic partition to list of record batches
     * @return map of request id to map of topic partition to duplicated batch metadata
     */
    public ProducerStateResult validateProducer(final Map<Integer, Map<TopicPartition, List<RecordBatch>>> requestsWithProducerId) {
        Objects.requireNonNull(requestsWithProducerId, "requestsWithProducerId cannot be null");
        final Map<Integer, Map<TopicPartition, List<RecordBatch>>> requests = new HashMap<>();
        Map<Integer, Map<TopicPartition, DuplicatedBatchMetadata>> duplicates = new HashMap<>();
        for (var requestEntry: requestsWithProducerId.entrySet()) {
            var requestId = requestEntry.getKey();

            for (var topicPartitionEntry: requestEntry.getValue().entrySet()) {
                var topicPartition = topicPartitionEntry.getKey();

                var producers = producerStateManagers.computeIfAbsent(topicPartition, this::getProducerStateManager);

                var topicPartitionDuplicates = duplicates.computeIfAbsent(requestId, ignore -> new HashMap<>());

                var batches = topicPartitionEntry.getValue();
                for (var batch : batches) {
                    // this is unexpected, all batches must have producer id
                    if (!batch.hasProducerId()) throw new IllegalStateException("Batch must have producer id");

                    var producerStateEntry = producers.lastEntry(batch.producerId());

                    // Check if the batch is a duplicate
                    var maybeDuplicates = producerStateEntry.flatMap(e -> e.findDuplicateBatch(batch));
                    maybeDuplicates.ifPresentOrElse(
                        batchMetadata -> topicPartitionDuplicates.put(
                            topicPartition,
                            new DuplicatedBatchMetadata(batch.baseOffset(), batchMetadata)
                        ),
                        () -> requests.computeIfAbsent(requestId, __ -> new HashMap<>()).computeIfAbsent(topicPartition, __ -> new ArrayList<>()).add(batch)
                    );
                }
            }
        }
        return new ProducerStateResult(requests, duplicates);
    }

    ProducerStateManager getProducerStateManager(TopicPartition tp) {
        try {
            final var logDir = Files.createTempDirectory("inkless-producer-state").toFile();
            return new ProducerStateManager(tp, logDir, 10000, config, time);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateProducer(final Map<Integer, Map<TopicPartition, List<RecordBatch>>> requests) {
        Objects.requireNonNull(requests, "requests cannot be null");
        Map<TopicPartition, Map<Long, ProducerAppendInfo>> loadedProducers = new HashMap<>();
        for (var requestEntry : requests.entrySet()) {
            for (var tpEntry : requestEntry.getValue().entrySet()) {
                var tp = tpEntry.getKey();
                for (var batch : tpEntry.getValue()) {
                    if (batch.hasProducerId()) {
                        final var producerStateManager = producerStateManagers
                            .computeIfAbsent(tp, this::getProducerStateManager);
                        final var appendInfo = loadedProducers
                            .computeIfAbsent(tp, ignore -> new HashMap<>())
                            .computeIfAbsent(batch.producerId(), prodId -> producerStateManager.prepareUpdate(prodId, AppendOrigin.CLIENT));
                        appendInfo.append(batch, Optional.empty());
                    }
                }
            }
        }
        for (var topicPartitionEntry : loadedProducers.entrySet()) {
            var psm = producerStateManagers.get(topicPartitionEntry.getKey());
            topicPartitionEntry.getValue().values().forEach(psm::update);
        }
    }

    public record ProducerStateResult(Map<Integer, Map<TopicPartition, List<RecordBatch>>> requests,
                                      Map<Integer, Map<TopicPartition, DuplicatedBatchMetadata>> duplicates) {
    }
}
