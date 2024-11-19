// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.MetadataView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendInterceptor.class);

    private final InklessConfig inklessConfig;
    private final MetadataView metadataView;

    public AppendInterceptor(final InklessConfig inklessConfig,
                             final MetadataView metadataView) {
        this.inklessConfig = inklessConfig;
        this.metadataView = metadataView;
    }

    /**
     * Intercept an attempt to append records.
     *
     * <p>If the interception happened, the {@code responseCallback} is called from inside the interceptor.
     * @return {@code true} if interception happened
     */
    public boolean intercept(final Map<TopicPartition, MemoryRecords> entriesPerPartition,
                             final Consumer<Map<TopicPartition, PartitionResponse>> responseCallback) {
        final EntrySeparationResult entrySeparationResult = separateEntries(entriesPerPartition);
        if (entrySeparationResult.bothTypesPresent()) {
            LOGGER.warn("Producing to Inkless and class topic in same request isn't supported");
            final var response = entriesPerPartition.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    ignored -> new PartitionResponse(Errors.INVALID_REQUEST)));
            responseCallback.accept(response);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (!entrySeparationResult.entitiesForNonInklessTopics.isEmpty()) {
            return false;
        }

        if (rejectIdempotentProduce(entriesPerPartition, responseCallback)) {
            return true;
        }

        return true;
    }

    private EntrySeparationResult separateEntries(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        final Map<TopicPartition, MemoryRecords> entitiesForInklessTopics = new HashMap<>();
        final Map<TopicPartition, MemoryRecords> entitiesForNonInklessTopics = new HashMap<>();
        for (final var entry : entriesPerPartition.entrySet()) {
            if (metadataView.isInklessTopic(entry.getKey().topic())) {
                entitiesForInklessTopics.put(entry.getKey(), entry.getValue());
            } else {
                entitiesForNonInklessTopics.put(entry.getKey(), entry.getValue());
            }
        }
        return new EntrySeparationResult(entitiesForInklessTopics, entitiesForNonInklessTopics);
    }

    private boolean rejectIdempotentProduce(final Map<TopicPartition, MemoryRecords> entriesPerPartition,
                                            final Consumer<Map<TopicPartition, PartitionResponse>> responseCallback) {
        boolean atLeastBatchHasProducerId = entriesPerPartition.values().stream().anyMatch(records -> {
            for (final var batch : records.batches()) {
                if (batch.hasProducerId()) {
                    return true;
                }
            }
            return false;
        });

        if (atLeastBatchHasProducerId) {
            final var result = entriesPerPartition.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    ignore -> new PartitionResponse(Errors.INVALID_REQUEST)));
            responseCallback.accept(result);
            return true;
        } else {
            return false;
        }
    }

    private record EntrySeparationResult(Map<TopicPartition, MemoryRecords> entitiesForInklessTopics,
                                         Map<TopicPartition, MemoryRecords> entitiesForNonInklessTopics) {
        boolean bothTypesPresent() {
            return !entitiesForInklessTopics.isEmpty() && !entitiesForNonInklessTopics.isEmpty();
        }
    }
}
