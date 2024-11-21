// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendInterceptor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendInterceptor.class);

    private final SharedState state;
    private final Writer writer;

    public AppendInterceptor(SharedState state) {
        this.state = state;
        final ObjectKeyCreator objectKeyCreator = (s) -> new PlainObjectKey(state.config().objectKeyPrefix(), s);
        this.writer = new Writer(
            state.time(), objectKeyCreator, state.storage(), state.controlPlane(),
            state.config().commitInterval(), state.config().produceBufferMaxBytes(),
                state.config().produceMaxUploadAttempts(), state.config().produceUploadBackoff());
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

        // TODO use purgatory
        final var resultFuture = writer.write(entriesPerPartition);
        resultFuture.whenComplete((result, e) -> {
            if (result == null) {
                // We don't really expect this future to fail, but in case it does...
                LOGGER.error("Write future failed", e);
                result = entriesPerPartition.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, ignore -> new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR)));
            }
            responseCallback.accept(result);
        });

        return true;
    }

    private EntrySeparationResult separateEntries(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        final Map<TopicPartition, MemoryRecords> entitiesForInklessTopics = new HashMap<>();
        final Map<TopicPartition, MemoryRecords> entitiesForNonInklessTopics = new HashMap<>();
        for (final var entry : entriesPerPartition.entrySet()) {
            if (state.metadata().isInklessTopic(entry.getKey().topic())) {
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
            LOGGER.warn("Idempotent produce found, rejecting request");
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

    @Override
    public void close() throws IOException {
        writer.close();
    }

    private record EntrySeparationResult(Map<TopicPartition, MemoryRecords> entitiesForInklessTopics,
                                         Map<TopicPartition, MemoryRecords> entitiesForNonInklessTopics) {
        boolean bothTypesPresent() {
            return !entitiesForInklessTopics.isEmpty() && !entitiesForNonInklessTopics.isEmpty();
        }
    }
}
