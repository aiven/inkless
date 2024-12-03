// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;

public class FetchInterceptor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchInterceptor.class);

    private final SharedState state;
    private final Reader reader;

    public FetchInterceptor(final SharedState state) {
        this(state, new Reader(state.time(), state.controlPlane(), state.storage()));
    }

    public FetchInterceptor(final SharedState state, final Reader reader) {
        this.state = state;
        this.reader = reader;
    }

    public boolean intercept(final FetchParams params,
                             final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                             final Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback) {

        final EntrySeparationResult entrySeparationResult = separateEntries(fetchInfos);
        if (entrySeparationResult.bothTypesPresent()) {
            LOGGER.warn("Consuming from Inkless and class topic in same request isn't supported");
            final var response = fetchInfos.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            ignored -> new FetchPartitionData(Errors.INVALID_REQUEST, -1, -1,
                                    null, Optional.empty(), OptionalLong.empty(),
                                    Optional.empty(), OptionalInt.empty(), false)));

            responseCallback.accept(response);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (!entrySeparationResult.entitiesForNonInklessTopics.isEmpty()) {
            return false;
        }

        final var resultFuture = reader.fetch(params, fetchInfos);
        resultFuture.whenComplete((result, e) -> {
            if (result == null) {
                // We don't really expect this future to fail, but in case it does...
                LOGGER.error("Read future failed", e);
                final var error = new FetchPartitionData(Errors.UNKNOWN_SERVER_ERROR, -1, -1,
                        MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(),
                        Optional.empty(), OptionalInt.empty(), false);
                result = fetchInfos.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, ignore -> error));
            }
            responseCallback.accept(result);
        });

        return true;
    }


    private EntrySeparationResult separateEntries(final Map<TopicIdPartition, FetchRequest.PartitionData> entriesPerPartition) {
        final Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForInklessTopics = new HashMap<>();
        final Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForNonInklessTopics = new HashMap<>();
        for (final var entry : entriesPerPartition.entrySet()) {
            if (state.metadata().isInklessTopic(entry.getKey().topic())) {
                entitiesForInklessTopics.put(entry.getKey(), entry.getValue());
            } else {
                entitiesForNonInklessTopics.put(entry.getKey(), entry.getValue());
            }
        }
        return new EntrySeparationResult(entitiesForInklessTopics, entitiesForNonInklessTopics);
    }

    @Override
    public void close() {
        reader.close();
    }

    private record EntrySeparationResult(Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForInklessTopics,
                                         Map<TopicIdPartition, FetchRequest.PartitionData> entitiesForNonInklessTopics) {
        boolean bothTypesPresent() {
            return !entitiesForInklessTopics.isEmpty() && !entitiesForNonInklessTopics.isEmpty();
        }
    }
}
