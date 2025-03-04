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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicTypeCounter;

public class FetchInterceptor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchInterceptor.class);

    private final SharedState state;
    private final Reader reader;

    private final TopicTypeCounter topicTypeCounter;

    public FetchInterceptor(final SharedState state) {
        this(state, new Reader(state.time(), state.objectKeyCreator(), state.keyAlignmentStrategy(), state.cache(), state.controlPlane(), state.storage()));
    }

    public FetchInterceptor(final SharedState state, final Reader reader) {
        this.state = state;
        this.reader = reader;

        this.topicTypeCounter = new TopicTypeCounter(this.state.metadata());
    }

    public boolean intercept(final FetchParams params,
                             final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
                             final Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback,
                             final Consumer<Void> delayCallback) {
        final TopicTypeCounter.Result countResult = topicTypeCounter.count(
            fetchInfos.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet())
        );
        if (countResult.bothTypesPresent()) {
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
        if (countResult.noInkless()) {
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
                responseCallback.accept(result);
                return;
            }
            int totalSize = result.values().stream()
                .mapToInt(fetchPartitionData -> fetchPartitionData.records.sizeInBytes())
                .sum();
            if (totalSize == 0) {
                delayCallback.accept(null);
            } else {
                responseCallback.accept(result);
            }
        });

        return true;
    }

    @Override
    public void close() {
        reader.close();
    }
}
