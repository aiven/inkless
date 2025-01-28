// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.internals.log.LogConfig;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicIdEnricher;
import io.aiven.inkless.common.TopicTypeCounter;

public class AppendInterceptor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendInterceptor.class);

    private final SharedState state;
    private final Writer writer;

    private final TopicTypeCounter topicTypeCounter;

    @DoNotMutate
    @CoverageIgnore
    public AppendInterceptor(final SharedState state) {
        this(
            state,
            new Writer(
                state.time(),
                state.brokerId(),
                state.objectKeyCreator(),
                state.storage(),
                state.keyAlignmentStrategy(),
                state.cache(),
                state.controlPlane(),
                state.config().commitInterval(),
                state.config().produceBufferMaxBytes(),
                state.config().produceMaxUploadAttempts(),
                state.config().produceUploadBackoff(),
                state.brokerTopicStats()
            )
        );
    }

    // Visible for tests
    AppendInterceptor(final SharedState state,
                      final Writer writer) {
        this.state = state;
        this.writer = writer;

        this.topicTypeCounter = new TopicTypeCounter(this.state.metadata());
    }

    /**
     * Intercept an attempt to append records.
     *
     * <p>If the interception happened, the {@code responseCallback} is called from inside the interceptor.
     *
     * @return {@code true} if interception happened
     */
    public boolean intercept(final Map<TopicPartition, MemoryRecords> entriesPerPartition,
                             final Consumer<Map<TopicPartition, PartitionResponse>> responseCallback) {
        final TopicTypeCounter.Result countResult = topicTypeCounter.count(entriesPerPartition.keySet());
        if (countResult.bothTypesPresent()) {
            LOGGER.warn("Producing to Inkless and class topic in same request isn't supported");
            respondAllWithError(entriesPerPartition, responseCallback, Errors.INVALID_REQUEST);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (countResult.noInkless()) {
            return false;
        }

        // This automatically reject transactional produce with this check.
        // However, it's likely that we allow idempotent produce earlier than we allow transactions.
        // Remember to adjust the code and tests accordingly!
        if (rejectIdempotentProduce(entriesPerPartition, responseCallback)) {
            return true;
        }

        final Map<TopicIdPartition, MemoryRecords> entriesPerPartitionEnriched;
        try {
            entriesPerPartitionEnriched = TopicIdEnricher.enrich(state.metadata(), entriesPerPartition);
        } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
            LOGGER.error("Cannot find UUID for topic {}", e.topicName);
            respondAllWithError(entriesPerPartition, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
            return true;
        }
        // TODO use purgatory
        final var resultFuture = writer.write(entriesPerPartitionEnriched, getTimestampTypes(entriesPerPartition));
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

    private void respondAllWithError(final Map<TopicPartition, MemoryRecords> entriesPerPartition,
                                     final Consumer<Map<TopicPartition, PartitionResponse>> responseCallback,
                                     final Errors error) {
        final var response = entriesPerPartition.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                ignored -> new PartitionResponse(error)));
        responseCallback.accept(response);
    }

    private Map<String, TimestampType> getTimestampTypes(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        final Map<String, Object> defaultTopicConfigs = state.defaultTopicConfigs().get().originals();
        final Map<String, TimestampType> result = new HashMap<>();
        for (final TopicPartition tp : entriesPerPartition.keySet()) {
            final var overrides = state.metadata().getTopicConfig(tp.topic());
            result.put(tp.topic(), LogConfig.fromProps(defaultTopicConfigs, overrides).messageTimestampType);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
