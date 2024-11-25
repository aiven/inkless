// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.common.SharedState;

public class AppendInterceptor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendInterceptor.class);

    private final SharedState state;
    private final Writer writer;

    @DoNotMutate
    @CoverageIgnore
    public AppendInterceptor(final SharedState state) {
        this(
            state,
            new Writer(
                state.time(),
                (s) -> new PlainObjectKey(state.config().objectKeyPrefix(), s),
                state.storage(),
                state.controlPlane(),
                state.config().commitInterval(),
                state.config().produceBufferMaxBytes(),
                state.config().produceMaxUploadAttempts(),
                state.config().produceUploadBackoff()
            )
        );
    }

    // Visible for tests
    AppendInterceptor(final SharedState state,
                      final Writer writer) {
        this.state = state;
        this.writer = writer;
    }

    /**
     * Intercept an attempt to append records.
     *
     * <p>If the interception happened, the {@code responseCallback} is called from inside the interceptor.
     * @return {@code true} if interception happened
     */
    public boolean intercept(final Map<TopicPartition, MemoryRecords> entriesPerPartition,
                             final Consumer<Map<TopicPartition, PartitionResponse>> responseCallback) {
        final EntryCountResult entryCountResult = countEntries(entriesPerPartition);
        if (entryCountResult.bothTypesPresent()) {
            LOGGER.warn("Producing to Inkless and class topic in same request isn't supported");
            final var response = entriesPerPartition.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    ignored -> new PartitionResponse(Errors.INVALID_REQUEST)));
            responseCallback.accept(response);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (entryCountResult.noInkless()) {
            return false;
        }

        // This automatically reject transactional produce with this check.
        // However, it's likely that we allow idempotent produce earlier than we allow transactions.
        // Remember to adjust the code and tests accordingly!
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

    private EntryCountResult countEntries(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        int entitiesForInklessTopics = 0;
        int entitiesForNonInklessTopics = 0;
        for (final var entry : entriesPerPartition.entrySet()) {
            if (state.metadata().isInklessTopic(entry.getKey().topic())) {
                entitiesForInklessTopics += 1;
            } else {
                entitiesForNonInklessTopics += 1;
            }
        }
        return new EntryCountResult(entitiesForInklessTopics, entitiesForNonInklessTopics);
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

    private record EntryCountResult(int entityCountForInklessTopics,
                                    int entityCountForNonInklessTopics) {
        boolean bothTypesPresent() {
            return entityCountForInklessTopics > 0 && entityCountForNonInklessTopics > 0;
        }

        boolean noInkless() {
            return entityCountForInklessTopics == 0;
        }
    }
}
