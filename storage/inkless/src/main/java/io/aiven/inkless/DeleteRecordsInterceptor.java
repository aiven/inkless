// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.Errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicIdEnricher;
import io.aiven.inkless.common.TopicTypeCounter;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;

import static org.apache.kafka.common.requests.DeleteRecordsResponse.INVALID_LOW_WATERMARK;

public class DeleteRecordsInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRecordsInterceptor.class);

    private final SharedState state;
    private final Executor executor;
    private final TopicTypeCounter topicTypeCounter;

    public DeleteRecordsInterceptor(final SharedState state) {
        this(state, Executors.newCachedThreadPool());
    }

    // Visible for testing.
    DeleteRecordsInterceptor(final SharedState state, final Executor executor) {
        this.state = state;
        this.executor = executor;
        this.topicTypeCounter = new TopicTypeCounter(this.state.metadata());
    }

    /**
     * Intercept an attempt to delete records.
     *
     * <p>If the interception happened, the {@code responseCallback} is called from inside the interceptor.
     *
     * @return {@code true} if interception happened
     */
    public boolean intercept(final Map<TopicPartition, Long> offsetPerPartition,
                             final Consumer<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback) {
        final TopicTypeCounter.Result countResult = topicTypeCounter.count(offsetPerPartition.keySet());
        if (countResult.bothTypesPresent()) {
            LOGGER.warn("Deleting records from Inkless and classic topic in same request isn't supported");
            respondAllWithError(offsetPerPartition, responseCallback, Errors.INVALID_REQUEST);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (countResult.noInkless()) {
            return false;
        }

        final Map<TopicIdPartition, Long> offsetPerPartitionEnriched;
        try {
            offsetPerPartitionEnriched = TopicIdEnricher.enrich(state.metadata(), offsetPerPartition);
        } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
            LOGGER.error("Cannot find UUID for topic {}", e.topicName);
            respondAllWithError(offsetPerPartition, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
            return true;
        }

        // TODO use purgatory
        executor.execute(() -> {
            try {
                final List<DeleteRecordsRequest> requests = offsetPerPartitionEnriched.entrySet().stream()
                    .map(kv -> new DeleteRecordsRequest(kv.getKey(), kv.getValue()))
                    .toList();
                final List<DeleteRecordsResponse> responses = state.controlPlane().deleteRecords(requests);
                final Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult> result = new HashMap<>();
                for (int i = 0; i < responses.size(); i++) {
                    final DeleteRecordsRequest request = requests.get(i);
                    final DeleteRecordsResponse response = responses.get(i);
                    final var value = new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                        .setPartitionIndex(request.topicIdPartition().partition())
                        .setErrorCode(response.errors().code())
                        .setLowWatermark(response.lowWatermark());
                    result.put(request.topicIdPartition().topicPartition(), value);
                }
                responseCallback.accept(result);
            } catch (final Exception e) {
                LOGGER.error("Unknown exception", e);
                respondAllWithError(offsetPerPartition, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
            }
        });
        return true;
    }

    private void respondAllWithError(final Map<TopicPartition, Long> offsetPerPartition,
                                     final Consumer<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback,
                                     final Errors error) {
        final var response = offsetPerPartition.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                kv -> new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(kv.getKey().partition())
                    .setErrorCode(error.code())
                    .setLowWatermark(INVALID_LOW_WATERMARK)
            ));
        responseCallback.accept(response);
    }
}
