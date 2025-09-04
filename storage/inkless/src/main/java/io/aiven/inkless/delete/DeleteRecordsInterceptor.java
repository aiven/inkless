/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.delete;

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
            LOGGER.warn("Deleting records from diskless and classic topic in same request isn't supported");
            respondAllWithError(offsetPerPartition, responseCallback, Errors.INVALID_REQUEST);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (countResult.noDiskless()) {
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
