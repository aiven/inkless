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
package io.aiven.inkless.consume;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicIdEnricher;
import io.aiven.inkless.common.TopicTypeCounter;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;

public class FetchOffsetInterceptor implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchOffsetInterceptor.class);

    private final SharedState state;
    private final TopicTypeCounter topicTypeCounter;
    private final ExecutorService metadataExecutor;
    private final ControlPlane controlPlane;

    public FetchOffsetInterceptor(SharedState state) {
        this.state = state;
        this.controlPlane = state.controlPlane();
        this.topicTypeCounter = new TopicTypeCounter(this.state.metadata());
        this.metadataExecutor = Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-offset-metadata", false));
    }

    public boolean intercept(
            List<ListOffsetsRequestData.ListOffsetsTopic> topics,
            Set<TopicPartition> duplicatePartitions,
            IsolationLevel isolationLevel,
            BiFunction<Errors, ListOffsetsRequestData.ListOffsetsPartition, ListOffsetsResponseData.ListOffsetsPartitionResponse> buildErrorResponse,
            Consumer<List<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback
    ) {
        Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> topicPartitions = topics.stream()
                .flatMap(t -> t.partitions().stream()
                        .map(p -> new AbstractMap.SimpleEntry<>(new TopicPartition(t.name(), p.partitionIndex()), p)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final TopicTypeCounter.Result countResult = topicTypeCounter.count(topicPartitions.keySet());
        if (countResult.bothTypesPresent()) {
            LOGGER.warn("Producing to Inkless and class topic in same request isn't supported");
            respondAllWithError(topics, responseCallback, Errors.INVALID_REQUEST);
            return true;
        }

        // This request produces only to classic topics, don't intercept.
        if (countResult.noInkless()) {
            return false;
        }

        if (!duplicatePartitions.isEmpty()) {
            LOGGER.error("Request specifies duplicate partitions {}", duplicatePartitions);
            respondAllWithError(topics, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
        }

        if (isolationLevel != IsolationLevel.READ_UNCOMMITTED) {
            LOGGER.error("Request uses invalid isolation level {}", isolationLevel);
            respondAllWithError(topics, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
        }

        final Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> entriesPerPartitionEnriched;
        try {
            entriesPerPartitionEnriched = TopicIdEnricher.enrich(state.metadata(), topicPartitions);
        } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
            LOGGER.error("Cannot find UUID for topic {}", e.topicName);
            respondAllWithError(topics, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
            return true;
        }
        // TODO use purgatory
        final var resultFuture = CompletableFuture.supplyAsync(() -> listOffsets(entriesPerPartitionEnriched));
        resultFuture.whenComplete((result, e) -> {
            if (result != null) {
                responseCallback.accept(result);
            } else {
                // We don't really expect this future to fail, but in case it does...
                LOGGER.error("ListOffsets future failed", e);
                respondAllWithError(topics, responseCallback, Errors.UNKNOWN_SERVER_ERROR);
            }
        });

        return true;
    }

    private List<ListOffsetsResponseData.ListOffsetsTopicResponse> listOffsets(
            Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> requests
    ) {
        List<ListOffsetsRequest> controlPlaneRequests = requests.entrySet()
                .stream().map(e -> new ListOffsetsRequest(e.getKey(), e.getValue().timestamp()))
                .collect(Collectors.toList());
        List<ListOffsetsResponse> controlPlaneResponses = controlPlane.listOffsets(controlPlaneRequests);
        return controlPlaneResponses
                .stream()
                .collect(Collectors.groupingBy(response -> response.topicIdPartition().topic()))
                .entrySet()
                .stream()
                .map(e -> new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(e.getKey())
                        .setPartitions(e.getValue()
                                .stream().map(p -> new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                        .setPartitionIndex(p.topicIdPartition().partition())
                                        .setErrorCode(p.errors().code())
                                        .setTimestamp(p.timestamp())
                                        .setOffset(p.offset())
                                ).collect(Collectors.toList()))
                )
                .collect(Collectors.toList());
    }

    private void respondAllWithError(final List<ListOffsetsRequestData.ListOffsetsTopic> topics,
                                     final Consumer<List<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback,
                                     final Errors error) {
        final var response = topics.stream()
                .map(topic -> new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic.name())
                        .setPartitions(topic.partitions().stream()
                                .map(partition -> new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                        .setPartitionIndex(partition.partitionIndex())
                                        .setErrorCode(error.code()))
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());
        responseCallback.accept(response);
    }

    @Override
    public void close() throws IOException {

    }
}
