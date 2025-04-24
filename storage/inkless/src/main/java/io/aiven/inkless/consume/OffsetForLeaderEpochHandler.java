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

import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicIdEnricher;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MetadataView;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class OffsetForLeaderEpochHandler implements Closeable {
    private final SharedState state;
    private final ExecutorService executor;

    public OffsetForLeaderEpochHandler(SharedState state) {
        this.state = state;
        this.executor = Executors.newCachedThreadPool(new InklessThreadFactory("inkless-offset-for-leader-epoch-metadata", false));
    }

    public Job createJob() {
        return new Job(state.metadata(), state.controlPlane(), executor);
    }

    @Override
    public void close() throws IOException {
        this.executor.shutdown();
    }

    public static class Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private final MetadataView metadata;
        private final ControlPlane controlPlane;
        private final ExecutorService executor;

        private final CompletableFuture<Void> cancelHandler = new CompletableFuture<>();
        private final Map<TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition> requests = new HashMap<>();
        private final Map<TopicPartition, CompletableFuture<OffsetForLeaderEpochResponseData.EpochEndOffset>> futures = new HashMap<>();

        public Job(final MetadataView metadata,
                   final ControlPlane controlPlane,
                   final ExecutorService executor) {
            this.metadata = metadata;
            this.controlPlane = controlPlane;
            this.executor = executor;
        }

        public boolean mustHandle(final String topic) {
            return metadata.isInklessTopic(topic);
        }

        public Future<Void> cancelHandler() {
            return cancelHandler;
        }

        public CompletableFuture<OffsetForLeaderEpochResponseData.EpochEndOffset> add(final TopicPartition topicPartition,
                                                                                      final OffsetForLeaderEpochRequestData.OffsetForLeaderPartition request) {
            requests.put(topicPartition, request);
            final CompletableFuture<OffsetForLeaderEpochResponseData.EpochEndOffset> result = new CompletableFuture<>();
            futures.put(topicPartition, result);
            return result;
        }

        public void start() {
            if (requests.isEmpty()) {
                return;
            }

            final Map<TopicIdPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition> requestsEnriched;
            try {
                requestsEnriched = TopicIdEnricher.enrich(metadata, requests);
            } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
                // This should not happen during normal execution, non-Inkless topics won't get here.
                LOGGER.error("Cannot find UUID for topic {}", e.topicName);
                throw new RuntimeException();
            }
            final Future<?> submitted = executor.submit(() -> queryControlPlane(requestsEnriched));
            cancelHandler.handle((_ignored, e) -> {
                if (e instanceof CancellationException) {
                    submitted.cancel(true);
                }
                return null;
            });
        }

        private void queryControlPlane(final Map<TopicIdPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition> requestsEnriched) {
            final List<ListOffsetsRequest> controlPlaneRequests = requestsEnriched.entrySet()
                .stream().map(e -> new ListOffsetsRequest(e.getKey(), ListOffsetsRequest.MAX_TIMESTAMP))
                .collect(Collectors.toList());

            final List<ListOffsetsResponse> controlPlaneResponses;
            try {
                controlPlaneResponses = controlPlane.listOffsets(controlPlaneRequests);
            } catch (final Exception exception) {
                // Handle global errors (e.g. control plane not available).
                for (final var future : futures.values()) {
                    future.complete(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    );
                }
                return;
            }

            for (final var response : controlPlaneResponses) {
                final var future = futures.get(response.topicIdPartition().topicPartition());
                final ApiException exception = response.errors().exception();
                if (exception == null) {
                    future.complete(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(response.topicIdPartition().partition())
                            .setLeaderEpoch(0) // TODO: OK to hard-code to zero
                            .setEndOffset(response.offset())
                    );
                } else {
                    future.complete(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setErrorCode(response.errors().code())
                    );
                }
            }
        }
    }
}
