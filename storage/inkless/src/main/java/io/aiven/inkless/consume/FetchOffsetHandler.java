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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicIdEnricher;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MetadataView;

public class FetchOffsetHandler implements Closeable {
    private final SharedState state;
    private final ExecutorService executor;

    public FetchOffsetHandler(SharedState state) {
        this.state = state;
        this.executor = Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-offset-metadata", false));
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
        private final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> requests = new HashMap<>();
        private final Map<TopicPartition, CompletableFuture<OffsetResultHolder.FileRecordsOrError>> futures = new HashMap<>();

        public Job(final MetadataView metadata,
                   final ControlPlane controlPlane,
                   final ExecutorService executor) {
            this.metadata = metadata;
            this.controlPlane = controlPlane;
            this.executor = executor;
        }

        public boolean mustHandle(final String topic) {
            return metadata.isDisklessTopic(topic);
        }

        public Future<Void> cancelHandler() {
            return cancelHandler;
        }

        public CompletableFuture<OffsetResultHolder.FileRecordsOrError> add(final TopicPartition topicPartition,
                                                                            final ListOffsetsRequestData.ListOffsetsPartition request) {
            requests.put(topicPartition, request);
            final CompletableFuture<OffsetResultHolder.FileRecordsOrError> result = new CompletableFuture<>();
            futures.put(topicPartition, result);
            return result;
        }

        public void start() {
            if (requests.isEmpty()) {
                return;
            }

            final Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> requestsEnriched;
            try {
                requestsEnriched = TopicIdEnricher.enrich(metadata, requests);
            } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
                // This should not happen during normal execution, non-Diskless topics won't get here.
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

        private void queryControlPlane(final Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> requestsEnriched) {
            final List<ListOffsetsRequest> controlPlaneRequests = requestsEnriched.entrySet()
                .stream().map(e -> new ListOffsetsRequest(e.getKey(), e.getValue().timestamp()))
                .collect(Collectors.toList());

            final List<ListOffsetsResponse> controlPlaneResponses;
            try {
                controlPlaneResponses = controlPlane.listOffsets(controlPlaneRequests);
            } catch (final Exception exception) {
                // Handle global errors (e.g. control plane not available).
                for (final var future : futures.values()) {
                    future.complete(new OffsetResultHolder.FileRecordsOrError(
                        Optional.of(exception),
                        Optional.empty()
                    ));
                }
                return;
            }

            for (final var response : controlPlaneResponses) {
                final var future = futures.get(response.topicIdPartition().topicPartition());
                final ApiException exception = response.errors().exception();
                if (exception == null) {
                    future.complete(new OffsetResultHolder.FileRecordsOrError(
                        Optional.empty(),
                        Optional.of(new FileRecords.TimestampAndOffset(response.timestamp(), response.offset(), Optional.of(LeaderAndIsr.INITIAL_LEADER_EPOCH)))
                    ));
                } else {
                    future.complete(new OffsetResultHolder.FileRecordsOrError(
                        Optional.of(exception),
                        Optional.empty()
                    ));
                }
            }
        }
    }
}
