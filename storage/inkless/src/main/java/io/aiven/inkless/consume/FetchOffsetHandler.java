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
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
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
    private final Time time;
    private final InklessFetchOffsetMetrics metrics;

    public FetchOffsetHandler(SharedState state) {
        this(
            state,
            Executors.newCachedThreadPool(new InklessThreadFactory("inkless-fetch-offset-metadata", false)),
            state.time(),
            new InklessFetchOffsetMetrics(state.time())
        );
    }

    // Visible for testing
    FetchOffsetHandler(
        final SharedState state,
        final ExecutorService executor,
        final Time time,
        final InklessFetchOffsetMetrics metrics
    ) {
        this.state = state;
        this.executor = executor;
        this.time = time;
        this.metrics = metrics;
    }

    public Job createJob() {
        return new Job(state.metadata(), state.controlPlane(), executor, time, metrics);
    }

    @Override
    public void close() throws IOException {
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 5, TimeUnit.SECONDS);
        metrics.close();
    }

    public static class Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private final MetadataView metadata;
        private final ControlPlane controlPlane;
        private final ExecutorService executor;

        private final CompletableFuture<Void> cancelHandler = new CompletableFuture<>();
        private final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> requests = new HashMap<>();
        private final Map<TopicPartition, CompletableFuture<OffsetResultHolder.FileRecordsOrError>> futures = new HashMap<>();

        private final Time time;
        private final InklessFetchOffsetMetrics metrics;
        private Instant startTime;

        public Job(final MetadataView metadata,
                   final ControlPlane controlPlane,
                   final ExecutorService executor,
                   final Time time,
                   final InklessFetchOffsetMetrics metrics) {
            this.metadata = metadata;
            this.controlPlane = controlPlane;
            this.executor = executor;
            this.time = time;
            this.metrics = metrics;
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
            this.startTime = TimeUtils.durationMeasurementNow(time);

            if (requests.isEmpty()) {
                return;
            }

            final Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> requestsEnriched;
            try {
                requestsEnriched = TopicIdEnricher.enrich(metadata, requests);
            } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
                // This should not happen during normal execution, non-Diskless topics won't get here.
                LOGGER.error("Cannot find UUID for topic {}", e.topicName);
                metrics.fetchOffsetFailed();
                throw new RuntimeException();
            }
            final Future<?> submitted = executor.submit(() -> queryControlPlane(requestsEnriched));
            cancelHandler.handle((_ignored, e) -> {
                if (e instanceof CancellationException) {
                    if (submitted.cancel(true)) {
                        metrics.fetchOffsetFailed();
                    }
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
                metrics.fetchOffsetFailed();
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
            metrics.fetchOffsetCompleted(startTime);
        }
    }
}
