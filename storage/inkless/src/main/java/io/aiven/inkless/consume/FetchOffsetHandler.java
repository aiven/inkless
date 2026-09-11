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
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.CrossTierLogStartCache;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.common.TopicIdEnricher;
import io.aiven.inkless.common.metrics.ThreadPoolMonitor;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MetadataView;

import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP;

public class FetchOffsetHandler implements Closeable {
    private static final String THREAD_NAME_PREFIX = "inkless-fetch-offset-metadata";
    private static final int QUEUE_CAPACITY_PER_THREAD = 100;

    private final SharedState state;
    private final ExecutorService executor;
    private final Time time;
    private final InklessFetchOffsetMetrics metrics;
    private final ThreadPoolMonitor threadPoolMonitor;

    public FetchOffsetHandler(SharedState state) {
        this(
            state,
            createExecutor(state.config().fetchOffsetThreadPoolSize()),
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
        this.threadPoolMonitor = executor instanceof ThreadPoolExecutor
            ? new ThreadPoolMonitor(THREAD_NAME_PREFIX, executor)
            : null;
    }

    private static ExecutorService createExecutor(final int poolSize) {
        return new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(poolSize * QUEUE_CAPACITY_PER_THREAD),
            new InklessThreadFactory(THREAD_NAME_PREFIX, false),
            // Reject instead of CallerRunsPolicy: the caller is a request handler thread.
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public Job createJob() {
        return new Job(state.metadata(), state.controlPlane(), state.crossTierLogStartCache(), executor, time, metrics);
    }

    @Override
    public void close() throws IOException {
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 5, TimeUnit.SECONDS);
        if (threadPoolMonitor != null) {
            threadPoolMonitor.close();
        }
        metrics.close();
    }

    public static class Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private final MetadataView metadata;
        private final ControlPlane controlPlane;
        private final CrossTierLogStartCache crossTierLogStartCache;
        private final ExecutorService executor;

        private final CompletableFuture<Void> cancelHandler = new CompletableFuture<>();
        private final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> requests = new HashMap<>();
        private final Map<TopicPartition, CompletableFuture<OffsetResultHolder.FileRecordsOrError>> futures = new HashMap<>();

        private final Time time;
        private final InklessFetchOffsetMetrics metrics;
        private Instant startTime;

        public Job(final MetadataView metadata,
                   final ControlPlane controlPlane,
                   final CrossTierLogStartCache crossTierLogStartCache,
                   final ExecutorService executor,
                   final Time time,
                   final InklessFetchOffsetMetrics metrics) {
            this.metadata = metadata;
            this.controlPlane = controlPlane;
            this.crossTierLogStartCache = crossTierLogStartCache;
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
                // Complete all pending futures with the error rather than throwing an unchecked
                // exception that propagates to the request handler, which may log the full request
                // context (all topic names) producing an oversized log entry.
                failAll(new RuntimeException("Topic ID not found: " + e.topicName, e));
                return;
            }
            final Future<?> submitted;
            try {
                submitted = executor.submit(() -> queryControlPlane(requestsEnriched));
            } catch (final RejectedExecutionException e) {
                metrics.fetchOffsetRejected();
                failAll(e);
                return;
            }
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
            final List<ListOffsetsRequest> controlPlaneRequests = new ArrayList<>();
            // Partitions whose EARLIEST result came from the control plane and should be cached for future reads.
            final Set<TopicPartition> cacheableEarliest = new HashSet<>();

            for (final var entry : requestsEnriched.entrySet()) {
                final TopicIdPartition topicIdPartition = entry.getKey();
                final long timestamp = entry.getValue().timestamp();
                if (isCrossTierEarliest(topicIdPartition, timestamp)) {
                    final Long cached = crossTierLogStartCache.get(topicIdPartition);
                    if (cached != null) {
                        completeEarliestFromCache(topicIdPartition.topicPartition(), cached);
                        continue;
                    }
                    cacheableEarliest.add(topicIdPartition.topicPartition());
                }
                controlPlaneRequests.add(new ListOffsetsRequest(topicIdPartition, timestamp));
            }

            if (controlPlaneRequests.isEmpty()) {
                // Every request was served from the cache.
                metrics.fetchOffsetCompleted(startTime);
                return;
            }

            final List<ListOffsetsResponse> controlPlaneResponses;
            try {
                controlPlaneResponses = controlPlane.listOffsets(controlPlaneRequests);
            } catch (final Exception exception) {
                // Handle global errors (e.g. control plane not available).
                failAll(exception);
                metrics.fetchOffsetFailed();
                return;
            }

            for (final var response : controlPlaneResponses) {
                final TopicPartition topicPartition = response.topicIdPartition().topicPartition();
                final var future = futures.get(topicPartition);
                final ApiException exception = response.errors().exception();
                if (exception == null) {
                    if (cacheableEarliest.contains(topicPartition)) {
                        crossTierLogStartCache.put(response.topicIdPartition(), response.offset());
                    }
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

        private void failAll(final Exception exception) {
            for (final var future : futures.values()) {
                if (!future.isDone()) {
                    future.complete(new OffsetResultHolder.FileRecordsOrError(
                        Optional.of(exception),
                        Optional.empty()
                    ));
                }
            }
        }

        private boolean isCrossTierEarliest(final TopicIdPartition topicIdPartition, final long timestamp) {
            return timestamp == EARLIEST_TIMESTAMP && metadata.isConsolidatingDisklessTopic(topicIdPartition.topic());
        }

        private void completeEarliestFromCache(final TopicPartition topicPartition, final long offset) {
            futures.get(topicPartition).complete(new OffsetResultHolder.FileRecordsOrError(
                Optional.empty(),
                Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, offset, Optional.of(LeaderAndIsr.INITIAL_LEADER_EPOCH)))
            ));
        }
    }
}
