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
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import io.aiven.inkless.control_plane.ControlPlaneUnavailableException;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MetadataView;

import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
import static org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP;

public class FetchOffsetHandler implements Closeable {
    private static final String THREAD_NAME_PREFIX = "inkless-fetch-offset-metadata";
    // Visible for testing.
    static final int QUEUE_CAPACITY_PER_THREAD = 100;

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

    // Visible for testing.
    static ExecutorService createExecutor(final int poolSize) {
        // ArrayBlockingQueue, as in Reader's lagging consumer pool. The queue is small, so eager allocation is irrelevant.
        // A task that is cancelled while queued keeps its slot until a worker skips it, as in Kafka's remote reader pool.
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
        private final Map<TopicPartition, CompletableFuture<OffsetResultHolder.FileRecordsOrError>> walStartFutures = new HashMap<>();

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

        /**
         * Adds a lookup of the diskless WAL start of the partition, the offset that `EARLIEST_LOCAL_TIMESTAMP`
         * returns. The lookup goes in the same control-plane call as the other lookups of the job, and the control
         * plane answers it after them, so the WAL start is at or past the one that a timestamp lookup of the same
         * partition saw.
         */
        public CompletableFuture<OffsetResultHolder.FileRecordsOrError> addWalStart(final TopicPartition topicPartition) {
            final CompletableFuture<OffsetResultHolder.FileRecordsOrError> result = new CompletableFuture<>();
            walStartFutures.put(topicPartition, result);
            return result;
        }

        public void start() {
            this.startTime = TimeUtils.durationMeasurementNow(time);

            if (requests.isEmpty() && walStartFutures.isEmpty()) {
                return;
            }

            final Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> requestsEnriched;
            final Map<TopicIdPartition, CompletableFuture<OffsetResultHolder.FileRecordsOrError>> walStartsEnriched;
            try {
                requestsEnriched = TopicIdEnricher.enrich(metadata, requests);
                walStartsEnriched = TopicIdEnricher.enrich(metadata, walStartFutures);
            } catch (final TopicIdEnricher.TopicIdNotFoundException e) {
                // This should not happen during normal execution, non-Diskless topics won't get here.
                LOGGER.error("Cannot find UUID for topic {}", e.topicName);
                metrics.fetchOffsetFailed();
                // Complete all pending futures with the error rather than throwing an unchecked
                // exception that propagates to the request handler, which may log the full request
                // context (all topic names) producing an oversized log entry.
                completeFuturesWithError(new RuntimeException("Topic ID not found: " + e.topicName, e));
                return;
            }

            final Future<?> submitted;
            try {
                submitted = executor.submit(() -> queryControlPlane(requestsEnriched, walStartsEnriched));
            } catch (final RejectedExecutionException e) {
                metrics.fetchOffsetRejected();
                completeFuturesWithError(new TimeoutException("Fetch offset thread pool queue is full", e));
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

        private void queryControlPlane(
            final Map<TopicIdPartition, ListOffsetsRequestData.ListOffsetsPartition> requestsEnriched,
            final Map<TopicIdPartition, CompletableFuture<OffsetResultHolder.FileRecordsOrError>> walStartsEnriched
        ) {
            // Lookups in request order: every control plane answers in that order, and a partition can appear twice.
            final List<PendingLookup> lookups = new ArrayList<>();

            for (final var entry : requestsEnriched.entrySet()) {
                final TopicIdPartition topicIdPartition = entry.getKey();
                final long timestamp = entry.getValue().timestamp();
                final CompletableFuture<OffsetResultHolder.FileRecordsOrError> future = futures.get(topicIdPartition.topicPartition());
                final boolean crossTierEarliest = isCrossTierEarliest(topicIdPartition, timestamp);
                if (crossTierEarliest) {
                    final Long cached = crossTierLogStartCache.get(topicIdPartition);
                    if (cached != null) {
                        completeEarliestFromCache(future, cached);
                        continue;
                    }
                }
                lookups.add(new PendingLookup(new ListOffsetsRequest(topicIdPartition, timestamp), future, crossTierEarliest));
            }
            // After the timestamp lookups, so a WAL start never predates the timestamp answer of its partition.
            for (final var entry : walStartsEnriched.entrySet()) {
                lookups.add(new PendingLookup(new ListOffsetsRequest(entry.getKey(), EARLIEST_LOCAL_TIMESTAMP), entry.getValue(), false));
            }

            if (lookups.isEmpty()) {
                // Every request was served from the cache.
                metrics.fetchOffsetCompleted(startTime);
                return;
            }

            final List<ListOffsetsResponse> controlPlaneResponses;
            try {
                controlPlaneResponses = controlPlane.listOffsets(lookups.stream().map(PendingLookup::request).toList());
            } catch (final ControlPlaneUnavailableException unavailableException) {
                // KAFKA_STORAGE_ERROR is retriable, unlike the UNKNOWN_SERVER_ERROR the raw
                // exception would map to below.
                LOGGER.debug("Rejecting diskless list offsets: {}", unavailableException.getMessage());
                completeFuturesWithError(new KafkaStorageException(unavailableException.getMessage()));
                metrics.fetchOffsetFailed();
                return;
            } catch (final Exception exception) {
                // Handle other global errors.
                completeFuturesWithError(exception);
                metrics.fetchOffsetFailed();
                return;
            }
            if (controlPlaneResponses.size() != lookups.size()) {
                completeFuturesWithError(new IllegalStateException(String.format(
                    "The control plane returned %d list offsets responses for %d requests",
                    controlPlaneResponses.size(), lookups.size())));
                metrics.fetchOffsetFailed();
                return;
            }
            for (int i = 0; i < lookups.size(); i++) {
                final TopicIdPartition requested = lookups.get(i).request().topicIdPartition();
                final TopicIdPartition answered = controlPlaneResponses.get(i).topicIdPartition();
                if (!requested.equals(answered)) {
                    completeFuturesWithError(new IllegalStateException(String.format(
                        "The control plane answered for %s at position %d of the list offsets requests, which is for %s",
                        answered, i, requested)));
                    metrics.fetchOffsetFailed();
                    return;
                }
            }

            for (int i = 0; i < lookups.size(); i++) {
                final PendingLookup lookup = lookups.get(i);
                final ListOffsetsResponse response = controlPlaneResponses.get(i);
                final ApiException exception = response.errors().exception();
                if (exception == null) {
                    if (lookup.cacheCrossTierEarliest()) {
                        crossTierLogStartCache.put(response.topicIdPartition(), response.offset());
                    }
                    lookup.future().complete(new OffsetResultHolder.FileRecordsOrError(
                        Optional.empty(),
                        Optional.of(new FileRecords.TimestampAndOffset(response.timestamp(), response.offset(), Optional.of(LeaderAndIsr.INITIAL_LEADER_EPOCH)))
                    ));
                } else {
                    lookup.future().complete(new OffsetResultHolder.FileRecordsOrError(
                        Optional.of(exception),
                        Optional.empty()
                    ));
                }
            }
            metrics.fetchOffsetCompleted(startTime);
        }

        private void completeFuturesWithError(final Exception exception) {
            for (final var future : futures.values()) {
                completeWithError(future, exception);
            }
            for (final var future : walStartFutures.values()) {
                completeWithError(future, exception);
            }
        }

        private static void completeWithError(final CompletableFuture<OffsetResultHolder.FileRecordsOrError> future,
                                              final Exception exception) {
            if (!future.isDone()) {
                future.complete(new OffsetResultHolder.FileRecordsOrError(
                    Optional.of(exception),
                    Optional.empty()
                ));
            }
        }

        private boolean isCrossTierEarliest(final TopicIdPartition topicIdPartition, final long timestamp) {
            return timestamp == EARLIEST_TIMESTAMP && metadata.isConsolidatingDisklessTopic(topicIdPartition.topic());
        }

        private static void completeEarliestFromCache(final CompletableFuture<OffsetResultHolder.FileRecordsOrError> future,
                                                      final long offset) {
            future.complete(new OffsetResultHolder.FileRecordsOrError(
                Optional.empty(),
                Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, offset, Optional.of(LeaderAndIsr.INITIAL_LEADER_EPOCH)))
            ));
        }

        private record PendingLookup(ListOffsetsRequest request,
                                     CompletableFuture<OffsetResultHolder.FileRecordsOrError> future,
                                     boolean cacheCrossTierEarliest) {
        }
    }
}
