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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import net.jqwik.api.statistics.Statistics;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.mockito.invocation.Invocation;
import org.testcontainers.junit.jupiter.Container;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;

@Tag("integration")
class WriterPropertyTest {
    @Container
    private static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private static final String TOPIC_0 = "topic0";
    private static final String TOPIC_1 = "topic1";
    private static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    private static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    private static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    private static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    private static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    private static final TopicIdPartition T1P1 = new TopicIdPartition(TOPIC_ID_1, 1, TOPIC_1);
    static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    static final ObjectCache OBJECT_CACHE = new NullCache();

    static final Map<String, LogConfig> TOPIC_CONFIGS = Map.of(
        TOPIC_0, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name)),
        TOPIC_1, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name))
    );

    static LogConfig logConfig(Map<String, ?> config) {
        return new LogConfig(config);
    }

    private static final Set<TopicIdPartition> ALL_TPS = Set.of(T0P0, T0P1, T1P0, T1P1);

    @BeforeContainer
    static void setUp() {
        pgContainer.start();
    }

    @AfterContainer
    static void tearDown() {
        pgContainer.stop();
    }

    // increase when ci is beefier
    @Property(tries = 500)
    void testInMemoryControlPlane(@ForAll @IntRange(max = 100) int requestCount,
                                  @ForAll @IntRange(min = 1, max = 10) int requestIntervalMsAvg,
                                  @ForAll @IntRange(min = 1, max = 100) int commitIntervalMsAvg,
                                  @ForAll @IntRange(min = 10, max = 30) int uploadDurationAvg,
                                  @ForAll @IntRange(min = 5, max = 10) int commitDurationAvg,
                                  @ForAll @IntRange(min = 5, max = 10) int completeDurationAvg,
                                  @ForAll @IntRange(min = 5, max = 10) int cacheStoreAvg,
                                  @ForAll @IntRange(min = 1, max = 1 * 1024) int maxBufferSize) throws Exception {
        try (final ControlPlane controlPlane = new InMemoryControlPlane(new MockTime(0, 0, 0))) {
            controlPlane.configure(Map.of());
            test(requestCount, requestIntervalMsAvg, commitIntervalMsAvg, uploadDurationAvg, commitDurationAvg, completeDurationAvg, cacheStoreAvg, maxBufferSize, controlPlane);
        }
    }

    // smaller than in-mem as it require more machinery to be setup in between
    @Property(tries = 100)
    void testPostgresControlPlane(@ForAll @IntRange(max = 100) int requestCount,
                                  @ForAll @IntRange(min = 1, max = 10) int requestIntervalMsAvg,
                                  @ForAll @IntRange(min = 1, max = 100) int commitIntervalMsAvg,
                                  @ForAll @IntRange(min = 10, max = 30) int uploadDurationAvg,
                                  @ForAll @IntRange(min = 5, max = 10) int commitDurationAvg,
                                  @ForAll @IntRange(min = 5, max = 10) int completeDurationAvg,
                                  @ForAll @IntRange(min = 5, max = 10) int cacheStoreAvg,
                                  @ForAll @IntRange(min = 1, max = 1 * 1024) int maxBufferSize) throws Exception {
        String dbName = "test-" + requestCount
            + "-" + requestIntervalMsAvg
            + "-" + commitIntervalMsAvg
            + "-" + uploadDurationAvg
            + "-" + commitDurationAvg
            + "-" + maxBufferSize
            + "_" + TestUtils.randomString(5);
        dbName = dbName.toLowerCase();

        pgContainer.createDatabase(dbName);

        try (final ControlPlane controlPlane = new PostgresControlPlane(new MockTime(0, 0, 0))) {
            controlPlane.configure(Map.of(
                "connection.string", pgContainer.getUserJdbcUrl(),
                "username", pgContainer.getUsername(),
                "password", pgContainer.getPassword()
            ));

            test(requestCount, requestIntervalMsAvg, commitIntervalMsAvg, uploadDurationAvg, commitDurationAvg, completeDurationAvg, cacheStoreAvg, maxBufferSize, controlPlane);
        }
    }

    void test(final int requestCount,
              final int requestIntervalMsAvg,
              final int commitIntervalMsAvg,
              final int uploadDurationAvg,
              final int commitDurationAvg,
              final int completeDurationAvg,
              final int cacheStoreDurationAvg,
              final int maxBufferSize,
              final ControlPlane controlPlane) throws InterruptedException, ExecutionException, StorageBackendException {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, T0P0.topic(), 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, T1P0.topic(), 2)
        );
        controlPlane.createTopicAndPartitions(createTopicAndPartitionsRequests);

        Statistics.label("requestCount").collect(requestCount);
        final MockTime time = new MockTime(0, 0, 0);

        final StorageBackend storage = mock(StorageBackend.class);
        final UploaderHandler uploaderHandler = new UploaderHandler(
            new MockExecutorServiceWithFutureSupport(),
            new Timer("upload",
                time,
                Instant.ofEpochMilli(time.milliseconds()),
                Arbitraries.longs().between(uploadDurationAvg - 5, uploadDurationAvg + 5))
        );
        final CommitterHandler committerHandler = new CommitterHandler(
            uploaderHandler,
            new MockExecutorServiceWithFutureSupport(),
            new Timer("commit",
                time,
                Instant.ofEpochMilli(time.milliseconds()),
                Arbitraries.longs().between(commitDurationAvg - 2, commitDurationAvg + 2))
        );
        final CompleterHandler completerHandler = new CompleterHandler(
                committerHandler,
                new MockExecutorServiceWithFutureSupport(),
                new Timer("complete",
                        time,
                        Instant.ofEpochMilli(time.milliseconds()),
                        Arbitraries.longs().between(completeDurationAvg - 2, completeDurationAvg + 2))
        );
        final CacheStoreHandler cacheStoreHandler = new CacheStoreHandler(
                uploaderHandler,
                new MockExecutorServiceWithFutureSupport(),
                new Timer("cacheStore",
                        time,
                        Instant.ofEpochMilli(time.milliseconds()),
                        Arbitraries.longs().between(cacheStoreDurationAvg - 2, cacheStoreDurationAvg + 2))
        );
        final FileCommitter fileCommitter = new FileCommitter(
            11,
            controlPlane,
            ObjectKey.creator("", false),
            storage,
            KEY_ALIGNMENT_STRATEGY,
            OBJECT_CACHE,
            time,
            1,
            Duration.ZERO,
            uploaderHandler.executorService,
            committerHandler.executorService,
            cacheStoreHandler.executorService,
            mock(FileCommitterMetrics.class)
        );

        final Writer writer = new Writer(
            time,
            Duration.ofMillis(commitIntervalMsAvg),  // it doesn't matter as the scheduling doesn't happen
            maxBufferSize,
            mock(ScheduledExecutorService.class),
            storage,
            fileCommitter,
            mock(WriterMetrics.class),
            new BrokerTopicStats()
        );

        final Arbitrary<Map<TopicIdPartition, MemoryRecords>> requestArbitrary = requests();
        final Requester requester = new Requester(
            writer, requestArbitrary, requestCount,
            new Timer("request",
                time, Instant.MIN, Arbitraries.longs().between(requestIntervalMsAvg - 5, requestIntervalMsAvg + 5)
            )
        );
        final CommitTicker commitTicker = new CommitTicker(
            writer,
            new Timer("commit-tick",
                time,
                Instant.ofEpochMilli(time.milliseconds()),
                Arbitraries.longs().between(commitIntervalMsAvg - 5, commitIntervalMsAvg + 5))
        );

        boolean finished = false;
        final int maxTime = 10_000;
        while (time.milliseconds() < maxTime) {
            if (requester.allRequestsSent() && requester.allRequestsFinished()) {
                finished = true;
                break;
            }

            requester.maybeSendRequest();
            requester.handleFinishedRequests();
            commitTicker.maybeTick();
            uploaderHandler.maybeRunNext();
            committerHandler.maybeRunNext();
            completerHandler.maybeRunNext();
            cacheStoreHandler.maybeRunNext();
            time.sleep(1);
        }
        assertThat(finished).withFailMessage(String.format("Not finished in %d virtual ms", maxTime)).isTrue();
        requester.checkResponses();

        if (requestCount > 0) {
            verify(storage, atLeast(1)).upload(any(ObjectKey.class), any(InputStream.class), anyLong());
        }
        final Collection<Invocation> uploadInvocations = mockingDetails(storage).getInvocations();
        Statistics.label("files").collect(uploadInvocations.size());
        for (final Invocation invocation : uploadInvocations) {
            final long uploadedBytesLength = invocation.getArgument(2);
            Statistics.label("file-size").collect(uploadedBytesLength);
        }
    }

    private static class Timer {
        private final String name;
        private final Time time;
        private final Arbitrary<Long> intervalArbitrary;
        private Instant prevTick;
        private Instant nextTick;

        Timer(final String name,
              final Time time,
              final Instant prevTick,
              final Arbitrary<Long> intervalArbitrary) {
            this.name = name;
            this.time = time;
            this.intervalArbitrary = intervalArbitrary;
            this.prevTick = prevTick;
            setNextTick();
        }

        boolean happensNow() {
            final Instant now = Instant.ofEpochMilli(time.milliseconds());
            if (now.equals(nextTick) || now.isAfter(nextTick)) {
                prevTick = now;
                setNextTick();
                return true;
            } else {
                return false;
            }
        }

        private void setNextTick() {
            long interval = Math.max(1, intervalArbitrary.sample());
            this.nextTick = prevTick.plusMillis(interval);
        }

        @Override
        public String toString() {
            return String.format("Timer[nextTick=%d]", nextTick.toEpochMilli());
        }
    }

    private static class Requester {
        private final Writer writer;
        private final Arbitrary<Map<TopicIdPartition, MemoryRecords>> writeRequestArbitrary;
        private final int maxRequestCount;
        private int requestCount = 0;
        private final Map<TopicIdPartition, List<MemoryRecords>> sentRequests = new HashMap<>();
        private List<CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>>> waitingResponseFutures =
            new ArrayList<>();
        private final Map<TopicPartition, List<Long>> assignedOffsets = new HashMap<>();
        private final Map<TopicPartition, List<Errors>> errors = new HashMap<>();

        private final Timer timer;

        private Requester(final Writer writer,
                          final Arbitrary<Map<TopicIdPartition, MemoryRecords>> writeRequestArbitrary,
                          final int maxRequestCount,
                          final Timer timer) {
            this.writer = writer;
            this.writeRequestArbitrary = writeRequestArbitrary;
            this.maxRequestCount = maxRequestCount;
            this.timer = timer;
        }

        void maybeSendRequest() {
            if (requestCount < maxRequestCount && timer.happensNow()) {
                final var request = writeRequestArbitrary.sample();
                for (final var entry : request.entrySet()) {
                    sentRequests.computeIfAbsent(entry.getKey(), ignore -> new ArrayList<>())
                        .add(entry.getValue());
                }
                final var responseFuture = writer.write(request, TOPIC_CONFIGS, RequestLocal.noCaching());
                waitingResponseFutures.add(responseFuture);
                requestCount += 1;
            }
        }

        void handleFinishedRequests() throws ExecutionException, InterruptedException {
            final List<CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>>> newWaitingResponseFutures =
                new ArrayList<>();
            for (final var f : waitingResponseFutures) {
                if (f.isDone()) {
                    for (final var entry : f.get().entrySet()) {
                        final ProduceResponse.PartitionResponse value = entry.getValue();
                        assignedOffsets.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                            .add(value.baseOffset);
                        if (value.error != Errors.NONE) {
                            errors.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                                .add(value.error);
                        }
                    }
                } else {
                    newWaitingResponseFutures.add(f);
                }
            }
            this.waitingResponseFutures = newWaitingResponseFutures;
            Statistics.label("waiting-response-futures").collect(waitingResponseFutures.size());
        }

        public boolean allRequestsSent() {
            assert requestCount <= maxRequestCount;
            return requestCount == maxRequestCount;
        }

        public boolean allRequestsFinished() {
            return waitingResponseFutures.isEmpty();
        }

        void checkResponses() {
            final Map<TopicPartition, List<Long>> expectedAssignedOffsets = new HashMap<>();
            for (final var entry : sentRequests.entrySet()) {
                final List<Long> offsets = expectedAssignedOffsets
                    .computeIfAbsent(entry.getKey().topicPartition(), ignore -> new ArrayList<>());

                offsets.add(0L);  // first is always 0
                for (int i = 0; i < entry.getValue().size() - 1; i++) {
                    final MemoryRecords prevRecords = entry.getValue().get(i);
                    int recordCount = (prevRecords.firstBatch() != null && prevRecords.firstBatch().countOrNull() != null)
                        ? prevRecords.firstBatch().countOrNull()
                        : 0;
                    final long expectedOffset = offsets.get(offsets.size() - 1) + recordCount;
                    offsets.add(expectedOffset);
                }
            }

            assertThat(errors).isEmpty();
            assertThat(assignedOffsets).isEqualTo(expectedAssignedOffsets);
            for (final List<MemoryRecords> recordList : sentRequests.values()) {
                Statistics.label("requests-per-topic-partition").collect(recordList.size());
                for (MemoryRecords records : recordList) {
                    Statistics.label("bytes-per-request").collect(records.sizeInBytes());
                }
            }
        }

        @Override
        public String toString() {
            return String.format("Requester[requests=%d, waiting=%d]", requestCount, waitingResponseFutures.size());
        }
    }

    private static class CommitTicker {
        private final Writer writer;
        private final Timer timer;

        private CommitTicker(final Writer writer,
                             final Timer timer) {
            this.writer = writer;
            this.timer = timer;
        }

        public void maybeTick() {
            if (timer.happensNow()) {
                writer.tick();
            }
        }
    }

    private static class MockExecutorService implements ExecutorService {
        protected final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        @Override
        public void execute(final Runnable command) {
            queue.offer(command);
        }

        boolean runNextIfExists() throws InterruptedException {
            final Runnable nextRunnable = queue.poll();
            if (nextRunnable != null) {
                nextRunnable.run();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        /* Not implemented functions below */

        @Override
        public @NotNull <T> Future<T> submit(@NotNull final Callable<T> task) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public @NotNull Future<?> submit(@NotNull final Runnable task) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public <T> Future<T> submit(final Runnable task, final T result) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks,
                                             final long timeout,
                                             final TimeUnit unit) throws InterruptedException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public <T> T invokeAny(final Collection<? extends Callable<T>> tasks,
                               final long timeout,
                               final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public boolean isShutdown() {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public boolean isTerminated() {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            throw new RuntimeException("Not implemented");
        }
    }

    private static class MockExecutorServiceWithFutureSupport extends MockExecutorService {
        private final LinkedBlockingQueue<Future<?>> returnedFutures = new LinkedBlockingQueue<>();

        @Override
        public void execute(final Runnable command) {
            this.submit(command);
        }

        @Override
        public Future<?> submit(final Runnable task) {
            return this.submit(() -> {
                task.run();
                return null;
            });
        }

        @Override
        public <T> Future<T> submit(final Callable<T> task) {
            final var result = new CompletableFuture<T>();
            returnedFutures.offer(result);
            queue.offer(() -> {
                try {
                    result.complete(task.call());
                } catch (final Exception e) {
                    result.completeExceptionally(e);
                }
            });
            return result;
        }

        @Override
        boolean runNextIfExists() throws InterruptedException {
            assertThat(returnedFutures.size()).isEqualTo(queue.size());
            final boolean result = super.runNextIfExists();
            if (result) {
                assert returnedFutures.take().isDone();
            }
            return result;
        }
    }

    private static class UploaderHandler {
        private final MockExecutorServiceWithFutureSupport executorService;
        private final Timer timer;

        private UploaderHandler(final MockExecutorServiceWithFutureSupport executorService,
                                final Timer timer) {
            this.executorService = executorService;
            this.timer = timer;
        }

        void maybeRunNext() throws InterruptedException {
            if (timer.happensNow()) {
                executorService.runNextIfExists();
            }
        }

        boolean oldestFutureIsDone() {
            return Optional.ofNullable(executorService.returnedFutures.peek())
                .map(Future::isDone)
                .orElse(true);
        }
    }

    private static class CommitterHandler {
        private final UploaderHandler uploaderHandler;
        private final MockExecutorServiceWithFutureSupport executorService;
        private final Timer timer;

        private CommitterHandler(final UploaderHandler uploaderHandler,
                                 final MockExecutorServiceWithFutureSupport executorService,
                                 final Timer timer) {
            this.uploaderHandler = uploaderHandler;
            this.executorService = executorService;
            this.timer = timer;
        }

        void maybeRunNext() throws InterruptedException {
            if (!timer.happensNow()) {
                return;
            }
            if (!uploaderHandler.oldestFutureIsDone()) {
                // Otherwise it'd block indefinitely.
                return;
            }
            executorService.runNextIfExists();
        }

        boolean oldestFutureIsDone() {
            return uploaderHandler.oldestFutureIsDone()
                    && Optional.ofNullable(executorService.returnedFutures.peek())
                    .map(Future::isDone)
                    .orElse(true);
        }
    }

    private static class CompleterHandler {
        private final CommitterHandler committerHandler;
        private final MockExecutorService executorService;
        private final Timer timer;

        private CompleterHandler(final CommitterHandler committerHandler,
                                 final MockExecutorService executorService,
                                 final Timer timer) {
            this.committerHandler = committerHandler;
            this.executorService = executorService;
            this.timer = timer;
        }

        void maybeRunNext() throws InterruptedException {
            if (!timer.happensNow()) {
                return;
            }
            if (!committerHandler.oldestFutureIsDone()) {
                // Otherwise it'd block indefinitely.
                return;
            }
            executorService.runNextIfExists();
        }
    }

    private static class CacheStoreHandler {
        private final UploaderHandler uploaderHandler;
        private final MockExecutorService executorService;
        private final Timer timer;

        private CacheStoreHandler(final UploaderHandler uploaderHandler,
                                 final MockExecutorService executorService,
                                 final Timer timer) {
            this.uploaderHandler = uploaderHandler;
            this.executorService = executorService;
            this.timer = timer;
        }

        void maybeRunNext() throws InterruptedException {
            if (!timer.happensNow()) {
                return;
            }
            if (!uploaderHandler.oldestFutureIsDone()) {
                // Otherwise it'd block indefinitely.
                return;
            }
            executorService.runNextIfExists();
        }
    }

    private Arbitrary<Map<TopicIdPartition, MemoryRecords>> requests() {
        final Arbitrary<MemoryRecords> memoryRecordsArbitrary = memoryRecords();
        return Arbitraries.subsetOf(ALL_TPS)
            // Empty requests are not allowed at the writer level. They are filtered out at the KafkaApis level.
            .ofMinSize(1)
            .map(tps -> {
                final Map<TopicIdPartition, MemoryRecords> result = new HashMap<>();
                for (final TopicIdPartition tp : tps) {
                    result.put(tp, memoryRecordsArbitrary.sample());
                }
                return result;
            });
    }

    private Arbitrary<MemoryRecords> memoryRecords() {
        final Arbitrary<byte[]> keyOrValueArbitrary = recordKeyOrValue();

        return Arbitraries.integers()
            .between(1, 100)
            .map(recordCount -> {
                final SimpleRecord[] records = new SimpleRecord[recordCount];
                for (int i = 0; i < recordCount; i++) {
                    records[i] = new SimpleRecord(0, keyOrValueArbitrary.sample(), keyOrValueArbitrary.sample());
                }
                return MemoryRecords.withRecords(Compression.NONE, records);
            });
    }

    private Arbitrary<byte[]> recordKeyOrValue() {
        return Arbitraries.bytes()
            .array(byte[].class)
            .ofMinSize(1)
            .ofMaxSize(100);
    }
}
