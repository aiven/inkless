// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.mockito.invocation.Invocation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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

import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.control_plane.TopicMetadataChangesSubscriber;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;

@Tag("integration")
class WriterPropertyTest {
    private static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    private static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    private static final TopicPartition T1P0 = new TopicPartition("topic1", 0);
    private static final TopicPartition T1P1 = new TopicPartition("topic1", 1);

    private static final Set<TopicPartition> ALL_TPS = Set.of(T0P0, T0P1, T1P0, T1P1);

    static final MetadataView METADATA_VIEW = new MetadataView() {
        private final Map<String, Uuid> uuids = Map.of(
            T0P0.topic(), new Uuid(0, 1),
            T1P0.topic(), new Uuid(0, 2)
        );

        @Override
        public Set<TopicPartition> getTopicPartitions(final String topicName) {
            return ALL_TPS;
        }

        @Override
        public Uuid getTopicId(final String topicName) {
            return uuids.get(topicName);
        }

        @Override
        public boolean isInklessTopic(final String topicName) {
            return true;
        }

        @Override
        public LogConfig getTopicConfig(final String topicName) {
            return LogConfig.fromProps(Map.of(), new Properties());
        }

        @Override
        public void subscribeToTopicMetadataChanges(final TopicMetadataChangesSubscriber subscriber) {
            // We don't create/delete topics/partitions, so this is no-op.
        }
    };

    @Property(tries = 1000)
    void testInMemoryControlPlane(@ForAll @IntRange(max = 100) int requestCount,
              @ForAll @IntRange(min = 1, max = 10) int requestIntervalMsAvg,
              @ForAll @IntRange(min = 1, max = 100) int commitIntervalMsAvg,
              @ForAll @IntRange(min = 10, max = 30) int uploadDurationAvg,
              @ForAll @IntRange(min = 5, max = 10) int commitDurationAvg,
              @ForAll @IntRange(min = 1, max = 1 * 1024) int maxBufferSize) throws InterruptedException, ExecutionException, StorageBackendException {
        test(requestCount, requestIntervalMsAvg, commitIntervalMsAvg, uploadDurationAvg, commitDurationAvg, maxBufferSize,
            new InMemoryControlPlane(new MockTime(0, 0, 0), METADATA_VIEW));
    }

    void test(final int requestCount,
              final int requestIntervalMsAvg,
              final int commitIntervalMsAvg,
              final int uploadDurationAvg,
              final int commitDurationAvg,
              final int maxBufferSize,
              final ControlPlane controlPlane) throws InterruptedException, ExecutionException, StorageBackendException {
        Statistics.label("requestCount").collect(requestCount);
        final MockTime time = new MockTime(0, 0, 0);

        final ObjectUploader objectUploader = mock(ObjectUploader.class);
        final UploaderHandler uploaderHandler = new UploaderHandler(
            new MockExecutorServiceWithFutureSupport(),
            new Timer("upload",
                time,
                Instant.ofEpochMilli(time.milliseconds()),
                Arbitraries.longs().between(uploadDurationAvg - 5, uploadDurationAvg + 5))
        );
        final CommitterHandler committerHandler = new CommitterHandler(
            uploaderHandler,
            new MockExecutorService(),
            new Timer("commit",
                time,
                Instant.ofEpochMilli(time.milliseconds()),
                Arbitraries.longs().between(commitDurationAvg - 2, commitDurationAvg + 2))
        );
        final FileCommitter fileCommitter = new FileCommitter(
            controlPlane,
            (String s) -> new PlainObjectKey("", s),
            objectUploader,
            time,
            1,
            Duration.ZERO,
            uploaderHandler.executorService,
            committerHandler.executorService,
            mock(FileCommitterMetrics.class)
        );

        final Writer writer = new Writer(
            time,
            Duration.ofMillis(commitIntervalMsAvg),  // it doesn't matter as the scheduling doesn't happen
            maxBufferSize,
            mock(ScheduledExecutorService.class),
            fileCommitter,
            new BrokerTopicStats()
        );

        final Arbitrary<Map<TopicPartition, MemoryRecords>> requestArbitrary = requests();
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
            time.sleep(1);
        }
        assertThat(finished).withFailMessage(String.format("Not finished in %d virtual ms", maxTime)).isTrue();
        requester.checkResponses();

        if (requestCount > 0) {
            verify(objectUploader, atLeast(1)).upload(any(), any());
        }
        final Collection<Invocation> uploadInvocations = mockingDetails(objectUploader).getInvocations();
        Statistics.label("files").collect(uploadInvocations.size());
        for (final Invocation invocation : uploadInvocations) {
            final byte[] uploadedBytes = invocation.getArgument(1);
            Statistics.label("file-size").collect(uploadedBytes.length);
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
        private final Arbitrary<Map<TopicPartition, MemoryRecords>> writeRequestArbitrary;
        private final int maxRequestCount;
        private int requestCount = 0;
        private final Map<TopicPartition, List<MemoryRecords>> sentRequests = new HashMap<>();
        private List<CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>>> waitingResponseFutures =
            new ArrayList<>();
        private final Map<TopicPartition, List<Long>> assignedOffsets = new HashMap<>();

        private final Timer timer;

        private Requester(final Writer writer,
                          final Arbitrary<Map<TopicPartition, MemoryRecords>> writeRequestArbitrary,
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
                final var responseFuture = writer.write(request);
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
                        assignedOffsets.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                            .add(entry.getValue().baseOffset);
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
                    .computeIfAbsent(entry.getKey(), ignore -> new ArrayList<>());

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
        private final MockExecutorService executorService;
        private final Timer timer;

        private CommitterHandler(final UploaderHandler uploaderHandler,
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

    private Arbitrary<Map<TopicPartition, MemoryRecords>> requests() {
        final Arbitrary<MemoryRecords> memoryRecordsArbitrary = memoryRecords();
        return Arbitraries.subsetOf(ALL_TPS).map(tps -> {
            final Map<TopicPartition, MemoryRecords> result = new HashMap<>();
            for (final TopicPartition tp : tps) {
                result.put(tp, memoryRecordsArbitrary.sample());
            }
            return result;
        });
    }

    private Arbitrary<MemoryRecords> memoryRecords() {
        final Arbitrary<byte[]> keyOrValueArbitrary = recordKeyOrValue();

        return Arbitraries.integers().between(1, 100).map(recordCount -> {
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
            .ofMinSize(0)
            .ofMaxSize(100);
    }
}
