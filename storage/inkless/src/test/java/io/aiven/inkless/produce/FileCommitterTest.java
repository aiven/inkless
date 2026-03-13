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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.CaffeineBatchCoordinateCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileCommitterTest {

    static final int BROKER_ID = 11;
    static final ObjectKey OBJECT_KEY = PlainObjectKey.create("prefix", "value");
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = new ObjectKeyCreator("prefix") {
        @Override
        public ObjectKey from(String value) {
            return OBJECT_KEY;
        }

        @Override
        public ObjectKey create(String value) {
            return OBJECT_KEY;
        }
    };
    static final TopicIdPartition TID0P0 = new TopicIdPartition(Uuid.randomUuid(), 0, "t0");
    static final ClosedFile FILE = new ClosedFile(
        Instant.EPOCH,
        Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
        Map.of(1, new CompletableFuture<>()),
        List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
        Map.of(),
        new byte[10]);
    static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    static final ObjectCache OBJECT_CACHE = new NullCache();
    static final BatchCoordinateCache BATCH_COORDINATE_CACHE = new CaffeineBatchCoordinateCache(Duration.ofSeconds(30));

    @Mock
    ControlPlane controlPlane;
    @Mock
    StorageBackend storage;
    @Mock
    Time time;
    @Mock
    FileCommitterMetrics metrics;

    // Use real executors that run tasks immediately for testing async flow
    private ExecutorService executorServiceUpload;
    private ExecutorService executorServiceCommit;
    private ExecutorService executorServiceCacheStore;

    @BeforeEach
    void setUp() {
        // Single-threaded real executors for deterministic ordering in tests.
        // Tasks still execute asynchronously on background threads, but one at a time per executor.
        executorServiceUpload = Executors.newSingleThreadExecutor();
        executorServiceCommit = Executors.newSingleThreadExecutor();
        executorServiceCacheStore = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        executorServiceUpload.shutdownNow();
        executorServiceCommit.shutdownNow();
        executorServiceCacheStore.shutdownNow();
    }

    @Test
    void success() throws Exception {
        doNothing()
            .when(storage).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) FILE.data().length));
        when(controlPlane.commitFile(any(), any(), anyInt(), anyLong(), any()))
            .thenReturn(List.of());

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
                metrics);

        verify(metrics).initTotalFilesInProgressMetric(any());
        verify(metrics).initTotalBytesInProgressMetric(any());

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        committer.commit(FILE);

        // Wait for async operations to complete - writeCompleted is the final callback
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(metrics).fileAdded(eq(FILE.size()));
            verify(metrics).fileUploadFinished(anyLong());
            verify(metrics).fileCommitFinished(anyLong());
            verify(metrics).fileCommitWaitFinished(anyLong());
            verify(metrics).fileFinished(eq(Instant.EPOCH), any());
            verify(metrics).writeCompleted();
        });
    }

    @Test
    void commitFailed() throws Exception {
        doNothing()
            .when(storage).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) FILE.data().length));

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        when(controlPlane.commitFile(any(), any(), anyInt(), anyLong(), any()))
            .thenThrow(new ControlPlaneException("error"));

        final FileCommitter committer = new FileCommitter(
            BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
            KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
            1, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
            metrics);

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        committer.commit(FILE);

        // Wait for async operations to complete - writeFailed is the final callback
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(metrics).fileAdded(eq(FILE.size()));
            verify(metrics).fileUploadFinished(anyLong());
            verify(metrics).fileCommitFinished(anyLong());
            verify(metrics, times(0)).fileFinished(any(), any());
            verify(metrics).fileCommitFailed();
            verify(metrics).writeFailed();
        });
    }

    @Test
    void uploadFailed() throws Exception {
        doThrow(new StorageBackendException("test"))
            .when(storage).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) FILE.data().length));

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                1, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
                metrics);

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        committer.commit(FILE);

        // Wait for async operations to complete - writeFailed is the final callback
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(metrics).fileAdded(eq(FILE.size()));
            verify(metrics).fileUploadFinished(anyLong());
            // fileCommitFinished is NOT called when upload fails because thenApplyAsync
            // doesn't run the commit job on failure
            verify(metrics, times(0)).fileCommitFinished(anyLong());
            verify(metrics, times(0)).fileFinished(any(), any());
            verify(metrics).fileUploadFailed();
            verify(metrics).writeFailed();
        });
    }

    @Test
    void closeGraceful() throws IOException, InterruptedException {
        // Use mock executors for this test since we're testing shutdown behavior
        ExecutorService mockUpload = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCommit = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCacheStore = org.mockito.Mockito.mock(ExecutorService.class);

        when(mockCommit.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                mockUpload, mockCommit, mockCacheStore, metrics);

        committer.close();

        // Upload pool rejects new work immediately.
        verify(mockUpload).shutdown();
        // Cache is best-effort, cancelled immediately.
        verify(mockCacheStore).shutdownNow();
        // Commits are awaited (they internally wait for their paired uploads).
        verify(mockCommit).awaitTermination(30, TimeUnit.SECONDS);
        // Graceful termination: no force-shutdown needed on commit pool.
        verify(mockCommit, never()).shutdownNow();
        // Remaining uploads with no queued commit are force-stopped.
        verify(mockUpload).shutdownNow();
        verify(metrics).close();
    }

    @Test
    void closeCommitPoolTimesOutThenTerminates() throws IOException, InterruptedException {
        ExecutorService mockUpload = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCommit = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCacheStore = org.mockito.Mockito.mock(ExecutorService.class);

        // First await returns false (timeout), after shutdownNow second await succeeds.
        when(mockCommit.awaitTermination(30, TimeUnit.SECONDS))
            .thenReturn(false)
            .thenReturn(true);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                mockUpload, mockCommit, mockCacheStore, metrics);

        committer.close();

        final InOrder commitOrder = inOrder(mockCommit);
        // Commit pool: shutdown → await → timeout → shutdownNow → await again.
        commitOrder.verify(mockCommit).shutdown();
        commitOrder.verify(mockCommit).awaitTermination(30, TimeUnit.SECONDS);
        commitOrder.verify(mockCommit).shutdownNow();
        commitOrder.verify(mockCommit).awaitTermination(30, TimeUnit.SECONDS);

        verify(mockUpload).shutdownNow();
        verify(metrics).close();
    }

    @Test
    void closeCommitPoolNeverTerminates() throws IOException, InterruptedException {
        ExecutorService mockUpload = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCommit = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCacheStore = org.mockito.Mockito.mock(ExecutorService.class);

        // Both awaits return false — pool never terminates.
        when(mockCommit.awaitTermination(30, TimeUnit.SECONDS))
            .thenReturn(false)
            .thenReturn(false);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                mockUpload, mockCommit, mockCacheStore, metrics);

        committer.close();

        verify(mockCommit).shutdownNow();
        // Cleanup still completes despite commit pool not terminating.
        verify(mockUpload).shutdownNow();
        verify(metrics).close();
    }

    @Test
    void closeInterruptedDuringAwait() throws IOException, InterruptedException {
        ExecutorService mockUpload = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCommit = org.mockito.Mockito.mock(ExecutorService.class);
        ExecutorService mockCacheStore = org.mockito.Mockito.mock(ExecutorService.class);

        when(mockCommit.awaitTermination(30, TimeUnit.SECONDS))
            .thenThrow(new InterruptedException("shutdown interrupted"));

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                mockUpload, mockCommit, mockCacheStore, metrics);

        committer.close();

        // On interrupt: commit pool is force-shutdown.
        verify(mockCommit).shutdownNow();
        // Rest of cleanup still runs.
        verify(mockUpload).shutdownNow();
        verify(metrics).close();
        // Interrupt flag is preserved.
        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, null, OBJECT_KEY_CREATOR,
                storage, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("controlPlane cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, null, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKeyCreator cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, null,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("storage cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    null, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("keyAlignmentStrategy cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, null, BATCH_COORDINATE_CACHE, time,
                    100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectCache cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, null, time,
                100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batchCoordinateCache cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, null,
                    100, Duration.ofMillis(1), 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    0, Duration.ofMillis(1), 8))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("maxFileUploadAttempts must be positive");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    100, null, 8))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fileUploadRetryBackoff cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    3, Duration.ofMillis(100),
                    null, executorServiceCommit, executorServiceCacheStore, metrics))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceUpload cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    3, Duration.ofMillis(100),
                    executorServiceUpload, null, executorServiceCacheStore, metrics))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceCommit cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    3, Duration.ofMillis(100),
                    executorServiceUpload, executorServiceCommit, null, metrics))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceCacheStore cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    3, Duration.ofMillis(100),
                    executorServiceUpload, executorServiceCommit, executorServiceCacheStore, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("metrics cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(1), 0)) // pool size has to be positive
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void commitNull() {
        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE,
                time, 3, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore, metrics);
        assertThatThrownBy(() -> committer.commit(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("file cannot be null");
    }

    @Test
    void commitsInSubmissionOrder() throws Exception {
        // Commits are guaranteed to happen in submission order even if upload2 completes before upload1.
        // This is critical for correct offset assignment.

        final CountDownLatch upload1Started = new CountDownLatch(1);
        final CountDownLatch upload2Completed = new CountDownLatch(1);
        final List<Integer> commitOrder = new ArrayList<>();

        // File 1 upload will wait until file 2 upload completes
        doAnswer(invocation -> {
            upload1Started.countDown();
            upload2Completed.await();
            Thread.sleep(50);  // Ensure upload2 definitely completes first
            return null;
        }).doAnswer(invocation -> {
            upload2Completed.countDown();
            return null;
        }).when(storage).upload(any(), any(InputStream.class), anyLong());

        // Track commit order by capturing the requestId from the CommitBatchRequest
        when(controlPlane.commitFile(any(), any(), anyInt(), anyLong(), any()))
            .thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                List<CommitBatchRequest> requests = (List<CommitBatchRequest>) invocation.getArgument(4);
                synchronized (commitOrder) {
                    // Record which file committed (by its requestId)
                    commitOrder.add(requests.get(0).requestId());
                }
                return List.of();
            });

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        ExecutorService multiThreadUpload = Executors.newFixedThreadPool(2);

        try {
            final FileCommitter committer = new FileCommitter(
                    BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                    KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                    1, Duration.ofMillis(100),
                    multiThreadUpload, executorServiceCommit, executorServiceCacheStore,
                    metrics);

            final ClosedFile file1 = new ClosedFile(
                Instant.EPOCH,
                Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
                Map.of(1, new CompletableFuture<>()),
                List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
                Map.of(),
                new byte[10]);

            final ClosedFile file2 = new ClosedFile(
                Instant.EPOCH,
                Map.of(2, Map.of(TID0P0, MemoryRecords.EMPTY)),
                Map.of(2, new CompletableFuture<>()),
                List.of(CommitBatchRequest.of(2, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
                Map.of(),
                new byte[10]);

            committer.commit(file1);
            upload1Started.await();
            committer.commit(file2);

            await().atMost(5, SECONDS).untilAsserted(() -> {
                synchronized (commitOrder) {
                    assertThat(commitOrder).hasSize(2);
                }
            });

            // Verify submission order is maintained
            synchronized (commitOrder) {
                assertThat(commitOrder).containsExactly(1, 2);
            }
        } finally {
            multiThreadUpload.shutdownNow();
        }
    }

    @Test
    void failedCommitDoesNotBlockSubsequentCommits() throws Exception {
        // Verify that if the first commit fails (e.g., control plane error),
        // the second commit can still succeed. This ensures error isolation.

        final CountDownLatch firstCommitFailed = new CountDownLatch(1);
        final CountDownLatch secondCommitStarted = new CountDownLatch(1);

        doNothing().when(storage).upload(any(), any(InputStream.class), anyLong());

        // First commit fails with control plane exception, second succeeds
        when(controlPlane.commitFile(any(), any(), anyInt(), anyLong(), any()))
            .thenAnswer(invocation -> {
                firstCommitFailed.countDown();
                throw new ControlPlaneException("first commit error");
            })
            .thenAnswer(invocation -> {
                secondCommitStarted.countDown();
                return List.of();
            });

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                1, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
                metrics);

        final ClosedFile file1 = new ClosedFile(
            Instant.EPOCH,
            Map.of(1, Map.of(TID0P0, MemoryRecords.EMPTY)),
            Map.of(1, new CompletableFuture<>()),
            List.of(CommitBatchRequest.of(1, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            Map.of(),
            new byte[10]);

        final ClosedFile file2 = new ClosedFile(
            Instant.EPOCH,
            Map.of(2, Map.of(TID0P0, MemoryRecords.EMPTY)),
            Map.of(2, new CompletableFuture<>()),
            List.of(CommitBatchRequest.of(2, TID0P0, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            Map.of(),
            new byte[10]);

        committer.commit(file1);
        committer.commit(file2);

        // Wait for both to complete
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(metrics).writeFailed();  // first commit failed
            verify(metrics).writeCompleted();  // second commit succeeded
        });

        // Verify the second commit was actually attempted
        assertThat(secondCommitStarted.await(1, SECONDS)).isTrue();
    }

    @Test
    void asyncModeCloseWhileUploadInFlightStillCommits() throws Exception {
        // Verify that in async mode, calling close() while an upload is still in-flight
        // does not cause the commit to be rejected - the close() method awaits pending work.

        final CountDownLatch uploadStarted = new CountDownLatch(1);
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final CountDownLatch commitCompleted = new CountDownLatch(1);

        // Upload will block until we signal it to proceed after close() starts
        doAnswer(invocation -> {
            uploadStarted.countDown();
            // Wait for close() to be called
            closeStarted.await();
            // Give close() a moment to start waiting for pending futures
            Thread.sleep(100);
            return null;
        }).when(storage).upload(any(), any(InputStream.class), anyLong());

        when(controlPlane.commitFile(any(), any(), anyInt(), anyLong(), any()))
            .thenAnswer(invocation -> {
                commitCompleted.countDown();
                return List.of();
            });

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                1, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
                metrics);  // async mode

        // Submit a file for commit
        committer.commit(FILE);

        // Wait for upload to start
        uploadStarted.await();

        // Start close() on a separate thread - it should wait for the pending commit
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
            closeStarted.countDown();
            try {
                committer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for close to complete - it should wait for the commit
        closeFuture.get(10, SECONDS);

        // Verify the commit actually happened despite close() being called during upload
        assertThat(commitCompleted.await(1, SECONDS)).isTrue();
        verify(controlPlane).commitFile(any(), any(), anyInt(), anyLong(), any());
        verify(metrics).writeCompleted();
    }

    @Test
    void cacheStoreRunsAfterUploadCompletesNotAfterCommit() throws Exception {
        // Verify that cache store is triggered by upload completion, not commit completion.
        // This ensures cache store starts as soon as upload is done, without waiting for
        // previous commits to finish.

        final CountDownLatch upload1Completed = new CountDownLatch(1);
        final CountDownLatch cacheStore1Started = new CountDownLatch(1);
        final CountDownLatch commit1Started = new CountDownLatch(1);
        final CountDownLatch allowCommit1ToComplete = new CountDownLatch(1);

        // Track whether cache store started before commit completed
        final java.util.concurrent.atomic.AtomicBoolean cacheStoreStartedBeforeCommitCompleted =
            new java.util.concurrent.atomic.AtomicBoolean(false);

        doAnswer(invocation -> {
            upload1Completed.countDown();
            return null;
        }).when(storage).upload(any(), any(InputStream.class), anyLong());

        when(controlPlane.commitFile(any(), any(), anyInt(), anyLong(), any()))
            .thenAnswer(invocation -> {
                commit1Started.countDown();
                // Block the commit
                allowCommit1ToComplete.await();
                return List.of();
            });

        when(time.nanoseconds()).thenReturn(10_000_000L);
        when(time.milliseconds()).thenReturn(10L);

        // Use a custom ObjectCache that tracks when put is called
        final ObjectCache trackingCache = new ObjectCache() {
            @Override
            public FileExtent get(CacheKey key) {
                return null;
            }

            @Override
            public void put(CacheKey key, FileExtent value) {
                // Check if commit has completed yet
                if (allowCommit1ToComplete.getCount() > 0) {
                    // Commit hasn't been signaled to complete, so cache store started before commit finished
                    cacheStoreStartedBeforeCommitCompleted.set(true);
                }
                cacheStore1Started.countDown();
            }

            @Override
            public CompletableFuture<FileExtent> computeIfAbsent(
                    CacheKey key, java.util.function.Function<CacheKey, FileExtent> load, java.util.concurrent.Executor loadExecutor) {
                return CompletableFuture.supplyAsync(() -> load.apply(key), loadExecutor);
            }

            @Override
            public boolean remove(CacheKey key) {
                return false;
            }

            @Override
            public long size() {
                return 0;
            }

            @Override
            public void close() {
            }
        };

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, trackingCache, BATCH_COORDINATE_CACHE, time,
                1, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
                metrics);

        committer.commit(FILE);

        // Wait for upload to complete
        assertThat(upload1Completed.await(5, SECONDS)).isTrue();

        // Cache store should start after upload, even while commit is blocked
        assertThat(cacheStore1Started.await(5, SECONDS))
            .as("Cache store should start after upload completes")
            .isTrue();

        // Verify cache store started while commit was still blocked
        assertThat(cacheStoreStartedBeforeCommitCompleted.get())
            .as("Cache store should run in parallel with commit, not after it")
            .isTrue();

        // Allow commit to complete
        allowCommit1ToComplete.countDown();

        // Wait for completion
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(metrics).writeCompleted();
        });
    }

}
