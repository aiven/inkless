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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

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
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
    ExecutorService executorServiceUpload;
    @Mock
    ExecutorService executorServiceCommit;
    @Mock
    ExecutorService executorServiceCacheStore;
    @Mock
    FileCommitterMetrics metrics;

    @Captor
    ArgumentCaptor<Callable<ObjectKey>> uploadCallableCaptor;
    @Captor
    ArgumentCaptor<Runnable> commitRunnableCaptor;

    @Test
    @SuppressWarnings("unchecked")
    void success() throws Exception {
        doNothing()
            .when(storage).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) FILE.data().length));

        when(time.nanoseconds()).thenReturn(10_000_000L);

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

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

        assertThat(committer.totalFilesInProgress()).isOne();
        assertThat(committer.totalBytesInProgress()).isEqualTo(FILE.data().length);

        verify(executorServiceUpload).submit(uploadCallableCaptor.capture());
        final Callable<ObjectKey> uploadCallable = uploadCallableCaptor.getValue();

        uploadCallable.call();

        verify(executorServiceCommit).execute(commitRunnableCaptor.capture());
        final Runnable commitRunnable = commitRunnableCaptor.getValue();

        commitRunnable.run();

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        verify(metrics).fileAdded(eq(FILE.size()));
        verify(metrics).fileUploadFinished(eq(0L));
        verify(metrics).fileCommitFinished(eq(0L));
        verify(metrics).fileFinished(eq(Instant.EPOCH), eq(Instant.ofEpochMilli(10L)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void commitFailed() throws Exception {
        doNothing()
            .when(storage).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) FILE.data().length));

        when(time.nanoseconds()).thenReturn(10_000_000L);

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

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

        assertThat(committer.totalFilesInProgress()).isOne();
        assertThat(committer.totalBytesInProgress()).isEqualTo(FILE.data().length);

        verify(executorServiceUpload).submit(uploadCallableCaptor.capture());
        final Callable<ObjectKey> uploadCallable = uploadCallableCaptor.getValue();

        uploadCallable.call();

        verify(executorServiceCommit).execute(commitRunnableCaptor.capture());
        final Runnable commitRunnable = commitRunnableCaptor.getValue();

        commitRunnable.run();

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        verify(metrics).fileAdded(eq(FILE.size()));
        verify(metrics).fileUploadFinished(eq(0L));
        verify(metrics).fileCommitFinished(eq(0L));
        verify(metrics, times(0)).fileFinished(any(), any());
        verify(metrics).fileCommitFailed();
        verify(metrics).writeFailed();
    }

    @Test
    @SuppressWarnings("unchecked")
    void uploadFailed() throws Exception {
        doNothing()
            .when(storage).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) FILE.data().length));

        when(time.nanoseconds()).thenReturn(10_000_000L);

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.failedFuture(new StorageBackendException("test"));
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore,
                metrics);

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        committer.commit(FILE);

        assertThat(committer.totalFilesInProgress()).isOne();
        assertThat(committer.totalBytesInProgress()).isEqualTo(FILE.data().length);

        verify(executorServiceUpload).submit(uploadCallableCaptor.capture());
        final Callable<ObjectKey> uploadCallable = uploadCallableCaptor.getValue();

        uploadCallable.call();

        verify(executorServiceCommit).execute(commitRunnableCaptor.capture());
        final Runnable commitRunnable = commitRunnableCaptor.getValue();

        commitRunnable.run();

        assertThat(committer.totalFilesInProgress()).isZero();
        assertThat(committer.totalBytesInProgress()).isZero();

        verify(metrics).fileAdded(eq(FILE.size()));
        verify(metrics).fileUploadFinished(eq(0L));
        verify(metrics).fileCommitFinished(eq(0L));
        verify(metrics, times(0)).fileFinished(any(), any());
        verify(metrics).fileUploadFailed();
        verify(metrics).writeFailed();
    }

    @Test
    void close() throws IOException {
        final FileCommitter committer = new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, BATCH_COORDINATE_CACHE, time,
                3, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, executorServiceCacheStore, metrics);

        committer.close();

        verify(executorServiceUpload).shutdown();
        verify(executorServiceCommit).shutdown();
        verify(metrics).close();
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
}
