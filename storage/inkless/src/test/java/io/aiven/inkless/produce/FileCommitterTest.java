// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
    static final ClosedFile FILE = new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), List.of(), new byte[10]);

    @Mock
    ControlPlane controlPlane;
    @Mock
    ObjectUploader objectUploader;
    @Mock
    Time time;
    @Mock
    ExecutorService executorServiceUpload;
    @Mock
    ExecutorService executorServiceCommit;
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
            .when(objectUploader).upload(eq(OBJECT_KEY), eq(FILE.data()));

        when(time.nanoseconds()).thenReturn(10_000_000L);

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

        final FileCommitter committer = new FileCommitter(
            BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit, metrics);

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
            .when(objectUploader).upload(eq(OBJECT_KEY), eq(FILE.data()));

        when(time.nanoseconds()).thenReturn(10_000_000L);

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.failedFuture(new StorageBackendException("test"));
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

        final FileCommitter committer = new FileCommitter(
            11, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit, metrics);

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
    void close() throws IOException {
        final FileCommitter committer = new FileCommitter(
            11, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit, metrics);

        committer.close();

        verify(executorServiceUpload).shutdown();
        verify(executorServiceCommit).shutdown();
        verify(metrics).close();
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() ->
            new FileCommitter(BROKER_ID, null, OBJECT_KEY_CREATOR, objectUploader, time, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("controlPlane cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(BROKER_ID, controlPlane, null, objectUploader, time, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKeyCreator cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, null, time, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectUploader cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, null, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 0, Duration.ofMillis(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("maxFileUploadAttempts must be positive");
        assertThatThrownBy(() ->
            new FileCommitter(BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 100, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fileUploadRetryBackoff cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
                null, executorServiceCommit, metrics))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceUpload cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
                executorServiceUpload, null, metrics))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceCommit cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
                executorServiceUpload, executorServiceCommit, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("metrics cannot be null");
    }

    @Test
    void commitNull() {
        final FileCommitter committer = new FileCommitter(
            BROKER_ID, controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit, metrics);
        assertThatThrownBy(() -> committer.commit(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("file cannot be null");
    }
}
