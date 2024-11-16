// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

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
    static final ObjectKey OBJECT_KEY = new PlainObjectKey("prefix/", "value");
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = s -> OBJECT_KEY;
    static final ClosedFile FILE = new ClosedFile(Map.of(), Map.of(), List.of(), List.of(), new byte[10]);

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

    @Captor
    ArgumentCaptor<Callable<ObjectKey>> uploadCallableCaptor;
    @Captor
    ArgumentCaptor<Runnable> commitRunnableCaptor;

    @Test
    @SuppressWarnings("unchecked")
    void success() throws Exception {
        doNothing()
            .when(objectUploader).upload(eq(OBJECT_KEY), eq(FILE.data()));

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

        final FileCommitter committer = new FileCommitter(
            controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit);

        assertThat(committer.totalBytesInProgress()).isZero();

        committer.commit(FILE);

        assertThat(committer.totalBytesInProgress()).isEqualTo(FILE.data().length);

        final Callable<ObjectKey> uploadCallable = verifyCommitCallable();
        uploadCallable.call();

        final Runnable commitRunnable = verifyCommitRunnable(uploadFuture);

        commitRunnable.run();

        assertThat(committer.totalBytesInProgress()).isZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    void commitFailed() throws Exception {
        doNothing()
            .when(objectUploader).upload(eq(OBJECT_KEY), eq(FILE.data()));

        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.failedFuture(new StorageBackendException("test"));
        when(executorServiceUpload.submit(any(Callable.class)))
            .thenReturn(uploadFuture);

        final FileCommitter committer = new FileCommitter(
            controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit);

        assertThat(committer.totalBytesInProgress()).isZero();

        committer.commit(FILE);

        assertThat(committer.totalBytesInProgress()).isEqualTo(FILE.data().length);

        final Callable<ObjectKey> uploadCallable = verifyCommitCallable();
        uploadCallable.call();

        final Runnable commitRunnable = verifyCommitRunnable(uploadFuture);

        commitRunnable.run();

        assertThat(committer.totalBytesInProgress()).isZero();
    }

    private Callable<ObjectKey> verifyCommitCallable() {
        verify(executorServiceUpload).submit(uploadCallableCaptor.capture());

        final Callable<ObjectKey> value = uploadCallableCaptor.getValue();
        assertThat(value)
            .isInstanceOf(FileUploadJob.class);
        assertThat(value)
            .extracting("objectKeyCreator")
            .isSameAs(OBJECT_KEY_CREATOR);
        assertThat(value)
            .extracting("objectUploader")
            .isSameAs(objectUploader);
        assertThat(value)
            .extracting("time")
            .isSameAs(time);
        assertThat(value)
            .extracting("attempts")
            .isEqualTo(3);
        assertThat(value)
            .extracting("retryBackoff")
            .isEqualTo(Duration.ofMillis(100));
        assertThat(value)
            .extracting("data")
            .isEqualTo(FILE.data());

        return value;
    }

    private Runnable verifyCommitRunnable(final Future<ObjectKey> uploadFuture) {
        verify(executorServiceCommit).submit(commitRunnableCaptor.capture());

        final Runnable value = commitRunnableCaptor.getValue();
        assertThat(value)
            .isInstanceOf(FileCommitJob.class);
        assertThat(value)
            .extracting("file")
            .isSameAs(FILE);
        assertThat(value)
            .extracting("uploadFuture")
            .isSameAs(uploadFuture);
        assertThat(value)
            .extracting("controlPlane")
            .isSameAs(controlPlane);

        return value;
    }

    @Test
    void close() throws IOException {
        final FileCommitter committer = new FileCommitter(
            controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit);

        committer.close();

        verify(executorServiceUpload).shutdown();
        verify(executorServiceCommit).shutdown();
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() ->
            new FileCommitter(null, OBJECT_KEY_CREATOR, objectUploader, time, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("controlPlane cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(controlPlane, null, objectUploader, time, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKeyCreator cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(controlPlane, OBJECT_KEY_CREATOR, null, time, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectUploader cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(controlPlane, OBJECT_KEY_CREATOR, objectUploader, null, 100, Duration.ofMillis(1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 0, Duration.ofMillis(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("maxFileUploadAttempts must be positive");
        assertThatThrownBy(() ->
            new FileCommitter(controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 100, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fileUploadRetryBackoff cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
                null, executorServiceCommit))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceUpload cannot be null");
        assertThatThrownBy(() ->
            new FileCommitter(
                controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
                executorServiceUpload, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("executorServiceCommit cannot be null");
    }

    @Test
    void commitNull() {
        final FileCommitter committer = new FileCommitter(
            controlPlane, OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100),
            executorServiceUpload, executorServiceCommit);
        assertThatThrownBy(() -> committer.commit(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("file cannot be null");
    }
}
