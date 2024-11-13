// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileCommitterTest {
    static final Executor SYNC_EXECUTOR = Runnable::run;

    static final ObjectKey OBJECT_KEY = new PlainObjectKey("prefix/", "value");
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = s -> OBJECT_KEY;

    @Mock
    ObjectUploader objectUploader;
    @Mock
    ControlPlane controlPlane;
    @Mock
    Time time;
    @Mock
    BiFunction<List<CommitBatchResponse>, Throwable, Void> callback;

    @Captor
    ArgumentCaptor<List<CommitBatchResponse>> callbackResultCaptor;
    @Captor
    ArgumentCaptor<Throwable> callbackThrowableCaptor;

    @Test
    void uploadSuccessFirstAttempt() throws StorageBackendException {
        final List<CommitBatchRequest> commitBatchRequests = List.of();
        final byte[] data = new byte[1];
        final List<CommitBatchResponse> commitResponse = List.of();

        doNothing().when(objectUploader).upload(any(), eq(data));
        when(controlPlane.commitFile(eq(OBJECT_KEY), eq(commitBatchRequests))).thenReturn(commitResponse);

        final FileCommitter committer = new FileCommitter(
            SYNC_EXECUTOR, OBJECT_KEY_CREATOR, objectUploader, controlPlane,
            time, 1, Duration.ofMillis(100),
            callback);

        committer.upload(commitBatchRequests, data);

        verify(objectUploader).upload(eq(OBJECT_KEY), eq(data));
        verify(time, never()).sleep(eq(100));
        verify(callback).apply(callbackResultCaptor.capture(), callbackThrowableCaptor.capture());
        assertThat(callbackResultCaptor.getValue()).isSameAs(commitResponse);
        assertThat(callbackThrowableCaptor.getValue()).isNull();
    }

    @Test
    void uploadSuccessAfterRetry() throws StorageBackendException {
        final List<CommitBatchRequest> commitBatchRequests = List.of();
        final byte[] data = new byte[1];
        final List<CommitBatchResponse> commitResponse = List.of();

        doThrow(new StorageBackendException("Test"))
            .doThrow(new StorageBackendException("Test"))
            .doNothing()
            .when(objectUploader).upload(any(), eq(data));
        when(controlPlane.commitFile(eq(OBJECT_KEY), eq(commitBatchRequests))).thenReturn(commitResponse);

        final FileCommitter committer = new FileCommitter(
            SYNC_EXECUTOR, OBJECT_KEY_CREATOR, objectUploader, controlPlane,
            time, 3, Duration.ofMillis(100),
            callback);
        committer.upload(commitBatchRequests, data);

        verify(objectUploader, times(3)).upload(eq(OBJECT_KEY), eq(data));
        // We don't sleep at the last attempt.
        verify(time, times(2)).sleep(eq(100L));
        verify(callback).apply(callbackResultCaptor.capture(), callbackThrowableCaptor.capture());
        assertThat(callbackResultCaptor.getValue()).isSameAs(commitResponse);
        assertThat(callbackThrowableCaptor.getValue()).isNull();
    }

    @Test
    void uploadFailure() throws StorageBackendException {
        final List<CommitBatchRequest> commitBatchRequests = List.of();
        final byte[] data = new byte[1];
        final StorageBackendException exception = new StorageBackendException("Test");

        doThrow(exception).when(objectUploader).upload(any(), eq(data));

        final FileCommitter committer = new FileCommitter(
            SYNC_EXECUTOR, OBJECT_KEY_CREATOR, objectUploader, controlPlane,
            time, 2, Duration.ofMillis(100),
            callback);
        committer.upload(commitBatchRequests, data);

        verify(objectUploader, times(2)).upload(eq(OBJECT_KEY), eq(data));
        // We don't sleep at the last attempt.
        verify(time, times(1)).sleep(eq(100L));
        verify(callback).apply(callbackResultCaptor.capture(), callbackThrowableCaptor.capture());
        assertThat(callbackResultCaptor.getValue()).isNull();
        assertThat(callbackThrowableCaptor.getValue()).isSameAs(exception);
    }
}
