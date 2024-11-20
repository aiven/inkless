// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.util.function.Consumer;

import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileUploadJobTest {
    static final ObjectKey OBJECT_KEY = new PlainObjectKey("prefix/", "value");
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = s -> OBJECT_KEY;

    @Mock
    ObjectUploader objectUploader;
    @Mock
    Time time;
    @Mock
    Consumer<Long> uploadTimeDurationCallback;

    @Test
    void successAtFirstAttempt() throws Exception {
        final byte[] data = new byte[1];

        doNothing().when(objectUploader).upload(any(), eq(data));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 1, Duration.ofMillis(100), data, uploadTimeDurationCallback);

        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        verify(objectUploader).upload(eq(OBJECT_KEY), eq(data));
        verify(time, never()).sleep(anyLong());
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void successAfterRetry() throws Exception {
        final byte[] data = new byte[1];

        doThrow(new StorageBackendException("Test"))
            .doThrow(new StorageBackendException("Test"))
            .doNothing()
            .when(objectUploader).upload(any(), eq(data));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100), data, uploadTimeDurationCallback);
        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        verify(objectUploader, times(3)).upload(eq(OBJECT_KEY), eq(data));
        // We don't sleep at the last attempt.
        verify(time, times(2)).sleep(eq(100L));
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void uploadStorageFailure() throws Exception {
        final byte[] data = new byte[1];
        final StorageBackendException exception = new StorageBackendException("Test");

        doThrow(exception).when(objectUploader).upload(any(), eq(data));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob committer = new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), data, uploadTimeDurationCallback);

        assertThatThrownBy(committer::call).isSameAs(exception);
        verify(objectUploader, times(2)).upload(eq(OBJECT_KEY), eq(data));
        // We don't sleep at the last attempt.
        verify(time, times(1)).sleep(eq(100L));
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void uploadOtherFailure() throws Exception {
        final byte[] data = new byte[1];
        final RuntimeException exception = new RuntimeException("Test");

        doThrow(exception).when(objectUploader).upload(any(), eq(data));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), data, uploadTimeDurationCallback);

        assertThatThrownBy(fileUploadJob::call).isSameAs(exception);
        verify(objectUploader, times(1)).upload(eq(OBJECT_KEY), eq(data));
        verify(time, never()).sleep(anyLong());
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() -> new FileUploadJob(
            null, objectUploader, time, 2, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKeyCreator cannot be null");
        assertThatThrownBy(() -> new FileUploadJob(
            OBJECT_KEY_CREATOR, null, time, 2, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectUploader cannot be null");
        assertThatThrownBy(() -> new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, null, 2, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() -> new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 0, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("attempts must be positive");
        assertThatThrownBy(() -> new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, null, new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("retryBackoff cannot be null");
        assertThatThrownBy(() -> new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), null, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
        assertThatThrownBy(() -> new FileUploadJob(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), new byte[1], null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("durationCallback cannot be null");
    }
}
