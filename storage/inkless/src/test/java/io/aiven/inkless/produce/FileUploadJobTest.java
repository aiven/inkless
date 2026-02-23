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

import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.Consumer;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Mock
    ObjectUploader objectUploader;
    @Mock
    Time time;
    @Mock
    Consumer<Long> uploadTimeDurationCallback;
    @Captor
    ArgumentCaptor<InputStream> inputStreamCaptor;

    @Test
    void successAtFirstAttempt() throws Exception {
        final byte[] data = new byte[1];

        doNothing().when(objectUploader).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) data.length));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 1, Duration.ofMillis(100), data, uploadTimeDurationCallback);

        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        verify(objectUploader).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) data.length));
        verify(time, never()).sleep(anyLong());
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void successAfterRetry() throws Exception {
        final byte[] data = new byte[1];

        doThrow(new StorageBackendException("Test"))
            .doThrow(new StorageBackendException("Test"))
            .doNothing()
            .when(objectUploader).upload(eq(OBJECT_KEY), inputStreamCaptor.capture(), eq((long) data.length));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 3, Duration.ofMillis(100), data, uploadTimeDurationCallback);
        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        verify(objectUploader, times(3)).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) data.length));
        // We don't sleep at the last attempt.
        verify(time, times(2)).sleep(eq(100L));
        verify(uploadTimeDurationCallback).accept(eq(10L));
        // Each time the upload is called, a new input stream should be used.
        assertEquals(3, new HashSet<>(inputStreamCaptor.getAllValues()).size());
    }

    @Test
    void uploadStorageFailure() throws Exception {
        final byte[] data = new byte[1];
        final StorageBackendException exception = new StorageBackendException("Test");

        doThrow(exception).when(objectUploader).upload(any(), any(InputStream.class), eq((long) data.length));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob committer = FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), data, uploadTimeDurationCallback);

        assertThatThrownBy(committer::call).isSameAs(exception);
        verify(objectUploader, times(2)).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) data.length));
        // We don't sleep at the last attempt.
        verify(time, times(1)).sleep(eq(100L));
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void uploadOtherFailure() throws Exception {
        final byte[] data = new byte[1];
        final RuntimeException exception = new RuntimeException("Test");

        doThrow(exception).when(objectUploader).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) data.length));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), data, uploadTimeDurationCallback);

        assertThatThrownBy(fileUploadJob::call).isSameAs(exception);
        verify(objectUploader, times(1)).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) data.length));
        verify(time, never()).sleep(anyLong());
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            null, objectUploader, time, 2, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKeyCreator cannot be null");
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, null, time, 2, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectUploader cannot be null");
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, null, 2, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 0, Duration.ofMillis(100), new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("attempts must be positive");
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, null, new byte[1], uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("retryBackoff cannot be null");
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), null, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
        assertThatThrownBy(() -> FileUploadJob.createFromByteArray(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), new byte[1], null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("durationCallback cannot be null");
    }

    // Zero-copy ByteBuffer upload tests

    @Test
    void zeroCopyUpload_usesDirectByteBuffer() throws Exception {
        final byte[] testData = {1, 2, 3, 4};
        final ByteBuffer directBuffer = ByteBuffer.allocateDirect(testData.length);
        directBuffer.put(testData);
        directBuffer.flip();

        // Create a mock BatchBufferData that provides a direct ByteBuffer
        final BatchBufferData batchBufferData = new BatchBufferData() {
            @Override
            public int size() {
                return testData.length;
            }

            @Override
            public java.util.function.Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(testData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(testData, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }

            @Override
            public Optional<ByteBuffer> asByteBuffer() {
                ByteBuffer buf = directBuffer.duplicate();
                buf.rewind();
                return Optional.of(buf.asReadOnlyBuffer());
            }
        };

        doNothing().when(objectUploader).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = FileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, objectUploader, time, 1, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback);

        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        // Verify ByteBuffer upload was called, not InputStream upload
        verify(objectUploader).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        verify(objectUploader, never()).upload(eq(OBJECT_KEY), any(InputStream.class), anyLong());
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void zeroCopyUpload_fallsBackToInputStream_whenNoDirectBuffer() throws Exception {
        final byte[] testData = {1, 2, 3, 4};

        // Create a mock BatchBufferData that does NOT provide a direct ByteBuffer (like HeapBatchBufferData)
        final BatchBufferData batchBufferData = new BatchBufferData() {
            @Override
            public int size() {
                return testData.length;
            }

            @Override
            public java.util.function.Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(testData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(testData, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }

            // Uses default implementation returning Optional.empty()
        };

        doNothing().when(objectUploader).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) testData.length));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = FileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, objectUploader, time, 1, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback);

        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        // Verify InputStream upload was called, not ByteBuffer upload
        verify(objectUploader).upload(eq(OBJECT_KEY), any(InputStream.class), eq((long) testData.length));
        verify(objectUploader, never()).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void zeroCopyUpload_retriesWithByteBuffer() throws Exception {
        final byte[] testData = {1, 2, 3, 4};
        final ByteBuffer directBuffer = ByteBuffer.allocateDirect(testData.length);
        directBuffer.put(testData);
        directBuffer.flip();

        final BatchBufferData batchBufferData = new BatchBufferData() {
            @Override
            public int size() {
                return testData.length;
            }

            @Override
            public java.util.function.Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(testData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(testData, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }

            @Override
            public Optional<ByteBuffer> asByteBuffer() {
                ByteBuffer buf = directBuffer.duplicate();
                buf.rewind();
                return Optional.of(buf.asReadOnlyBuffer());
            }
        };

        // First attempt fails, second succeeds
        doThrow(new StorageBackendException("Test"))
            .doNothing()
            .when(objectUploader).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final FileUploadJob fileUploadJob = FileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, objectUploader, time, 2, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback);

        final ObjectKey objectKey = fileUploadJob.call();

        assertThat(objectKey).isEqualTo(OBJECT_KEY);
        verify(objectUploader, times(2)).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        verify(time, times(1)).sleep(eq(100L));
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }
}
