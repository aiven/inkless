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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.storage_backend.common.Storage;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class AsyncFileUploadJobTest {
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
    Storage storage;
    @Mock
    Time time;
    @Mock
    Consumer<Long> uploadTimeDurationCallback;
    @Captor
    ArgumentCaptor<ByteBuffer> byteBufferCaptor;

    @Test
    void successAtFirstAttempt() {
        final byte[] data = {1, 2, 3, 4};
        final BatchBufferData batchBufferData = createHeapBatchBufferData(data);

        when(storage.upload(eq(OBJECT_KEY), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final AsyncFileUploadJob job = AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 1, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback);

        final ObjectKey result = job.uploadAsync().join();

        assertThat(result).isEqualTo(OBJECT_KEY);
        verify(storage).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        verify(uploadTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void successAfterRetry() {
        final byte[] data = {1, 2, 3, 4};
        final BatchBufferData batchBufferData = createHeapBatchBufferData(data);

        when(storage.upload(eq(OBJECT_KEY), byteBufferCaptor.capture()))
            .thenReturn(CompletableFuture.failedFuture(new StorageBackendException("Test")))
            .thenReturn(CompletableFuture.failedFuture(new StorageBackendException("Test")))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final AsyncFileUploadJob job = AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 3, Duration.ZERO, batchBufferData, uploadTimeDurationCallback);

        final ObjectKey result = job.uploadAsync().join();

        assertThat(result).isEqualTo(OBJECT_KEY);
        verify(storage, times(3)).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        verify(uploadTimeDurationCallback).accept(any());
    }

    @Test
    void uploadFailureAfterMaxAttempts() {
        final byte[] data = {1, 2, 3, 4};
        final BatchBufferData batchBufferData = createHeapBatchBufferData(data);

        when(storage.upload(eq(OBJECT_KEY), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.failedFuture(new StorageBackendException("Test")));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final AsyncFileUploadJob job = AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 2, Duration.ZERO, batchBufferData, uploadTimeDurationCallback);

        assertThatThrownBy(() -> job.uploadAsync().join())
            .hasCauseInstanceOf(StorageBackendException.class);
        verify(storage, times(2)).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
    }

    /**
     * Tests that buffer position is correctly reset between retry attempts.
     *
     * <p>This test simulates a scenario where the storage layer consumes the ByteBuffer
     * (advancing its position) during the first attempt, then fails. The second retry
     * must still be able to read the full buffer content.
     *
     * <p>Without the buffer.rewind() fix, the second attempt would receive a buffer
     * with position at the end, resulting in zero bytes being uploaded.
     */
    @Test
    void retryWithBufferPositionConsumed_stillSucceeds() {
        final byte[] data = {1, 2, 3, 4, 5};
        final AtomicInteger releaseCount = new AtomicInteger(0);

        // Create a BatchBufferData that returns a shared ByteBuffer (simulating pooled buffer)
        final ByteBuffer sharedBuffer = ByteBuffer.wrap(data);
        final BatchBufferData batchBufferData = new BatchBufferData() {
            @Override
            public int size() {
                return data.length;
            }

            @Override
            public java.util.function.Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(data);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(data, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
                releaseCount.incrementAndGet();
            }

            @Override
            public Optional<ByteBuffer> asByteBuffer() {
                // Return the shared buffer - simulates what PooledBatchBufferData does
                return Optional.of(sharedBuffer);
            }
        };

        // First call: consume the buffer (advance position to end), then fail
        // Second call: verify buffer has correct content, then succeed
        when(storage.upload(eq(OBJECT_KEY), byteBufferCaptor.capture()))
            .thenAnswer(invocation -> {
                final ByteBuffer buffer = invocation.getArgument(1);
                // Simulate storage layer consuming the buffer
                while (buffer.hasRemaining()) {
                    buffer.get();
                }
                // After consuming, the shared buffer's position is now at end
                // This affects the next duplicate() call if rewind() is not called
                return CompletableFuture.failedFuture(new StorageBackendException("Simulated failure"));
            })
            .thenAnswer(invocation -> {
                final ByteBuffer buffer = invocation.getArgument(1);
                // Verify the buffer has correct content on retry
                assertThat(buffer.remaining()).isEqualTo(data.length);
                final byte[] readData = new byte[buffer.remaining()];
                buffer.get(readData);
                assertThat(readData).isEqualTo(data);
                return CompletableFuture.completedFuture(null);
            });
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final AsyncFileUploadJob job = AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 2, Duration.ZERO, batchBufferData, uploadTimeDurationCallback);

        final ObjectKey result = job.uploadAsync().join();

        assertThat(result).isEqualTo(OBJECT_KEY);
        verify(storage, times(2)).upload(eq(OBJECT_KEY), any(ByteBuffer.class));
        // Verify buffer was released after completion
        assertThat(releaseCount.get()).isEqualTo(1);
    }

    @Test
    void bufferIsReleasedOnSuccess() {
        final byte[] data = {1, 2, 3, 4};
        final AtomicInteger releaseCount = new AtomicInteger(0);
        final BatchBufferData batchBufferData = createTrackingBatchBufferData(data, releaseCount);

        when(storage.upload(eq(OBJECT_KEY), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final AsyncFileUploadJob job = AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 1, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback);

        job.uploadAsync().join();

        assertThat(releaseCount.get()).isEqualTo(1);
    }

    @Test
    void bufferIsReleasedOnFailure() {
        final byte[] data = {1, 2, 3, 4};
        final AtomicInteger releaseCount = new AtomicInteger(0);
        final BatchBufferData batchBufferData = createTrackingBatchBufferData(data, releaseCount);

        when(storage.upload(eq(OBJECT_KEY), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.failedFuture(new StorageBackendException("Test")));
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final AsyncFileUploadJob job = AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 1, Duration.ZERO, batchBufferData, uploadTimeDurationCallback);

        assertThatThrownBy(() -> job.uploadAsync().join())
            .hasCauseInstanceOf(StorageBackendException.class);

        assertThat(releaseCount.get()).isEqualTo(1);
    }

    @Test
    void constructorInvalidArguments() {
        final BatchBufferData batchBufferData = createHeapBatchBufferData(new byte[1]);

        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            null, storage, time, 2, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKeyCreator cannot be null");
        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, null, time, 2, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("storage cannot be null");
        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, null, 2, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 0, Duration.ofMillis(100), batchBufferData, uploadTimeDurationCallback))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("maxAttempts must be positive");
        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 2, null, batchBufferData, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("retryBackoff cannot be null");
        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 2, Duration.ofMillis(100), null, uploadTimeDurationCallback))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batchBufferData cannot be null");
        assertThatThrownBy(() -> AsyncFileUploadJob.createFromBatchBufferData(
            OBJECT_KEY_CREATOR, storage, time, 2, Duration.ofMillis(100), batchBufferData, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("durationCallback cannot be null");
    }

    private BatchBufferData createHeapBatchBufferData(byte[] data) {
        return new BatchBufferData() {
            @Override
            public int size() {
                return data.length;
            }

            @Override
            public java.util.function.Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(data);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(data, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }
            // Uses default asByteBuffer() returning Optional.empty()
        };
    }

    private BatchBufferData createTrackingBatchBufferData(byte[] data, AtomicInteger releaseCount) {
        return new BatchBufferData() {
            @Override
            public int size() {
                return data.length;
            }

            @Override
            public java.util.function.Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(data);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(data, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
                releaseCount.incrementAndGet();
            }
        };
    }
}
