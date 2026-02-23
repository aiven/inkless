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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.produce.buffer.HeapBatchBufferData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CacheStoreJob.
 * Tests the caching behavior after file upload completes.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class CacheStoreJobTest {

    private static final ObjectKey OBJECT_KEY = PlainObjectKey.create("prefix", "test-object");

    @Mock
    private Time time;
    @Mock
    private ObjectCache cache;
    @Mock
    private Consumer<Long> durationCallback;
    @Captor
    private ArgumentCaptor<CacheKey> cacheKeyCaptor;
    @Captor
    private ArgumentCaptor<FileExtent> fileExtentCaptor;

    @Test
    void storeToCache_afterSuccessfulUpload() {
        // Data to cache
        byte[] rawData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        BatchBufferData data = new HeapBatchBufferData(rawData);

        // Upload future completes successfully
        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);

        // Use large block alignment so entire data is in one block
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Verify cache.put was called
        verify(cache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        // Verify the cache key (range is determined by alignment strategy, not data size)
        CacheKey cacheKey = cacheKeyCaptor.getValue();
        assertThat(cacheKey.object()).isEqualTo(OBJECT_KEY.value());
        assertThat(cacheKey.range().offset()).isZero();
        // Note: cache key range length is the alignment block size, not the data size

        // Verify the cached data is a COPY of the original (not the same reference)
        FileExtent extent = fileExtentCaptor.getValue();
        assertThat(extent.object()).isEqualTo(OBJECT_KEY.value());
        assertThat(extent.range().offset()).isZero();
        assertThat(extent.range().length()).isEqualTo(rawData.length);  // Extent uses actual data range
        assertThat(extent.data()).containsExactly(rawData);
        assertThat(extent.data()).isNotSameAs(rawData);  // Must be a copy, not same array

        // Verify duration callback was invoked
        verify(durationCallback).accept(anyLong());
    }

    @Test
    void skipCache_whenUploadFails() throws Exception {
        BatchBufferData data = new HeapBatchBufferData(new byte[] {1, 2, 3, 4, 5});

        // Upload future fails
        CompletableFuture<ObjectKey> uploadFuture = new CompletableFuture<>();
        uploadFuture.completeExceptionally(new RuntimeException("Upload failed"));

        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Cache should NOT be called
        verify(cache, never()).put(any(), any());
        verify(durationCallback, never()).accept(anyLong());
    }

    @Test
    void storeToCache_multipleAlignedRanges() {
        // Data larger than block size to trigger multiple ranges
        byte[] rawData = new byte[100];
        for (int i = 0; i < rawData.length; i++) {
            rawData[i] = (byte) i;
        }
        BatchBufferData data = new HeapBatchBufferData(rawData);

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);

        // Use small block size to get multiple ranges
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(50);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L, 3_000_000L, 4_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Should have 2 cache puts (100 bytes / 50 byte blocks = 2 blocks)
        verify(cache, times(2)).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        // Verify both extents have correct data ranges (order is not guaranteed due to Set)
        assertThat(fileExtentCaptor.getAllValues()).hasSize(2);

        // Sort extents by their first byte to verify content
        var extents = fileExtentCaptor.getAllValues().stream()
            .sorted((e1, e2) -> Byte.compare(e1.data()[0], e2.data()[0]))
            .toList();

        // First extent: bytes 0-49
        FileExtent extent1 = extents.get(0);
        assertThat(extent1.data()).hasSize(50);
        assertThat(extent1.data()[0]).isEqualTo((byte) 0);
        assertThat(extent1.data()[49]).isEqualTo((byte) 49);

        // Second extent: bytes 50-99
        FileExtent extent2 = extents.get(1);
        assertThat(extent2.data()).hasSize(50);
        assertThat(extent2.data()[0]).isEqualTo((byte) 50);
        assertThat(extent2.data()[49]).isEqualTo((byte) 99);
    }

    @Test
    void dataIsCopied_notReferenced() {
        // This test verifies that modifications to the original array
        // don't affect the cached data
        byte[] rawData = new byte[] {1, 2, 3, 4, 5};
        BatchBufferData data = new HeapBatchBufferData(rawData);

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        verify(cache).put(any(), fileExtentCaptor.capture());

        // Get the cached extent
        FileExtent extent = fileExtentCaptor.getValue();
        byte[] cachedData = extent.data();

        // Verify it's a copy
        assertThat(cachedData).containsExactly(1, 2, 3, 4, 5);

        // Modify original array
        rawData[0] = 100;

        // Cached data should be unaffected (it's a copy)
        assertThat(cachedData[0]).isEqualTo((byte) 1);
    }

    @Test
    void emptyData_storesEmptyExtent() {
        BatchBufferData data = HeapBatchBufferData.EMPTY;

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Even for empty data, alignment creates a block, so cache is called with empty extent
        verify(cache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        FileExtent extent = fileExtentCaptor.getValue();
        assertThat(extent.data()).isEmpty();
        assertThat(extent.range().length()).isZero();
    }

    @Test
    void uploadFuture_waitsForCompletion() throws Exception {
        BatchBufferData data = new HeapBatchBufferData(new byte[] {1, 2, 3});

        // Use a future that we control
        CompletableFuture<ObjectKey> uploadFuture = new CompletableFuture<>();
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        // Start job in separate thread
        Thread jobThread = new Thread(() -> {
            CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
            job.run();
        });
        jobThread.start();

        // Give thread time to start and block on future.get()
        Thread.sleep(50);

        // Cache should not have been called yet
        verify(cache, never()).put(any(), any());

        // Complete the future
        uploadFuture.complete(OBJECT_KEY);

        // Wait for job to finish
        jobThread.join(1000);

        // Now cache should have been called
        verify(cache).put(any(), any());
    }

    @Test
    void uploadFuture_executionException_skipsCache() {
        BatchBufferData data = new HeapBatchBufferData(new byte[] {1, 2, 3});

        CompletableFuture<ObjectKey> uploadFuture = new CompletableFuture<>();
        uploadFuture.completeExceptionally(new ExecutionException("Wrapped error", new RuntimeException("Cause")));

        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Should silently skip caching on any exception
        verify(cache, never()).put(any(), any());
    }

    @Test
    void uploadFuture_interruptedException_skipsCache() throws Exception {
        BatchBufferData data = new HeapBatchBufferData(new byte[] {1, 2, 3});

        // Create a future that will block indefinitely
        CompletableFuture<ObjectKey> uploadFuture = new CompletableFuture<>();
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        Thread jobThread = new Thread(() -> {
            CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
            job.run();
        });
        jobThread.start();

        // Give thread time to start
        Thread.sleep(50);

        // Interrupt the thread
        jobThread.interrupt();

        // Wait for thread to finish
        jobThread.join(1000);

        // Cache should not have been called
        verify(cache, never()).put(any(), any());
    }

    // Zero-copy ByteBuffer slice tests

    @Test
    void zeroCopyPath_usesAsSlice_whenAvailable() {
        final byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final ByteBuffer directBuffer = ByteBuffer.allocateDirect(testData.length);
        directBuffer.put(testData);
        directBuffer.flip();

        // Create a BatchBufferData that provides asSlice()
        final BatchBufferData data = new BatchBufferData() {
            @Override
            public int size() {
                return testData.length;
            }

            @Override
            public Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(testData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                // This should NOT be called when asSlice() is available
                throw new AssertionError("copyTo should not be called when asSlice is available");
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }

            @Override
            public Optional<ByteBuffer> asSlice(ByteRange range) {
                // Return a slice of the direct buffer for the requested range
                ByteBuffer slice = directBuffer.duplicate();
                slice.position((int) range.offset());
                slice.limit((int) (range.offset() + range.size()));
                return Optional.of(slice.slice().asReadOnlyBuffer());
            }
        };

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Verify cache.put was called with correct data
        verify(cache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        FileExtent extent = fileExtentCaptor.getValue();
        assertThat(extent.data()).containsExactly(testData);
    }

    @Test
    void zeroCopyPath_fallsBackToCopyTo_whenAsSliceReturnsEmpty() {
        final byte[] testData = new byte[] {1, 2, 3, 4, 5};
        final boolean[] copyToCalled = {false};

        // Create a BatchBufferData that returns empty from asSlice()
        final BatchBufferData data = new BatchBufferData() {
            @Override
            public int size() {
                return testData.length;
            }

            @Override
            public Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(testData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                copyToCalled[0] = true;
                System.arraycopy(testData, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }

            // Default asSlice() returns Optional.empty()
        };

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Verify copyTo was called as fallback
        assertThat(copyToCalled[0]).isTrue();

        // Verify cache.put was called with correct data
        verify(cache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        FileExtent extent = fileExtentCaptor.getValue();
        assertThat(extent.data()).containsExactly(testData);
    }

    @Test
    void zeroCopyPath_handlesMultipleRanges() {
        final byte[] testData = new byte[100];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }
        final ByteBuffer directBuffer = ByteBuffer.allocateDirect(testData.length);
        directBuffer.put(testData);
        directBuffer.flip();

        // Create a BatchBufferData that provides asSlice() for zero-copy
        final BatchBufferData data = new BatchBufferData() {
            @Override
            public int size() {
                return testData.length;
            }

            @Override
            public Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(testData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                throw new AssertionError("copyTo should not be called when asSlice is available");
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
            }

            @Override
            public Optional<ByteBuffer> asSlice(ByteRange range) {
                ByteBuffer slice = directBuffer.duplicate();
                slice.position((int) range.offset());
                slice.limit((int) (range.offset() + range.size()));
                return Optional.of(slice.slice().asReadOnlyBuffer());
            }
        };

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        // Use small block size to get multiple ranges
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(50);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L, 3_000_000L, 4_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Should have 2 cache puts (100 bytes / 50 byte blocks = 2 blocks)
        verify(cache, times(2)).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        // Verify both extents have correct data
        var extents = fileExtentCaptor.getAllValues().stream()
            .sorted((e1, e2) -> Byte.compare(e1.data()[0], e2.data()[0]))
            .toList();

        // First extent: bytes 0-49
        assertThat(extents.get(0).data()).hasSize(50);
        assertThat(extents.get(0).data()[0]).isEqualTo((byte) 0);

        // Second extent: bytes 50-99
        assertThat(extents.get(1).data()).hasSize(50);
        assertThat(extents.get(1).data()[0]).isEqualTo((byte) 50);
    }

    // Buffer release tests

    @Test
    void bufferIsReleased_afterSuccessfulCacheStore() {
        final boolean[] released = {false};
        final BatchBufferData data = createTrackingBufferData(new byte[] {1, 2, 3}, () -> released[0] = true);

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Verify buffer was released
        assertThat(released[0]).isTrue();
    }

    @Test
    void bufferIsReleased_whenCachePutThrows() {
        final boolean[] released = {false};
        final BatchBufferData data = createTrackingBufferData(new byte[] {1, 2, 3}, () -> released[0] = true);

        Future<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        when(time.nanoseconds()).thenReturn(1_000_000L, 2_000_000L);
        // Make cache.put() throw an exception
        doThrow(new RuntimeException("Cache failure")).when(cache).put(any(), any());

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);

        // Should not throw - exceptions are swallowed
        job.run();

        // Buffer must still be released despite the exception
        assertThat(released[0]).isTrue();
    }

    @Test
    void bufferIsReleased_whenUploadFails() {
        final boolean[] released = {false};
        final BatchBufferData data = createTrackingBufferData(new byte[] {1, 2, 3}, () -> released[0] = true);

        CompletableFuture<ObjectKey> uploadFuture = new CompletableFuture<>();
        uploadFuture.completeExceptionally(new RuntimeException("Upload failed"));

        KeyAlignmentStrategy alignment = new FixedBlockAlignment(Integer.MAX_VALUE);

        CacheStoreJob job = new CacheStoreJob(time, cache, alignment, data, uploadFuture, durationCallback);
        job.run();

        // Buffer must still be released even when upload fails
        assertThat(released[0]).isTrue();
    }

    /**
     * Creates a BatchBufferData that tracks when release() is called.
     */
    private static BatchBufferData createTrackingBufferData(byte[] rawData, Runnable onRelease) {
        return new BatchBufferData() {
            @Override
            public int size() {
                return rawData.length;
            }

            @Override
            public Supplier<InputStream> asInputStreamSupplier() {
                return () -> new java.io.ByteArrayInputStream(rawData);
            }

            @Override
            public void copyTo(int srcOffset, byte[] dest, int destOffset, int length) {
                System.arraycopy(rawData, srcOffset, dest, destOffset, length);
            }

            @Override
            public BatchBufferData retain() {
                return this;
            }

            @Override
            public void release() {
                onRelease.run();
            }
        };
    }
}
