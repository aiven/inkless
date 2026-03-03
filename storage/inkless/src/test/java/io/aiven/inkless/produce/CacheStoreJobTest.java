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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.function.Consumer;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class CacheStoreJobTest {

    static final ObjectKey OBJECT_KEY = PlainObjectKey.create("prefix", "test-object");
    static final byte[] DATA = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    @Mock
    ObjectCache objectCache;
    @Mock
    Consumer<Long> cacheStoreDurationCallback;
    @Captor
    ArgumentCaptor<CacheKey> cacheKeyCaptor;
    @Captor
    ArgumentCaptor<FileExtent> fileExtentCaptor;

    Time time;
    KeyAlignmentStrategy keyAlignmentStrategy;

    @BeforeEach
    void setUp() {
        time = new MockTime();
        // Use a block size larger than data to get a single cache key
        keyAlignmentStrategy = new FixedBlockAlignment(Integer.MAX_VALUE);
    }

    @Test
    void acceptSuccessStoresDataToCache() {
        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            DATA,
            cacheStoreDurationCallback
        );

        // Simulate successful upload completion
        job.onUploadComplete(OBJECT_KEY, null);

        // Verify cache.put was called
        verify(objectCache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        // Verify the cache key
        final CacheKey capturedKey = cacheKeyCaptor.getValue();
        assertThat(capturedKey.object()).isEqualTo(OBJECT_KEY.value());
        assertThat(capturedKey.range().offset()).isZero();
        assertThat(capturedKey.range().length()).isEqualTo(Integer.MAX_VALUE);

        // Verify the file extent contains the data
        final FileExtent capturedExtent = fileExtentCaptor.getValue();
        assertThat(capturedExtent.object()).isEqualTo(OBJECT_KEY.value());
        assertThat(capturedExtent.data()).isEqualTo(DATA);

        // Verify duration callback was invoked
        verify(cacheStoreDurationCallback).accept(any());
    }

    @Test
    void acceptFailureDoesNotStoreToCache() {
        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            DATA,
            cacheStoreDurationCallback
        );

        // Simulate failed upload completion
        job.onUploadComplete(null, new StorageBackendException("Upload failed"));

        // Verify cache.put was NOT called
        verify(objectCache, never()).put(any(), any());

        // Verify duration callback was NOT invoked
        verifyNoInteractions(cacheStoreDurationCallback);
    }

    @Test
    void acceptWithNullObjectKeyAndNullErrorDoesNotStore() {
        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            DATA,
            cacheStoreDurationCallback
        );

        // Edge case: both null (shouldn't happen in practice, but test the guard)
        job.onUploadComplete(null, null);

        // Verify cache.put was NOT called
        verify(objectCache, never()).put(any(), any());
        verifyNoInteractions(cacheStoreDurationCallback);
    }

    @Test
    void acceptWithObjectKeyAndErrorDoesNotStore() {
        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            DATA,
            cacheStoreDurationCallback
        );

        // Edge case: both present (shouldn't happen, but error takes precedence)
        // The condition is: objectKey != null && error == null
        job.onUploadComplete(OBJECT_KEY, new RuntimeException("Some error"));

        // Verify cache.put was NOT called since error is not null
        verify(objectCache, never()).put(any(), any());
        verifyNoInteractions(cacheStoreDurationCallback);
    }

    @Test
    void acceptWithMultipleBlocksStoresAllBlocks() {
        // Use a smaller block size to create multiple cache entries
        final int blockSize = 4;
        keyAlignmentStrategy = new FixedBlockAlignment(blockSize);

        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            DATA,  // 10 bytes = 3 blocks (0-3, 4-7, 8-11)
            cacheStoreDurationCallback
        );

        // Simulate successful upload completion
        job.onUploadComplete(OBJECT_KEY, null);

        // Verify cache.put was called 3 times (for each block)
        verify(objectCache, times(3)).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        // Verify duration callback was invoked 3 times
        verify(cacheStoreDurationCallback, times(3)).accept(any());
    }

    @Test
    void acceptStoresCorrectDataRange() {
        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            DATA,
            cacheStoreDurationCallback
        );

        job.onUploadComplete(OBJECT_KEY, null);

        verify(objectCache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        final FileExtent extent = fileExtentCaptor.getValue();
        assertThat(extent.range().offset()).isZero();
        assertThat(extent.range().length()).isEqualTo(DATA.length);
        assertThat(extent.data()).hasSize(DATA.length);
    }

    @Test
    void acceptWithEmptyDataStoresEmptyExtent() {
        final byte[] emptyData = new byte[0];

        final CacheStoreJob job = new CacheStoreJob(
            time,
            objectCache,
            keyAlignmentStrategy,
            emptyData,
            cacheStoreDurationCallback
        );

        job.onUploadComplete(OBJECT_KEY, null);

        // With empty data and MAX_VALUE block size, alignment still produces one block
        // but the extent will have empty data
        verify(objectCache).put(cacheKeyCaptor.capture(), fileExtentCaptor.capture());

        final FileExtent extent = fileExtentCaptor.getValue();
        assertThat(extent.data()).isEmpty();
    }
}
