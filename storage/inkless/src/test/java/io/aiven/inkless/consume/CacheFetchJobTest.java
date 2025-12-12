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
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import io.aiven.inkless.cache.MemoryCache;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class CacheFetchJobTest {

    @Mock
    ObjectFetcher fetcher;

    final Time time = new MockTime();
    final static ObjectKey objectA = PlainObjectKey.create("a", "a");

    private CacheFetchJob cacheFetchJob(ObjectCache cache, ByteRange byteRange) {
        return cacheFetchJob(cache, byteRange, CacheFetchJob.NO_BATCH_TIMESTAMP);
    }

    private CacheFetchJob cacheFetchJob(ObjectCache cache, ByteRange byteRange, long batchTimestamp) {
        return new CacheFetchJob(cache, fetcher, CacheFetchJobTest.objectA, byteRange, time,
            durationMs -> {}, cacheEntrySize -> {}, batchTimestamp);
    }

    @Test
    public void testCacheMiss() throws Exception {
        int size = 10;
        byte[] array = new byte[10];
        for (int i = 0; i < size; i++) {
            array[i] = (byte) i;
        }
        ByteRange range = new ByteRange(0, size);
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));

        final ReadableByteChannel channel = mock(ReadableByteChannel.class);
        when(fetcher.fetch(objectA, range)).thenReturn(channel);
        when(fetcher.readToByteBuffer(channel)).thenReturn(ByteBuffer.wrap(array));

        ObjectCache cache = new NullCache();
        CacheFetchJob cacheFetchJob = cacheFetchJob(cache, range);
        FileExtent actualFile = cacheFetchJob.call();

        assertThat(actualFile).isEqualTo(expectedFile);
    }

    @Test
    public void testCacheHit() {
        int size = 10;
        byte[] array = new byte[10];
        for (int i = 0; i < size; i++) {
            array[i] = (byte) i;
        }
        ByteRange range = new ByteRange(0, size);
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));

        ObjectCache cache = new MemoryCache();
        cache.put(CacheFetchJob.createCacheKey(objectA, range), expectedFile);
        CacheFetchJob cacheFetchJob = cacheFetchJob(cache, range);
        FileExtent actualFile = cacheFetchJob.call();

        assertThat(actualFile).isEqualTo(expectedFile);
        verifyNoInteractions(fetcher);
    }

    @Test
    public void testCacheFetchJobWithTimestampUsesTimestampAwareMethod() {
        // Given: A mock cache that tracks method calls
        ObjectCache mockCache = mock(ObjectCache.class);

        int size = 10;
        byte[] array = new byte[size];
        ByteRange range = new ByteRange(0, size);
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));

        long batchTimestamp = 12345L;

        // Mock the timestamp-aware computeIfAbsent to return expected file
        when(mockCache.computeIfAbsent(any(), any(), eq(batchTimestamp))).thenReturn(expectedFile);

        // When
        CacheFetchJob job = cacheFetchJob(mockCache, range, batchTimestamp);
        FileExtent result = job.call();

        // Then: Should use the timestamp-aware method
        assertThat(result).isEqualTo(expectedFile);
        verify(mockCache).computeIfAbsent(any(), any(), eq(batchTimestamp));
    }

    @Test
    public void testCacheFetchJobWithoutTimestampUsesDefaultTimestamp() {
        // Given: A mock cache
        ObjectCache mockCache = mock(ObjectCache.class);

        int size = 10;
        byte[] array = new byte[size];
        ByteRange range = new ByteRange(0, size);
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));

        // Mock the timestamp-aware computeIfAbsent with NO_BATCH_TIMESTAMP
        when(mockCache.computeIfAbsent(any(), any(), eq(CacheFetchJob.NO_BATCH_TIMESTAMP))).thenReturn(expectedFile);

        // When: Using constructor without timestamp
        CacheFetchJob job = cacheFetchJob(mockCache, range);
        FileExtent result = job.call();

        // Then: Should use NO_BATCH_TIMESTAMP
        assertThat(result).isEqualTo(expectedFile);
        verify(mockCache).computeIfAbsent(any(), any(), eq(CacheFetchJob.NO_BATCH_TIMESTAMP));
    }
}
