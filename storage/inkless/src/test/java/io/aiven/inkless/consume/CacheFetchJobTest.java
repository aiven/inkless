// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import io.aiven.inkless.cache.MemoryCache;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class CacheFetchJobTest {

    @Mock
    ObjectFetcher fetcher;

    Time time = new MockTime();
    ObjectKey objectA = PlainObjectKey.create("a", "a");

    @Test
    public void testCacheMiss() throws Exception {
        int size = 10;
        byte[] array = new byte[10];
        for (int i = 0; i < size; i++) {
            array[i] = (byte) i;
        }
        ByteRange range = new ByteRange(0, size);
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));
        when(fetcher.fetch(objectA, range)).thenReturn(new ByteArrayInputStream(array));

        ObjectCache cache = new NullCache();
        CacheFetchJob cacheFetchJob = cacheFetchJob(cache, objectA, range);
        FileExtent actualFile = cacheFetchJob.call();

        assertThat(actualFile).isEqualTo(expectedFile);
    }

    @Test
    public void testCacheHit() throws Exception {
        int size = 10;
        byte[] array = new byte[10];
        for (int i = 0; i < size; i++) {
            array[i] = (byte) i;
        }
        ByteRange range = new ByteRange(0, size);
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));

        ObjectCache cache = new MemoryCache();
        cache.put(CacheFetchJob.createCacheKey(objectA, range), expectedFile);
        CacheFetchJob cacheFetchJob = cacheFetchJob(cache, objectA, range);
        FileExtent actualFile = cacheFetchJob.call();

        assertThat(actualFile).isEqualTo(expectedFile);
        verifyNoInteractions(fetcher);
    }

    private CacheFetchJob cacheFetchJob(
            ObjectCache cache,
            ObjectKey objectKey,
            ByteRange byteRange
    ) {
        return new CacheFetchJob(cache, objectKey, byteRange, time, fetcher,
                durationMs -> {}, durationMs -> {}, hitBool -> {}, durationMs -> {});
    }

}
