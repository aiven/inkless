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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FileFetchJobTest {

    @Mock
    ObjectFetcher fetcher;

    Time time = new MockTime();
    ObjectKey objectA = PlainObjectKey.create("a", "a");

    @Test
    public void testOversizeFileFetch() {
        assertThrows(IllegalArgumentException.class, () -> new FileFetchJob(time, fetcher, objectA, ByteRange.maxRange(), durationMs -> {}));
    }

    @Test
    public void testFetch() throws Exception {
        int size = 10;
        byte[] array = new byte[10];
        for (int i = 0; i < size; i++) {
            array[i] = (byte) i;
        }
        ByteRange range = new ByteRange(0, size);
        FileFetchJob job = new FileFetchJob(time, fetcher, objectA, range, durationMs -> { });
        FileExtent expectedFile = FileFetchJob.createFileExtent(objectA, range, ByteBuffer.wrap(array));

        final ReadableByteChannel channel = mock(ReadableByteChannel.class);
        when(fetcher.fetch(objectA, range)).thenReturn(channel);
        when(fetcher.readToByteBuffer(channel)).thenReturn(ByteBuffer.wrap(array));
        FileExtent actualFile = job.call();

        assertThat(actualFile).isEqualTo(expectedFile);
    }

    @Test
    public void testShortReadThrowsStorageBackendException() throws Exception {
        final int expectedSize = 10;
        final ByteRange range = new ByteRange(0, expectedSize);
        final FileFetchJob job = new FileFetchJob(time, fetcher, objectA, range, durationMs -> { });

        final ReadableByteChannel channel = mock(ReadableByteChannel.class);
        when(fetcher.fetch(objectA, range)).thenReturn(channel);
        // Return fewer bytes than requested (simulates short read / truncated stream).
        when(fetcher.readToByteBuffer(channel)).thenReturn(ByteBuffer.allocate(expectedSize - 1));

        assertThrows(StorageBackendException.class, job::call);
    }

    private List<FileExtent> createCacheAlignedFileExtents(int fileSize, int blockSize) {
        byte[] array = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            array[i] = (byte) i;
        }
        var fixedAlignment = new FixedBlockAlignment(blockSize);
        var ranges = fixedAlignment.align(List.of(new ByteRange(0, fileSize)));

        var fileExtents = new ArrayList<FileExtent>();
        for (ByteRange range : ranges) {
            var startOffset = Math.toIntExact(range.offset());
            var length = Math.min(blockSize, fileSize - startOffset);
            var endOffset = startOffset + length;
            ByteBuffer buffer = ByteBuffer.wrap(Arrays.copyOfRange(array, startOffset, endOffset));
            fileExtents.add(FileFetchJob.createFileExtent(objectA, range, buffer));
        }
        return fileExtents;
    }


    @Test
    public void testFileSizeNotMultipleOfBlockSize() {
        List<FileExtent> fileExtents = createCacheAlignedFileExtents(250, 100);
        List<FileExtent.ByteRange> fileRanges = fileExtents.stream().map(FileExtent::range).toList();

        List<FileExtent.ByteRange> expectedRanges = List.of(
            new FileExtent.ByteRange().setOffset(0).setLength(100),
            new FileExtent.ByteRange().setOffset(100).setLength(100),
            new FileExtent.ByteRange().setOffset(200).setLength(50)
        );

        assertThat(fileRanges).containsExactlyInAnyOrderElementsOf(expectedRanges);
    }

    @Test
    public void testFileSizeEqualsBlockSize() {
        List<FileExtent> fileExtents = createCacheAlignedFileExtents(100, 100);
        List<FileExtent.ByteRange> fileRanges = fileExtents.stream().map(FileExtent::range).toList();

        List<FileExtent.ByteRange> expectedRanges = List.of(
            new FileExtent.ByteRange().setOffset(0).setLength(100)
        );

        assertThat(fileRanges).containsExactlyInAnyOrderElementsOf(expectedRanges);
    }

    @Test
    public void testFileSizeMultipleOfBlockSize() {
        List<FileExtent> fileExtents = createCacheAlignedFileExtents(200, 100);
        List<FileExtent.ByteRange> fileRanges = fileExtents.stream().map(FileExtent::range).toList();

        List<FileExtent.ByteRange> expectedRanges = List.of(
            new FileExtent.ByteRange().setOffset(0).setLength(100),
            new FileExtent.ByteRange().setOffset(100).setLength(100)
        );

        assertThat(fileRanges).containsExactlyInAnyOrderElementsOf(expectedRanges);
    }

    @Test
    public void testSingleFileExtentLessThanBlockSize() {
        List<FileExtent> fileExtents = createCacheAlignedFileExtents(87, 100);
        List<FileExtent.ByteRange> fileRanges = fileExtents.stream().map(FileExtent::range).toList();

        List<FileExtent.ByteRange> expectedRanges = List.of(
            new FileExtent.ByteRange().setOffset(0).setLength(87)
        );

        assertThat(fileRanges).containsExactlyInAnyOrderElementsOf(expectedRanges);
    }

}
