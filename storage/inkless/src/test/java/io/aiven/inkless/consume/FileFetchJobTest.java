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

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

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
}
