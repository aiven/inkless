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
package io.aiven.inkless.log;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class RangeFetchRequestsPerformerTest {
    static final PlainObjectKey KEY = PlainObjectKey.create("x", "y");

    @Mock
    ObjectFetcher fetcher;

    @Test
    void successfulFetches() throws IOException, StorageBackendException, ExecutionException, InterruptedException {
        final Random random = new Random(424242);
        final byte[] data = new byte[1024];
        random.nextBytes(data);
        when(fetcher.fetch(eq(KEY), any())).thenAnswer(inv -> {
            final ByteRange byteRange = inv.getArgument(1, ByteRange.class);
            final var resultData = Arrays.copyOfRange(data, byteRange.bufferOffset(), (int) byteRange.endOffset() + 1);
            return Channels.newChannel(new ByteArrayInputStream(resultData));
        });

        final ByteRange range1 = new ByteRange(100, 10);
        final CompletableFuture<ByteBuffer> f1 = new CompletableFuture<>();
        final ByteRange range2 = new ByteRange(1000, 0);  // empty
        final CompletableFuture<ByteBuffer> f2 = new CompletableFuture<>();
        final ByteRange range3 = new ByteRange(20, 2);
        final CompletableFuture<ByteBuffer> f3 = new CompletableFuture<>();
        final ByteRange range4 = new ByteRange(50, 100);  // overlap
        final CompletableFuture<ByteBuffer> f4 = new CompletableFuture<>();
        final ByteRange range5 = new ByteRange(50, 100);  // duplicate
        final CompletableFuture<ByteBuffer> f5 = new CompletableFuture<>();

        final RangeFetchRequests requests = new RangeFetchRequests(KEY, List.of(
            new ByteRangeWithFuture(range1, f1),
            new ByteRangeWithFuture(range2, f2),
            new ByteRangeWithFuture(range3, f3),
            new ByteRangeWithFuture(range4, f4),
            new ByteRangeWithFuture(range5, f5)
        ));

        final RangeFetchRequestsPerformer performer = new RangeFetchRequestsPerformer(fetcher);
        performer.perform(requests);

        checkFutureResult(data, f1, range1);
        checkFutureResult(data, f2, range2);
        checkFutureResult(data, f3, range3);
        checkFutureResult(data, f4, range4);
        checkFutureResult(data, f5, range5);
    }

    void checkFutureResult(final byte[] data,
                           final CompletableFuture<ByteBuffer> future,
                           final ByteRange range) throws ExecutionException, InterruptedException {
        final byte[] arr1 = new byte[range.bufferSize()];
        final ByteBuffer byteBuffer = future.get();
        byteBuffer.get(arr1);
        assertThat(byteBuffer.hasRemaining()).isFalse();
        assertThat(arr1).isEqualTo(Arrays.copyOfRange(data, range.bufferOffset(), (int) range.endOffset() + 1));

    }

    @Test
    void validArguments() {
        assertThatThrownBy(() -> new RangeFetchRequestsPerformer(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fetcher cannot be null");

        final RangeFetchRequestsPerformer performer = new RangeFetchRequestsPerformer(fetcher);
        assertThatThrownBy(() -> performer.perform(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("requests cannot be null");
        assertThatThrownBy(() -> performer.perform(new RangeFetchRequests(KEY, List.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("At least one range must be requested");
    }

    @Test
    void returnedTooFewBytes() throws IOException, StorageBackendException {
        when(fetcher.fetch(eq(KEY), any())).thenReturn(Channels.newChannel(new ByteArrayInputStream(new byte[5])));

        final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        final RangeFetchRequests requests = new RangeFetchRequests(KEY, List.of(
            new ByteRangeWithFuture(new ByteRange(0, 10), future)
        ));

        final RangeFetchRequestsPerformer performer = new RangeFetchRequestsPerformer(fetcher);
        performer.perform(requests);

        assertThatThrownBy(future::get)
            .hasRootCauseInstanceOf(RuntimeException.class)
            .hasRootCauseMessage("Not enough data to fill buffer");
    }

    @Test
    void byteChannelDoesNotReturnInOneGo() throws IOException, StorageBackendException, ExecutionException, InterruptedException {
        final ReadableByteChannel readableByteChannel = new ReadableByteChannel() {
            private int call = 0;

            @Override
            public int read(final ByteBuffer dst) {
                if (call < 3) {
                    dst.put((byte) 1);
                    dst.put((byte) 1);
                    dst.put((byte) 1);
                    call++;
                    return 3;
                } else {
                    return -1;
                }
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {
            }
        };
        when(fetcher.fetch(eq(KEY), any())).thenReturn(readableByteChannel);

        final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        final RangeFetchRequests requests = new RangeFetchRequests(KEY, List.of(
            new ByteRangeWithFuture(new ByteRange(0, 9), future)
        ));

        final RangeFetchRequestsPerformer performer = new RangeFetchRequestsPerformer(fetcher);
        performer.perform(requests);

        final byte[] bytes = new byte[9];
        final ByteBuffer byteBuffer = future.get();
        byteBuffer.get(bytes);
        assertThat(byteBuffer.hasRemaining()).isFalse();
        assertThat(bytes).isEqualTo(new byte[]{1,1,1,1,1,1,1,1,1});
    }

    @ParameterizedTest
    @MethodSource
    void invalidRangeException(final Exception exception) throws IOException, StorageBackendException {
        when(fetcher.fetch(eq(KEY), any())).thenThrow(exception);

        final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        final RangeFetchRequests requests = new RangeFetchRequests(KEY, List.of(
            new ByteRangeWithFuture(new ByteRange(0, 10), future)
        ));

        final RangeFetchRequestsPerformer performer = new RangeFetchRequestsPerformer(fetcher);
        performer.perform(requests);

        assertThatThrownBy(future::get)
            .hasRootCause(exception);
    }

    private static Stream<Arguments> invalidRangeException() {
        return Stream.of(
            Arguments.of(new InvalidRangeException("test")),
            Arguments.of(new KeyNotFoundException(null, KEY))
        );
    }
}
