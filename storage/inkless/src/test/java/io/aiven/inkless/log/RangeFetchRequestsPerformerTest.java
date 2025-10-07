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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
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
        future.get().get(arr1);
        assertThat(arr1).isEqualTo(Arrays.copyOfRange(data, range.bufferOffset(), (int) range.endOffset() + 1));

    }


}
