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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.utils.Time;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ObjectFetchManagerTest {
    static final PlainObjectKey KEY = PlainObjectKey.create("x", "y");

    @Mock
    ObjectFetcher fetcher;
    @Mock
    ScheduledExecutorService pool;

    @Test
    void jobsScheduled() {
        new ObjectFetchManager(Time.SYSTEM, fetcher, 0, 3, pool);
        verify(pool, times(3)).scheduleWithFixedDelay(any(), eq(0L), eq(1L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void requestsRegisteredAndProcessed() throws ExecutionException, InterruptedException, IOException, StorageBackendException {
        final AtomicReference<Runnable> runnableRef = new AtomicReference<>();
        when(pool.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any())).thenAnswer(inv -> {
            final Runnable runnable = inv.getArgument(0, Runnable.class);
            runnableRef.set(runnable);
            return null;
        });

        when(fetcher.fetch(eq(KEY), any()))
            .thenReturn(Channels.newChannel(new ByteArrayInputStream(new byte[]{101})));

        final ObjectFetchManager manager = new ObjectFetchManager(Time.SYSTEM, fetcher, 0, 1, pool);
        final var future = manager.request(KEY, new ByteRange(0, 1));

        assertThat(runnableRef).isNotNull();

        while (!future.isDone()) {
            runnableRef.get().run();
        }
        final ByteBuffer byteBuffer = future.get();
        assertThat(byteBuffer.get()).isEqualTo((byte) 101);
        assertThat(byteBuffer.hasRemaining()).isFalse();
    }

    @Test
    void constructorValidArguments() {
        assertThatThrownBy(() -> new ObjectFetchManager(null, fetcher, 0, 1))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() -> new ObjectFetchManager(Time.SYSTEM, null, 0, 1))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fetcher cannot be null");
        assertThatThrownBy(() -> new ObjectFetchManager(Time.SYSTEM, fetcher, 0, 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("numThreads must be at least 1");
    }

    @Test
    void requestValidArguments() {
        final ObjectFetchManager manager = new ObjectFetchManager(Time.SYSTEM, fetcher, 0, 1);
        assertThatThrownBy(() -> manager.request(null, new ByteRange(0, 1)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKey cannot be null");
        assertThatThrownBy(() -> manager.request(KEY, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("range cannot be null");
    }
}