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

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.InklessThreadFactory;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ObjectFetchManager {
    private final Time time;
    private final ObjectFetcher fetcher;
    private final ScheduledExecutorService pool;
    private final RangeFetchRequestQueue requests;

    ObjectFetchManager(
        final Time time,
        final ObjectFetcher fetcher,
        final long requestDelayMs,
        final int numThreads
    ) {
        this(time, fetcher, requestDelayMs, numThreads,
            Executors.newScheduledThreadPool(numThreads, new InklessThreadFactory("inkless-object-fetch-manager-", true))
        );
    }

    // Visible for testing
    ObjectFetchManager(
        final Time time,
        final ObjectFetcher fetcher,
        final long requestDelayMs,
        final int numThreads,
        final ScheduledExecutorService pool
    ) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher cannot be null");
        this.requests = new RangeFetchRequestQueue(time, requestDelayMs);
        this.pool = Objects.requireNonNull(pool, "pool cannot be null");
        for (int i = 0; i < numThreads; i++) {
            pool.scheduleWithFixedDelay(this::iteration, 0, 0, TimeUnit.MILLISECONDS);
        }
    }

    private void iteration() {
//        final RangeFetchRequest request = requests.take();
    }

    void shutdown() {
        pool.shutdownNow();
    }

    public CompletableFuture<ByteBuffer> request(final ObjectKey objectKey, final ByteRange range) {
        final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        requests.addRequest(objectKey, new ByteRangeWithFuture(range, future));
        return future;
    }
}
