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

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectFetchManager {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectFetchManager.class);

    private final RangeFetchRequestPerformer requestPerformer;
    private final ScheduledExecutorService pool;
    private final RangeFetchRequestQueue requestQueue;

    ObjectFetchManager(
        final Time time,
        final ObjectFetcher fetcher,
        final long requestDelayMs,
        final int numThreads
    ) {
        this(time, fetcher, requestDelayMs, numThreads,
            Executors.newScheduledThreadPool(
                numThreads,
                ThreadUtils.createThreadFactory("inkless-object-fetch-manager-%d", true,
                    (t, e) -> LOG.error("Uncaught exception in thread '{}':", t.getName(), e)))
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
        this.requestQueue = new RangeFetchRequestQueue(
            Objects.requireNonNull(time, "time cannot be null"),
            requestDelayMs
        );
        this.requestPerformer = new RangeFetchRequestPerformer(
            Objects.requireNonNull(fetcher, "fetcher cannot be null")
        );

        this.pool = Objects.requireNonNull(pool, "pool cannot be null");
        if (numThreads <= 0) {
            throw new IllegalArgumentException("numThreads must be at least 1");
        }
        for (int i = 0; i < numThreads; i++) {
            // TODO anything better than this? Probably we should just run threads.
            pool.scheduleWithFixedDelay(this::iteration, 0, 1, TimeUnit.MILLISECONDS);
        }
    }

    private void iteration() {
        final RangeFetchRequests polled;
        try {
            polled = requestQueue.poll(1, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            LOG.error("Thread interrupted");
            return;
        }

        if (polled != null) {
            requestPerformer.perform(polled);
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("No requests in queue");
            }
        }
    }

    ObjectFetchTask request(final ObjectKey objectKey, final BatchInfo batchInfo, final ByteRange range) {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        Objects.requireNonNull(range, "range cannot be null");
        final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        final ObjectFetchTask task = new ObjectFetchTask(batchInfo, future);
        requestQueue.addRequest(objectKey, new ByteRangeWithFetchTask(range, task));
        return task;
    }

    void shutdown() {
        pool.shutdownNow();
    }
}
