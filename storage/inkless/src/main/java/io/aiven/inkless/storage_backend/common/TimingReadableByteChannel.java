/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.storage_backend.common;

import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;

/**
 * A {@link ReadableByteChannel} decorator that measures time-to-first-byte (TTFB):
 * the duration from a caller-supplied start time to when the first {@code read()} call returns data.
 *
 * <p>The start time is provided externally so that it can be captured <em>before</em> the storage
 * fetch is initiated (e.g., before {@code ObjectFetcher.fetch()}). This ensures TTFB includes
 * all setup overhead (DNS, TLS, HTTP request, metadata lookup) whether the backend downloads the
 * payload inside {@code fetch()} (S3, Azure, in-memory) or streams it on read (GCS).
 *
 * <p><strong>What the number means:</strong> the timestamp is captured <em>after</em>
 * {@code delegate.read(dst)} returns, so the reported value includes the transfer that read
 * triggered, not the instant the first byte arrived on the wire. For a backend that downloads
 * inside {@code fetch()} the first read returns once the whole payload is already in memory, so
 * TTFB is effectively whole-fetch time. Only for a streaming backend does it approximate
 * first-byte latency.
 *
 * <p>The callback is invoked at most once, on the first successful read that returns &gt; 0 bytes.
 * If the channel is closed or exhausted without ever returning data, the callback is never invoked.
 */
public class TimingReadableByteChannel implements ReadableByteChannel {

    private final ReadableByteChannel delegate;
    private final Instant startTime;
    private final Time time;
    private final Consumer<Long> ttfbCallback;
    // Not volatile: channel is confined to a single thread within FileFetchJob.doWork().
    private boolean firstByteRecorded;

    /**
     * Returns a timing decorator that preserves the delegate's {@link SizedReadableByteChannel}
     * capability, so a sized channel still reaches {@link ObjectFetcher#readToByteBuffer}.
     */
    public static TimingReadableByteChannel wrap(ReadableByteChannel delegate, Time time, Instant startTime, Consumer<Long> ttfbCallback) {
        if (delegate instanceof SizedReadableByteChannel sized) {
            return new Sized(sized, time, startTime, ttfbCallback);
        }
        return new TimingReadableByteChannel(delegate, time, startTime, ttfbCallback);
    }

    private TimingReadableByteChannel(ReadableByteChannel delegate, Time time, Instant startTime, Consumer<Long> ttfbCallback) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.time = Objects.requireNonNull(time, "time");
        this.startTime = Objects.requireNonNull(startTime, "startTime");
        this.ttfbCallback = Objects.requireNonNull(ttfbCallback, "ttfbCallback");
        this.firstByteRecorded = false;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        final int bytesRead = delegate.read(dst);
        if (bytesRead > 0 && !firstByteRecorded) {
            firstByteRecorded = true;
            final Instant now = TimeUtils.durationMeasurementNow(time);
            ttfbCallback.accept(Duration.between(startTime, now).toMillis());
        }
        return bytesRead;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private static final class Sized extends TimingReadableByteChannel implements SizedReadableByteChannel {
        private final int contentLength;

        private Sized(SizedReadableByteChannel delegate, Time time, Instant startTime, Consumer<Long> ttfbCallback) {
            super(delegate, time, startTime, ttfbCallback);
            this.contentLength = delegate.contentLength();
        }

        @Override
        public int contentLength() {
            return contentLength;
        }
    }
}
