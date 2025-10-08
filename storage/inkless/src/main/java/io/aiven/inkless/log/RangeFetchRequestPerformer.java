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

import org.apache.kafka.common.utils.ExponentialBackoff;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RangeFetchRequestPerformer {
    private static final Logger LOG = LoggerFactory.getLogger(RangeFetchRequestPerformer.class);

    private final ObjectFetcher fetcher;

    RangeFetchRequestPerformer(final ObjectFetcher fetcher) {
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher cannot be null");
    }

    void perform(final RangeFetchRequests requests) {
        Objects.requireNonNull(requests, "requests cannot be null");
        if (requests.requests().isEmpty()) {
            throw new IllegalArgumentException("At least one range must be requested");
        }

        final ByteRange unitedRange = requests.requests().stream()
            .map(ByteRangeWithFuture::range)
            .reduce(ByteRange::union)
            .get();

        LOG.debug("Requesting {} from {}", unitedRange, requests.objectKey());

        final ByteBuffer buffer = fetchWithRetries(unitedRange, requests);
        if (buffer == null) {
            // All futures are completed exceptionally, nothing to do here.
            return;
        }

        final long globalOffset = unitedRange.offset();
        for (final ByteRangeWithFuture r : requests.requests()) {
            final ByteRange range = r.range();
            if (range.empty()) {
                r.future().complete(ByteBuffer.allocate(0));
            } else {
                final ByteBuffer rangeBuffer = buffer.duplicate();
                final int start = Math.toIntExact(range.offset() - globalOffset);
                final int end = Math.toIntExact(range.endOffset() - globalOffset + 1);
                rangeBuffer.position(start);
                rangeBuffer.limit(end);
                r.future().complete(rangeBuffer.slice());
            }
        }
    }

    private ByteBuffer fetchWithRetries(final ByteRange unitedRange,
                                        final RangeFetchRequests requests) {
        final ExponentialBackoff backoff = new ExponentialBackoff(100, 2, 60 * 1000, 0.2);
        long attempt = 0;
        while (true) {
            final ByteBuffer buffer = ByteBuffer.allocate(unitedRange.bufferSize());
            try (final ReadableByteChannel fetched = fetcher.fetch(requests.objectKey(), unitedRange)) {
                int bytesRead;
                do {
                    bytesRead = fetched.read(buffer);
                } while (bytesRead > 0);
                if (buffer.hasRemaining()) {
                    // This shouldn't normally happen, just a precaution.
                    completeAllFuturesExceptionally(requests, new RuntimeException("Not enough data to fill buffer"));
                    return null;
                }
                return buffer;
            } catch (final InvalidRangeException | KeyNotFoundException e) {
                // These are fatal errors, we can only fail the futures.
                completeAllFuturesExceptionally(requests, e);
                return null;
            } catch (final StorageBackendException | IOException e) {
                // These are retriable errors. Retry indefinitely.
                final long delay = backoff.backoff(attempt++);
                LOG.error("Error while reading {}, retrying after {} ms", requests.objectKey(), delay, e);
                try {
                    Thread.sleep(delay);
                } catch (final InterruptedException e1) {
                    completeAllFuturesExceptionally(requests, e1);
                    return null;
                }
            }
        }
    }

    private void completeAllFuturesExceptionally(final RangeFetchRequests requests, final Exception exception) {
        for (final ByteRangeWithFuture r : requests.requests()) {
            r.future().completeExceptionally(exception);
        }
    }
}
