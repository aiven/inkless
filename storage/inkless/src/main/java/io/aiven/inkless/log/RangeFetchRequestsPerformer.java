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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

class RangeFetchRequestsPerformer {
    private final ObjectFetcher fetcher;

    RangeFetchRequestsPerformer(final ObjectFetcher fetcher) {
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

        final ByteBuffer buffer = ByteBuffer.allocate(unitedRange.bufferSize());
        try (final ReadableByteChannel fetched = fetcher.fetch(requests.objectKey(), unitedRange)) {
            fetched.read(buffer);
        } catch (final InvalidRangeException | KeyNotFoundException e) {
            // These are fatal errors, we can only fail the futures.
            for (final ByteRangeWithFuture r : requests.requests()) {
                r.future().completeExceptionally(e);
            }
        } catch (final StorageBackendException | IOException e) {
            // These are retriable errors. Retry indefinitely.
            // TODO retries
        }

        final long globalOffset = unitedRange.offset();
        for (final ByteRangeWithFuture r : requests.requests()) {
            final ByteRange range = r.range();
            if (range.empty()) {
                r.future().complete(ByteBuffer.allocate(0));
            } else {
                final int start = Math.toIntExact(range.offset() - globalOffset);
                final int end = Math.toIntExact(range.endOffset() - globalOffset);
                buffer.rewind();
                buffer.position(start);
                buffer.limit(end);
                r.future().complete(buffer.slice());
            }
        }
    }
}
