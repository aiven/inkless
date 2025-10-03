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
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import org.apache.kafka.common.utils.Time;

import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RangeFetcher {
    private final Time time;
    private final ObjectFetcher fetcher;

    public RangeFetcher(
        final Time time,
        final ObjectFetcher fetcher
    ) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher cannot be null");
    }

    public CompletableFuture<ReadableByteChannel> request(final ObjectKey objectKey, final ByteRange range) {
         return null;
    }
}
