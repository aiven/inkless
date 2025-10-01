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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;

public class FetchHandler implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchHandler.class);

    private final Reader reader;

    public FetchHandler(final SharedState state) {
        this(
            new Reader(
                state.time(),
                state.objectKeyCreator(),
                state.cache(),
                state.controlPlane(),
                state.config().storage(),
                state.brokerTopicStats(),
                state.config().fetchMetadataThreadPoolSize(),
                state.config().fetchDataThreadPoolSize(),
                state.config().maxBatchesPerPartitionToFind()
            )
        );
    }

    public FetchHandler(final Reader reader) {
        this.reader = reader;
    }

    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> handle(
        final FetchParams params,
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos
    ) {
        if (fetchInfos.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> resultFuture = reader.fetch(params, fetchInfos);
        return resultFuture.handle((result, e) -> {
            if (result == null) {
                // We don't really expect this future to fail, but in case it does...
                LOGGER.error("Read future failed", e);
                final var error = new FetchPartitionData(Errors.UNKNOWN_SERVER_ERROR, -1, -1,
                    MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(),
                    Optional.empty(), OptionalInt.empty(), false);
                result = fetchInfos.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, ignore -> error));
                return result;
            }
            return result;
        });
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
