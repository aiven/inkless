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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.groupcdg.pitest.annotations.DoNotMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.aiven.inkless.common.SharedState;

public class AppendHandler implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendHandler.class);

    private final SharedState state;
    private final Writer writer;

    @DoNotMutate
    @CoverageIgnore
    public AppendHandler(final SharedState state) {
        this(
            state,
            new Writer(
                state.time(),
                state.brokerId(),
                state.objectKeyCreator(),
                state.buildStorage(),
                state.keyAlignmentStrategy(),
                state.cache(),
                state.controlPlane(),
                state.config().commitInterval(),
                state.config().produceBufferMaxBytes(),
                state.config().produceMaxUploadAttempts(),
                state.config().produceUploadBackoff(),
                state.config().produceUploadThreadPoolSize(),
                state.brokerTopicStats()
            )
        );
    }

    // Visible for tests
    AppendHandler(final SharedState state,
                  final Writer writer) {
        this.state = state;
        this.writer = writer;
    }

    /**
     * Intercept an attempt to append records.
     *
     * <p>If the interception happened, the {@code responseCallback} is called from inside the interceptor.
     *
     * @return {@code true} if interception happened
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> handle(final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
                                                                            final RequestLocal requestLocal) {
        if (entriesPerPartition.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        // This automatically reject transactional produce with this check.
        if (requestContainsTransactionalProduce(entriesPerPartition)) {
            LOGGER.warn("Transactional produce found, rejecting request");
            return CompletableFuture.completedFuture(entriesPerPartition.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, ignore -> new PartitionResponse(Errors.INVALID_REQUEST)))
            );
        }

        return writer.write(entriesPerPartition, getLogConfigs(entriesPerPartition.keySet()), requestLocal);
    }

    private boolean requestContainsTransactionalProduce(final Map<TopicIdPartition, MemoryRecords> entriesPerPartition) {
        return entriesPerPartition.values().stream().anyMatch(records -> {
            for (final var batch : records.batches()) {
                if (batch.hasProducerId() && batch.isTransactional()) {
                    return true;
                }
            }
            return false;
        });
    }

    private Map<String, LogConfig> getLogConfigs(final Set<TopicIdPartition> topicIdPartitions) {
        final Map<String, Object> defaultTopicConfigs = state.defaultTopicConfigs().get().originals();
        final Map<String, LogConfig> result = new HashMap<>();
        for (final TopicIdPartition tp : topicIdPartitions) {
            final var overrides = state.metadata().getTopicConfig(tp.topic());
            result.put(tp.topic(), LogConfig.fromProps(defaultTopicConfigs, overrides));
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
