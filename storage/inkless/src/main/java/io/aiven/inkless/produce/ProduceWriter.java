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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for produce writers in Inkless.
 *
 * <p>This interface abstracts the produce writing functionality, allowing for different
 * implementations such as the lock-based {@link Writer} and the pipelined {@link PipelinedWriter}.
 */
public interface ProduceWriter extends Closeable {

    /**
     * Write records to the diskless storage.
     *
     * @param entriesPerPartition the records to write, keyed by partition
     * @param topicConfigs the log configurations for each topic
     * @param requestLocal the request-local context
     * @return a future that completes with the partition responses when the write is committed
     */
    CompletableFuture<Map<TopicIdPartition, PartitionResponse>> write(
        Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        Map<String, LogConfig> topicConfigs,
        RequestLocal requestLocal
    );
}
