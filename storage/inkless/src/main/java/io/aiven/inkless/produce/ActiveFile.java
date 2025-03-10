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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.TimeUtils;

/**
 * An active file.
 *
 * <p>This class is not thread-safe and is supposed to be protected with a lock at the call site.
 */
class ActiveFile {
    private final Instant start;

    private int requestId = -1;

    private final BatchBuffer buffer;
    private final BatchValidator batchValidator;

    private final Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests = new HashMap<>();
    private final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = new HashMap<>();

    private final BrokerTopicStats brokerTopicStats;

    ActiveFile(final Time time,
               final BrokerTopicStats brokerTopicStats) {
        this.buffer = new BatchBuffer();
        this.batchValidator = new BatchValidator(time);
        this.start = TimeUtils.durationMeasurementNow(time);
        this.brokerTopicStats = brokerTopicStats;
    }

    // For testing
    ActiveFile(final Time time, final Instant start) {
        this.buffer = new BatchBuffer();
        this.batchValidator = new BatchValidator(time);
        this.start = start;
        this.brokerTopicStats = new BrokerTopicStats();
    }

    CompletableFuture<Map<TopicPartition, PartitionResponse>> add(
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        final Map<String, LogConfig> topicConfigs
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(topicConfigs, "topicConfigs cannot be null");

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);

        for (final var entry : entriesPerPartition.entrySet()) {
            final TopicIdPartition topicIdPartition = entry.getKey();
            brokerTopicStats.topicStats(topicIdPartition.topic()).totalProduceRequestRate().mark();
            brokerTopicStats.allTopicsStats().totalProduceRequestRate().mark();

            final LogConfig config = topicConfigs.get(topicIdPartition.topic());
            if (config == null) {
                throw new IllegalArgumentException("Config not provided for topic " + topicIdPartition.topic());
            }
            final TimestampType messageTimestampType = config.messageTimestampType;

            for (final var batch : entry.getValue().batches()) {
                batchValidator.validateAndMaybeSetMaxTimestamp(batch, messageTimestampType);

                buffer.addBatch(topicIdPartition, batch, requestId);

                brokerTopicStats.topicStats(topicIdPartition.topic()).bytesInRate().mark(batch.sizeInBytes());
                brokerTopicStats.allTopicsStats().bytesInRate().mark(batch.sizeInBytes());

                final long numMessages = batch.nextOffset() - batch.baseOffset();
                brokerTopicStats.topicStats(topicIdPartition.topic()).messagesInRate().mark(numMessages);
                brokerTopicStats.allTopicsStats().messagesInRate().mark(numMessages);
            }
        }

        final CompletableFuture<Map<TopicPartition, PartitionResponse>> result = new CompletableFuture<>();
        awaitingFuturesByRequest.put(requestId, result);

        return result;
    }

    boolean isEmpty() {
        return requestId < 0;
    }

    int size() {
        return buffer.totalSize();
    }

    ClosedFile close() {
        BatchBuffer.CloseResult closeResult = buffer.close();
        return new ClosedFile(
            start,
            originalRequests,
            awaitingFuturesByRequest,
            closeResult.commitBatchRequests(),
            closeResult.data()
        );
    }
}
