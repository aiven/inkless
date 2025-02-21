// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
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
    private final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> awaitingFuturesByRequest = new HashMap<>();

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

    Map<TopicPartition, CompletableFuture<PartitionResponse>> add(
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        final Map<String, TimestampType> timestampTypes
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(timestampTypes, "timestampTypes cannot be null");

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);

        final Map<TopicPartition, CompletableFuture<PartitionResponse>> result = new HashMap<>();

        for (final var entry : entriesPerPartition.entrySet()) {
            final TopicIdPartition topicIdPartition = entry.getKey();
            brokerTopicStats.topicStats(topicIdPartition.topic()).totalProduceRequestRate().mark();
            brokerTopicStats.allTopicsStats().totalProduceRequestRate().mark();

            final TimestampType messageTimestampType = timestampTypes.get(topicIdPartition.topic());
            if (messageTimestampType == null) {
                throw new IllegalArgumentException("Timestamp type not provided for topic " + topicIdPartition.topic());
            }

            for (final var batch : entry.getValue().batches()) {
                batchValidator.validateAndMaybeSetMaxTimestamp(batch, messageTimestampType);

                buffer.addBatch(topicIdPartition, batch, requestId);

                brokerTopicStats.topicStats(topicIdPartition.topic()).bytesInRate().mark(batch.sizeInBytes());
                brokerTopicStats.allTopicsStats().bytesInRate().mark(batch.sizeInBytes());

                final long numMessages = batch.nextOffset() - batch.baseOffset();
                brokerTopicStats.topicStats(topicIdPartition.topic()).messagesInRate().mark(numMessages);
                brokerTopicStats.allTopicsStats().messagesInRate().mark(numMessages);
            }

            result.put(topicIdPartition.topicPartition(), new CompletableFuture<>());
        }

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
            closeResult.requestIds(),
            closeResult.data()
        );
    }
}
