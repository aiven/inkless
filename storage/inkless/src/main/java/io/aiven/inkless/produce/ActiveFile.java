// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

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

    private final Map<Integer, Map<TopicPartition, MemoryRecords>> originalRequests = new HashMap<>();
    private final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = new HashMap<>();

    private final BrokerTopicMetricMarks brokerTopicMetricMarks;

    ActiveFile(final Time time,
               final BrokerTopicMetricMarks brokerTopicMetricMarks) {
        this.buffer = new BatchBuffer();
        this.batchValidator = new BatchValidator(time);
        this.start = TimeUtils.durationMeasurementNow(time);
        this.brokerTopicMetricMarks = brokerTopicMetricMarks;
    }

    // For testing
    ActiveFile(final Time time, final Instant start) {
        this.buffer = new BatchBuffer();
        this.batchValidator = new BatchValidator(time);
        this.start = start;
        this.brokerTopicMetricMarks = new BrokerTopicMetricMarks();
    }

    CompletableFuture<Map<TopicPartition, PartitionResponse>> add(
        final Map<TopicPartition, MemoryRecords> entriesPerPartition,
        final Map<String, TimestampType> timestampTypes
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(timestampTypes, "timestampTypes cannot be null");

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);

        for (final var entry : entriesPerPartition.entrySet()) {
            final String topic = entry.getKey().topic();
            brokerTopicMetricMarks.requestRateMark.accept(topic);
            final TopicPartition topicPartition = entry.getKey();
            final TimestampType messageTimestampType = timestampTypes.get(topicPartition.topic());
            if (messageTimestampType == null) {
                throw new IllegalArgumentException("Timestamp type not provided for topic " + topicPartition.topic());
            }

            for (final var batch : entry.getValue().batches()) {
                batchValidator.validateAndMaybeSetMaxTimestamp(batch);

                buffer.addBatch(topicPartition, messageTimestampType, batch, requestId);

                brokerTopicMetricMarks.bytesInRateMark.accept(topicPartition.topic(), batch.sizeInBytes());
                brokerTopicMetricMarks.messagesInRateMark.accept(topicPartition.topic(), batch.nextOffset() - batch.baseOffset());
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
            closeResult.requestIds(),
            closeResult.data()
        );
    }
}
