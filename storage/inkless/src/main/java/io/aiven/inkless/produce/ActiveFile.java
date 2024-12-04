// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.TimeUtils;

/**
 * An active file that accumulates batches until it is closed.
 * It gets recycled when file is closed.
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

    private final Map<Integer, Map<TopicPartition, List<RecordBatch>>> requestsWithProducerIds = new HashMap<>();

    private final BrokerTopicMetricMarks brokerTopicMetricMarks;

    ActiveFile(final Time time,
               final BrokerTopicMetricMarks brokerTopicMetricMarks) {
        this.buffer = new BatchBuffer();
        this.batchValidator = new BatchValidator(time);
        this.start = TimeUtils.monotonicNow(time);
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
        final Map<TopicPartition, MemoryRecords> entriesPerPartition
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);

        for (final var entry : entriesPerPartition.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            brokerTopicMetricMarks.requestRateMark.accept(topicPartition.topic());

            for (final var batch : entry.getValue().batches()) {

                batchValidator.validateAndMaybeSetMaxTimestamp(batch);

                buffer.addBatch(topicPartition, batch, requestId);

                if (batch.hasProducerId()) {
                    // build up requests to check producer state
                    requestsWithProducerIds.computeIfAbsent(requestId, ignore -> new HashMap<>())
                        .computeIfAbsent(topicPartition, ignore -> new ArrayList<>())
                        .add(batch);
                }

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

    /**
     * @return batches per topic partition, per request id
     */
    Map<Integer, Map<TopicPartition, List<RecordBatch>>> requestsWithProducerIds() {
        return requestsWithProducerIds;
    }

    ClosedFile close(Map<Integer, Map<TopicPartition, DuplicatedBatchMetadata>> duplicatedBatches) {
        BatchBuffer.CloseResult closeResult = buffer.close(duplicatedBatches);
        final var duplicatedBatchResponses = buildDuplicatedBatchResponses(duplicatedBatches);
        return new ClosedFile(
            start,
            originalRequests,
            awaitingFuturesByRequest,
            duplicatedBatchResponses,
            closeResult.commitBatchRequests(),
            closeResult.requestIds(),
            closeResult.data()
        );
    }

    private static Map<Integer, Map<TopicPartition, PartitionResponse>> buildDuplicatedBatchResponses(
        final Map<Integer, Map<TopicPartition, DuplicatedBatchMetadata>> duplicatedBatches
    ) {
        Map<Integer, Map<TopicPartition, PartitionResponse>> duplicatedBatchResponses = new HashMap<>();
        for (var requestEntry : duplicatedBatches.entrySet()) {
            for (var batchEntry : requestEntry.getValue().entrySet()) {
                final DuplicatedBatchMetadata duplicatedBatchMeta = batchEntry.getValue();
                final BatchMetadata batchMetadata = duplicatedBatchMeta.batchMetadata();
                duplicatedBatchResponses.computeIfAbsent(requestEntry.getKey(), ignore -> new HashMap<>())
                    .put(
                        batchEntry.getKey(),
                        new PartitionResponse(
                            Errors.NONE,
                            batchMetadata.firstOffset(),
                            batchMetadata.lastOffset,
                            batchMetadata.timestamp,
                            -1, // unknown log start offset, used when UNKNOWN_PRODUCER_ERROR thrown
                            List.of(),
                            null
                        ));
            }
        }
        return duplicatedBatchResponses;
    }
}
