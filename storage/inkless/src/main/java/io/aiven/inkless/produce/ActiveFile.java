// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
    private final Map<Integer, Map<TopicPartition, MemoryRecords>> originalRequests = new HashMap<>();
    private final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = new HashMap<>();
    private final Consumer<String> requestRateMark;

    ActiveFile(final Time time,
               final Consumer<String> requestRateMark,
               final BiConsumer<String, Integer> bytesInRateMark,
               final BiConsumer<String, Long> messagesInRateMark) {
        this.buffer = new BatchBuffer(time, bytesInRateMark, messagesInRateMark);
        this.start = TimeUtils.monotonicNow(time);
        this.requestRateMark = requestRateMark;
    }

    // For testing
    ActiveFile(final Time time, final Instant start) {
        this.buffer = new BatchBuffer(time, (topic, bytes) -> {}, (topic, messages) -> {});
        this.start = start;
        this.requestRateMark = request -> {};
    }

    CompletableFuture<Map<TopicPartition, PartitionResponse>> add(
        final Map<TopicPartition, MemoryRecords> entriesPerPartition
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);

        for (final var entry : entriesPerPartition.entrySet()) {
            final String topic = entry.getKey().topic();
            requestRateMark.accept(topic);

            for (final var batch : entry.getValue().batches()) {
                buffer.addBatch(entry.getKey(), batch, requestId);
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
