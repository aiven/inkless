// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ActiveFileTest {
    static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    static final TopicPartition T1P0 = new TopicPartition("topic1", 0);

    @Test
    void addNull() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThatThrownBy(() -> file.add(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("entriesPerPartition cannot be null");
    }

    @Test
    void add() {
        final Consumer<String> requestsMark = mock(Consumer.class);
        final BiConsumer<String, Integer> bytesInMark = mock(BiConsumer.class);
        final BiConsumer<String, Long> messagesInMark = mock(BiConsumer.class);
        final ActiveFile file = new ActiveFile(
            Time.SYSTEM,
            new BrokerTopicMetricMarks(requestsMark, bytesInMark, messagesInMark));

        final MemoryRecords records = MemoryRecords.withRecords(
            10,
            Compression.NONE,
            new SimpleRecord(new byte[10]),
            new SimpleRecord(new byte[10])
        );
        final var result = file.add(Map.of(T0P0, records));
        assertThat(result).isNotCompleted();
        // non-idempotent requests are not tracked
        assertThat(file.requestsWithProducerIds()).isEmpty();
        verify(requestsMark, times(1)).accept("topic0");
        verify(bytesInMark, times(1)).accept("topic0", records.sizeInBytes());
        verify(messagesInMark, times(1)).accept("topic0", 2L);
    }

    @Test
    void addIdempotent() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.requestsWithProducerIds()).isEmpty();

        final MemoryRecords batch = MemoryRecords.withIdempotentRecords(Compression.NONE, 42L, (short) 1, 1, new SimpleRecord(new byte[10]));
        final var result = file.add(Map.of(
            T0P0, batch
        ));
        assertThat(result).isNotCompleted();
        assertThat(file.requestsWithProducerIds())
            .hasSize(1)
            .containsOnlyKeys(0);
        assertThat(file.requestsWithProducerIds().get(0))
            .containsOnlyKeys(T0P0)
            .containsValues(StreamSupport.stream(batch.batches().spliterator(), false).collect(Collectors.toList()));
    }

    @Test
    void empty() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.isEmpty()).isTrue();

        file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));

        assertThat(file.isEmpty()).isFalse();
    }

    @Test
    void size() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.size()).isZero();

        file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));

        assertThat(file.size()).isEqualTo(78);
    }

    @Test
    void closeEmpty() {
        final Instant start = Instant.ofEpochMilli(10);
        final ActiveFile file = new ActiveFile(Time.SYSTEM, start);
        final ClosedFile result = file.close(Map.of());

        assertThat(result)
            .usingRecursiveComparison()
            .ignoringFields("data")
            .isEqualTo(new ClosedFile(start, Map.of(), Map.of(), Map.of(), List.of(), List.of(), new byte[0]));
        assertThat(result.data()).isEmpty();
    }

    @Test
    void closeWithMaxTimestampValidation() {
        final Instant start = Instant.ofEpochMilli(10);
        final Time time = new MockTime();
        final ActiveFile file = new ActiveFile(time, start);
        // Given a record with a timestamp that is greater than the max timestamp
        final MemoryRecords records = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord(time.milliseconds(), new byte[10]),
            new SimpleRecord(time.milliseconds() + 10, new byte[10]));
        ByteBuffer before = records.buffer().duplicate(); // with right max timestamp

        // When the max timestamp is set to a value that is less than the latest timestamp
        final MutableRecordBatch mutableRecords = records.batches().iterator().next();
        mutableRecords.setMaxTimestamp(TimestampType.CREATE_TIME, time.milliseconds() - 1);
        ByteBuffer after = ByteBuffer.allocate(mutableRecords.sizeInBytes());
        mutableRecords.writeTo(after); // with wrong max timestamp

        final Map<TopicPartition, MemoryRecords> request = Map.of(T0P0, records);
        file.add(request);

        // Then the timestamp should be set back to the original value after validation
        final ClosedFile result = file.close(Map.of());
        assertThat(result.data())
            .isNotEqualTo(after.array())
            .containsExactly(before.array());
    }

    @Test
    void closeNonEmpty() {
        final Instant start = Instant.ofEpochMilli(10);
        final ActiveFile file = new ActiveFile(Time.SYSTEM, start);
        final Map<TopicPartition, MemoryRecords> request1 = Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        );
        file.add(request1);
        final Map<TopicPartition, MemoryRecords> request2 = Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        );
        file.add(request2);

        final ClosedFile result = file.close(Map.of());

        assertThat(result.start())
            .isEqualTo(start);
        assertThat(result.originalRequests())
            .isEqualTo(Map.of(0, request1, 1, request2));
        assertThat(result.awaitingFuturesByRequest()).hasSize(2);
        assertThat(result.awaitingFuturesByRequest().get(0)).isNotCompleted();
        assertThat(result.awaitingFuturesByRequest().get(1)).isNotCompleted();
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, 78, 0, 0),
            CommitBatchRequest.of(T0P1, 78, 78, 0, 0),
            CommitBatchRequest.of(T0P1, 156, 78, 0, 0),
            CommitBatchRequest.of(T1P0, 234, 78, 0, 0)
        );
        assertThat(result.duplicatedBatchResponses()).isEmpty();
        assertThat(result.requestIds()).containsExactly(0, 0, 1, 1);
        assertThat(result.data()).hasSize(312);
    }

    @Test
    void closeWithDuplicates() {
        final Instant start = Instant.ofEpochMilli(10);
        final ActiveFile file = new ActiveFile(Time.SYSTEM, start);
        final Map<TopicPartition, MemoryRecords> requestWithDup = Map.of(
            T0P0, MemoryRecords.withRecords(10, Compression.NONE, new SimpleRecord(new byte[10])),
            T0P1, MemoryRecords.withRecords(11, Compression.NONE, new SimpleRecord(new byte[10])) // dup
        );
        file.add(requestWithDup);
        final Map<TopicPartition, MemoryRecords> request2 = Map.of(
            T0P1, MemoryRecords.withRecords(12, Compression.NONE, new SimpleRecord(new byte[10])),
            T1P0, MemoryRecords.withRecords(13, Compression.NONE, new SimpleRecord(new byte[10]))
        );
        file.add(request2);

        final ClosedFile result = file.close(Map.of(
            0,
            Map.of(
                T0P1, new DuplicatedBatchMetadata(11, new BatchMetadata(0, 21, 1, 101))
            )
        ));

        assertThat(result.start())
            .isEqualTo(start);
        assertThat(result.originalRequests())
            .isEqualTo(Map.of(0, requestWithDup, 1, request2));
        assertThat(result.awaitingFuturesByRequest()).hasSize(2);
        assertThat(result.awaitingFuturesByRequest().get(0)).isNotCompleted();
        assertThat(result.awaitingFuturesByRequest().get(1)).isNotCompleted();
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, 78, 10, 10),
            CommitBatchRequest.of(T0P1, 78, 78, 12, 12),
            CommitBatchRequest.of(T1P0, 156, 78, 13, 13));

        assertThat(result.duplicatedBatchResponses())
            .containsOnlyKeys(0)
            .containsValues(Map.of(
                T0P1, new ProduceResponse.PartitionResponse(Errors.NONE, 20, 21, 101, -1, List.of(), null)
            ));
        assertThat(result.requestIds()).containsExactly(0, 1, 1);
        assertThat(result.data()).hasSize(234);
    }
}
