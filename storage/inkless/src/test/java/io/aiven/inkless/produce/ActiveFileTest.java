// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        final var result = file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));
        assertThat(result).isNotCompleted();
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
        final ClosedFile result = file.close();

        assertThat(result)
            .usingRecursiveComparison()
            .ignoringFields("data")
            .isEqualTo(new ClosedFile(start, Map.of(), Map.of(), List.of(), List.of(), new byte[0]));
        assertThat(result.data()).isEmpty();
    }

    @Test
    void closeNonEmpty() {
        final Instant start = Instant.ofEpochMilli(10);
        final ActiveFile file = new ActiveFile(Time.SYSTEM, start);
        final Map<TopicPartition, MemoryRecords> request1 = Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10])),
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(2000, new byte[10]))
        );
        file.add(request1);
        final Map<TopicPartition, MemoryRecords> request2 = Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(3000, new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(4000, new byte[10]))
        );
        file.add(request2);

        final ClosedFile result = file.close();

        assertThat(result.start())
            .isEqualTo(start);
        assertThat(result.originalRequests())
            .isEqualTo(Map.of(0, request1, 1, request2));
        assertThat(result.awaitingFuturesByRequest()).hasSize(2);
        assertThat(result.awaitingFuturesByRequest().get(0)).isNotCompleted();
        assertThat(result.awaitingFuturesByRequest().get(1)).isNotCompleted();
        assertThat(result.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, 78, 1, 1000),
            new CommitBatchRequest(T0P1, 78, 78, 1, 2000),
            new CommitBatchRequest(T0P1, 156, 78, 1, 3000),
            new CommitBatchRequest(T1P0, 234, 78, 1, 4000)
        );
        assertThat(result.requestIds()).containsExactly(0, 0, 1, 1);
        assertThat(result.data()).hasSize(312);
    }
}
