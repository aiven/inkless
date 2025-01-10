// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActiveFileTest {
    static final Uuid TOPIC_ID_0 = new Uuid(1000, 1000);
    static final Uuid TOPIC_ID_1 = new Uuid(2000, 2000);
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    static final Map<String, TimestampType> TIMESTAMP_TYPES = Map.of(
        TOPIC_0, TimestampType.CREATE_TIME,
        TOPIC_1, TimestampType.LOG_APPEND_TIME
    );

    @Test
    void addNull() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThatThrownBy(() -> file.add(null, TIMESTAMP_TYPES))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("entriesPerPartition cannot be null");
        assertThatThrownBy(() -> file.add(Map.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("timestampTypes cannot be null");
    }

    @Test
    void add() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        final var result = file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), TIMESTAMP_TYPES);
        assertThat(result).isNotCompleted();
    }

    @Test
    void addWithoutTimestampType() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThatThrownBy(() -> file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Timestamp type not provided for topic " + TOPIC_0);
    }

    @Test
    void empty() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.isEmpty()).isTrue();

        file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), TIMESTAMP_TYPES);

        assertThat(file.isEmpty()).isFalse();
    }

    @Test
    void size() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.size()).isZero();

        file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), TIMESTAMP_TYPES);

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
        final Time time = new MockTime();
        final ActiveFile file = new ActiveFile(time, start);
        final Map<TopicIdPartition, MemoryRecords> request1 = Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10])),
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(2000, new byte[10]))
        );
        file.add(request1, TIMESTAMP_TYPES);
        final Map<TopicIdPartition, MemoryRecords> request2 = Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(3000, new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(4000, new byte[10]))
        );
        file.add(request2, TIMESTAMP_TYPES);

        final ClosedFile result = file.close();

        assertThat(result.start())
            .isEqualTo(start);
        assertThat(result.originalRequests())
            .isEqualTo(Map.of(0, request1, 1, request2));
        assertThat(result.awaitingFuturesByRequest()).hasSize(2);
        assertThat(result.awaitingFuturesByRequest().get(0)).isNotCompleted();
        assertThat(result.awaitingFuturesByRequest().get(1)).isNotCompleted();
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, 78, 0, 0, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(T0P1, 78, 78, 0, 0, 2000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(T0P1, 156, 78, 0, 0, 3000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(T1P0, 234, 78, 0, 0, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
        );
        assertThat(result.requestIds()).containsExactly(0, 0, 1, 1);
        assertThat(result.data()).hasSize(312);
    }
}
