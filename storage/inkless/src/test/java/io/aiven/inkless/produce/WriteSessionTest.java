// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;

import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteSessionTest {
    static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    static final TopicPartition T1P0 = new TopicPartition("topic1", 0);

    @Test
    void add() {
        final WriteSession session = new WriteSession(1000);

        final var result = session.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));
        assertThat(result).isNotCompleted();
    }

    @Test
    void addAfterClosing() {
        final WriteSession session = new WriteSession(1000);
        session.closeAndPrepareForCommit();
        assertThatThrownBy(() -> session.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        )))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Attempt to add after closing");

    }

    @Test
    void canAddMore() {
        final WriteSession session = new WriteSession(1070);

        assertThat(session.canAddMore()).isTrue();

        session.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[1000]))
        ));

        assertThat(session.canAddMore()).isFalse();
    }

    @Test
    void closeAndPrepareForCommit() {
        final WriteSession session = new WriteSession(1000);

        session.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));
        session.add(Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));

        final BatchBufferCloseResult result = session.closeAndPrepareForCommit();
        assertThat(result.commitBatchRequests()).hasSize(4);
        assertThat(result.commitBatchRequests().get(0).topicPartition()).isEqualTo(T0P0);
        assertThat(result.commitBatchRequests().get(1).topicPartition()).isEqualTo(T0P1);
        assertThat(result.commitBatchRequests().get(2).topicPartition()).isEqualTo(T0P1);
        assertThat(result.commitBatchRequests().get(3).topicPartition()).isEqualTo(T1P0);
        assertThat(result.requestIds()).containsExactly(0, 0, 1, 1);
        assertThat(result.data()).isNotEmpty();
    }

    @Test
    void closeAfterClosing() {
        final WriteSession session = new WriteSession(1000);
        session.closeAndPrepareForCommit();
        assertThatThrownBy(session::closeAndPrepareForCommit)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Attempt to close after closing");

    }

    @Test
    void commitFinishedSuccessfully() throws ExecutionException, InterruptedException {
        final WriteSession session = new WriteSession(1000);

        final var result1 = session.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));
        final var result2 = session.add(Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));

        session.closeAndPrepareForCommit();
        session.finishCommit(List.of(
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1),  // some arbitrary error
            new CommitBatchResponse(Errors.NONE, 20),
            new CommitBatchResponse(Errors.NONE, 30)
        ), null);

        assertThat(result1).isCompletedWithValue(Map.of(
            T0P0, new ProduceResponse.PartitionResponse(Errors.NONE, 0, -1, -1),
            T0P1, new ProduceResponse.PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(result2).isCompletedWithValue(Map.of(
            T0P1, new ProduceResponse.PartitionResponse(Errors.NONE, 20, -1, -1),
            T1P0, new ProduceResponse.PartitionResponse(Errors.NONE, 30, -1, -1)
        ));
    }

    @Test
    void commitFinishedWithError() {
        final WriteSession session = new WriteSession(1000);

        final var result1 = session.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));
        final var result2 = session.add(Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ));

        session.closeAndPrepareForCommit();
        session.finishCommit(null, new StorageBackendException("error uploading"));

        assertThat(result1).isCompletedWithValue(Map.of(
            T0P0, new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T0P1, new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        assertThat(result2).isCompletedWithValue(Map.of(
            T0P1, new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T1P0, new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
    }

    @Test
    void commitFinishedBeforeClosing() {
        final WriteSession session = new WriteSession(1000);
        assertThatThrownBy(() -> session.finishCommit(null, null))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Attempt to finish commit before closing");

    }
}
