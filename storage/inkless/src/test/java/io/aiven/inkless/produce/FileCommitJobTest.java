// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileCommitJobTest {
    static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    static final TopicPartition T1P0 = new TopicPartition("topic1", 0);
    static final TopicPartition T1P1 = new TopicPartition("topic1", 1);

    static final Map<TopicPartition, MemoryRecords> REQUEST_0 = Map.of(
        T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
        T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
    );
    static final Map<TopicPartition, MemoryRecords> REQUEST_1 = Map.of(
        T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
        T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
    );
    static final Map<Integer, Map<TopicPartition, MemoryRecords>> REQUESTS = Map.of(
        0, REQUEST_0,
        1, REQUEST_1
    );
    static final List<CommitBatchRequest> COMMIT_BATCH_REQUESTS = List.of(
        new CommitBatchRequest(T0P0, 0, 100, 10),
        new CommitBatchRequest(T0P1, 100, 100, 10),
        new CommitBatchRequest(T0P1, 200, 100, 10),
        new CommitBatchRequest(T1P0, 300, 100, 10)
    );
    static final List<Integer> REQUEST_IDS = List.of(0, 0, 1, 1);

    static final byte[] DATA = new byte[10];
    static final ObjectKey OBJECT_KEY = new PlainObjectKey("", "s");

    @Mock
    ControlPlane controlPlane;
    @Mock
    Consumer<Integer> sizeCallback;

    @Test
    void commitFinishedSuccessfully() {
        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1),  // some arbitrary uploadError
            new CommitBatchResponse(Errors.NONE, 20),
            new CommitBatchResponse(Errors.NONE, 30)
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY), eq(COMMIT_BATCH_REQUESTS)))
            .thenReturn(commitBatchResponses);

        final ClosedFile file = new ClosedFile(REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, REQUEST_IDS, DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(file, uploadFuture, controlPlane, sizeCallback);

        job.run();

        assertThat(awaitingFuturesByRequest.get(0)).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 0, -1, -1),
            T0P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(awaitingFuturesByRequest.get(1)).isCompletedWithValue(Map.of(
            T0P1, new PartitionResponse(Errors.NONE, 20, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 30, -1, -1)
        ));

        verify(sizeCallback).accept(eq(DATA.length));
    }

    @Test
    void commitFinishedWithError() {
        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final ClosedFile file = new ClosedFile(REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, REQUEST_IDS, DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.failedFuture(new StorageBackendException("test"));
        final FileCommitJob job = new FileCommitJob(file, uploadFuture, controlPlane, sizeCallback);

        job.run();

        assertThat(awaitingFuturesByRequest.get(0)).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T0P1, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        assertThat(awaitingFuturesByRequest.get(1)).isCompletedWithValue(Map.of(
            T0P1, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T1P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));

        verify(sizeCallback).accept(eq(DATA.length));
    }
}
