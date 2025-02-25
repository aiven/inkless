// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.aiven.inkless.FutureUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchRequestContext;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectDeleter;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileCommitJobTest {
    static final int BROKER_ID = 11;

    static final Uuid TOPIC_ID_0 = new Uuid(1000, 1000);
    static final Uuid TOPIC_ID_1 = new Uuid(2000, 2000);
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    private static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    private static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    private static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    static final List<CommitBatchRequest> COMMIT_BATCH_REQUESTS = List.of(
        CommitBatchRequest.of(T0P0, 0, 100, 0, 9, 1000, TimestampType.CREATE_TIME),
        CommitBatchRequest.of(T0P1, 100, 100, 0, 9, 1000, TimestampType.CREATE_TIME),
        CommitBatchRequest.of(T0P1, 200, 100, 0, 9, 1000, TimestampType.CREATE_TIME),
        CommitBatchRequest.of(T1P0, 300, 100, 0, 9, 1000, TimestampType.LOG_APPEND_TIME)
    );
    static final List<CommitBatchRequestContext> COMMIT_BATCH_REQUESTS_BY_REQUEST = List.of(
        new CommitBatchRequestContext(0, T0P0.topicPartition(), COMMIT_BATCH_REQUESTS.get(0)),
        new CommitBatchRequestContext(0, T0P1.topicPartition(), COMMIT_BATCH_REQUESTS.get(1)),
        new CommitBatchRequestContext(1, T0P1.topicPartition(), COMMIT_BATCH_REQUESTS.get(2)),
        new CommitBatchRequestContext(1, T1P0.topicPartition(), COMMIT_BATCH_REQUESTS.get(3))
    );

    static final byte[] DATA = new byte[10];
    static final long FILE_SIZE = DATA.length;
    static final String OBJECT_KEY_MAIN_PART = "obj";
    static final ObjectKey OBJECT_KEY = PlainObjectKey.create("", OBJECT_KEY_MAIN_PART);

    @Mock
    Time time;
    @Mock
    InMemoryControlPlane controlPlane;
    @Mock
    ObjectDeleter objectDeleter;
    @Mock
    Consumer<Long> commitTimeDurationCallback;

    @Test
    void commitFinishedSuccessfully() {
        final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest = Map.of(
            0, Map.of(T0P0.topicPartition(), new CompletableFuture<>(), T0P1.topicPartition(), new CompletableFuture<>()),
            1, Map.of(T0P1.topicPartition(), new CompletableFuture<>(), T1P0.topicPartition(), new CompletableFuture<>())
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            CommitBatchResponse.success(0, 10, 0, COMMIT_BATCH_REQUESTS.get(0)),
            CommitBatchResponse.of(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1),  // some arbitrary uploadError
            CommitBatchResponse.success(20, 10, 0, COMMIT_BATCH_REQUESTS.get(2)),
            CommitBatchResponse.success(30, 10, 0, COMMIT_BATCH_REQUESTS.get(3))
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(BROKER_ID), eq(FILE_SIZE), anyList()))
            .thenReturn(commitBatchResponses);
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, allFuturesByRequest, COMMIT_BATCH_REQUESTS_BY_REQUEST, DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback);

        job.run();

        assertThat(FutureUtils.combineMapOfFutures(allFuturesByRequest.get(0))).isCompletedWithValue(Map.of(
            T0P0.topicPartition(), new PartitionResponse(Errors.NONE, 0, -1, 0),
            T0P1.topicPartition(), new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(FutureUtils.combineMapOfFutures(allFuturesByRequest.get(1))).isCompletedWithValue(Map.of(
            T0P1.topicPartition(), new PartitionResponse(Errors.NONE, 20, -1, 0),
            T1P0.topicPartition(), new PartitionResponse(Errors.NONE, 30, 10, 0)
        ));
        verify(commitTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void commitFinishedSuccessfullyZeroBatches() {
        // We sent two requests, both without any batch.

        final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, Map.of(),
            1, Map.of()
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of();

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenReturn(commitBatchResponses);
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, awaitingFuturesByRequest, List.of(), new byte[0]);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback);

        job.run();

        assertThat(FutureUtils.combineMapOfFutures(awaitingFuturesByRequest.get(0))).isCompletedWithValue(Map.of());
        assertThat(FutureUtils.combineMapOfFutures(awaitingFuturesByRequest.get(1))).isCompletedWithValue(Map.of());
        verify(commitTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void commitFinishedWithError() {
        final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, Map.of(T0P0.topicPartition(), new CompletableFuture<>(), T0P1.topicPartition(), new CompletableFuture<>()),
            1, Map.of(T0P1.topicPartition(), new CompletableFuture<>(), T1P0.topicPartition(), new CompletableFuture<>())
        );

        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS_BY_REQUEST, DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.failedFuture(new StorageBackendException("test"));
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback);

        job.run();

        assertThat(FutureUtils.combineMapOfFutures(awaitingFuturesByRequest.get(0))).isCompletedWithValue(Map.of(
            T0P0.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T0P1.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        assertThat(FutureUtils.combineMapOfFutures(awaitingFuturesByRequest.get(1))).isCompletedWithValue(Map.of(
            T0P1.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T1P0.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        verify(commitTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void deleteObjectWhenFailureOnCommit() throws Exception {
        final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, Map.of(T0P0.topicPartition(), new CompletableFuture<>(), T0P1.topicPartition(), new CompletableFuture<>()),
            1, Map.of(T0P1.topicPartition(), new CompletableFuture<>(), T1P0.topicPartition(), new CompletableFuture<>())
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenThrow(new ControlPlaneException("test"));

        final ClosedFile file = new ClosedFile(Instant.EPOCH, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS_BY_REQUEST, DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback);

        job.run();

        verify(objectDeleter).delete(eq(OBJECT_KEY));
        assertThat(FutureUtils.combineMapOfFutures(awaitingFuturesByRequest.get(0))).isCompletedWithValue(Map.of(
            T0P0.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T0P1.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        assertThat(FutureUtils.combineMapOfFutures(awaitingFuturesByRequest.get(1))).isCompletedWithValue(Map.of(
            T0P1.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T1P0.topicPartition(), new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
    }

    @Test
    void failIfFutureAlreadyCompleted() {
        final var future0 = new CompletableFuture<PartitionResponse>();
        final var future1 = new CompletableFuture<PartitionResponse>();
        final Map<Integer, Map<TopicPartition, CompletableFuture<PartitionResponse>>> allFuturesByRequest = Map.of(
            0, Map.of(T0P0.topicPartition(), new CompletableFuture<>(), T0P1.topicPartition(), future0),
            1, Map.of(T0P1.topicPartition(), new CompletableFuture<>(), T1P0.topicPartition(), future1)
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            CommitBatchResponse.success(0, 10, 0, COMMIT_BATCH_REQUESTS.get(0)),
            CommitBatchResponse.of(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1),  // some arbitrary uploadError
            CommitBatchResponse.success(20, 10, 0, COMMIT_BATCH_REQUESTS.get(2)),
            CommitBatchResponse.success(30, 10, 0, COMMIT_BATCH_REQUESTS.get(3))
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenReturn(commitBatchResponses);
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, allFuturesByRequest, COMMIT_BATCH_REQUESTS_BY_REQUEST, DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback);

        // When the future is already completed
        future1.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
        // Then the job should fail
        assertThatThrownBy(job::run)
            .isInstanceOf(RuntimeException.class)
            .cause()
            .isInstanceOf(ControlPlaneException.class)
            .hasMessage("Not all futures were completed by commit job for file " + file);
        // and all other futures should be completed
        assertThat(future0).isCompleted();

        verify(commitTimeDurationCallback).accept(eq(10L));
    }
}
