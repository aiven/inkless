/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectDeleter;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

    static final Map<TopicIdPartition, MemoryRecords> REQUEST_0 = Map.of(
        T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
        T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
    );
    static final Map<TopicIdPartition, MemoryRecords> REQUEST_1 = Map.of(
        T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10])),
        T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
    );
    static final Map<Integer, Map<TopicIdPartition, MemoryRecords>> REQUESTS = Map.of(
        0, REQUEST_0,
        1, REQUEST_1
    );
    static final List<CommitBatchRequest> COMMIT_BATCH_REQUESTS = List.of(
        CommitBatchRequest.of(0, T0P0, 0, 100, 0, 9, 1000, TimestampType.CREATE_TIME),
        CommitBatchRequest.of(0, T0P1, 100, 100, 0, 9, 1000, TimestampType.CREATE_TIME),
        CommitBatchRequest.of(1, T0P1, 200, 100, 0, 9, 1000, TimestampType.CREATE_TIME),
        CommitBatchRequest.of(1, T1P0, 300, 100, 0, 9, 1000, TimestampType.LOG_APPEND_TIME)
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
    @Mock
    Consumer<Long> commitWaitTimeDurationCallback;

    @Test
    void commitFinishedSuccessfully() {
        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            CommitBatchResponse.success(0, 10, 0, "objectKey", COMMIT_BATCH_REQUESTS.get(0)),
            CommitBatchResponse.of(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1),  // some arbitrary uploadError
            CommitBatchResponse.success(20, 10, 0, "objectKey", COMMIT_BATCH_REQUESTS.get(2)),
            CommitBatchResponse.success(30, 10, 0, "objectKey", COMMIT_BATCH_REQUESTS.get(3))
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenReturn(commitBatchResponses);
        when(time.milliseconds()).thenReturn(10_000L, 10_200L);
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback, commitWaitTimeDurationCallback);

        job.get();

        verify(commitWaitTimeDurationCallback).accept(eq(200L));
        verify(commitTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void commitFinishedSuccessfullyZeroBatches() {
        // We sent two requests, both without any batch.

        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of();

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenReturn(commitBatchResponses);
        when(time.milliseconds()).thenReturn(10_000L, 10_200L);
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback, commitWaitTimeDurationCallback);

        job.get();

        verify(commitWaitTimeDurationCallback).accept(eq(200L));
        verify(commitTimeDurationCallback).accept(eq(10L));
    }

    @Test
    void commitFinishedWithError() {
        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        when(time.milliseconds()).thenReturn(10_000L, 10_200L);
        when(time.nanoseconds()).thenReturn(10_000_000L, 20_000_000L);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.failedFuture(new StorageBackendException("test"));
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback, commitWaitTimeDurationCallback);

        Assert.assertThrows(RuntimeException.class, job::get);

        verify(commitWaitTimeDurationCallback).accept(eq(200L));
        verify(commitTimeDurationCallback).accept(eq(10L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void deleteObjectWhenFailureOnCommitIsFromControlPlane(boolean isSafeToDelete) throws Exception {
        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenThrow(new ControlPlaneException("test"));
        when(controlPlane.isSafeToDeleteFile(eq(OBJECT_KEY_MAIN_PART))).thenReturn(isSafeToDelete);

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback, commitWaitTimeDurationCallback);

        Assert.assertThrows(RuntimeException.class, job::get);

        verify(objectDeleter, times(isSafeToDelete ? 1 : 0)).delete(eq(OBJECT_KEY));
    }

    @Test
    void doNotDeleteObjectWhenFailureOnCommitIsNotFromControlPlane() throws Exception {
        final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        when(controlPlane.commitFile(eq(OBJECT_KEY_MAIN_PART), eq(ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT), eq(BROKER_ID), eq(FILE_SIZE), eq(COMMIT_BATCH_REQUESTS)))
            .thenThrow(new RuntimeException("test"));

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final CompletableFuture<ObjectKey> uploadFuture = CompletableFuture.completedFuture(OBJECT_KEY);
        final FileCommitJob job = new FileCommitJob(BROKER_ID, file, uploadFuture, time, controlPlane, objectDeleter, commitTimeDurationCallback, commitWaitTimeDurationCallback);

        Assert.assertThrows(RuntimeException.class, job::get);

        verify(objectDeleter, never()).delete(eq(OBJECT_KEY));
    }
}
