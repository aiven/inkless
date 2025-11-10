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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class AppendCompleterTest {
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

    @Test
    void commitFinishedSuccessfully() throws Exception {
        final Map<Integer, CompletableFuture<Map<TopicIdPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            CommitBatchResponse.success(0, 10, 0, "objectKey", COMMIT_BATCH_REQUESTS.get(0)),
            CommitBatchResponse.of(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1),  // some arbitrary uploadError
            CommitBatchResponse.success(20, 10, 0, "objectKey", COMMIT_BATCH_REQUESTS.get(2)),
            CommitBatchResponse.success(30, 10, 0, "objectKey", COMMIT_BATCH_REQUESTS.get(3))
        );

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final BatchCoordinateCache cache = mock(BatchCoordinateCache.class);
        final AppendCompleter job = new AppendCompleter(file, cache);

        job.finishCommitSuccessfully(commitBatchResponses);

        assertThat(awaitingFuturesByRequest.get(0)).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 0, -1, 0),
            T0P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(awaitingFuturesByRequest.get(1)).isCompletedWithValue(Map.of(
            T0P1, new PartitionResponse(Errors.NONE, 20, -1, 0),
            T1P0, new PartitionResponse(Errors.NONE, 30, 10, 0)
        ));

        verify(cache).put(T0P0, commitBatchResponses.get(0).cacheBatchCoordinate());
        verify(cache).put(T0P1, commitBatchResponses.get(2).cacheBatchCoordinate());
        verify(cache).put(T1P0, commitBatchResponses.get(3).cacheBatchCoordinate());
        verifyNoMoreInteractions(cache);
    }

    @Test
    void commitFinishedSuccessfullyZeroBatches() {
        // We sent two requests, both without any batch.

        final Map<Integer, CompletableFuture<Map<TopicIdPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of();

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final BatchCoordinateCache cache = mock(BatchCoordinateCache.class);
        final AppendCompleter job = new AppendCompleter(file, cache);

        job.finishCommitSuccessfully(commitBatchResponses);

        assertThat(awaitingFuturesByRequest.get(0)).isCompletedWithValue(Map.of());
        assertThat(awaitingFuturesByRequest.get(1)).isCompletedWithValue(Map.of());
        verify(cache, never()).put(any(), any());
    }


    @Test
    void requestContainedOnlyInvalidRequests() {
        // We sent two requests, both without any batch.

        final Map<Integer, CompletableFuture<Map<TopicIdPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
                0, new CompletableFuture<>(),
                1, new CompletableFuture<>()
        );
        // All partitions within these requests contained validation errors
        Map<Integer, Map<TopicIdPartition, PartitionResponse>> invalidResponses = Map.of(
            0, Map.of(
                T0P0, new PartitionResponse(Errors.INVALID_TIMESTAMP, -1, -1, -1),
                T0P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
            ),
            1, Map.of(
                T0P1, new PartitionResponse(Errors.CORRUPT_MESSAGE, -1, -1, -1),
                T1P0, new PartitionResponse(Errors.INVALID_RECORD, -1, -1, -1)
            )
        );

        final List<CommitBatchResponse> commitBatchResponses = List.of();

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, List.of(), invalidResponses, new byte[0]);
        final BatchCoordinateCache cache = mock(BatchCoordinateCache.class);
        final AppendCompleter job = new AppendCompleter(file, cache);

        job.finishCommitSuccessfully(commitBatchResponses);

        assertThat(awaitingFuturesByRequest.get(0)).isCompletedWithValue(Map.of(
                T0P0, new PartitionResponse(Errors.INVALID_TIMESTAMP, -1, -1, -1),
                T0P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(awaitingFuturesByRequest.get(1)).isCompletedWithValue(Map.of(
                T0P1, new PartitionResponse(Errors.CORRUPT_MESSAGE, -1, -1, -1),
                T1P0, new PartitionResponse(Errors.INVALID_RECORD, -1, -1, -1)
        ));
        verify(cache, never()).put(any(), any());
    }

    @Test
    void commitFinishedWithError() {
        final Map<Integer, CompletableFuture<Map<TopicIdPartition, PartitionResponse>>> awaitingFuturesByRequest = Map.of(
            0, new CompletableFuture<>(),
            1, new CompletableFuture<>()
        );

        final ClosedFile file = new ClosedFile(Instant.EPOCH, REQUESTS, awaitingFuturesByRequest, COMMIT_BATCH_REQUESTS, Map.of(), DATA);
        final BatchCoordinateCache cache = mock(BatchCoordinateCache.class);
        final AppendCompleter job = new AppendCompleter(file, cache);

        job.finishCommitWithError();

        assertThat(awaitingFuturesByRequest.get(0)).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T0P1, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        assertThat(awaitingFuturesByRequest.get(1)).isCompletedWithValue(Map.of(
            T0P1, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data"),
            T1P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")
        ));
        verify(cache, never()).put(any(), any());
    }
}
