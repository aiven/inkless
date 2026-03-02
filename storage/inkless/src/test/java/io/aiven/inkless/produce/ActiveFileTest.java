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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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

    static final Map<String, LogConfig> TOPIC_CONFIGS = Map.of(
        TOPIC_0, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name)),
        TOPIC_1, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name))
    );

    static final RequestLocal REQUEST_LOCAL = RequestLocal.noCaching();

    static LogConfig logConfig(Map<String, Object> config) {
        return new LogConfig(config);
    }

    @Test
    void addNull() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThatThrownBy(() -> file.add(null, TOPIC_CONFIGS, REQUEST_LOCAL))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("entriesPerPartition cannot be null");
        assertThatThrownBy(() -> file.add(Map.of(), null, REQUEST_LOCAL))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicConfigs cannot be null");
        assertThatThrownBy(() -> file.add(Map.of(), TOPIC_CONFIGS, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("requestLocal cannot be null");
    }

    @Test
    void add() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        final var result = file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), TOPIC_CONFIGS, REQUEST_LOCAL);
        assertThat(result).isNotCompleted();
    }

    @Test
    void addWithoutConfig() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThatThrownBy(() -> file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), Map.of(), REQUEST_LOCAL))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Config not provided for topic " + TOPIC_0);
    }

    @Test
    void empty() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.isEmpty()).isTrue();

        file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), TOPIC_CONFIGS, REQUEST_LOCAL);

        assertThat(file.isEmpty()).isFalse();
    }

    @Test
    void size() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        assertThat(file.size()).isZero();

        file.add(Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))
        ), TOPIC_CONFIGS, REQUEST_LOCAL);

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
            .isEqualTo(new ClosedFile(start, Map.of(), Map.of(), List.of(), Map.of(), new byte[0]));
        assertThat(result.data()).isEmpty();
        assertThat(result.isEmpty()).isTrue();
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
        file.add(request1, TOPIC_CONFIGS, REQUEST_LOCAL);
        final Map<TopicIdPartition, MemoryRecords> request2 = Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(3000, new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(4000, new byte[10]))
        );
        file.add(request2, TOPIC_CONFIGS, REQUEST_LOCAL);

        final ClosedFile result = file.close();

        assertThat(result.start())
            .isEqualTo(start);
        assertThat(result.originalRequests())
            .isEqualTo(Map.of(0, request1, 1, request2));
        assertThat(result.awaitingFuturesByRequest()).hasSize(2);
        assertThat(result.awaitingFuturesByRequest().get(0)).isNotCompleted();
        assertThat(result.awaitingFuturesByRequest().get(1)).isNotCompleted();
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(0, T0P0, 0, 78, 0, 0, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P1, 78, 78, 0, 0, 2000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(1, T0P1, 156, 78, 0, 0, 3000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(1, T1P0, 234, 78, 0, 0, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
        );
        assertThat(result.data()).hasSize(312);
        assertThat(result.isEmpty()).isFalse();
    }

    @Test
    void gatherInvalidResponses() {
        final Instant start = Instant.ofEpochMilli(10);
        final Time time = new MockTime();
        final ActiveFile file = new ActiveFile(time, start);
        final Map<TopicIdPartition, MemoryRecords> request1 = Map.of(
            T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10])),
            // invalid request batch, forces invalid response as initial offset is not 0
            T0P1, MemoryRecords.withRecords(1, Compression.NONE, new SimpleRecord(2000, new byte[10]))
        );
        file.add(request1, TOPIC_CONFIGS, REQUEST_LOCAL);
        final Map<TopicIdPartition, MemoryRecords> request2 = Map.of(
            T0P1, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(3000, new byte[10])),
            T1P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(4000, new byte[10]))
        );
        file.add(request2, TOPIC_CONFIGS, REQUEST_LOCAL);

        final ClosedFile result = file.close();

        assertThat(result.start())
            .isEqualTo(start);
        assertThat(result.originalRequests())
            .isEqualTo(Map.of(0, request1, 1, request2));
        assertThat(result.awaitingFuturesByRequest()).hasSize(2);
        assertThat(result.awaitingFuturesByRequest().get(0)).isNotCompleted();
        assertThat(result.awaitingFuturesByRequest().get(1)).isNotCompleted();
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(0, T0P0, 0, 78, 0, 0, 1000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(1, T0P1, 78, 78, 0, 0, 3000, TimestampType.CREATE_TIME),
            CommitBatchRequest.of(1, T1P0, 156, 78, 0, 0, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
        );
        assertThat(result.invalidResponseByRequest().get(0))
            .containsExactly(Map.entry(T0P1, new ProduceResponse.PartitionResponse(Errors.INVALID_RECORD)));
        assertThat(result.data()).hasSize(312 - 78); // 78 bytes of the invalid batch
    }

    // ========== Tests for PipelinedWriter support methods ==========

    /**
     * Test addBatchDirect adds pre-validated batches to the buffer.
     * This method is used by PipelinedWriter where validation is done in a separate stage.
     *
     * <p>Note: addBatchDirect only adds batches to the buffer but doesn't update the
     * requestId counter. The full pipelined flow requires calling addAwaitingFuture
     * to register the request and update isEmpty() status.
     */
    @Test
    void addBatchDirect() {
        final Instant start = Instant.ofEpochMilli(10);
        final ActiveFile file = new ActiveFile(Time.SYSTEM, start);

        // Create a record and extract the batch
        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10]));
        final MutableRecordBatch batch = records.batches().iterator().next();

        // Initially empty and zero size
        assertThat(file.size()).isZero();

        // Add batch directly (bypassing validation)
        file.addBatchDirect(T0P0, batch, 0);

        // Size is updated, but isEmpty() requires addAwaitingFuture to be called
        assertThat(file.size()).isEqualTo(78);

        // Register the request to complete the pipelined flow
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future = new CompletableFuture<>();
        file.addAwaitingFuture(0, future, Map.of(T0P0, records), Map.of());

        // Now isEmpty() returns false because a request was registered
        assertThat(file.isEmpty()).isFalse();
    }

    /**
     * Test addBatchDirect with multiple batches to different partitions.
     */
    @Test
    void addBatchDirectMultiplePartitions() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        final MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10]));
        final MemoryRecords records2 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(2000, new byte[10]));

        file.addBatchDirect(T0P0, records1.batches().iterator().next(), 0);
        file.addBatchDirect(T0P1, records2.batches().iterator().next(), 0);

        assertThat(file.size()).isEqualTo(156);  // 78 * 2

        final ClosedFile closed = file.close();
        assertThat(closed.commitBatchRequests()).hasSize(2);
    }

    /**
     * Test addAwaitingFuture registers futures for completion.
     * This method is used by PipelinedWriter to store futures after validation.
     */
    @Test
    void addAwaitingFuture() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        // Add some batches first
        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10]));
        file.addBatchDirect(T0P0, records.batches().iterator().next(), 0);

        // Create and register a future
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> resultFuture = new CompletableFuture<>();
        final Map<TopicIdPartition, MemoryRecords> originalRecords = Map.of(T0P0, records);
        final Map<TopicIdPartition, ProduceResponse.PartitionResponse> invalidBatches = Map.of();

        file.addAwaitingFuture(0, resultFuture, originalRecords, invalidBatches);

        // Close and verify the future is tracked
        final ClosedFile closed = file.close();
        assertThat(closed.awaitingFuturesByRequest()).hasSize(1);
        assertThat(closed.awaitingFuturesByRequest().get(0)).isSameAs(resultFuture);
        assertThat(closed.originalRequests().get(0)).isEqualTo(originalRecords);
    }

    /**
     * Test addAwaitingFuture with invalid batches.
     */
    @Test
    void addAwaitingFutureWithInvalidBatches() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        // Add some batches
        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10]));
        file.addBatchDirect(T0P0, records.batches().iterator().next(), 0);

        // Create future with some invalid batches
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> resultFuture = new CompletableFuture<>();
        final Map<TopicIdPartition, MemoryRecords> originalRecords = Map.of(
            T0P0, records,
            T0P1, records  // This one will be "invalid"
        );
        final Map<TopicIdPartition, ProduceResponse.PartitionResponse> invalidBatches = Map.of(
            T0P1, new ProduceResponse.PartitionResponse(Errors.INVALID_RECORD)
        );

        file.addAwaitingFuture(0, resultFuture, originalRecords, invalidBatches);

        final ClosedFile closed = file.close();
        assertThat(closed.invalidResponseByRequest().get(0)).containsKey(T0P1);
        assertThat(closed.invalidResponseByRequest().get(0).get(T0P1).error).isEqualTo(Errors.INVALID_RECORD);
    }

    /**
     * Test multiple requests using addBatchDirect and addAwaitingFuture.
     */
    @Test
    void multiplePipelinedRequests() {
        final ActiveFile file = new ActiveFile(Time.SYSTEM, Instant.EPOCH);

        // Request 0
        final MemoryRecords records0 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10]));
        file.addBatchDirect(T0P0, records0.batches().iterator().next(), 0);
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future0 = new CompletableFuture<>();
        file.addAwaitingFuture(0, future0, Map.of(T0P0, records0), Map.of());

        // Request 1
        final MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(2000, new byte[10]));
        file.addBatchDirect(T0P1, records1.batches().iterator().next(), 1);
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future1 = new CompletableFuture<>();
        file.addAwaitingFuture(1, future1, Map.of(T0P1, records1), Map.of());

        // Request 2
        final MemoryRecords records2 = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(3000, new byte[10]));
        file.addBatchDirect(T1P0, records2.batches().iterator().next(), 2);
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future2 = new CompletableFuture<>();
        file.addAwaitingFuture(2, future2, Map.of(T1P0, records2), Map.of());

        final ClosedFile closed = file.close();

        // Verify all requests are tracked
        assertThat(closed.awaitingFuturesByRequest()).hasSize(3);
        assertThat(closed.originalRequests()).hasSize(3);
        assertThat(closed.commitBatchRequests()).hasSize(3);

        // Verify futures are not yet completed
        assertThat(future0).isNotCompleted();
        assertThat(future1).isNotCompleted();
        assertThat(future2).isNotCompleted();
    }

    /**
     * Test that addBatchDirect initializes start time on first call.
     */
    @Test
    void addBatchDirectInitializesStartTime() {
        final Time time = new MockTime();
        final ActiveFile file = new ActiveFile(time, (Instant) null);

        // start is null initially
        assertThat(file.isEmpty()).isTrue();

        final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000, new byte[10]));
        file.addBatchDirect(T0P0, records.batches().iterator().next(), 0);

        // After addBatchDirect, the file should have a start time
        final ClosedFile closed = file.close();
        assertThat(closed.start()).isNotNull();
    }
}
