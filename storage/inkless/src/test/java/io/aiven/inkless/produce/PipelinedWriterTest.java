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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.storage_backend.common.StorageBackend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link PipelinedWriter}.
 *
 * <p>These tests verify the SEDA-based pipelined writer functionality,
 * including tick-based rotation which was a source of bugs.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PipelinedWriterTest {
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    static final Map<String, LogConfig> TOPIC_CONFIGS = Map.of(
        TOPIC_0, new LogConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name)),
        TOPIC_1, new LogConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name))
    );
    static final RequestLocal REQUEST_LOCAL = RequestLocal.noCaching();

    MockTime time;
    ScheduledExecutorService tickScheduler;
    BrokerTopicStats brokerTopicStats;
    WriterTestUtils.RecordCreator recordCreator;

    @Mock
    StorageBackend storage;
    @Mock
    FileCommitter fileCommitter;
    @Mock
    WriterMetrics writerMetrics;

    PipelinedWriter writer;

    @BeforeEach
    void setup() throws InterruptedException {
        time = new MockTime();
        tickScheduler = Executors.newScheduledThreadPool(1);
        brokerTopicStats = new BrokerTopicStats();
        recordCreator = new WriterTestUtils.RecordCreator();

        // Set up mock to complete futures when commit is called
        // This simulates what AppendCompleter does in production
        doAnswer(invocation -> {
            ClosedFile closedFile = invocation.getArgument(0);
            // Complete all awaiting futures with success responses
            closedFile.awaitingFuturesByRequest().forEach((requestId, future) -> {
                Map<TopicIdPartition, PartitionResponse> response = new HashMap<>();
                closedFile.originalRequests().getOrDefault(requestId, Map.of()).keySet().forEach(tip ->
                    response.put(tip, new PartitionResponse(Errors.NONE, 0, -1, 0))
                );
                // Also include invalid batches in response
                response.putAll(closedFile.invalidResponseByRequest().getOrDefault(requestId, Map.of()));
                future.complete(response);
            });
            return null;
        }).when(fileCommitter).commit(any(ClosedFile.class));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
        tickScheduler.shutdownNow();
    }

    private PipelinedWriter createWriter(Duration commitInterval, int maxBufferSize, int validationThreads) {
        return new PipelinedWriter(
            time,
            commitInterval,
            maxBufferSize,
            tickScheduler,
            storage,
            fileCommitter,
            writerMetrics,
            brokerTopicStats,
            validationThreads
        );
    }

    /**
     * Test that tick-based rotation works correctly.
     *
     * <p>This test verifies that rotation triggers are properly processed
     * when the buffer doesn't fill up (requiring tick-based rotation).
     */
    @Test
    void tickBasedRotationCompletesFutures() throws InterruptedException, ExecutionException, TimeoutException {
        // Use a very short commit interval so tick fires quickly
        final Duration commitInterval = Duration.ofMillis(50);
        // Use a large buffer so size-based rotation doesn't trigger
        final int maxBufferSize = 10 * 1024 * 1024;  // 10 MB

        writer = createWriter(commitInterval, maxBufferSize, 4);

        // Write a small request (won't trigger size-based rotation)
        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 10)  // small batch
        );

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
            writer.write(writeRequest, TOPIC_CONFIGS, REQUEST_LOCAL);

        // The future should complete within a reasonable time via tick-based rotation
        // If tick-based rotation is broken, this will timeout
        Map<TopicIdPartition, PartitionResponse> result = future.get(5, TimeUnit.SECONDS);

        // Verify the response
        assertThat(result).containsKey(T0P0);
        assertThat(result.get(T0P0).error).isEqualTo(Errors.NONE);

        // Verify commit was called (rotation happened)
        verify(fileCommitter, atLeastOnce()).commit(any());
    }

    /**
     * Test that size-based rotation works when buffer fills up.
     */
    @Test
    void sizeBasedRotationCompletesFutures() throws InterruptedException, ExecutionException, TimeoutException {
        // Use a long commit interval so tick doesn't interfere
        final Duration commitInterval = Duration.ofMinutes(10);
        // Use a small buffer so size-based rotation triggers
        final int maxBufferSize = 1000;  // Very small

        writer = createWriter(commitInterval, maxBufferSize, 4);

        // Write a request that will exceed the buffer size
        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 100)  // larger batch
        );

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
            writer.write(writeRequest, TOPIC_CONFIGS, REQUEST_LOCAL);

        // Should complete quickly via size-based rotation
        Map<TopicIdPartition, PartitionResponse> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).containsKey(T0P0);
        assertThat(result.get(T0P0).error).isEqualTo(Errors.NONE);
        verify(fileCommitter, atLeastOnce()).commit(any());
    }

    /**
     * Test that multiple writes are batched correctly.
     */
    @Test
    void multipleWritesAreBatched() throws InterruptedException, ExecutionException, TimeoutException {
        final Duration commitInterval = Duration.ofMillis(100);
        final int maxBufferSize = 10 * 1024 * 1024;

        writer = createWriter(commitInterval, maxBufferSize, 4);

        // Write multiple small requests
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future1 =
            writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), TOPIC_CONFIGS, REQUEST_LOCAL);
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future2 =
            writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), TOPIC_CONFIGS, REQUEST_LOCAL);
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future3 =
            writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), TOPIC_CONFIGS, REQUEST_LOCAL);

        // All should complete via tick-based rotation
        Map<TopicIdPartition, PartitionResponse> result1 = future1.get(5, TimeUnit.SECONDS);
        Map<TopicIdPartition, PartitionResponse> result2 = future2.get(5, TimeUnit.SECONDS);
        Map<TopicIdPartition, PartitionResponse> result3 = future3.get(5, TimeUnit.SECONDS);

        assertThat(result1.get(T0P0).error).isEqualTo(Errors.NONE);
        assertThat(result2.get(T0P0).error).isEqualTo(Errors.NONE);
        assertThat(result3.get(T0P0).error).isEqualTo(Errors.NONE);
    }

    /**
     * Test writes to multiple partitions in a single request.
     */
    @Test
    void multiplePartitionsInSingleRequest() throws InterruptedException, ExecutionException, TimeoutException {
        final Duration commitInterval = Duration.ofMillis(100);
        final int maxBufferSize = 10 * 1024 * 1024;

        writer = createWriter(commitInterval, maxBufferSize, 4);

        // Write to multiple partitions in one request
        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 5),
            T0P1, recordCreator.create(T0P1.topicPartition(), 5),
            T1P0, recordCreator.create(T1P0.topicPartition(), 5)
        );

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
            writer.write(writeRequest, TOPIC_CONFIGS, REQUEST_LOCAL);

        Map<TopicIdPartition, PartitionResponse> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).containsKeys(T0P0, T0P1, T1P0);
        assertThat(result.get(T0P0).error).isEqualTo(Errors.NONE);
        assertThat(result.get(T0P1).error).isEqualTo(Errors.NONE);
        assertThat(result.get(T1P0).error).isEqualTo(Errors.NONE);
    }

    /**
     * Test that writer rejects writes after close.
     */
    @Test
    void rejectsWritesAfterClose() throws IOException {
        writer = createWriter(Duration.ofMillis(100), 10 * 1024 * 1024, 4);

        writer.close();

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
            writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), TOPIC_CONFIGS, REQUEST_LOCAL);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(RuntimeException.class);
    }

    /**
     * Test that empty requests are rejected.
     */
    @Test
    void rejectsEmptyRequests() {
        writer = createWriter(Duration.ofMillis(100), 10 * 1024 * 1024, 4);

        assertThatThrownBy(() -> writer.write(Map.of(), TOPIC_CONFIGS, REQUEST_LOCAL))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("empty");
    }

    /**
     * Test that null parameters are rejected.
     */
    @Test
    void rejectsNullParameters() {
        writer = createWriter(Duration.ofMillis(100), 10 * 1024 * 1024, 4);

        assertThatThrownBy(() -> writer.write(null, TOPIC_CONFIGS, REQUEST_LOCAL))
            .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), null, REQUEST_LOCAL))
            .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), TOPIC_CONFIGS, null))
            .isInstanceOf(NullPointerException.class);
    }

    /**
     * Test constructor validation.
     */
    @Test
    void constructorValidation() {
        // null time
        assertThatThrownBy(() -> new PipelinedWriter(
            null, Duration.ofMillis(100), 1024, tickScheduler, storage, fileCommitter, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(NullPointerException.class);

        // null commitInterval
        assertThatThrownBy(() -> new PipelinedWriter(
            time, null, 1024, tickScheduler, storage, fileCommitter, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(NullPointerException.class);

        // non-positive maxBufferSize
        assertThatThrownBy(() -> new PipelinedWriter(
            time, Duration.ofMillis(100), 0, tickScheduler, storage, fileCommitter, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new PipelinedWriter(
            time, Duration.ofMillis(100), -1, tickScheduler, storage, fileCommitter, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(IllegalArgumentException.class);

        // null tickScheduler
        assertThatThrownBy(() -> new PipelinedWriter(
            time, Duration.ofMillis(100), 1024, null, storage, fileCommitter, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(NullPointerException.class);

        // null storage
        assertThatThrownBy(() -> new PipelinedWriter(
            time, Duration.ofMillis(100), 1024, tickScheduler, null, fileCommitter, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(NullPointerException.class);

        // null fileCommitter
        assertThatThrownBy(() -> new PipelinedWriter(
            time, Duration.ofMillis(100), 1024, tickScheduler, storage, null, writerMetrics, brokerTopicStats, 4
        )).isInstanceOf(NullPointerException.class);

        // null writerMetrics
        assertThatThrownBy(() -> new PipelinedWriter(
            time, Duration.ofMillis(100), 1024, tickScheduler, storage, fileCommitter, null, brokerTopicStats, 4
        )).isInstanceOf(NullPointerException.class);
    }

    /**
     * Test that validation worker count of 0 uses auto-detection.
     */
    @Test
    void validationWorkerCountAutoDetection() throws InterruptedException, ExecutionException, TimeoutException {
        // Use 0 for auto-detection
        writer = createWriter(Duration.ofMillis(100), 10 * 1024 * 1024, 0);

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
            writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 5)), TOPIC_CONFIGS, REQUEST_LOCAL);

        Map<TopicIdPartition, PartitionResponse> result = future.get(5, TimeUnit.SECONDS);
        assertThat(result.get(T0P0).error).isEqualTo(Errors.NONE);
    }

    /**
     * Test close without any writes.
     */
    @Test
    void closeWithoutWrites() throws IOException, InterruptedException {
        writer = createWriter(Duration.ofMillis(100), 10 * 1024 * 1024, 4);

        writer.close();

        // Should not call commit since there was no data
        verify(fileCommitter, never()).commit(any());
    }

    /**
     * Test that close is idempotent.
     */
    @Test
    void closeIsIdempotent() throws IOException {
        writer = createWriter(Duration.ofMillis(100), 10 * 1024 * 1024, 4);

        writer.close();
        writer.close();  // Should not throw

        // Mark as null so tearDown doesn't try to close again
        writer = null;
    }

    /**
     * Test concurrent writes from multiple threads.
     */
    @Test
    void concurrentWrites() throws InterruptedException, ExecutionException, TimeoutException {
        writer = createWriter(Duration.ofMillis(200), 10 * 1024 * 1024, 4);

        final int numThreads = 10;
        final int writesPerThread = 5;

        CompletableFuture<?>[] futures = new CompletableFuture[numThreads * writesPerThread];

        for (int t = 0; t < numThreads; t++) {
            for (int w = 0; w < writesPerThread; w++) {
                int index = t * writesPerThread + w;
                futures[index] = CompletableFuture.runAsync(() -> {
                    try {
                        writer.write(
                            Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 3)),
                            TOPIC_CONFIGS,
                            REQUEST_LOCAL
                        ).get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

        // All should have completed without error
        verify(fileCommitter, atLeastOnce()).commit(any());
    }
}
