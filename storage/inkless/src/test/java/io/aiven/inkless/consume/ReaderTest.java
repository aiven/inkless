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
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ReaderTest {
    private static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();

    // Spy executors for top-level tests (testReaderEmptyRequests, testClose)
    // Note: Nested test classes (RateLimitingTests, ErrorMetricTests) create their own executors
    // in @BeforeEach for proper lifecycle management and avoid interfering with spy verification
    @Spy
    private final ExecutorService metadataExecutor = Executors.newSingleThreadExecutor();
    @Spy
    private final ExecutorService fetchDataExecutor = Executors.newSingleThreadExecutor();
    // Spy executor for lagging fetch operations used by top-level tests; nested tests manage their own lagging executors
    @Spy
    private final ExecutorService laggingFetchDataExecutor = Executors.newSingleThreadExecutor();

    @Mock
    private ControlPlane controlPlane;
    @Mock
    private ObjectFetcher objectFetcher;
    @Mock
    private FetchParams fetchParams;
    @Mock
    private InklessFetchMetrics fetchMetrics;

    private final Time time = new MockTime();

    @Test
    public void testReaderEmptyRequests() throws IOException {
        try (final var reader = getReader()) {
            final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch = reader.fetch(fetchParams, Collections.emptyMap());
            verify(metadataExecutor, atLeastOnce()).execute(any());
            verifyNoInteractions(fetchDataExecutor);
            assertThat(fetch.join()).isEqualTo(Collections.emptyMap());
        }
    }

    @Test
    public void testClose() throws Exception {
        final var reader = getReader();
        reader.close();
        verify(metadataExecutor, atLeastOnce()).shutdown();
        verify(fetchDataExecutor, atLeastOnce()).shutdown();
        verify(laggingFetchDataExecutor, atLeastOnce()).shutdown();
    }

    @Nested
    class AllOfFileExtentsTests {
        /**
         * Tests that allOfFileExtents preserves the original input order regardless of completion order.
         */
        @Test
        public void testAllOfFileExtentsPreservesOrder() {
            // Create file extents with distinct identifiers
            final ObjectKey objectKeyA = PlainObjectKey.create("prefix", "object-a");
            final ObjectKey objectKeyB = PlainObjectKey.create("prefix", "object-b");
            final ObjectKey objectKeyC = PlainObjectKey.create("prefix", "object-c");

            final FileExtent extentA = FileFetchJob.createFileExtent(objectKeyA, new ByteRange(0, 10), ByteBuffer.allocate(10));
            final FileExtent extentB = FileFetchJob.createFileExtent(objectKeyB, new ByteRange(0, 10), ByteBuffer.allocate(10));
            final FileExtent extentC = FileFetchJob.createFileExtent(objectKeyC, new ByteRange(0, 10), ByteBuffer.allocate(10));

            // Create uncompleted futures
            final CompletableFuture<FileExtent> futureA = new CompletableFuture<>();
            final CompletableFuture<FileExtent> futureB = new CompletableFuture<>();
            final CompletableFuture<FileExtent> futureC = new CompletableFuture<>();

            // Complete in reverse order: C, B, A
            futureC.complete(extentC);
            futureB.complete(extentB);
            futureA.complete(extentA);

            // Create the ordered list: A, B, C
            final List<CompletableFuture<FileExtent>> orderedFutures = List.of(futureA, futureB, futureC);

            // Call allOfFileExtents
            final CompletableFuture<List<FileExtentResult>> resultFuture = Reader.allOfFileExtents(orderedFutures);

            // Verify result order is preserved as A, B, C (not C, B, A which was the completion order)
            final List<FileExtentResult> result = resultFuture.join();
            assertThat(result)
                .hasSize(3)
                .allSatisfy(r -> assertThat(r).isInstanceOf(FileExtentResult.Success.class))
                .extracting(r -> ((FileExtentResult.Success) r).extent().object())
                .containsExactly(objectKeyA.value(), objectKeyB.value(), objectKeyC.value());
        }

        /**
         * Tests that allOfFileExtents returns immediately without blocking the calling thread.
         */
        @Test
        public void testAllOfFileExtentsDoesNotBlock() {
            // Create an incomplete future
            final CompletableFuture<FileExtent> incompleteFuture = new CompletableFuture<>();

            final List<CompletableFuture<FileExtent>> futures = List.of(incompleteFuture);

            // Call allOfFileExtents - this should return immediately without blocking
            final CompletableFuture<List<FileExtentResult>> resultFuture = Reader.allOfFileExtents(futures);

            // Verify the result is not yet complete (proves non-blocking behavior)
            assertThat(resultFuture).isNotCompleted();

            // Complete the input future
            final ObjectKey objectKey = PlainObjectKey.create("prefix", "object");
            final FileExtent extent = FileFetchJob.createFileExtent(objectKey, new ByteRange(0, 10), ByteBuffer.allocate(10));
            incompleteFuture.complete(extent);

            // Verify the result completes and contains the expected value
            assertThat(resultFuture.join())
                .hasSize(1)
                .allSatisfy(r -> assertThat(r).isInstanceOf(FileExtentResult.Success.class))
                .extracting(r -> ((FileExtentResult.Success) r).extent().object())
                .containsExactly(objectKey.value());
        }

        /**
         * Tests partial failure handling: when some futures fail, allOfFileExtents returns Failure results
         * for failed fetches while preserving successful results.
         * <p>
         * This simulates the critical scenario where lagging consumer requests are rejected (e.g., due to
         * rate limiting or executor rejection) while hot path requests succeed.
         */
        @Test
        public void testPartialFailureHandling() {
            // Create successful extents for hot path
            final ObjectKey hotKey1 = PlainObjectKey.create("prefix", "hot-object-1");
            final ObjectKey hotKey2 = PlainObjectKey.create("prefix", "hot-object-2");
            final FileExtent hotExtent1 = FileFetchJob.createFileExtent(hotKey1, new ByteRange(0, 10), ByteBuffer.allocate(10));
            final FileExtent hotExtent2 = FileFetchJob.createFileExtent(hotKey2, new ByteRange(0, 10), ByteBuffer.allocate(10));

            // Create futures: success, failure, success pattern
            final CompletableFuture<FileExtent> successFuture1 = CompletableFuture.completedFuture(hotExtent1);
            final CompletableFuture<FileExtent> failedFuture = CompletableFuture.failedFuture(
                new RuntimeException("Lagging consumer request rejected")
            );
            final CompletableFuture<FileExtent> successFuture2 = CompletableFuture.completedFuture(hotExtent2);

            final List<CompletableFuture<FileExtent>> mixedFutures = List.of(
                successFuture1,
                failedFuture,
                successFuture2
            );

            // Call allOfFileExtents
            final CompletableFuture<List<FileExtentResult>> resultFuture = Reader.allOfFileExtents(mixedFutures);

            // Verify the result completes successfully (no exception propagation)
            assertThat(resultFuture).succeedsWithin(java.time.Duration.ofSeconds(1));

            final List<FileExtentResult> result = resultFuture.join();

            // Verify we have 3 results (not failing the entire request)
            assertThat(result).hasSize(3);

            // Verify first result is successful
            assertThat(result.get(0)).isInstanceOf(FileExtentResult.Success.class);
            final FileExtent extent1 = ((FileExtentResult.Success) result.get(0)).extent();
            assertThat(extent1.object()).isEqualTo(hotKey1.value());
            assertThat(extent1.data()).isNotEmpty();

            // Verify second result is failure
            assertThat(result.get(1)).isInstanceOf(FileExtentResult.Failure.class);
            final Throwable error = ((FileExtentResult.Failure) result.get(1)).error();
            assertThat(error).hasMessageContaining("Lagging consumer request rejected");

            // Verify third result is successful
            assertThat(result.get(2)).isInstanceOf(FileExtentResult.Success.class);
            final FileExtent extent3 = ((FileExtentResult.Success) result.get(2)).extent();
            assertThat(extent3.object()).isEqualTo(hotKey2.value());
            assertThat(extent3.data()).isNotEmpty();
        }

        /**
         * Tests that when all futures fail, allOfFileExtents returns all Failure results.
         * This ensures the partial failure mechanism works even in worst-case scenarios.
         */
        @Test
        public void testAllFailuresReturnAllFailureResults() {
            // Create multiple failed futures
            final CompletableFuture<FileExtent> failedFuture1 = CompletableFuture.failedFuture(
                new RuntimeException("Failure 1")
            );
            final CompletableFuture<FileExtent> failedFuture2 = CompletableFuture.failedFuture(
                new RuntimeException("Failure 2")
            );
            final CompletableFuture<FileExtent> failedFuture3 = CompletableFuture.failedFuture(
                new RuntimeException("Failure 3")
            );

            final List<CompletableFuture<FileExtent>> allFailedFutures = List.of(
                failedFuture1,
                failedFuture2,
                failedFuture3
            );

            // Call allOfFileExtents
            final CompletableFuture<List<FileExtentResult>> resultFuture = Reader.allOfFileExtents(allFailedFutures);

            // Verify the result completes successfully (no exception propagation)
            assertThat(resultFuture).succeedsWithin(java.time.Duration.ofSeconds(1));

            final List<FileExtentResult> result = resultFuture.join();

            // Verify all results are Failure instances
            assertThat(result).hasSize(3);
            result.forEach(r -> {
                assertThat(r).isInstanceOf(FileExtentResult.Failure.class);
                assertThat(((FileExtentResult.Failure) r).error()).isInstanceOf(RuntimeException.class);
            });
        }

        /**
         * Tests mixed success and failure with different exception types.
         * Verifies that the partial failure mechanism handles various exception types uniformly.
         */
        @Test
        public void testPartialFailureWithDifferentExceptionTypes() {
            // Create one successful extent
            final ObjectKey successKey = PlainObjectKey.create("prefix", "success-object");
            final FileExtent successExtent = FileFetchJob.createFileExtent(
                successKey, new ByteRange(0, 10), ByteBuffer.allocate(10)
            );

            // Create futures with different exception types
            final CompletableFuture<FileExtent> successFuture = CompletableFuture.completedFuture(successExtent);
            final CompletableFuture<FileExtent> rejectedExecutionFuture = CompletableFuture.failedFuture(
                new java.util.concurrent.RejectedExecutionException("Executor queue full")
            );
            final CompletableFuture<FileExtent> storageBackendFuture = CompletableFuture.failedFuture(
                new StorageBackendException("S3 error")
            );
            final CompletableFuture<FileExtent> genericFuture = CompletableFuture.failedFuture(
                new IllegalStateException("Unexpected error")
            );

            final List<CompletableFuture<FileExtent>> mixedFutures = List.of(
                successFuture,
                rejectedExecutionFuture,
                storageBackendFuture,
                genericFuture
            );

            // Call allOfFileExtents
            final CompletableFuture<List<FileExtentResult>> resultFuture = Reader.allOfFileExtents(mixedFutures);

            // Verify the result completes successfully
            assertThat(resultFuture).succeedsWithin(java.time.Duration.ofSeconds(1));

            final List<FileExtentResult> result = resultFuture.join();

            // Verify we have all results
            assertThat(result).hasSize(4);

            // First should be successful
            assertThat(result.get(0)).isInstanceOf(FileExtentResult.Success.class);
            final FileExtent extent0 = ((FileExtentResult.Success) result.get(0)).extent();
            assertThat(extent0.object()).isEqualTo(successKey.value());
            assertThat(extent0.data()).isNotEmpty();

            // Rest should be Failure results
            for (int i = 1; i < 4; i++) {
                assertThat(result.get(i)).isInstanceOf(FileExtentResult.Failure.class);
            }
        }

        /**
         * Tests that order is preserved even with partial failures.
         * This is critical for FetchCompleter to correctly map extents to partitions.
         */
        @Test
        public void testPartialFailurePreservesOrder() {
            // Create extents with identifiable data
            final ObjectKey key1 = PlainObjectKey.create("prefix", "object-1");
            final ObjectKey key2 = PlainObjectKey.create("prefix", "object-2");
            final ObjectKey key3 = PlainObjectKey.create("prefix", "object-3");
            final ObjectKey key4 = PlainObjectKey.create("prefix", "object-4");

            final FileExtent extent1 = FileFetchJob.createFileExtent(key1, new ByteRange(0, 10), ByteBuffer.allocate(10));
            final FileExtent extent3 = FileFetchJob.createFileExtent(key3, new ByteRange(0, 10), ByteBuffer.allocate(10));

            // Create pattern: success, failure, success, failure
            final CompletableFuture<FileExtent> future1 = CompletableFuture.completedFuture(extent1);
            final CompletableFuture<FileExtent> future2 = CompletableFuture.failedFuture(new RuntimeException("Failed"));
            final CompletableFuture<FileExtent> future3 = CompletableFuture.completedFuture(extent3);
            final CompletableFuture<FileExtent> future4 = CompletableFuture.failedFuture(new RuntimeException("Failed"));

            final List<CompletableFuture<FileExtent>> orderedFutures = List.of(future1, future2, future3, future4);

            // Call allOfFileExtents
            final CompletableFuture<List<FileExtentResult>> resultFuture = Reader.allOfFileExtents(orderedFutures);

            final List<FileExtentResult> result = resultFuture.join();

            // Verify order: success at 0, failure at 1, success at 2, failure at 3
            assertThat(result).hasSize(4);
            assertThat(result.get(0)).isInstanceOf(FileExtentResult.Success.class);
            assertThat(((FileExtentResult.Success) result.get(0)).extent().object()).isEqualTo(key1.value());

            assertThat(result.get(1)).isInstanceOf(FileExtentResult.Failure.class);

            assertThat(result.get(2)).isInstanceOf(FileExtentResult.Success.class);
            assertThat(((FileExtentResult.Success) result.get(2)).extent().object()).isEqualTo(key3.value());

            assertThat(result.get(3)).isInstanceOf(FileExtentResult.Failure.class);
        }
    }

    /**
     * Tests for error handling and metric recording in Reader.fetch().
     * Each test simulates different failure scenarios and verifies that
     * the appropriate exceptions are thrown and metrics are recorded.
     */
    @Nested
    class ErrorMetricTests {
        // Common test data for fetch requests
        private final Uuid topicId = Uuid.randomUuid();
        private final TopicIdPartition partition = new TopicIdPartition(topicId, 0, "test-topic");
        private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition,
            new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        private final MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        // Setup for successful fetch - return a proper response with single batch
        private final FindBatchResponse singleResponse = FindBatchResponse.success(
            List.of(
                new BatchInfo(
                    1L,
                    "object-key",
                    BatchMetadata.of(
                        partition,
                        0,
                        records.sizeInBytes(),
                        0,
                        0,
                        10L,
                        10L,
                        TimestampType.CREATE_TIME
                    )
                )
            ),
            0L,
            1L
        );

        private ExecutorService metadataExecutor;
        private ExecutorService dataExecutor;

        @BeforeEach
        public void setup() {
            metadataExecutor = Executors.newSingleThreadExecutor();
            dataExecutor = Executors.newSingleThreadExecutor();
        }

        @AfterEach
        public void cleanup() {
            metadataExecutor.shutdown();
            dataExecutor.shutdown();
        }

        /**
         * Tests that FindBatchesException is properly caught and metrics are recorded.
         * Exception hierarchy: FindBatchesException -> CompletionException
         */
        @Test
        public void testFindBatchesException() throws IOException {
            // Simulate control plane throwing an exception
            when(controlPlane.findBatches(anyList(), anyInt(), anyInt()))
                .thenThrow(new RuntimeException("Control plane error"));

            try (final var reader = getReader()) {
                final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch = reader.fetch(fetchParams, fetchInfos);

                // Verify the exception is properly wrapped
                assertThatThrownBy(fetch::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(FindBatchesException.class)
                    .satisfies(e -> {
                        assertThat(e.getCause()).isInstanceOf(FindBatchesException.class);
                        assertThat(e.getCause().getCause()).isInstanceOf(RuntimeException.class);
                        assertThat(e.getCause().getCause().getMessage()).isEqualTo("Control plane error");
                    });

                // Verify metrics are properly recorded
                verify(fetchMetrics).fetchFailed();
                verify(fetchMetrics).findBatchesFailed();
                verify(fetchMetrics, never()).fileFetchFailed();
                verify(fetchMetrics, never()).fetchCompleted(any());
            }
        }

        /**
         * Tests that FileFetchException is properly handled with partial failure support.
         * With partial failure handling, exceptions are caught and converted to empty FileExtents,
         * resulting in KAFKA_STORAGE_ERROR responses that allow consumers to retry.
         * <p>
         * Exception flow: StorageBackendException -> FileFetchException -> caught by allOfFileExtents
         * -> empty FileExtent -> KAFKA_STORAGE_ERROR
         */
        @Test
        public void testFileFetchException() throws Exception {
            // Setup for successful fetch - return a proper response with single batch
            when(controlPlane.findBatches(any(), anyInt(), anyInt()))
                .thenReturn(List.of(singleResponse));

            // Simulate fetcher failing and throwing an exception
            when(objectFetcher.fetch(any(ObjectKey.class), any(ByteRange.class)))
                .thenThrow(new StorageBackendException("Storage backend error"));

            try (final var reader = getReader()) {
                final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch = reader.fetch(fetchParams, fetchInfos);

                // Verify fetch completes successfully (partial failure handling)
                final Map<TopicIdPartition, FetchPartitionData> result = fetch.join();

                // Verify partition returns KAFKA_STORAGE_ERROR (from empty FileExtent)
                assertThat(result).hasSize(1);
                final FetchPartitionData data = result.get(partition);
                assertThat(data.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
                assertThat(data.records.records()).isEmpty();

                // Verify metrics are properly recorded
                // Note: fileFetchFailed() is not called with partial failure handling.
                // Individual failures are handled gracefully and don't trigger failure metrics.
                // This is intentional - only complete fetch failures trigger fetchFailed().
                verify(fetchMetrics, never()).fetchFailed();
                verify(fetchMetrics, never()).findBatchesFailed();
                verify(fetchMetrics, never()).fileFetchFailed();
                verify(fetchMetrics).fetchCompleted(any());
            }
        }

        /**
         * Tests that FetchException is properly caught and metrics are recorded.
         * Exception hierarchy: FetchException -> CompletionException
         */
        @Test
        public void testFetchException() throws Exception {
            // Setup for successful fetch - return a proper response with single batch
            when(controlPlane.findBatches(any(), anyInt(), anyInt()))
                .thenReturn(List.of(singleResponse));

            // Simulate fetcher returning invalid data that causes FetchException
            final ReadableByteChannel file1Channel = mock(ReadableByteChannel.class);
            when(objectFetcher.fetch(any(), any())).thenReturn(file1Channel);
            // Will be read as MemoryRecords but invalid data will cause exception
            final ByteBuffer corruptedRecords = ByteBuffer.wrap("corrupt-data".getBytes(StandardCharsets.UTF_8));
            when(objectFetcher.readToByteBuffer(file1Channel)).thenReturn(corruptedRecords);

            try (final var reader = getReader()) {
                final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch = reader.fetch(fetchParams, fetchInfos);
                // Verify the exception is properly wrapped
                assertThatThrownBy(fetch::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(FetchException.class)
                    .satisfies(e -> {
                        assertThat(e.getCause()).isInstanceOf(FetchException.class);
                        assertThat(e.getCause().getCause()).isInstanceOf(IllegalStateException.class);
                        assertThat(e.getCause().getCause().getMessage()).isEqualTo("Backing file should have at least one batch");
                    });

                // Verify metrics are properly recorded
                verify(fetchMetrics).fetchFailed();
                verify(fetchMetrics, never()).findBatchesFailed();
                verify(fetchMetrics, never()).fileFetchFailed();
                verify(fetchMetrics, never()).fetchCompleted(any());
            }
        }

        /**
         * Tests that successful fetches properly record metrics.
         */
        @Test
        public void testSuccessfulFetchMetrics() throws Exception {
            // Setup for successful fetch - return a proper response with single batch
            when(controlPlane.findBatches(any(), anyInt(), anyInt()))
                .thenReturn(List.of(singleResponse));

            // Simulate fetcher returning valid data
            final ReadableByteChannel file1Channel = mock(ReadableByteChannel.class);
            when(objectFetcher.fetch(any(), any())).thenReturn(file1Channel);
            when(objectFetcher.readToByteBuffer(file1Channel)).thenReturn(records.buffer());

            try (final var reader = getReader()) {
                final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch = reader.fetch(fetchParams, fetchInfos);
                final Map<TopicIdPartition, FetchPartitionData> result = fetch.join();
                assertThat(result)
                    .hasSize(1)
                    .hasEntrySatisfying(
                        partition,
                        data -> {
                            assertThat(data.error).isEqualTo(Errors.NONE);
                            assertThat(data.records.records()).containsAll(records.records());
                        }
                    );

                // Verify metrics are properly recorded
                verify(fetchMetrics, never()).fetchFailed();
                verify(fetchMetrics, never()).findBatchesFailed();
                verify(fetchMetrics, never()).fileFetchFailed();
                verify(fetchMetrics).fetchCompleted(any());
            }
        }
    }

    private Reader getReader() {
        return new Reader(
            time,
            OBJECT_KEY_CREATOR,
            KEY_ALIGNMENT_STRATEGY,
            OBJECT_CACHE,
            controlPlane,
            objectFetcher,
            0,
            metadataExecutor,
            fetchDataExecutor,
            objectFetcher,
            Long.MAX_VALUE,
            0,
            laggingFetchDataExecutor,
            fetchMetrics,
            new BrokerTopicStats());
    }

    @Nested
    class RateLimitingTests {
        private final int RATE_LIMIT_REQ_PER_SEC = 10; // Low rate limit for testing
        private final long LAGGING_THRESHOLD_MS = 1000; // 1 second threshold

        // Common test data for fetch requests
        private final Uuid topicId = Uuid.randomUuid();
        private final TopicIdPartition partition = new TopicIdPartition(topicId, 0, "test-topic");
        private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition,
            new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        private final MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));

        private ExecutorService metadataExecutor;
        private ExecutorService fetchDataExecutor;
        private ExecutorService laggingFetchDataExecutor;

        @BeforeEach
        public void setup() {
            metadataExecutor = Executors.newSingleThreadExecutor();
            fetchDataExecutor = Executors.newSingleThreadExecutor();
            laggingFetchDataExecutor = Executors.newSingleThreadExecutor();
        }

        @AfterEach
        public void cleanup() {
            metadataExecutor.shutdown();
            fetchDataExecutor.shutdown();
            laggingFetchDataExecutor.shutdown();
        }

        /**
         * Tests that rate limiting is applied when enabled and metrics are properly recorded.
         * Verifies that:
         * 1. Requests are throttled to the configured rate
         * 2. Wait time metrics include both zero-wait and non-zero-wait cases
         * 3. Lagging consumer requests are properly tracked
         */
        @Test
        public void testRateLimitingWithLoad() throws Exception {
            // Create old data (beyond lagging threshold) to trigger cold path
            final long oldTimestamp = time.milliseconds() - (LAGGING_THRESHOLD_MS * 2);
            final FindBatchResponse oldResponse = FindBatchResponse.success(
                List.of(
                    new BatchInfo(
                        1L,
                        "object-key",
                        BatchMetadata.of(
                            partition,
                            0,
                            records.sizeInBytes(),
                            0,
                            0,
                            oldTimestamp,
                            oldTimestamp,
                            TimestampType.CREATE_TIME
                        )
                    )
                ),
                0L,
                1L
            );

            when(controlPlane.findBatches(any(), anyInt(), anyInt()))
                .thenReturn(List.of(oldResponse));

            final ReadableByteChannel channel = mock(ReadableByteChannel.class);
            when(objectFetcher.fetch(any(), any())).thenReturn(channel);
            when(objectFetcher.readToByteBuffer(channel)).thenReturn(records.buffer());

            try (final var reader = new Reader(
                time,
                OBJECT_KEY_CREATOR,
                KEY_ALIGNMENT_STRATEGY,
                OBJECT_CACHE,
                controlPlane,
                objectFetcher,
                0,
                metadataExecutor,
                fetchDataExecutor,
                objectFetcher,
                LAGGING_THRESHOLD_MS,
                RATE_LIMIT_REQ_PER_SEC,
                laggingFetchDataExecutor,
                fetchMetrics,
                new BrokerTopicStats())) {

                // Submit multiple requests to trigger rate limiting
                final int numRequests = RATE_LIMIT_REQ_PER_SEC * 2; // 2 seconds worth of requests
                final List<CompletableFuture<Map<TopicIdPartition, FetchPartitionData>>> futures = new ArrayList<>();

                for (int i = 0; i < numRequests; i++) {
                    futures.add(reader.fetch(fetchParams, fetchInfos));
                }

                // Wait for all requests to complete with rate limiting applied
                // 20 requests @ 10 req/s = ~2 seconds minimum
                await().atMost(5, SECONDS)
                    .pollDelay(1, SECONDS)
                    .untilAsserted(() ->
                        assertThat(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])))
                            .isCompleted());

                // Verify lagging consumer request metrics were recorded
                verify(fetchMetrics, atLeastOnce()).recordLaggingConsumerRequest();
                // Verify rate limit wait time metrics were recorded (including zero-wait cases)
                verify(fetchMetrics, atLeastOnce()).recordRateLimitWaitTime(anyLong());
            }
        }

        /**
         * Tests that rate limiting is NOT applied when disabled (rate limit = 0).
         * Verifies that:
         * 1. Requests complete quickly without throttling
         * 2. Rate limit wait time metrics are NOT recorded
         * 3. Lagging consumer requests are still tracked
         */
        @Test
        public void testRateLimitingDisabled() throws Exception {
            // Create old data to trigger cold path
            final long oldTimestamp = time.milliseconds() - (LAGGING_THRESHOLD_MS * 2);
            final FindBatchResponse oldResponse = FindBatchResponse.success(
                List.of(
                    new BatchInfo(
                        1L,
                        "object-key",
                        BatchMetadata.of(
                            partition,
                            0,
                            records.sizeInBytes(),
                            0,
                            0,
                            oldTimestamp,
                            oldTimestamp,
                            TimestampType.CREATE_TIME
                        )
                    )
                ),
                0L,
                1L
            );

            when(controlPlane.findBatches(any(), anyInt(), anyInt()))
                .thenReturn(List.of(oldResponse));

            final ReadableByteChannel channel = mock(ReadableByteChannel.class);
            when(objectFetcher.fetch(any(), any())).thenReturn(channel);
            when(objectFetcher.readToByteBuffer(channel)).thenReturn(records.buffer());

            try (final var reader = new Reader(
                time,
                OBJECT_KEY_CREATOR,
                KEY_ALIGNMENT_STRATEGY,
                OBJECT_CACHE,
                controlPlane,
                objectFetcher,
                0,
                metadataExecutor,
                fetchDataExecutor,
                objectFetcher,
                LAGGING_THRESHOLD_MS,
                0, // Rate limiting disabled
                laggingFetchDataExecutor,
                fetchMetrics,
                new BrokerTopicStats())) {

                // Submit multiple requests
                final int numRequests = RATE_LIMIT_REQ_PER_SEC * 2;
                final List<CompletableFuture<Map<TopicIdPartition, FetchPartitionData>>> futures = new ArrayList<>();

                for (int i = 0; i < numRequests; i++) {
                    futures.add(reader.fetch(fetchParams, fetchInfos));
                }

                // Verify requests complete quickly without rate limiting (within 2 seconds)
                await().atMost(2, SECONDS)
                    .untilAsserted(() ->
                        assertThat(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])))
                            .isCompleted());

                // Verify lagging consumer request metrics were recorded
                verify(fetchMetrics, atLeastOnce()).recordLaggingConsumerRequest();
                // Verify rate limit wait time metrics were NOT recorded (rate limiting disabled)
                verify(fetchMetrics, never()).recordRateLimitWaitTime(anyLong());
            }
        }

        /**
         * Tests that when a single fetch request includes BOTH lagging and recent partitions,
         * lagging partition failures don't prevent recent partitions from returning data.
         * <p>
         * This is the critical scenario for handling mixed hot/cold workloads:
         * - Recent partitions (hot path) use cache + fetchDataExecutor
         * - Lagging partitions (cold path) use laggingFetchDataExecutor + rate limiting
         * - If lagging executor is saturated and rejects requests, those partitions return empty
         * - Recent partitions continue to work and return data
         * - Consumers can retry only the failed lagging partitions
         */
        @Test
        public void testMixedLaggingAndRecentPartitions() throws Exception {
            // Setup: Create two partitions - one recent, one lagging
            final Uuid topicId = Uuid.randomUuid();
            final TopicIdPartition recentPartition = new TopicIdPartition(topicId, 0, "test-topic");
            final TopicIdPartition laggingPartition = new TopicIdPartition(topicId, 1, "test-topic");

            // Recent partition data (within threshold)
            final long recentTimestamp = time.milliseconds() - (LAGGING_THRESHOLD_MS / 2);
            final FindBatchResponse recentResponse = FindBatchResponse.success(
                List.of(
                    new BatchInfo(
                        1L,
                        "recent-object-key",
                        BatchMetadata.of(
                            recentPartition,
                            0,
                            records.sizeInBytes(),
                            0,
                            0,
                            recentTimestamp,
                            recentTimestamp,
                            TimestampType.CREATE_TIME
                        )
                    )
                ),
                0L,
                1L
            );

            // Lagging partition data (beyond threshold)
            final long laggingTimestamp = time.milliseconds() - (LAGGING_THRESHOLD_MS * 2);
            final FindBatchResponse laggingResponse = FindBatchResponse.success(
                List.of(
                    new BatchInfo(
                        1L,
                        "lagging-object-key",
                        BatchMetadata.of(
                            laggingPartition,
                            0,
                            records.sizeInBytes(),
                            0,
                            0,
                            laggingTimestamp,
                            laggingTimestamp,
                            TimestampType.CREATE_TIME
                        )
                    )
                ),
                0L,
                1L
            );

            // Configure control plane to return responses in the same order as requested
            // This is critical because FindBatchesJob maps responses by index
            when(controlPlane.findBatches(anyList(), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    List<FindBatchRequest> requests = invocation.getArgument(0);
                    List<FindBatchResponse> responses = new ArrayList<>();
                    
                    for (FindBatchRequest request : requests) {
                        if (request.topicIdPartition().equals(recentPartition)) {
                            responses.add(recentResponse);
                        } else if (request.topicIdPartition().equals(laggingPartition)) {
                            responses.add(laggingResponse);
                        }
                    }
                    
                    return responses;
                });

            // Setup object fetcher to succeed for all requests (hot path will use this)
            final ReadableByteChannel channel = mock(ReadableByteChannel.class);
            when(objectFetcher.fetch(any(ObjectKey.class), any(ByteRange.class))).thenReturn(channel);
            // Return a fresh buffer each time to avoid buffer exhaustion issues
            when(objectFetcher.readToByteBuffer(channel)).thenAnswer(invocation -> records.buffer().duplicate());

            // Create a lagging executor and immediately shut it down - will reject all tasks
            final ExecutorService saturatedLaggingExecutor = Executors.newSingleThreadExecutor();
            saturatedLaggingExecutor.shutdownNow(); // Force immediate rejection
            
            // Use dedicated executors for this test to avoid interference
            final ExecutorService testMetadataExecutor = Executors.newSingleThreadExecutor();
            final ExecutorService testFetchDataExecutor = Executors.newSingleThreadExecutor();

            try (final var reader = new Reader(
                time,
                OBJECT_KEY_CREATOR,
                KEY_ALIGNMENT_STRATEGY,
                OBJECT_CACHE,
                controlPlane,
                objectFetcher,
                0,
                testMetadataExecutor,
                testFetchDataExecutor,
                objectFetcher, // Use same fetcher for lagging to simplify test
                LAGGING_THRESHOLD_MS,
                RATE_LIMIT_REQ_PER_SEC,
                saturatedLaggingExecutor, // Saturated executor for lagging path
                fetchMetrics,
                new BrokerTopicStats())) {

                // Create a fetch request with BOTH partitions
                final Map<TopicIdPartition, FetchRequest.PartitionData> mixedFetchInfos = Map.of(
                    recentPartition, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty()),
                    laggingPartition, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
                );

                final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetchFuture =
                    reader.fetch(fetchParams, mixedFetchInfos);

                // Wait for the fetch to complete
                await().atMost(3, SECONDS)
                    .untilAsserted(() -> assertThat(fetchFuture).isCompleted());

                final Map<TopicIdPartition, FetchPartitionData> result = fetchFuture.join();

                // Verify we get results for both partitions
                assertThat(result).hasSize(2);

                // Verify recent partition has data (hot path succeeded)
                assertThat(result).containsKey(recentPartition);
                final FetchPartitionData recentData = result.get(recentPartition);
                assertThat(recentData.error).isEqualTo(Errors.NONE);
                assertThat(recentData.records.records()).isNotEmpty();

                // Verify lagging partition returns KAFKA_STORAGE_ERROR (cold path rejected)
                // Empty FileExtent from rejection results in KAFKA_STORAGE_ERROR, signaling Kafka
                // to return empty response so consumer can retry only the failed partition
                assertThat(result).containsKey(laggingPartition);
                final FetchPartitionData laggingData = result.get(laggingPartition);
                assertThat(laggingData.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
                assertThat(laggingData.records.records()).isEmpty();

                // Verify metrics recorded both paths
                verify(fetchMetrics, atLeastOnce()).recordRecentDataRequest();
                verify(fetchMetrics, atLeastOnce()).recordLaggingConsumerRequest();
                verify(fetchMetrics, atLeastOnce()).recordLaggingConsumerRejection();
            }
        }

        /**
         * Tests that recent data requests bypass rate limiting.
         * Verifies that:
         * 1. Recent data uses hot path (cache + dataExecutor)
         * 2. Rate limiting is NOT applied to recent data
         * 3. Recent data request metrics are recorded
         */
        @Test
        public void testRecentDataBypassesRateLimiting() throws Exception {
            // Create recent data (within lagging threshold) to trigger hot path
            final long recentTimestamp = time.milliseconds() - (LAGGING_THRESHOLD_MS / 2);
            final FindBatchResponse recentResponse = FindBatchResponse.success(
                List.of(
                    new BatchInfo(
                        1L,
                        "object-key",
                        BatchMetadata.of(
                            partition,
                            0,
                            records.sizeInBytes(),
                            0,
                            0,
                            recentTimestamp,
                            recentTimestamp,
                            TimestampType.CREATE_TIME
                        )
                    )
                ),
                0L,
                1L
            );

            when(controlPlane.findBatches(any(), anyInt(), anyInt()))
                .thenReturn(List.of(recentResponse));

            final ReadableByteChannel channel = mock(ReadableByteChannel.class);
            when(objectFetcher.fetch(any(), any())).thenReturn(channel);
            when(objectFetcher.readToByteBuffer(channel)).thenReturn(records.buffer());

            try (final var reader = new Reader(
                time,
                OBJECT_KEY_CREATOR,
                KEY_ALIGNMENT_STRATEGY,
                OBJECT_CACHE,
                controlPlane,
                objectFetcher,
                0,
                metadataExecutor,
                fetchDataExecutor,
                objectFetcher,
                LAGGING_THRESHOLD_MS,
                RATE_LIMIT_REQ_PER_SEC,
                laggingFetchDataExecutor,
                fetchMetrics,
                new BrokerTopicStats())) {

                // Submit multiple requests
                final int numRequests = RATE_LIMIT_REQ_PER_SEC * 2;
                final List<CompletableFuture<Map<TopicIdPartition, FetchPartitionData>>> futures = new ArrayList<>();

                for (int i = 0; i < numRequests; i++) {
                    futures.add(reader.fetch(fetchParams, fetchInfos));
                }

                // Verify requests complete quickly without rate limiting (within 2 seconds)
                await().atMost(2, SECONDS)
                    .untilAsserted(() ->
                        assertThat(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])))
                            .isCompleted());

                // Verify recent data request metrics were recorded
                verify(fetchMetrics, atLeastOnce()).recordRecentDataRequest();
                // Verify lagging consumer metrics were NOT recorded (hot path)
                verify(fetchMetrics, never()).recordLaggingConsumerRequest();
                // Verify rate limit wait time metrics were NOT recorded (hot path bypasses rate limiting)
                verify(fetchMetrics, never()).recordRateLimitWaitTime(anyLong());
            }
        }
    }
}
