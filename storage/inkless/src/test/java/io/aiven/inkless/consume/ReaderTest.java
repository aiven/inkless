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
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
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

    @Spy
    private final ExecutorService metadataExecutor = Executors.newSingleThreadExecutor();
    @Spy
    private final ExecutorService dataExecutor = Executors.newSingleThreadExecutor();

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
            verifyNoInteractions(dataExecutor);
            assertThat(fetch.join()).isEqualTo(Collections.emptyMap());
        }
    }

    @Test
    public void testClose() throws Exception {
        final var reader = getReader();
        reader.close();
        verify(metadataExecutor, atLeastOnce()).shutdown();
        verify(dataExecutor, atLeastOnce()).shutdown();
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
            final CompletableFuture<List<FileExtent>> resultFuture = Reader.allOfFileExtents(orderedFutures);

            // Verify result order is preserved as A, B, C (not C, B, A which was the completion order)
            final List<FileExtent> result = resultFuture.join();
            assertThat(result)
                .hasSize(3)
                .extracting(FileExtent::object)
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
            final CompletableFuture<List<FileExtent>> resultFuture = Reader.allOfFileExtents(futures);

            // Verify the result is not yet complete (proves non-blocking behavior)
            assertThat(resultFuture).isNotCompleted();

            // Complete the input future
            final ObjectKey objectKey = PlainObjectKey.create("prefix", "object");
            final FileExtent extent = FileFetchJob.createFileExtent(objectKey, new ByteRange(0, 10), ByteBuffer.allocate(10));
            incompleteFuture.complete(extent);

            // Verify the result completes and contains the expected value
            assertThat(resultFuture.join())
                .hasSize(1)
                .extracting(FileExtent::object)
                .containsExactly(objectKey.value());
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
         * Tests that FileFetchException is properly caught and metrics are recorded.
         * Exception hierarchy: FileFetchException -> CompletionException
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

                // Verify the exception is properly wrapped
                assertThatThrownBy(fetch::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(FileFetchException.class)
                    .satisfies(e -> {
                        assertThat(e.getCause()).isInstanceOf(FileFetchException.class);
                        assertThat(e.getCause().getCause()).isInstanceOf(StorageBackendException.class);
                        assertThat(e.getCause().getCause().getMessage()).isEqualTo("Storage backend error");
                    });

                // Verify metrics are properly recorded
                verify(fetchMetrics).fetchFailed();
                verify(fetchMetrics, never()).findBatchesFailed();
                verify(fetchMetrics).fileFetchFailed();
                verify(fetchMetrics, never()).fetchCompleted(any());
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
            dataExecutor,
            fetchMetrics,
            new BrokerTopicStats());
    }
}
