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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.consume.FetchPlanner.ObjectFetchRequest;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchPlannerTest {
    static final String OBJECT_KEY_PREFIX = "prefix";
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator(OBJECT_KEY_PREFIX, false);
    static final String OBJECT_KEY_A_MAIN_PART = "a";
    static final String OBJECT_KEY_B_MAIN_PART = "b";
    static final ObjectKey OBJECT_KEY_A = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_KEY_A_MAIN_PART);
    static final ObjectKey OBJECT_KEY_B = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_KEY_B_MAIN_PART);

    @Mock
    ObjectFetcher fetcher;
    @Mock
    InklessFetchMetrics metrics;

    ExecutorService dataExecutor = Executors.newSingleThreadExecutor();

    ObjectCache cache = new NullCache();
    KeyAlignmentStrategy keyAlignmentStrategy = new FixedBlockAlignment(Integer.MAX_VALUE);
    ByteRange requestRange = new ByteRange(0, Integer.MAX_VALUE);
    Time time = new MockTime();
    Uuid topicId = Uuid.randomUuid();
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "diskless-topic");
    TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "diskless-topic");

    @AfterEach
    void tearDown() {
        dataExecutor.shutdownNow();
    }

    @Test
    public void planEmptyRequest() {
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of();
        FetchPlanner planner = getFetchPlanner(coordinates);

        List<ObjectFetchRequest> result = planner.planJobs(coordinates);

        assertThat(result).isEmpty();
    }

    @Test
    public void planSingleRequest() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                new ObjectFetchRequest(OBJECT_KEY_A, requestRange)
            )
        );
    }

    @Test
    public void planRequestsForMultipleObjects() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition0, 0, 10, 1, 1, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 2)
            ),
            Set.of(
                new ObjectFetchRequest(OBJECT_KEY_A, requestRange),
                new ObjectFetchRequest(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planRequestsForMultiplePartitions() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                new ObjectFetchRequest(OBJECT_KEY_A, requestRange),
                new ObjectFetchRequest(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planMergedRequestsForSameObject() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition1, 30, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                // When batches for same object are merged, they create a single aligned fetch request
                new ObjectFetchRequest(OBJECT_KEY_A, requestRange)
            )
        );
    }

    @Test
    public void planOffsetOutOfRange() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.offsetOutOfRange(0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                new ObjectFetchRequest(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planUnknownTopicOrPartition() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.unknownTopicOrPartition(),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                new ObjectFetchRequest(OBJECT_KEY_B, requestRange)
            )
        );
    }

    @Test
    public void planUnknownServerError() {
        assertBatchPlan(
            Map.of(
                partition0, FindBatchResponse.unknownServerError(),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, 10, 0, 0, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 1)
            ),
            Set.of(
                new ObjectFetchRequest(OBJECT_KEY_B, requestRange)
            )
        );
    }

    private void assertBatchPlan(
        final Map<TopicIdPartition, FindBatchResponse> coordinates,
        final Set<ObjectFetchRequest> expectedJobs
    ) {
        FetchPlanner planner = getFetchPlanner(coordinates);

        // Use the package-private planJobs method to verify the exact jobs planned
        List<ObjectFetchRequest> actualJobs = planner.planJobs(coordinates);

        assertThat(new HashSet<>(actualJobs)).isEqualTo(expectedJobs);
    }

    @Test
    public void testMultipleAsyncRequests() throws Exception {
        // This test verifies that the FetchPlanner correctly handles multiple batches from different objects
        // within a single fetch request. This scenario occurs when a Kafka consumer fetches from a partition
        // that has records stored across multiple objects in remote storage.
        //
        // What we're testing:
        // 1. Multiple batch coordinates are converted into separate fetch requests (one per object)
        // 2. Each fetch request returns a CompletableFuture that executes asynchronously
        // 3. All futures complete successfully with the correct data
        // 4. Each object is fetched exactly once (not duplicated)
        //
        // Note: This test uses a single-threaded executor, so fetches execute sequentially.
        // In production, a larger thread pool enables parallel execution for improved throughput.
        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
            final byte[] dataA = "data-for-a".getBytes();
            final byte[] dataB = "data-for-bb".getBytes();

            // Mock the fetcher's two-step process: fetch() is called first, then readToByteBuffer()
            // For this test, we only care about the final data returned by readToByteBuffer()
            when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class)))
                .thenReturn(null); // Return value doesn't matter, readToByteBuffer() is also mocked
            when(fetcher.fetch(eq(OBJECT_KEY_B), any(ByteRange.class)))
                .thenReturn(null); // Return value doesn't matter, readToByteBuffer() is also mocked

            // Mock readToByteBuffer to return the test data we want to verify
            // Order matters: first call returns dataA, second call returns dataB
            when(fetcher.readToByteBuffer(any()))
                .thenReturn(ByteBuffer.wrap(dataA))
                .thenReturn(ByteBuffer.wrap(dataB));

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, OBJECT_KEY_B.value(),
                        BatchMetadata.of(partition0, 0, 10, 1, 1, 11, 21, TimestampType.CREATE_TIME))
                ), 0, 2)
            );

            final FetchPlanner planner = getFetchPlanner(caffeineCache, coordinates);

            // Execute and verify
            assertThat(planner.get())
                .map(CompletableFuture::join)
                .map(FileExtent::data)
                .containsExactlyInAnyOrder(dataA, dataB);

            verify(fetcher).fetch(eq(OBJECT_KEY_A), any(ByteRange.class));
            verify(fetcher).fetch(eq(OBJECT_KEY_B), any(ByteRange.class));
        }
    }

    @Test
    public void testCacheMiss() throws Exception {
        // Setup: Cache miss scenario - data not in cache, must fetch from remote
        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
            final byte[] expectedData = "test-data".getBytes();
            final ByteBuffer byteBuffer = ByteBuffer.wrap(expectedData);

            // Mock the fetcher to return data via ByteBuffer
            when(fetcher.fetch(any(ObjectKey.class), any(ByteRange.class)))
                .thenReturn(null); // channel not used directly
            when(fetcher.readToByteBuffer(any()))
                .thenReturn(byteBuffer);

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            );

            final FetchPlanner planner = getFetchPlanner(caffeineCache, coordinates);

            // Execute: Trigger the fetch operation
            final List<CompletableFuture<FileExtent>> futures = planner.get();

            // Verify: Should have one future
            assertThat(futures).hasSize(1);

            // Wait for completion and verify the result
            final FileExtent result = futures.get(0).get();
            assertThat(result).isNotNull();
            assertThat(result.data()).isEqualTo(expectedData);

            // Verify remote fetch was called (cache miss)
            verify(fetcher).fetch(any(ObjectKey.class), any(ByteRange.class));

            // Verify the result is now in cache
            final ObjectFetchRequest request = new ObjectFetchRequest(OBJECT_KEY_A, requestRange);
            final CacheKey cacheKey = request.toCacheKey();
            assertThat(caffeineCache.get(cacheKey)).isNotNull();
        }
    }

    @Test
    public void testCacheHit() throws Exception {
        // Setup: Cache hit scenario - data already in cache
        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
            final byte[] expectedData = "cached-data".getBytes();
            final FileExtent cachedFileExtent = new FileExtent().setData(expectedData);

            // Pre-populate the cache
            final ObjectFetchRequest request = new ObjectFetchRequest(OBJECT_KEY_A, requestRange);
            final CacheKey cacheKey = request.toCacheKey();
            caffeineCache.put(cacheKey, cachedFileExtent);

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            );

            final FetchPlanner planner = getFetchPlanner(caffeineCache, coordinates);

            // Execute: Trigger the fetch operation
            final List<CompletableFuture<FileExtent>> futures = planner.get();

            // Verify: Should have one future
            assertThat(futures).hasSize(1);

            // Wait for completion and verify the result comes from cache
            final FileExtent result = futures.get(0).get();
            assertThat(result).isNotNull();
            assertThat(result.data()).isEqualTo(expectedData);

            // Verify remote fetch was NOT called (cache hit)
            verify(fetcher, never()).fetch(any(ObjectKey.class), any(ByteRange.class));
        }
    }

    @Test
    public void testFetchFailure() throws Exception {
        // Test that fetch failures are properly wrapped and propagated
        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {

            // Mock fetcher to throw exception
            when(fetcher.fetch(any(ObjectKey.class), any(ByteRange.class)))
                .thenThrow(new RuntimeException("S3 unavailable"));

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            );

            final FetchPlanner planner = getFetchPlanner(caffeineCache, coordinates);

            // Execute: Trigger fetch operation
            final List<CompletableFuture<FileExtent>> futures = planner.get();

            // Verify: Should have one future
            assertThat(futures).hasSize(1);

            // Verify exception is wrapped in CompletableFuture
            assertThatThrownBy(() -> futures.get(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FileFetchException.class);

            // Verify remote fetch was attempted
            verify(fetcher).fetch(any(ObjectKey.class), any(ByteRange.class));
        }
    }

    @Test
    public void testFailedFetchesAreRetried() throws Exception {
        // Test that when a fetch fails, the error is not cached, and subsequent requests
        // for the same key will retry the fetch operation.
        // This ensures transient errors (network issues, temporary S3 unavailability) don't
        // permanently block access to data.

        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
            final byte[] expectedData = "recovered-data".getBytes();

            // First call fails, second call succeeds
            when(fetcher.fetch(any(ObjectKey.class), any(ByteRange.class)))
                .thenThrow(new RuntimeException("Transient S3 error"))
                .thenReturn(null);
            when(fetcher.readToByteBuffer(any()))
                .thenReturn(ByteBuffer.wrap(expectedData));

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            );

            // First attempt - should fail
            final FetchPlanner planner1 = getFetchPlanner(caffeineCache, coordinates);

            final List<CompletableFuture<FileExtent>> futures1 = planner1.get();
            assertThatThrownBy(() -> futures1.get(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FileFetchException.class);

            // Second attempt - should retry and succeed
            final FetchPlanner planner2 = getFetchPlanner(caffeineCache, coordinates);

            final List<CompletableFuture<FileExtent>> futures2 = planner2.get();
            final FileExtent result = futures2.get(0).get();

            // Verify the second attempt succeeded
            assertThat(result.data()).isEqualTo(expectedData);

            // Verify fetch was called twice (once for failure, once for success)
            verify(fetcher, times(2)).fetch(any(ObjectKey.class), any(ByteRange.class));
        }
    }

    @Test
    public void testKeyAlignmentCreatesMultipleFetchRequests() {
        // Test that byte range alignment can split a single batch into multiple fetch requests
        // when the batch spans multiple alignment blocks.

        final KeyAlignmentStrategy alignment = new FixedBlockAlignment(1024);

        final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(),
                    BatchMetadata.of(partition0, 0, 2000, 0, 0, 10, 20, TimestampType.CREATE_TIME))
            ), 0, 1)
        );

        final FetchPlanner planner = getFetchPlanner(alignment, coordinates);

        final List<ObjectFetchRequest> result = planner.planJobs(coordinates);

        // Should create 2 fetch requests due to alignment splitting the range
        assertThat(result).hasSize(2);

        // Both requests should be for the same object
        assertThat(result.stream().map(ObjectFetchRequest::objectKey).distinct()).containsExactly(OBJECT_KEY_A);

        // The byte ranges should be aligned
        final Set<ByteRange> ranges = result.stream().map(ObjectFetchRequest::byteRange).collect(Collectors.toSet());
        // FixedBlockAlignment(1024) should create ranges aligned to 1024-byte blocks
        assertThat(ranges).hasSize(2);
    }

    @Test
    public void testPlanningDeduplicatesSameObjectRequests() throws Exception {
        // Test that when multiple batches for the SAME object and byte range arrive,
        // the planning phase merges them into a single fetch request via groupingBy(objectKey).
        //
        // This test demonstrates fetch deduplication at the planning level, not at the cache level.
        // When batches with the same objectKey are grouped together during planning,
        // only one ObjectFetchRequest is created, resulting in a single fetch operation.
        //
        // This validates the planner's ability to eliminate redundant fetches by merging
        // requests for the same object before they're even submitted to the cache.

        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
            final byte[] expectedData = "shared-data".getBytes();

            // Mock fetcher to return data
            when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class)))
                .thenReturn(null);
            when(fetcher.readToByteBuffer(any()))
                .thenReturn(ByteBuffer.wrap(expectedData));

            // Create coordinates with TWO batches that map to the SAME cache key
            // (same object, same byte range after alignment)
            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(2L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition1, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                ), 0, 1)
            );

            final FetchPlanner planner = getFetchPlanner(caffeineCache, coordinates);

            // Execute: Both batches will create fetch requests for the same cache key
            final List<CompletableFuture<FileExtent>> futures = planner.get();

            // Should have only 1 future because planning merges batches with the same object key
            assertThat(futures).hasSize(1);

            // Wait for completion
            final FileExtent result = futures.get(0).get();

            // Verify data is correct
            assertThat(result.data()).isEqualTo(expectedData);

            // Verify fetch was called **only once** despite multiple requests
            verify(fetcher).fetch(eq(OBJECT_KEY_A), any(ByteRange.class));
        }
    }

    @Test
    public void testMetricsAreRecordedCorrectly() throws Exception {
        // Test that the FetchPlanner records metrics at various stages of the fetch operation.
        // This ensures observability into the system's behavior.

        try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
            final byte[] dataA = "data-a".getBytes();
            final byte[] dataB = "data-bb".getBytes();

            when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
            when(fetcher.fetch(eq(OBJECT_KEY_B), any(ByteRange.class))).thenReturn(null);
            when(fetcher.readToByteBuffer(any()))
                .thenReturn(ByteBuffer.wrap(dataA))
                .thenReturn(ByteBuffer.wrap(dataB));

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 5, 20, TimestampType.CREATE_TIME)),
                    new BatchInfo(2L, OBJECT_KEY_B.value(),
                        BatchMetadata.of(partition0, 0, 10, 1, 1, 7, 21, TimestampType.CREATE_TIME))
                ), 0, 2)
            );

            final FetchPlanner planner = getFetchPlanner(caffeineCache, coordinates);

            final List<CompletableFuture<FileExtent>> futures = planner.get();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

            // Verify fetch batch size metric was recorded (2 batches in the response)
            verify(metrics).recordFetchBatchSize(2);

            // Verify fetch objects size metric was recorded (2 unique objects)
            verify(metrics).recordFetchObjectsSize(2);

            // Verify cache entry size metrics were recorded for both fetches
            verify(metrics).cacheEntrySize(dataA.length);
            verify(metrics).cacheEntrySize(dataB.length);

            // Verify fetch plan finished metric was recorded
            verify(metrics).fetchPlanFinished(any(Long.class));
        }
    }

    private FetchPlanner getFetchPlanner(
        final KeyAlignmentStrategy keyAlignment,
        final ObjectCache objectCache,
        final Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture
    ) {
        return new FetchPlanner(
            time, FetchPlannerTest.OBJECT_KEY_CREATOR, keyAlignment,
            objectCache, fetcher, dataExecutor, batchCoordinatesFuture, metrics
        );
    }

    private FetchPlanner getFetchPlanner(
        final KeyAlignmentStrategy keyAlignment,
        final Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture
    ) {
        return getFetchPlanner(keyAlignment, cache, batchCoordinatesFuture);
    }

    private FetchPlanner getFetchPlanner(
        final ObjectCache cache,
        final Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture
    ) {
        return getFetchPlanner(keyAlignmentStrategy, cache, batchCoordinatesFuture);
    }

    private FetchPlanner getFetchPlanner(final Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture) {
        return getFetchPlanner(keyAlignmentStrategy, cache, batchCoordinatesFuture);
    }
}
