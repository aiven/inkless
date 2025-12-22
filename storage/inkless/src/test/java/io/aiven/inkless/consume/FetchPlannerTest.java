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
import org.junit.jupiter.api.Nested;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.InklessThreadFactory;
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
import io.github.bucket4j.Bucket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Structure:
 * - Core Planning Tests: Basic fetch planning logic (no execution)
 * - Execution Tests (No Lagging Consumer Feature): Tests with feature disabled
 * - Lagging Consumer Feature Tests: Tests validating hot/cold path separation
 */
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

    ExecutorService fetchDataExecutor = Executors.newSingleThreadExecutor();
    ExecutorService laggingFetchDataExecutor = Executors.newSingleThreadExecutor();

    ObjectCache cache = new NullCache();
    KeyAlignmentStrategy keyAlignmentStrategy = new FixedBlockAlignment(Integer.MAX_VALUE);
    ByteRange requestRange = new ByteRange(0, Integer.MAX_VALUE);
    Time time = new MockTime();
    Uuid topicId = Uuid.randomUuid();
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "diskless-topic");
    TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "diskless-topic");

    @AfterEach
    void tearDown() {
        fetchDataExecutor.shutdownNow();
        laggingFetchDataExecutor.shutdownNow();
    }

    // Basic fetch planning logic (no execution)
    @Nested
    class PlanningTests {

        @Test
        public void planEmptyRequest() {
            Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of();
            FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, cache, coordinates);

            List<ObjectFetchRequest> result = planner.planJobs(coordinates);

            assertThat(result).isEmpty();
        }

        @Test
        public void planRequestWithEmptyBatches() {
            // Test that a response with Errors.NONE but empty batches list is handled gracefully.
            // This verifies the defensive check in createFetchRequests that handles empty batches.
            // In practice, empty batches won't create any object key groups, so createFetchRequests
            // won't be called, but this test ensures the overall flow handles it correctly.
            Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(), 0, 1) // Empty batches list
            );
            FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, cache, coordinates);

            List<ObjectFetchRequest> result = planner.planJobs(coordinates);

            // Should return empty list since there are no batches to process
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
                    new ObjectFetchRequest(OBJECT_KEY_A, requestRange, 20)
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
                    new ObjectFetchRequest(OBJECT_KEY_A, requestRange, 20),
                    new ObjectFetchRequest(OBJECT_KEY_B, requestRange, 21)
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
                    new ObjectFetchRequest(OBJECT_KEY_A, requestRange, 20),
                    new ObjectFetchRequest(OBJECT_KEY_B, requestRange, 21)
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
                    // When batches for same object are merged, timestamp is max(20, 21) = 21
                    new ObjectFetchRequest(OBJECT_KEY_A, requestRange, 21)
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
                    new ObjectFetchRequest(OBJECT_KEY_B, requestRange, 21)
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
                    new ObjectFetchRequest(OBJECT_KEY_B, requestRange, 21)
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
                    new ObjectFetchRequest(OBJECT_KEY_B, requestRange, 21)
                )
            );
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

            final FetchPlanner planner = createFetchPlannerHotPathOnly(alignment, cache, coordinates);

            final List<ObjectFetchRequest> result = planner.planJobs(coordinates);

            // Should create 2 fetch requests due to alignment splitting the range
            assertThat(result).hasSize(2);

            // Both requests should be for the same object
            assertThat(result.stream().map(ObjectFetchRequest::objectKey).distinct()).containsExactly(OBJECT_KEY_A);

            // Both should have the same timestamp (since they're from the same batch)
            assertThat(result.stream().map(ObjectFetchRequest::timestamp).distinct()).containsExactly(20L);

            // The byte ranges should be aligned
            final Set<ByteRange> ranges = result.stream().map(ObjectFetchRequest::byteRange).collect(Collectors.toSet());
            // FixedBlockAlignment(1024) should create ranges aligned to 1024-byte blocks
            assertThat(ranges).hasSize(2);
        }

        @Test
        public void testTimestampAggregationUsesMaxValue() {
            // Test that when multiple batches for the same object have different timestamps,
            // the fetch request uses the maximum timestamp value.
            //
            // This is important for hot/cold path decisions - the most recent timestamp
            // should be used to determine if data is "hot" (recent) or "cold" (old).

            final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                    new BatchInfo(1L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 100, TimestampType.CREATE_TIME))
                ), 0, 1),
                partition1, FindBatchResponse.success(List.of(
                    new BatchInfo(2L, OBJECT_KEY_A.value(),
                        BatchMetadata.of(partition1, 30, 10, 0, 0, 15, 500, TimestampType.CREATE_TIME))
                ), 0, 1)
            );

            final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, cache, coordinates);
            final List<ObjectFetchRequest> result = planner.planJobs(coordinates);

            // Should merge into single request for OBJECT_KEY_A
            assertThat(result).hasSize(1);

            final ObjectFetchRequest request = result.get(0);
            assertThat(request.objectKey()).isEqualTo(OBJECT_KEY_A);
            // Timestamp should be max(100, 500) = 500
            assertThat(request.timestamp()).isEqualTo(500L);
        }
    }

    // Execution Tests - No Lagging Consumer Feature
    // These tests validate core fetch execution with the lagging consumer feature disabled
    @Nested
    class ExecutionWithoutLaggingConsumerFeature {

        @Test
        public void testMultipleAsyncFetchOperations() throws Exception {
            // This test verifies that the FetchPlanner correctly handles multiple batches from different objects
            // within a single fetch request. This scenario occurs when a Kafka consumer fetches from a partition
            // that has records stored across multiple objects in remote storage.
            //
            // What we're testing:
            // 1. Multiple batch coordinates are converted into separate fetch requests (one per object)
            // 2. Each fetch request executes asynchronously (returns CompletableFuture immediately)
            // 3. All futures complete successfully with the correct data
            // 4. Each object is fetched exactly once (not duplicated)
            //
            // Note: Uses single-threaded executors (sequential execution), not concurrent.
            // The test validates async API behavior (non-blocking submission), not parallel execution.
            //
            // Why this matters:
            // - Ensures multiple objects can be fetched through async API
            // - Validates that the async execution model works correctly
            // - Confirms data integrity when handling multiple async operations
            try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                final byte[] dataA = "data-for-a".getBytes();
                final byte[] dataB = "data-for-b".getBytes();

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

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

                // Execute: Trigger fetch operations
                final List<CompletableFuture<FileExtent>> futures = planner.get();

                // Verify: Should have two futures
                assertThat(futures).hasSize(2);

                // Wait for all to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

                // Verify both were fetched
                verify(fetcher).fetch(eq(OBJECT_KEY_A), any(ByteRange.class));
                verify(fetcher).fetch(eq(OBJECT_KEY_B), any(ByteRange.class));

                // Verify correct data for each
                final List<FileExtent> results = futures.stream()
                    .map(f -> {
                        try {
                            return f.get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());

                assertThat(results).hasSize(2);

                // Verify the actual data content matches expected values
                // Note: We use containsExactlyInAnyOrder because the API doesn't guarantee
                // ordering of results (even though this test uses sequential execution).
                assertThat(results)
                    .extracting(FileExtent::data)
                    .containsExactlyInAnyOrder(dataA, dataB);
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

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

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
                final ObjectFetchRequest request = new ObjectFetchRequest(
                    OBJECT_KEY_A, requestRange, 20
                );
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
                final ObjectFetchRequest request = new ObjectFetchRequest(
                    OBJECT_KEY_A, requestRange, 20
                );
                final CacheKey cacheKey = request.toCacheKey();
                caffeineCache.put(cacheKey, cachedFileExtent);

                final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                    partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(1L, OBJECT_KEY_A.value(),
                            BatchMetadata.of(partition0, 0, 10, 0, 0, 10, 20, TimestampType.CREATE_TIME))
                    ), 0, 1)
                );

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

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

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

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
                final FetchPlanner planner1 = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

                final List<CompletableFuture<FileExtent>> futures1 = planner1.get();
                assertThatThrownBy(() -> futures1.get(0).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(FileFetchException.class);

                // Second attempt - should retry and succeed
                final FetchPlanner planner2 = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

                final List<CompletableFuture<FileExtent>> futures2 = planner2.get();
                final FileExtent result = futures2.get(0).get();

                // Verify the second attempt succeeded
                assertThat(result.data()).isEqualTo(expectedData);

                // Verify fetch was called twice (once for failure, once for success)
                verify(fetcher, times(2)).fetch(any(ObjectKey.class), any(ByteRange.class));
            }
        }

        @Test
        public void testConcurrentRequestsToSameKeyFetchOnlyOnce() throws Exception {
            // Test that when multiple requests for the SAME cache key arrive concurrently,
            // the underlying fetch operation is performed only ONCE, and all requesters
            // receive the same result.
            //
            // This validates the cache's deduplication behavior which prevents redundant
            // fetches to object storage when multiple threads/requests need the same data.

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

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

                // Execute: Both batches will create fetch requests for the same cache key
                final List<CompletableFuture<FileExtent>> futures = planner.get();

                // Should have only 1 future because the cache deduplicates same-key requests
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

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

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

        @Test
        public void testOldDataUsesHotPathWhenLaggingConsumerFeatureDisabled() throws Exception {
            // When lagging consumer feature is disabled (laggingConsumerExecutor = null),
            // ALL data should use the hot path (cache + recentDataExecutor), regardless of data age.
            // This test verifies that old data that would normally use cold path
            // still uses hot path when the feature is disabled.
            try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                final byte[] expectedData = "old-data-but-hot-path".getBytes();

                when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                // Very old timestamp - would be "lagging" if feature was enabled
                final long veryOldTimestamp = time.milliseconds() - 3600_000L; // 1 hour ago
                final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                    partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(1L, OBJECT_KEY_A.value(),
                            BatchMetadata.of(partition0, 0, 10, 0, 0, 10, veryOldTimestamp, TimestampType.CREATE_TIME))
                    ), 0, 1)
                );

                // Feature disabled: laggingConsumerExecutor = null
                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

                final List<CompletableFuture<FileExtent>> futures = planner.get();
                assertThat(futures).hasSize(1);

                final FileExtent result = futures.get(0).get();
                assertThat(result.data()).isEqualTo(expectedData);

                // Verify HOT path metrics were recorded (not cold path)
                verify(metrics).recordRecentDataRequest();
                verify(metrics, never()).recordLaggingConsumerRequest();
                verify(metrics, never()).recordRateLimitWaitTime(any(Long.class));

                // Verify data was fetched successfully
                verify(fetcher).fetch(eq(OBJECT_KEY_A), any(ByteRange.class));
            }
        }

        @Test
        public void testExecutionWithEmptyBatches() throws Exception {
            // Test that execution path handles empty batches gracefully.
            // This verifies the defensive check in createFetchRequests works correctly
            // and that the full execution flow (planning + execution) handles empty batches.
            try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                // Response with Errors.NONE but empty batches list
                final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                    partition0, FindBatchResponse.success(List.of(), 0, 1) // Empty batches
                );

                final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);

                // Execute: Should return empty list of futures (no batches to fetch)
                final List<CompletableFuture<FileExtent>> futures = planner.get();

                // Verify: Should have no futures since there are no batches
                assertThat(futures).isEmpty();

                // Verify no fetch operations were attempted
                verify(fetcher, never()).fetch(any(ObjectKey.class), any(ByteRange.class));

                // Verify metrics were still recorded (batch size = 0)
                verify(metrics).recordFetchBatchSize(0);
            }
        }
    }

    // Lagging Consumer Feature Tests
    // These tests validate the hot/cold path separation when the feature is enabled
    @Nested
    class LaggingConsumerFeatureTests {

        @Nested
        class HotPathTests {

            @Test
            public void recentDataUsesRecentExecutorWithoutRateLimit() throws Exception {
                // Validates hot path: recent data uses recent executor, no rate limiting
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "recent-data".getBytes();
                    final Bucket rateLimiter = Bucket.builder()
                        .addLimit(limit -> limit.capacity(1).refillGreedy(1, java.time.Duration.ofSeconds(1)))
                        .build();

                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long recentTimestamp = time.milliseconds() - 30000L; // 30 seconds ago (recent)
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, recentTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    final long threshold = 60 * 1000L;
                    final FetchPlanner planner = createFetchPlannerWithCustomThreshold(
                        keyAlignmentStrategy, caffeineCache, coordinates,
                        threshold, laggingFetchDataExecutor, rateLimiter
                    );

                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    final FileExtent result = futures.get(0).get();
                    assertThat(result.data()).isEqualTo(expectedData);

                    // Verify hot path metrics
                    verify(metrics).recordRecentDataRequest();
                    verify(metrics, never()).recordLaggingConsumerRequest();
                    verify(metrics, never()).recordRateLimitWaitTime(any(Long.class));
                }
            }

            @Test
            public void boundaryConditionExactlyAtThreshold() throws Exception {
                // Validates boundary: dataAge == threshold uses hot path
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "boundary-data".getBytes();
                    final long threshold = 60 * 1000L;

                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long exactThresholdTimestamp = time.milliseconds() - threshold;
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, exactThresholdTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    final FetchPlanner planner = createFetchPlannerWithCustomThreshold(
                        keyAlignmentStrategy, caffeineCache, coordinates,
                        threshold, laggingFetchDataExecutor, null
                    );

                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);
                    futures.get(0).get();

                    // dataAge <= threshold → hot path
                    verify(metrics).recordRecentDataRequest();
                    verify(metrics, never()).recordLaggingConsumerRequest();
                }
            }
        }

        @Nested
        class ColdPathTests {

            @Test
            public void laggingDataUsesLaggingExecutorWithRateLimit() throws Exception {
                // Validates cold path: old data uses lagging executor with rate limiting
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "old-data".getBytes();
                    final Bucket rateLimiter = Bucket.builder()
                        .addLimit(limit -> limit.capacity(10).refillGreedy(10, java.time.Duration.ofSeconds(1)))
                        .build();

                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long oldTimestamp = time.milliseconds() - 120000L; // 2 minutes ago (old)
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    final long threshold = 60 * 1000L;
                    final FetchPlanner planner = createFetchPlannerWithCustomThreshold(
                        keyAlignmentStrategy, caffeineCache, coordinates,
                        threshold, laggingFetchDataExecutor, rateLimiter
                    );

                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    final FileExtent result = futures.get(0).get();
                    assertThat(result.data()).isEqualTo(expectedData);

                    // Verify cold path metrics with rate limiting
                    verify(metrics, never()).recordRecentDataRequest();
                    verify(metrics).recordLaggingConsumerRequest();
                    // Rate limit wait time should be recorded (including zero-wait cases for accurate histogram)
                    verify(metrics).recordRateLimitWaitTime(any(Long.class));
                }
            }

            @Test
            public void laggingDataWithoutRateLimiter() throws Exception {
                // Validates cold path works without rate limiter (optional feature)
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "old-data-no-limit".getBytes();

                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long oldTimestamp = time.milliseconds() - 120000L;
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    final long threshold = 60 * 1000L;
                    final FetchPlanner planner = createFetchPlannerWithCustomThreshold(
                        keyAlignmentStrategy, caffeineCache, coordinates,
                        threshold, laggingFetchDataExecutor, null // No rate limiter
                    );

                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    final FileExtent result = futures.get(0).get();
                    assertThat(result.data()).isEqualTo(expectedData);

                    // Verify cold path but no rate limiting metrics
                    verify(metrics, never()).recordRecentDataRequest();
                    verify(metrics).recordLaggingConsumerRequest();
                    verify(metrics, never()).recordRateLimitWaitTime(any(Long.class));
                }
            }

            @Test
            public void fetchFailureInColdPathPropagatesException() throws Exception {
                // Test that fetch failures in the cold path are properly wrapped and propagated
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    // Mock fetcher to throw exception
                    when(fetcher.fetch(any(ObjectKey.class), any(ByteRange.class)))
                        .thenThrow(new RuntimeException("S3 unavailable"));

                    final long oldTimestamp = time.milliseconds() - 120000L;
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    final long threshold = 60 * 1000L;
                    final FetchPlanner planner = createFetchPlannerWithCustomThreshold(
                        keyAlignmentStrategy, caffeineCache, coordinates,
                        threshold, laggingFetchDataExecutor, null // No rate limiter
                    );

                    // Execute: Trigger fetch operation via cold path
                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    // Verify exception is wrapped in CompletableFuture
                    assertThatThrownBy(() -> futures.get(0).get())
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(FileFetchException.class);

                    // Verify cold path was used
                    verify(metrics).recordLaggingConsumerRequest();
                    verify(metrics, never()).recordRecentDataRequest();

                    // Verify remote fetch was attempted
                    verify(fetcher).fetch(any(ObjectKey.class), any(ByteRange.class));
                }
            }

            @Test
            public void executorQueueFullThrowsRejectedExecutionException() throws Exception {
                // Test that RejectedExecutionException is thrown when lagging consumer executor queue is full.
                // This validates the AbortPolicy backpressure mechanism.
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    // Create a saturated executor: 1 thread + 1 queue slot, both occupied
                    final CountDownLatch blockLatch = new CountDownLatch(1);
                    final CountDownLatch threadStartedLatch = new CountDownLatch(1);

                    final ExecutorService saturatedExecutor = new ThreadPoolExecutor(
                        1,  // corePoolSize
                        1,  // maximumPoolSize
                        0L, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(1),  // Queue capacity of 1
                        new InklessThreadFactory("test-saturated-", false),
                        new ThreadPoolExecutor.AbortPolicy()  // Reject when full
                    );

                    try {
                        // Block the single thread
                        saturatedExecutor.submit(() -> {
                            threadStartedLatch.countDown();
                            try {
                                blockLatch.await(); // Block until test completes
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

                        // Wait for thread to start
                        assertThat(threadStartedLatch.await(2, TimeUnit.SECONDS))
                            .as("Thread should have started").isTrue();

                        // Fill the queue (1 slot) - task will be queued but not execute until thread is free
                        saturatedExecutor.submit(() -> {
                            try {
                                blockLatch.await(); // Block until test completes
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

                        // Executor is now saturated (thread busy, queue full)

                        // Setup coordinates for old data (triggers cold path)
                        final long oldTimestamp = time.milliseconds() - 120000L;
                        final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                            partition0, FindBatchResponse.success(List.of(
                                new BatchInfo(1L, OBJECT_KEY_A.value(),
                                    BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                            ), 0, 1)
                        );

                        final long threshold = 60 * 1000L;
                        final FetchPlanner planner = new FetchPlanner(
                            time,
                            OBJECT_KEY_CREATOR,
                            keyAlignmentStrategy,
                            caffeineCache,
                            fetcher,
                            fetchDataExecutor,
                            fetcher, // laggingConsumerFetcher
                            threshold,
                            null, // No rate limiter
                            saturatedExecutor, // Saturated executor
                            coordinates,
                            metrics
                        );

                        // Execute: Cold path returns failed future instead of throwing
                        final List<CompletableFuture<FileExtent>> futures = planner.get();

                        // Verify: Got one future
                        assertThat(futures).hasSize(1);

                        // Verify: Future completed exceptionally with RejectedExecutionException
                        assertThat(futures.get(0)).isCompletedExceptionally();
                        assertThatThrownBy(() -> futures.get(0).join())
                            .isInstanceOf(CompletionException.class)
                            .hasCauseInstanceOf(RejectedExecutionException.class);

                        // Verify: Rejection metric was recorded by FetchPlanner.submitSingleRequest()
                        verify(metrics).recordLaggingConsumerRejection();

                        // Verify: Cold path was attempted
                        verify(metrics).recordLaggingConsumerRequest();

                    } finally {
                        // Cleanup: Unblock and shutdown executor
                        blockLatch.countDown();
                        saturatedExecutor.shutdown();
                        saturatedExecutor.awaitTermination(2, TimeUnit.SECONDS);
                    }
                }
            }

            @Test
            public void executorShutdownTrackedAsRejection() throws Exception {
                // Test that RejectedExecutionException (from executor shutdown) is tracked as rejection.
                // This validates that shutdown scenarios are properly monitored.
                // Note: This tests shutdown behavior, which is different from queue saturation
                // (tested in executorQueueFullThrowsRejectedExecutionException).
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    // Create an executor that we'll shut down
                    final ExecutorService shutdownExecutor = new ThreadPoolExecutor(
                        1,  // corePoolSize
                        1,  // maximumPoolSize
                        0L, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(10),
                        new InklessThreadFactory("test-shutdown-", false),
                        new ThreadPoolExecutor.AbortPolicy()
                    );

                    // Shutdown immediately to cause rejection
                    shutdownExecutor.shutdown();

                    // Setup coordinates for old data (triggers cold path)
                    final long oldTimestamp = time.milliseconds() - 120000L;
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    final long threshold = 60 * 1000L;
                    final FetchPlanner planner = new FetchPlanner(
                        time,
                        OBJECT_KEY_CREATOR,
                        keyAlignmentStrategy,
                        caffeineCache,
                        fetcher,
                        fetchDataExecutor,
                        fetcher, // laggingConsumerFetcher
                        threshold,
                        null, // No rate limiter
                        shutdownExecutor, // Shutdown executor
                        coordinates,
                        metrics
                    );

                    // Execute: Cold path returns failed future instead of throwing
                    final List<CompletableFuture<FileExtent>> futures = planner.get();

                    // Verify: Got one future
                    assertThat(futures).hasSize(1);

                    // Verify: Future completed exceptionally with RejectedExecutionException
                    assertThat(futures.get(0)).isCompletedExceptionally();
                    assertThatThrownBy(() -> futures.get(0).join())
                        .isInstanceOf(CompletionException.class)
                        .hasCauseInstanceOf(RejectedExecutionException.class);

                    // Verify: Rejection metric was recorded
                    verify(metrics).recordLaggingConsumerRejection();

                    // Cleanup
                    shutdownExecutor.awaitTermination(1, TimeUnit.SECONDS);
                }
            }
        }

        @Nested
        class MixedWorkloadTests {

            @Test
            public void multipleRequestsMixedHotAndColdPaths() throws Exception {
                // Validates path selection is per-request: some hot, some cold
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] recentData = "recent".getBytes();
                    final byte[] oldData = "old".getBytes();
                    final long threshold = 60 * 1000L;

                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.fetch(eq(OBJECT_KEY_B), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any()))
                        .thenReturn(ByteBuffer.wrap(recentData))
                        .thenReturn(ByteBuffer.wrap(oldData));

                    final long recentTimestamp = time.milliseconds() - 30000L; // 30s ago (recent)
                    final long oldTimestamp = time.milliseconds() - 120000L;  // 2min ago (old)

                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, recentTimestamp, TimestampType.CREATE_TIME)),
                            new BatchInfo(2L, OBJECT_KEY_B.value(),
                                BatchMetadata.of(partition0, 0, 10, 1, 1, 11, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 2)
                    );

                    final FetchPlanner planner = createFetchPlannerWithCustomThreshold(
                        keyAlignmentStrategy, caffeineCache, coordinates,
                        threshold, laggingFetchDataExecutor, null
                    );

                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(2);
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

                    // One request on each path
                    verify(metrics).recordRecentDataRequest();
                    verify(metrics).recordLaggingConsumerRequest();
                }
            }

            @Test
            public void hotAndColdPathsExecuteConcurrently() throws Exception {
                // Validates that hot and cold paths execute concurrently on separate executors.
                // This ensures thread pool isolation works correctly and both paths can make progress
                // simultaneously without blocking each other.
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] recentData = "recent-concurrent".getBytes();
                    final byte[] oldData = "old-concurrent".getBytes();
                    final long threshold = 60 * 1000L;

                    // Use separate executors with multiple threads to verify concurrency
                    final ExecutorService hotExecutor = Executors.newFixedThreadPool(2);
                    final ExecutorService coldExecutor = Executors.newFixedThreadPool(2);

                    try {
                        when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                        when(fetcher.fetch(eq(OBJECT_KEY_B), any(ByteRange.class))).thenReturn(null);
                        when(fetcher.readToByteBuffer(any()))
                            .thenReturn(ByteBuffer.wrap(recentData))
                            .thenReturn(ByteBuffer.wrap(oldData));

                        final long recentTimestamp = time.milliseconds() - 30000L; // 30s ago (recent)
                        final long oldTimestamp = time.milliseconds() - 120000L;  // 2min ago (old)

                        final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                            partition0, FindBatchResponse.success(List.of(
                                new BatchInfo(1L, OBJECT_KEY_A.value(),
                                    BatchMetadata.of(partition0, 0, 10, 0, 0, 10, recentTimestamp, TimestampType.CREATE_TIME)),
                                new BatchInfo(2L, OBJECT_KEY_B.value(),
                                    BatchMetadata.of(partition1, 0, 10, 0, 0, 11, oldTimestamp, TimestampType.CREATE_TIME))
                            ), 0, 2)
                        );

                        final FetchPlanner planner = new FetchPlanner(
                            time,
                            OBJECT_KEY_CREATOR,
                            keyAlignmentStrategy,
                            caffeineCache,
                            fetcher,
                            hotExecutor, // Hot path executor
                            fetcher, // laggingConsumerFetcher
                            threshold,
                            null, // No rate limiter
                            coldExecutor, // Cold path executor
                            coordinates,
                            metrics
                        );

                        // Execute both paths concurrently
                        final List<CompletableFuture<FileExtent>> futures = planner.get();
                        assertThat(futures).hasSize(2);

                        // Both should complete successfully
                        final List<FileExtent> results = futures.stream()
                            .map(f -> {
                                try {
                                    return f.get();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .collect(Collectors.toList());

                        assertThat(results).hasSize(2);
                        assertThat(results)
                            .extracting(FileExtent::data)
                            .containsExactlyInAnyOrder(recentData, oldData);

                        // Verify both paths were used
                        verify(metrics).recordRecentDataRequest();
                        verify(metrics).recordLaggingConsumerRequest();
                    } finally {
                        hotExecutor.shutdown();
                        coldExecutor.shutdown();
                        hotExecutor.awaitTermination(2, TimeUnit.SECONDS);
                        coldExecutor.awaitTermination(2, TimeUnit.SECONDS);
                    }
                }
            }
        }

        @Nested
        class FeatureToggleTests {

            @Test
            public void allRequestsUseRecentPathWhenFeatureDisabled() throws Exception {
                // Validates: laggingConsumerExecutor = null → feature disabled, all use hot path
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "all-recent".getBytes();
                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long oldTimestamp = time.milliseconds() - 120000L; // Would be lagging if feature enabled
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    // Pass null for laggingConsumerExecutor to disable feature
                    final FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, caffeineCache, coordinates);
                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    final FileExtent result = futures.get(0).get();
                    assertThat(result.data()).isEqualTo(expectedData);

                    // Even with old timestamp, uses hot path
                    verify(metrics).recordRecentDataRequest();
                    verify(metrics, never()).recordLaggingConsumerRequest();
                }
            }

            @Test
            public void rateLimiterCanBeDisabledIndependently() throws Exception {
                // Validates: rateLimiter = null → cold path without rate limiting
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "cold-no-limit".getBytes();
                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long oldTimestamp = time.milliseconds() - 120000L;
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    // Executor enabled, but no rate limiter
                    final FetchPlanner planner = createFetchPlannerWithLaggingConsumerFeature(keyAlignmentStrategy, caffeineCache, coordinates, laggingFetchDataExecutor, null);
                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    final FileExtent result = futures.get(0).get();
                    assertThat(result.data()).isEqualTo(expectedData);

                    // Uses cold path but no rate limiting
                    verify(metrics, never()).recordRecentDataRequest();
                    verify(metrics).recordLaggingConsumerRequest();
                    verify(metrics, never()).recordRateLimitWaitTime(any(Long.class));
                }
            }

            @Test
            public void bothFeaturesCanBeEnabled() throws Exception {
                // Validates: both executor and rate limiter enabled → full cold path
                try (CaffeineCache caffeineCache = new CaffeineCache(100, 3600, 180)) {
                    final byte[] expectedData = "cold-with-limit".getBytes();
                    final Bucket rateLimiter = Bucket.builder()
                        .addLimit(limit -> limit.capacity(10).refillGreedy(10, java.time.Duration.ofSeconds(1)))
                        .build();
                    when(fetcher.fetch(eq(OBJECT_KEY_A), any(ByteRange.class))).thenReturn(null);
                    when(fetcher.readToByteBuffer(any())).thenReturn(ByteBuffer.wrap(expectedData));

                    final long oldTimestamp = time.milliseconds() - 120000L;
                    final Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                        partition0, FindBatchResponse.success(List.of(
                            new BatchInfo(1L, OBJECT_KEY_A.value(),
                                BatchMetadata.of(partition0, 0, 10, 0, 0, 10, oldTimestamp, TimestampType.CREATE_TIME))
                        ), 0, 1)
                    );

                    // Both features enabled
                    final FetchPlanner planner = createFetchPlannerWithLaggingConsumerFeature(keyAlignmentStrategy, caffeineCache, coordinates, laggingFetchDataExecutor, rateLimiter);
                    final List<CompletableFuture<FileExtent>> futures = planner.get();
                    assertThat(futures).hasSize(1);

                    final FileExtent result = futures.get(0).get();
                    assertThat(result.data()).isEqualTo(expectedData);

                    // Full cold path with rate limiting
                    verify(metrics, never()).recordRecentDataRequest();
                    verify(metrics).recordLaggingConsumerRequest();
                    verify(metrics).recordRateLimitWaitTime(any(Long.class));
                }
            }
        }
    }

    /**
     * Creates a FetchPlanner with full control over all parameters.
     * Use when testing specific threshold values or custom configurations.
     */
    private FetchPlanner createFetchPlannerWithCustomThreshold(
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture,
        long thresholdMs,
        ExecutorService laggingConsumerExecutor,
        Bucket rateLimiter
    ) {
        return new FetchPlanner(
            time,
            FetchPlannerTest.OBJECT_KEY_CREATOR,
            keyAlignmentStrategy,
            cache,
            fetcher,
            fetchDataExecutor,
            fetcher,
            thresholdMs,
            rateLimiter,
            laggingConsumerExecutor,
            batchCoordinatesFuture,
            metrics
        );
    }

    /**
     * Creates a FetchPlanner with lagging consumer feature enabled (default 60s threshold).
     * Use for testing hot/cold path separation with standard threshold.
     */
    private FetchPlanner createFetchPlannerWithLaggingConsumerFeature(
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture,
        ExecutorService laggingConsumerExecutor,
        Bucket rateLimiter
    ) {
        return createFetchPlannerWithCustomThreshold(
            keyAlignmentStrategy, cache, batchCoordinatesFuture,
            60 * 1000L, laggingConsumerExecutor, rateLimiter
        );
    }

    /**
     * Creates a FetchPlanner with lagging consumer feature DISABLED (hot path only).
     * Use for testing basic fetch functionality without hot/cold path separation.
     */
    private FetchPlanner createFetchPlannerHotPathOnly(
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        Map<TopicIdPartition, FindBatchResponse> batchCoordinatesFuture
    ) {
        return createFetchPlannerWithCustomThreshold(
            keyAlignmentStrategy, cache, batchCoordinatesFuture,
            60 * 1000L, null, null
        );
    }

    private void assertBatchPlan(Map<TopicIdPartition, FindBatchResponse> coordinates, Set<ObjectFetchRequest> expectedJobs) {
        FetchPlanner planner = createFetchPlannerHotPathOnly(keyAlignmentStrategy, cache, coordinates);
        List<ObjectFetchRequest> actualJobs = planner.planJobs(coordinates);
        assertThat(new HashSet<>(actualJobs)).isEqualTo(expectedJobs);
    }
}
