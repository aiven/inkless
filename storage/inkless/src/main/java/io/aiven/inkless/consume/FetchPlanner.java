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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

/**
 * Plans and executes fetch operations for diskless topics.
 *
 * <p>Execution flow:
 * <ol>
 *   <li><b>Planning:</b> Converts batch coordinates into fetch requests (merges, aligns ranges)</li>
 *   <li><b>Submission:</b> Submits requests to cache with async execution</li>
 *   <li><b>Execution:</b> Cache misses trigger remote storage fetch on executor</li>
 * </ol>
 *
 * If a fetch request hits in the cache, the corresponding future is already completed with the cached value.
 *
 * <p>This class uses async cache operations to avoid blocking threads. The cache returns
 * CompletableFuture immediately, and actual fetches run on the provided executor.
 */
public class FetchPlanner implements Supplier<List<CompletableFuture<FileExtent>>> {

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignment;
    private final ObjectCache cache;
    private final ObjectFetcher objectFetcher;
    private final ExecutorService dataExecutor;
    private final Map<TopicIdPartition, FindBatchResponse> batchCoordinates;
    private final InklessFetchMetrics metrics;

    public FetchPlanner(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignment,
        ObjectCache cache,
        ObjectFetcher objectFetcher,
        ExecutorService dataExecutor,
        Map<TopicIdPartition, FindBatchResponse> batchCoordinates,
        InklessFetchMetrics metrics
    ) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.keyAlignment = keyAlignment;
        this.cache = cache;
        this.objectFetcher = objectFetcher;
        this.dataExecutor = dataExecutor;
        this.batchCoordinates = batchCoordinates;
        this.metrics = metrics;
    }

    /**
     * Executes the fetch plan: plans requests, submits to cache, returns futures.
     *
     * @return list of futures that will complete with fetched file extents
     */
    @Override
    public List<CompletableFuture<FileExtent>> get() {
        return TimeUtils.measureDurationMsSupplier(
            time,
            () -> {
                final List<ObjectFetchRequest> requests = planJobs(batchCoordinates);
                return submitAllRequests(requests);
            },
            metrics::fetchPlanFinished
        );
    }

    /**
     * Plans fetch jobs from batch coordinates.
     *
     * @param batchCoordinates map of partition to batch information
     * @return list of planned fetch requests
     */
    List<ObjectFetchRequest> planJobs(final Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        // Group batches by object key
        final Set<Map.Entry<String, List<ByteRange>>> objectKeysToRanges =
            batchCoordinates.values().stream()
                // Filter out responses with errors
                .filter(findBatch -> findBatch.errors() == Errors.NONE)
                .map(FindBatchResponse::batches)
                .peek(batches -> metrics.recordFetchBatchSize(batches.size()))
                .flatMap(List::stream)
                .collect(Collectors.groupingBy(
                    BatchInfo::objectKey,
                    Collectors.mapping(batch -> batch.metadata().range(), Collectors.toList())
                ))
                .entrySet();

        metrics.recordFetchObjectsSize(objectKeysToRanges.size());

        // Create fetch requests with aligned byte ranges
        return objectKeysToRanges.stream()
            .flatMap(e -> createFetchRequests(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    }

    /**
     * Creates fetch requests for a single object with multiple batches.
     * Aligns byte ranges for efficient fetching.
     */
    private Stream<ObjectFetchRequest> createFetchRequests(
        final String objectKey,
        final List<ByteRange> byteRanges
    ) {
        // Align byte ranges for efficient fetching
        final Set<ByteRange> alignedRanges = keyAlignment.align(byteRanges);

        // Create a fetch request for each aligned range
        return alignedRanges.stream()
            .map(byteRange -> new ObjectFetchRequest(
                objectKeyCreator.from(objectKey),
                byteRange
            ));
    }

    private List<CompletableFuture<FileExtent>> submitAllRequests(final List<ObjectFetchRequest> requests) {
        return requests.stream()
            .map(this::submitSingleRequest)
            .collect(Collectors.toList());
    }

    private CompletableFuture<FileExtent> submitSingleRequest(final ObjectFetchRequest request) {
        // Cache API performs lookup on the calling thread and returns a CompletableFuture immediately (non-blocking).
        // On cache miss, fetchFileExtent executes on dataExecutor; on cache hit, a completed future is returned immediately.
        return cache.computeIfAbsent(
            request.toCacheKey(),
            k -> fetchFileExtent(request),
            dataExecutor
        );
    }

    /**
     * Fetches a file extent from remote storage.
     * This method is called by the cache (on dataExecutor) when a cache miss occurs.
     *
     * @param request the fetch request with object key and byte range
     * @return the fetched file extent
     * @throws FileFetchException if remote fetch fails
     */
    private FileExtent fetchFileExtent(final ObjectFetchRequest request) {
        try {
            final FileFetchJob job = new FileFetchJob(
                time,
                objectFetcher,
                request.objectKey(),
                request.byteRange(),
                metrics::fetchFileFinished
            );
            final FileExtent fileExtent = job.call();

            // Record cache entry size for monitoring
            metrics.cacheEntrySize(fileExtent.data().length);

            return fileExtent;
        } catch (final Exception e) {
            throw new FileFetchException(e);
        }
    }

    /**
     * Represents a request to fetch objects from diskless storage.
     * Contains all information needed to fetch and cache a file extent.
     *
     * @param objectKey the storage object key
     * @param byteRange the range of bytes to fetch
     */
    record ObjectFetchRequest(
        ObjectKey objectKey,
        ByteRange byteRange
    ) {
        CacheKey toCacheKey() {
            return new CacheKey()
                .setObject(objectKey.value())
                .setRange(new CacheKey.ByteRange()
                    .setOffset(byteRange.offset())
                    .setLength(byteRange.size()));
        }
    }
}
