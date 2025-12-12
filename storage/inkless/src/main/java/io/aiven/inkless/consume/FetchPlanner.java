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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

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

    private List<CompletableFuture<FileExtent>> doWork(final Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        final List<CacheFetchJob> jobs = planJobs(batchCoordinates);
        return submitAll(jobs);
    }

    /**
     * Helper class to track byte ranges and the oldest batch timestamp for an object key.
     */
    private static class ObjectFetchInfo {
        final List<ByteRange> ranges = new ArrayList<>();
        long oldestTimestamp = Long.MAX_VALUE;

        void addBatch(BatchInfo batch) {
            ranges.add(batch.metadata().range());
            oldestTimestamp = Math.min(oldestTimestamp, batch.metadata().timestamp());
        }
    }

    private List<CacheFetchJob> planJobs(final Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        // Group batches by object key, tracking ranges and oldest timestamp
        final Map<String, ObjectFetchInfo> objectKeysToFetchInfo = new HashMap<>();

        batchCoordinates.values().stream()
            .filter(findBatch -> findBatch.errors() == Errors.NONE)
            .map(FindBatchResponse::batches)
            .peek(batches -> metrics.recordFetchBatchSize(batches.size()))
            .flatMap(List::stream)
            .forEach(batch ->
                objectKeysToFetchInfo
                    .computeIfAbsent(batch.objectKey(), k -> new ObjectFetchInfo())
                    .addBatch(batch)
            );

        metrics.recordFetchObjectsSize(objectKeysToFetchInfo.size());

        return objectKeysToFetchInfo.entrySet().stream()
            .flatMap(e -> {
                final String objectKey = e.getKey();
                final ObjectFetchInfo fetchInfo = e.getValue();
                // Note: keyAlignment.align() returns fixed-size aligned blocks (e.g., 16 MiB).
                // This aligned byteRange is used for both caching and rate limiting (if enabled).
                // Rate limiting uses the aligned block size (not actual batch size) as a conservative
                // estimate, since the actual fetch size is only known after the fetch completes.
                return keyAlignment.align(fetchInfo.ranges)
                    .stream()
                    .map(byteRange -> getCacheFetchJob(objectKey, byteRange, fetchInfo.oldestTimestamp));
            })
            .collect(Collectors.toList());
    }

    private CacheFetchJob getCacheFetchJob(final String objectKey, final ByteRange byteRange, final long batchTimestamp) {
        return new CacheFetchJob(
                cache,
                objectFetcher,
                objectKeyCreator.from(objectKey),
                byteRange,
                time,
                metrics::fetchFileFinished,
                metrics::cacheEntrySize,
                batchTimestamp
        );

    }

    private List<CompletableFuture<FileExtent>> submitAll(List<CacheFetchJob> jobs) {
        return jobs.stream()
            .map(job -> CompletableFuture.supplyAsync(job::call, dataExecutor))
            .collect(Collectors.toList());
    }

    @Override
    public List<CompletableFuture<FileExtent>> get() {
        return TimeUtils.measureDurationMsSupplier(time, () -> doWork(batchCoordinates), metrics::fetchPlanFinished);
    }
}
