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

import io.aiven.inkless.common.ByteRange;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class FetchPlanner implements Supplier<List<Future<FileExtent>>> {

    private final Time time;
    private final ObjectKeyCreator objectKeyCreator;
    private final KeyAlignmentStrategy keyAlignment;
    private final ObjectCache cache;
    private final ObjectFetcher objectFetcher;
    private final ExecutorService dataExecutor;
    private final Map<TopicIdPartition, FindBatchResponse> batchCoordinates;
    private final Consumer<Long> fetchPlanDurationCallback;
    private final Consumer<Long> cacheQueryDurationCallback;
    private final Consumer<Long> cacheStoreDurationCallback;
    private final Consumer<Boolean> cacheHitRateCallback;
    private final Consumer<Long> fileFetchDurationCallback;

    public FetchPlanner(
        Time time,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignment,
        ObjectCache cache,
        ObjectFetcher objectFetcher,
        ExecutorService dataExecutor,
        Map<TopicIdPartition, FindBatchResponse> batchCoordinates,
        Consumer<Long> fetchPlanDurationCallback,
        Consumer<Long> cacheQueryDurationCallback,
        Consumer<Long> cacheStoreDurationCallback,
        Consumer<Boolean> cacheHitRateCallback,
        Consumer<Long> fileFetchDurationCallback
    ) {
        this.time = time;
        this.objectKeyCreator = objectKeyCreator;
        this.keyAlignment = keyAlignment;
        this.cache = cache;
        this.objectFetcher = objectFetcher;
        this.dataExecutor = dataExecutor;
        this.batchCoordinates = batchCoordinates;
        this.fetchPlanDurationCallback = fetchPlanDurationCallback;
        this.cacheQueryDurationCallback = cacheQueryDurationCallback;
        this.cacheStoreDurationCallback = cacheStoreDurationCallback;
        this.cacheHitRateCallback = cacheHitRateCallback;
        this.fileFetchDurationCallback = fileFetchDurationCallback;
    }

    private List<Future<FileExtent>> doWork(final Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        final List<Callable<FileExtent>> jobs = planJobs(batchCoordinates);
        return submitAll(jobs);
    }

    private List<Callable<FileExtent>> planJobs(Map<TopicIdPartition, FindBatchResponse> batchCoordinates) {
        return batchCoordinates.values().stream()
            .filter(findBatch -> findBatch.errors() == Errors.NONE)
            .map(FindBatchResponse::batches)
            .flatMap(List::stream)
            // Merge batch requests
            .collect(Collectors.groupingBy(BatchInfo::objectKey, Collectors.mapping(b -> b, Collectors.toList())))
            .entrySet()
            .stream()
            .map(e -> {
                // IDEA: let's have a single job that gets the whole range a single GET request, but caches per topic partition.
                // the fan-out can be large as N cache put, but could be down to a single put if fetches are for a single partition.
                // I don't see much value on the alignment strategy, as the cache is per topic partition offset pointing to a batch.
                    return buildCacheFetchJob(e.getKey(), e.getValue()); // Let's redefine this, the general batch can be defined inside the cache job
//                    return keyAlignment.align(ranges)
//                        .stream()
//                        .map(byteRange -> buildCacheFetchJob(e.getKey(), batches, byteRange));
                }
            )
            .collect(Collectors.toList());
    }

    private CacheFetchJob buildCacheFetchJob(
        final String objectKey,
        final List<ByteRange> ranges,
        final ByteRange byteRange
    ) {
        return new CacheFetchJob(
            cache,
            objectFetcher,
            objectKeyCreator.from(objectKey),
            byteRange,
            time,
            cacheQueryDurationCallback,
            cacheStoreDurationCallback,
            cacheHitRateCallback,
            fileFetchDurationCallback
        );
    }

    private List<Future<FileExtent>> submitAll(List<Callable<FileExtent>> jobs) {
        return jobs.stream()
            .map(dataExecutor::submit)
            .collect(Collectors.toList());
    }

    @Override
    public List<Future<FileExtent>> get() {
        return TimeUtils.measureDurationMsSupplier(time, () -> doWork(batchCoordinates), fetchPlanDurationCallback);
    }
}
