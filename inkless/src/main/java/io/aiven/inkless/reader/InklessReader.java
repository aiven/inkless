package io.aiven.inkless.reader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import io.aiven.inkless.common.BatchCoordinates;
import io.aiven.inkless.common.BatchInfo;
import io.aiven.inkless.common.FindBatchRequest;
import io.aiven.inkless.common.FindBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class InklessReader {
    private static final Logger logger = LoggerFactory.getLogger(InklessReader.class);

    private final ControlPlane controlPlane;
    private final FetchPlanner fetchPlanner = new FetchPlanner();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ObjectFetcher objectFetcher;

    public InklessReader(final ControlPlane controlPlane, final ObjectFetcher objectFetcher) {
        this.controlPlane = controlPlane;
        this.objectFetcher = objectFetcher;
    }

    public CompletableFuture<Map<TopicPartition, FetchPartitionData>> read(
        final boolean minOneMessage,
        final int maxBytes,
        final Iterable<Tuple2<TopicIdPartition, FetchRequest.PartitionData>> fetchInfos
    ) {
        // TODO This is unoptimized and untested, needs refactoring, etc.

        final List<FindBatchRequest> findBatchRequests = new ArrayList<>();
        for (final Tuple2<TopicIdPartition, FetchRequest.PartitionData> fetchInfo : fetchInfos) {
            findBatchRequests.add(new FindBatchRequest(
                fetchInfo._1.topicPartition(), fetchInfo._2.fetchOffset, fetchInfo._2.maxBytes
            ));
        }
        final Map<TopicPartition, FindBatchResponse> batchesToFetch =
            controlPlane.findBatchesForFetch(findBatchRequests, minOneMessage, maxBytes);
        return fetchBatches(batchesToFetch).thenApply(fetchedBatches -> {
            final Map<TopicPartition, FetchPartitionData> result = new HashMap<>();

            for (final Map.Entry<TopicPartition, FindBatchResponse> entry : batchesToFetch.entrySet()) {
                final TopicPartition tp = entry.getKey();
                final FindBatchResponse findBatchResponse = entry.getValue();
                final FetchPartitionData fetchPartitionData;
                if (findBatchResponse.batches.isEmpty()) {
                    fetchPartitionData = new FetchPartitionData(
                        Errors.NONE,
                        findBatchResponse.highWatermark,
                        0L,  // TODO insert correct
                        MemoryRecords.EMPTY,
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false
                    );
                } else {
                    final List<BatchWithData> batchWithData = fetchedBatches.get(tp);
                    final Records records = MemoryRecords.readableRecords(assembleRecords(batchWithData));
                    // LSO == high watermark in Inkless TODO check this
                    long lastStableOffset = findBatchResponse.highWatermark;
                    fetchPartitionData = new FetchPartitionData(
                        Errors.NONE,
                        findBatchResponse.highWatermark,
                        batchWithData.get(0).batchCoordinates.offset,
                        records,
                        Optional.empty(),
                        OptionalLong.of(lastStableOffset),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false
                    );
                }

                result.put(tp, fetchPartitionData);
            }

            return result;
        });
    }

    private CompletableFuture <Map<TopicPartition, List<BatchWithData>>> fetchBatches(
        final Map<TopicPartition, FindBatchResponse> batches
    ) {
        final Map<TopicPartition, List<BatchInfo>> planRequest = new HashMap<>();
        batches.forEach((k, v) -> planRequest.put(k, v.batches));
        final List<FileFetch> plan = fetchPlanner.plan(planRequest);
        final ConcurrentHashMap<BatchCoordinates, CompletableFuture<BatchWithData>> fetchedBatches = executeFetch(plan);

        return CompletableFuture.allOf(fetchedBatches.values().toArray(new CompletableFuture[0]))
            .thenApplyAsync(ignore -> {
                try {
                    return groupByTopicPartitionAndOrder(fetchedBatches);
                } catch (final ExecutionException | InterruptedException e) {
                    // TODO handle
                    logger.error("Error downloading", e);
                    throw new RuntimeException(e);
                }
            }, executor);
    }

    private ConcurrentHashMap<BatchCoordinates, CompletableFuture<BatchWithData>> executeFetch(
        final List<FileFetch> fetchPlan
    ) {
        final ConcurrentHashMap<BatchCoordinates, CompletableFuture<BatchWithData>> result = new ConcurrentHashMap<>();
        for (final FileFetch fileFetch : fetchPlan) {
            final CompletableFuture<byte[]> fetchFuture = CompletableFuture.supplyAsync(
                () -> objectFetcher.fetch(fileFetch.filePath, fileFetch.startOffset, fileFetch.rangeSize),
                executor
            );
            for (final BatchInfo batch : fileFetch.batches) {
                fetchFuture.whenComplete((content, exception) -> {
                    final CompletableFuture<BatchWithData> resultFuture = result.get(batch.coordinates);
                    final ByteBuffer buffer = ByteBuffer.wrap(content, batch.byteOffset, batch.byteSize);
                    resultFuture.complete(new BatchWithData(batch.coordinates, buffer));
                });
            }
        }
        return result;
    }

    private Map<TopicPartition, List<BatchWithData>> groupByTopicPartitionAndOrder(
        final ConcurrentHashMap<BatchCoordinates, CompletableFuture<BatchWithData>> fetches
    ) throws ExecutionException, InterruptedException {
        final Map<TopicPartition, List<BatchWithData>> result = new HashMap<>();
        for (final Map.Entry<BatchCoordinates, CompletableFuture<BatchWithData>> entry : fetches.entrySet()) {
            result.computeIfAbsent(entry.getKey().topicPartition, ignore -> new ArrayList<>())
                .add(entry.getValue().get());
        }
        result.values().forEach(batches -> batches.sort(Comparator.comparing(b -> b.batchCoordinates.offset)));
        return result;
    }

    private ByteBuffer assembleRecords(final List<BatchWithData> batches) {
        final int totalBytes = batches.stream().mapToInt(b -> b.byteBuffer.limit()).sum();
        final ByteBuffer finalBuffer = ByteBuffer.allocate(totalBytes);
        batches.forEach(b -> finalBuffer.put(b.byteBuffer));
        return finalBuffer;
    }
}
