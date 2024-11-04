package io.aiven.inkless.reader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

import io.aiven.inkless.common.BatchCoordinates;
import io.aiven.inkless.common.BatchInfo;

public class Fetcher {
    private final FetchPlanner fetchPlanner = new FetchPlanner();

    public Map<BatchCoordinates, CompletableFuture<ByteBuffer>> fetchBatches(
        final Map<TopicPartition, List<BatchInfo>> batches
    ) {
        final ConcurrentHashMap<BatchCoordinates, CompletableFuture<ByteBuffer>> result = new ConcurrentHashMap<>();
        for (final Map.Entry<TopicPartition, List<BatchInfo>> entry : batches.entrySet()) {
            for (final BatchInfo batchInfo : entry.getValue()) {
                result.put(batchInfo.coordinates, new CompletableFuture<>());
            }
        }

        final List<FileFetch> fetchPlan = fetchPlanner.plan(batches);
        for (final FileFetch fetch : fetchPlan) {
//            final CompletableFuture<byte[]> fetchFuture =
//                fetchFile(fetch.filePath, fetch.startOffset, fetch.rangeSize);
//            for (final BatchInfo batch : fetch.batches) {
//                fetchFuture.whenComplete((content, exception) -> {
//                    final CompletableFuture<ByteBuffer> resultFuture = result.get(batch.coordinates);
//                    ByteBuffer buffer = ByteBuffer.wrap(content, batch.byteOffset, batch.byteSize);
//                    resultFuture.complete(buffer);
//                });
//            }
        }

        return result;
    }
}
