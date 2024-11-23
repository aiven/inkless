// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchCompleterJobTest {

    Uuid topicId = Uuid.randomUuid();
    ObjectKey objectA = new PlainObjectKey("a", "a");
    ObjectKey objectB = new PlainObjectKey("b", "b");
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "inkless-topic");

    @Test
    public void testEmptyFetch() {
        FetchCompleterJob job = new FetchCompleterJob(Collections.emptyMap(), CompletableFuture.completedFuture(Collections.emptyMap()), CompletableFuture.completedFuture(Collections.emptyList()));
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFetchWithoutCoordinates() {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        FetchCompleterJob job = new FetchCompleterJob(fetchInfos, CompletableFuture.completedFuture(Collections.emptyMap()), CompletableFuture.completedFuture(Collections.emptyList()));
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(Errors.KAFKA_STORAGE_ERROR, data.error);
    }

    @Test
    public void testFetchWithoutBatches() {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStart = 0;
        int highWatermark = 0;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(Collections.emptyList(), logStart, highWatermark)
        );
        FetchCompleterJob job = new FetchCompleterJob(fetchInfos, CompletableFuture.completedFuture(coordinates), CompletableFuture.completedFuture(Collections.emptyList()));
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(Errors.NONE, data.error);
        assertEquals(MemoryRecords.EMPTY, data.records);
        assertEquals(logStart, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }

    @Test
    public void testFetchWithoutFiles() {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStart = 0;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, 10, 0, 1)
                ), logStart, highWatermark)
        );
        FetchCompleterJob job = new FetchCompleterJob(fetchInfos, CompletableFuture.completedFuture(coordinates), CompletableFuture.completedFuture(Collections.emptyList()));
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(Errors.KAFKA_STORAGE_ERROR, data.error);
    }

    @Test
    public void testSingleFetch() {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStart = 0;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, records.sizeInBytes(), 0, 1),
                        new BatchInfo(objectB, 0, records.sizeInBytes(), 0, 1)
                ), logStart, highWatermark)
        );

        List<Future<FetchedFile>> files = Stream.of(
                new FetchedFile(objectA, new ByteRange(0, records.sizeInBytes()), records.buffer()),
                new FetchedFile(objectB, new ByteRange(0, records.sizeInBytes()), records.buffer())
        ).map(CompletableFuture::completedFuture).collect(Collectors.toList());
        FetchCompleterJob job = new FetchCompleterJob(fetchInfos, CompletableFuture.completedFuture(coordinates), CompletableFuture.completedFuture(files));
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(2 * records.sizeInBytes(), data.records.sizeInBytes());
        assertEquals(logStart, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }


    @Test
    public void testMultiFetch() {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStart = 0;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
                partition0, FindBatchResponse.success(List.of(
                        new BatchInfo(objectA, 0, records.sizeInBytes(), 0, 1)
                ), logStart, highWatermark)
        );

        List<Future<FetchedFile>> files = Stream.of(
                new FetchedFile(objectA, new ByteRange(0, records.sizeInBytes()), records.buffer())
        ).map(CompletableFuture::completedFuture).collect(Collectors.toList());
        FetchCompleterJob job = new FetchCompleterJob(fetchInfos, CompletableFuture.completedFuture(coordinates), CompletableFuture.completedFuture(files));
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(records.sizeInBytes(), data.records.sizeInBytes());
        assertEquals(logStart, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }
}
