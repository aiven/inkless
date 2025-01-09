// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.generated.FileExtent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchCompleterJobTest {
    static final String OBJECT_KEY_PREFIX = "prefix/";
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = PlainObjectKey.creator(OBJECT_KEY_PREFIX);
    static final String OBJECT_KEY_A_MAIN_PART = "a";
    static final String OBJECT_KEY_B_MAIN_PART = "b";
    static final ObjectKey OBJECT_KEY_A = new PlainObjectKey(OBJECT_KEY_PREFIX, OBJECT_KEY_A_MAIN_PART);
    static final ObjectKey OBJECT_KEY_B = new PlainObjectKey(OBJECT_KEY_PREFIX, OBJECT_KEY_B_MAIN_PART);

    Uuid topicId = Uuid.randomUuid();
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "inkless-topic");

    @Test
    public void testEmptyFetch() {
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            Collections.emptyMap(),
            CompletableFuture.completedFuture(Collections.emptyMap()),
            CompletableFuture.completedFuture(Collections.emptyList()),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFetchWithoutCoordinates() {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            CompletableFuture.completedFuture(Collections.emptyMap()),
            CompletableFuture.completedFuture(Collections.emptyList()),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(Errors.KAFKA_STORAGE_ERROR, data.error);
    }

    @Test
    public void testFetchWithoutBatches() {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        int highWatermark = 0;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(Collections.emptyList(), logStartOffset, highWatermark)
        );
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            CompletableFuture.completedFuture(coordinates),
            CompletableFuture.completedFuture(Collections.emptyList()),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(Errors.NONE, data.error);
        assertEquals(MemoryRecords.EMPTY, data.records);
        assertEquals(logStartOffset, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }

    @Test
    public void testFetchWithoutFiles() {
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                BatchInfo.of(OBJECT_KEY_A_MAIN_PART, 0, 10, 0, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)
            ), logStartOffset, highWatermark)
        );
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            CompletableFuture.completedFuture(coordinates),
            CompletableFuture.completedFuture(Collections.emptyList()),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(Errors.KAFKA_STORAGE_ERROR, data.error);
    }

    @Test
    public void testFetchSingleFile() {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                BatchInfo.of(OBJECT_KEY_A_MAIN_PART, 0, records.sizeInBytes(), 0, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)
            ), logStartOffset, highWatermark)
        );

        List<Future<FileExtent>> files = Stream.of(
            FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, records.sizeInBytes()), records.buffer())
        ).map(CompletableFuture::completedFuture).collect(Collectors.toList());
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            CompletableFuture.completedFuture(coordinates),
            CompletableFuture.completedFuture(files),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(records.sizeInBytes(), data.records.sizeInBytes());
        assertEquals(logStartOffset, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }


    @Test
    public void testFetchMultipleFiles() {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                BatchInfo.of(OBJECT_KEY_A_MAIN_PART, 0, records.sizeInBytes(), 0, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME),
                BatchInfo.of(OBJECT_KEY_B_MAIN_PART, 0, records.sizeInBytes(), 0, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)
            ), logStartOffset, highWatermark)
        );

        List<Future<FileExtent>> files = Stream.of(
            FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, records.sizeInBytes()), records.buffer()),
            FileFetchJob.createFileExtent(OBJECT_KEY_B, new ByteRange(0, records.sizeInBytes()), records.buffer())
        ).map(CompletableFuture::completedFuture).collect(Collectors.toList());
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            CompletableFuture.completedFuture(coordinates),
            CompletableFuture.completedFuture(files),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(2 * records.sizeInBytes(), data.records.sizeInBytes());
        assertEquals(logStartOffset, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }

    @Test
    public void testFetchMultipleBatches() {
        byte[] firstValue = {1};
        byte[] secondValue = {2};
        MemoryRecords recordsA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(firstValue));
        MemoryRecords recordsB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(secondValue));

        int totalSize = recordsA.sizeInBytes() + recordsB.sizeInBytes();
        ByteBuffer concatenatedBuffer = ByteBuffer.allocate(totalSize);
        concatenatedBuffer.put(recordsA.buffer());
        concatenatedBuffer.put(recordsB.buffer());

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 10L;
        int highWatermark = 2;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                BatchInfo.of(OBJECT_KEY_A_MAIN_PART, 0, recordsA.sizeInBytes(), 0, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME),
                BatchInfo.of(OBJECT_KEY_A_MAIN_PART, recordsA.sizeInBytes(), recordsB.sizeInBytes(), 1, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)
            ), logStartOffset, highWatermark)
        );

        List<Future<FileExtent>> files = Stream.of(
            FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, totalSize), concatenatedBuffer)
        ).map(CompletableFuture::completedFuture).collect(Collectors.toList());
        FetchCompleterJob job = new FetchCompleterJob(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            CompletableFuture.completedFuture(coordinates),
            CompletableFuture.completedFuture(files),
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(totalSize, data.records.sizeInBytes());
        assertEquals(logStartOffset, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
        Iterator<Record> iterator = data.records.records().iterator();
        assertTrue(iterator.hasNext());
        assertEquals(ByteBuffer.wrap(firstValue), iterator.next().value());
        assertTrue(iterator.hasNext());
        assertEquals(ByteBuffer.wrap(secondValue), iterator.next().value());
    }
}
