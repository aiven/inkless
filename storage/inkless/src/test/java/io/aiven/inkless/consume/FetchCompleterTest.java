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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.FindBatchResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchCompleterTest {
    static final String OBJECT_KEY_PREFIX = "prefix";
    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator(OBJECT_KEY_PREFIX, false);
    static final String OBJECT_KEY_A_MAIN_PART = "a";
    static final String OBJECT_KEY_B_MAIN_PART = "b";
    static final ObjectKey OBJECT_KEY_A = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_KEY_A_MAIN_PART);
    static final ObjectKey OBJECT_KEY_B = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_KEY_B_MAIN_PART);

    Uuid topicId = Uuid.randomUuid();
    TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "diskless-topic");

    @Test
    public void testEmptyFetch() {
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyList(),
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
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            Collections.emptyMap(),
            Collections.emptyList(),
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
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            Collections.emptyList(),
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
                new BatchInfo(1L, OBJECT_KEY_A_MAIN_PART, BatchMetadata.of(partition0, 0, 10, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            Collections.emptyList(),
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
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, records.sizeInBytes()), records.buffer()))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
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
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, records.sizeInBytes()), records.buffer())),
            new FileExtentResult.Success(FileFetchJob.createFileExtent(OBJECT_KEY_B, new ByteRange(0, records.sizeInBytes()), records.buffer()))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(2 * records.sizeInBytes(), data.records.sizeInBytes());
        assertEquals(logStartOffset, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
    }

    @Test
    public void testFetchMultipleFilesForSameBatch() {
        var blockSize = 4;
        byte[] firstValue = {1};
        byte[] secondValue = {2};
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(firstValue), new SimpleRecord(secondValue));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        var fixedAlignment = new FixedBlockAlignment(blockSize);
        var ranges = fixedAlignment.align(List.of(new ByteRange(0, records.sizeInBytes())));

        var fileExtents = new ArrayList<FileExtentResult>();
        for (ByteRange range : ranges) {
            var startOffset = Math.toIntExact(range.offset());
            var length = Math.min(blockSize, records.sizeInBytes() - startOffset);
            var endOffset = startOffset + length;
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(records.buffer().duplicate().position(startOffset).limit(endOffset).slice());
            fileExtents.add(new FileExtentResult.Success(FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertEquals(records.sizeInBytes(), data.records.sizeInBytes());
        assertEquals(logStartOffset, data.logStartOffset);
        assertEquals(highWatermark, data.highWatermark);
        Iterator<Record> iterator = data.records.records().iterator();
        assertTrue(iterator.hasNext());
        assertEquals(ByteBuffer.wrap(firstValue), iterator.next().value());
        assertTrue(iterator.hasNext());
        assertEquals(ByteBuffer.wrap(secondValue), iterator.next().value());
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
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, recordsA.sizeInBytes(), 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, recordsA.sizeInBytes(), recordsB.sizeInBytes(), 1, 1, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, totalSize), concatenatedBuffer))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
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

    @Test
    public void testFetchMultipleFilesForMultipleBatches() {
        var blockSize = 16;
        byte[] firstValue = {1};
        byte[] secondValue = {2};
        MemoryRecords recordsBatch1 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(firstValue));
        MemoryRecords recordsBatch2 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(secondValue));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 10000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 2;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, recordsBatch1.sizeInBytes(),
                        0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, recordsBatch1.sizeInBytes(),
                        recordsBatch2.sizeInBytes(), 1, 1, logAppendTimestamp, maxBatchTimestamp,
                        TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );
        int totalSize = recordsBatch1.sizeInBytes() + recordsBatch2.sizeInBytes();
        ByteBuffer concatenatedBuffer = ByteBuffer.allocate(totalSize);
        concatenatedBuffer.put(recordsBatch1.buffer());
        concatenatedBuffer.put(recordsBatch2.buffer());
        var fixedAlignment = new FixedBlockAlignment(blockSize);
        var ranges = fixedAlignment.align(List.of(new ByteRange(0, totalSize)));

        var fileExtents = new ArrayList<FileExtentResult>();
        for (ByteRange range : ranges) {
            var startOffset = Math.toIntExact(range.offset());
            var length = Math.min(blockSize, totalSize - startOffset);
            var endOffset = startOffset + length;
            ByteBuffer copy = ByteBuffer.allocate(blockSize);
            copy.put(concatenatedBuffer.duplicate().position(startOffset).limit(endOffset).slice());
            fileExtents.add(new FileExtentResult.Success(FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
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
