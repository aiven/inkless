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
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Mock
    InklessFetchMetrics metrics;

    @Test
    public void testEmptyFetch() {
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyList(),
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        assertThat(result).isEmpty();
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
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
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
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.NONE);
        assertThat(data.records).isEqualTo(MemoryRecords.EMPTY);
        assertThat(data.logStartOffset).isEqualTo(logStartOffset);
        assertThat(data.highWatermark).isEqualTo(highWatermark);
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
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
        verify(metrics, times(1)).recordPartitionStorageError();
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

        final ByteRange range = new ByteRange(0, records.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, records.buffer()))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.records.sizeInBytes()).isEqualTo(records.sizeInBytes());
        assertThat(data.logStartOffset).isEqualTo(logStartOffset);
        assertThat(data.highWatermark).isEqualTo(highWatermark);
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

        final ByteRange range = new ByteRange(0, records.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, records.buffer())),
            new FileExtentResult.Success(OBJECT_KEY_B, range, FileFetchJob.createFileExtent(OBJECT_KEY_B, range, records.buffer()))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.records.sizeInBytes()).isEqualTo(2 * records.sizeInBytes());
        assertThat(data.logStartOffset).isEqualTo(logStartOffset);
        assertThat(data.highWatermark).isEqualTo(highWatermark);
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
            fileExtents.add(new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.records.sizeInBytes()).isEqualTo(records.sizeInBytes());
                assertThat(d.logStartOffset).isEqualTo(logStartOffset);
                assertThat(d.highWatermark).isEqualTo(highWatermark);
            });
        
        Iterator<Record> iterator = data.records.records().iterator();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(firstValue));
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(secondValue));
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

        final ByteRange range = new ByteRange(0, totalSize);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, concatenatedBuffer))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.records.sizeInBytes()).isEqualTo(totalSize);
                assertThat(d.logStartOffset).isEqualTo(logStartOffset);
                assertThat(d.highWatermark).isEqualTo(highWatermark);
            });
        
        Iterator<Record> iterator = data.records.records().iterator();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(firstValue));
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(secondValue));
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
            fileExtents.add(new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.records.sizeInBytes()).isEqualTo(totalSize);
                assertThat(d.logStartOffset).isEqualTo(logStartOffset);
                assertThat(d.highWatermark).isEqualTo(highWatermark);
            });
        
        Iterator<Record> iterator = data.records.records().iterator();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(firstValue));
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(secondValue));
    }

    @Test
    public void testMultipleExtentsForSameObjectReturnedInOrder() {
        // Test that when multiple file extents for the same object all succeed,
        // both are returned in order (sorted by range offset)
        MemoryRecords records1 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("data1".getBytes()));
        MemoryRecords records2 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("data2".getBytes()));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 2;

        final int batch1Size = records1.sizeInBytes();
        final int batch2Size = records2.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batch1Size, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, batch1Size, batch2Size, 1, 1, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // Both ranges succeed - should return both in order
        final ByteRange range1 = new ByteRange(0, batch1Size);
        final ByteRange range2 = new ByteRange(batch1Size, batch2Size);
        // Note: order in list doesn't matter - groupFileData sorts by range offset
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range2, FileFetchJob.createFileExtent(OBJECT_KEY_A, range2, records2.buffer())),
            new FileExtentResult.Success(OBJECT_KEY_A, range1, FileFetchJob.createFileExtent(OBJECT_KEY_A, range1, records1.buffer()))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should succeed with both batches in order
        assertThat(data.error).isEqualTo(Errors.NONE);
        assertThat(data.records.sizeInBytes()).isEqualTo(batch1Size + batch2Size);
    }

    @Test
    public void testFirstFailureStopsProcessingNoDataReturned() {
        // Test that when the first extent for an object key fails, processing stops
        // and no data is returned (to avoid gaps)
        MemoryRecords records1 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("data1".getBytes()));
        MemoryRecords records2 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("data2".getBytes()));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 2;

        final int batch1Size = records1.sizeInBytes();
        final int batch2Size = records2.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batch1Size, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, batch1Size, batch2Size, 1, 1, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // First range fails, second succeeds - should stop at failure and not return second
        final ByteRange range1 = new ByteRange(0, batch1Size);
        final ByteRange range2 = new ByteRange(batch1Size, batch2Size);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Failure(OBJECT_KEY_A, range1, new RuntimeException("Fetch failed for first range")),
            new FileExtentResult.Success(OBJECT_KEY_A, range2, FileFetchJob.createFileExtent(OBJECT_KEY_A, range2, records2.buffer()))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should return KAFKA_STORAGE_ERROR because no data is available
        // (first range failed, so we stop and don't process the second range to avoid gaps)
        assertThat(data.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
        assertThat(data.records).isEqualTo(MemoryRecords.EMPTY);
    }

    @Test
    public void testMultipleBatchesPartialFailureReturnsCompleteBatches() {
        // Test that when we have multiple separate batches and one fails, we return only complete batches.
        // This is different from a single batch spanning multiple extents - here batch 1 and batch 2 are separate.
        // If batch 1 is complete and batch 2 fails, we return batch 1 (complete batches are useful).
        MemoryRecords records1 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("data1".getBytes()));
        MemoryRecords records2 = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("data2".getBytes()));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        int highWatermark = 2;

        final int batch1Size = records1.sizeInBytes();
        final int batch2Size = records2.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batch1Size, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, batch1Size, batch2Size, 1, 1, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // Batch 1 extent succeeds, batch 2 extent fails - should return batch 1 only (complete batch)
        final ByteRange range1 = new ByteRange(0, batch1Size);
        final ByteRange range2 = new ByteRange(batch1Size, batch2Size);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range1, FileFetchJob.createFileExtent(OBJECT_KEY_A, range1, records1.buffer())),
            new FileExtentResult.Failure(OBJECT_KEY_A, range2, new RuntimeException("Fetch failed for second range"))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should return batch 1 only (complete batch) since batch 2 is incomplete
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.error).isEqualTo(Errors.NONE);
                assertThat(d.records.sizeInBytes()).isEqualTo(batch1Size);
            });
    }

    @Test
    public void testSingleBatchWithMultipleExtentsAllSucceed() {
        // Test that a single batch spanning multiple extents succeeds when all extents are present
        var blockSize = 16;
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

        final int batchSize = records.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batchSize, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // Split batch into multiple extents (simulating block alignment)
        var fixedAlignment = new FixedBlockAlignment(blockSize);
        var ranges = fixedAlignment.align(List.of(new ByteRange(0, batchSize)));

        var fileExtents = new ArrayList<FileExtentResult>();
        for (ByteRange range : ranges) {
            var startOffset = Math.toIntExact(range.offset());
            var length = Math.min(blockSize, batchSize - startOffset);
            var endOffset = startOffset + length;
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(records.buffer().duplicate().position(startOffset).limit(endOffset).slice());
            fileExtents.add(new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should succeed with complete batch
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.error).isEqualTo(Errors.NONE);
                assertThat(d.records.sizeInBytes()).isEqualTo(batchSize);
            });
        
        Iterator<Record> iterator = data.records.records().iterator();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(firstValue));
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().value()).isEqualTo(ByteBuffer.wrap(secondValue));
    }

    @Test
    public void testSingleBatchWithMissingMiddleExtentFails() {
        // Test that a single batch spanning multiple extents fails when a middle extent is missing
        // Incomplete batches are useless to consumers, so we fail the entire batch
        var blockSize = 16;
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

        final int batchSize = records.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batchSize, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // Split batch into multiple extents, but simulate missing middle extent
        var fixedAlignment = new FixedBlockAlignment(blockSize);
        var ranges = fixedAlignment.align(List.of(new ByteRange(0, batchSize)));

        var fileExtents = new ArrayList<FileExtentResult>();
        boolean skipMiddle = true;
        for (ByteRange range : ranges) {
            if (skipMiddle && range.offset() > 0 && range.offset() < batchSize / 2) {
                // Simulate missing middle extent - don't add it
                skipMiddle = false;
                continue;
            }
            var startOffset = Math.toIntExact(range.offset());
            var length = Math.min(blockSize, batchSize - startOffset);
            var endOffset = startOffset + length;
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(records.buffer().duplicate().position(startOffset).limit(endOffset).slice());
            fileExtents.add(new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should fail because batch is incomplete (missing middle extent)
        // Incomplete batches are useless to consumers, so we return KAFKA_STORAGE_ERROR
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
                assertThat(d.records).isEqualTo(MemoryRecords.EMPTY);
            });
    }

    @Test
    public void testSingleBatchWithFailedMiddleExtentFails() {
        // Test that a single batch spanning multiple extents fails when a middle extent fetch fails
        // groupFileData() stops at first failure, so only extents before failure are included
        // The batch is incomplete and should fail
        var blockSize = 16;
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

        final int batchSize = records.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batchSize, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // Split batch into multiple extents, with middle extent failing
        var fixedAlignment = new FixedBlockAlignment(blockSize);
        var ranges = fixedAlignment.align(List.of(new ByteRange(0, batchSize)));

        var fileExtents = new ArrayList<FileExtentResult>();
        boolean failMiddle = true;
        for (ByteRange range : ranges) {
            if (failMiddle && range.offset() > 0 && range.offset() < batchSize / 2) {
                // Simulate failed middle extent
                fileExtents.add(new FileExtentResult.Failure(OBJECT_KEY_A, range, new RuntimeException("Fetch failed for middle extent")));
                failMiddle = false;
                // groupFileData() stops at first failure, so subsequent extents won't be processed
                continue;
            }
            var startOffset = Math.toIntExact(range.offset());
            var length = Math.min(blockSize, batchSize - startOffset);
            var endOffset = startOffset + length;
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(records.buffer().duplicate().position(startOffset).limit(endOffset).slice());
            fileExtents.add(new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            fileExtents,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should fail because batch is incomplete (middle extent failed, groupFileData stops at first failure)
        // Incomplete batches are useless to consumers, so we return KAFKA_STORAGE_ERROR
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
                assertThat(d.records).isEqualTo(MemoryRecords.EMPTY);
            });
    }

    @Test
    public void testSingleBatchWithGapInExtentsFails() {
        // Test that a single batch fails when extents have a gap (not contiguous)
        // This shouldn't happen with groupFileData() which stops at first failure,
        // but we validate it anyway to be safe
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

        final int batchSize = records.sizeInBytes();
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, batchSize, 0, 0, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        // Create extents with a gap (first and last, missing middle)
        final int firstExtentSize = batchSize / 3;
        final int gapSize = batchSize / 3;
        final int lastExtentSize = batchSize - firstExtentSize - gapSize;

        ByteBuffer firstExtent = ByteBuffer.allocate(firstExtentSize);
        firstExtent.put(records.buffer().duplicate().position(0).limit(firstExtentSize).slice());

        ByteBuffer lastExtent = ByteBuffer.allocate(lastExtentSize);
        lastExtent.put(records.buffer().duplicate().position(firstExtentSize + gapSize).limit(batchSize).slice());

        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, new ByteRange(0, firstExtentSize), 
                FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(0, firstExtentSize), firstExtent)),
            // Gap: missing extent from firstExtentSize to firstExtentSize + gapSize
            new FileExtentResult.Success(OBJECT_KEY_A, new ByteRange(firstExtentSize + gapSize, lastExtentSize), 
                FileFetchJob.createFileExtent(OBJECT_KEY_A, new ByteRange(firstExtentSize + gapSize, lastExtentSize), lastExtent))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        // Should fail because batch has a gap (not contiguous)
        // Incomplete batches are useless to consumers, so we return KAFKA_STORAGE_ERROR
        assertThat(data)
            .satisfies(d -> {
                assertThat(d.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
                assertThat(d.records).isEqualTo(MemoryRecords.EMPTY);
            });
    }

    @Test
    public void testFetchCoalescedRunRelocatesOffsets() {
        // A coalesced run (commit_file_v2): a SINGLE BatchInfo whose byte range contains three
        // byte-contiguous physical batches. The control plane assigned the run base offset 5, so the
        // physical batches must be relocated to absolute offsets 5,6 | 7 | 8,9.
        byte[] v0 = {0};
        byte[] v1 = {1};
        byte[] v2 = {2};
        byte[] v3 = {3};
        byte[] v4 = {4};
        // Batch A: 2 records, Batch B: 1 record, Batch C: 2 records. Each stored with base offset 0.
        MemoryRecords batchA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v0), new SimpleRecord(v1));
        MemoryRecords batchB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v2));
        MemoryRecords batchC = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v3), new SimpleRecord(v4));

        final int sizeA = batchA.sizeInBytes();
        final int sizeB = batchB.sizeInBytes();
        final int sizeC = batchC.sizeInBytes();
        final int totalSize = sizeA + sizeB + sizeC;
        ByteBuffer concatenated = ByteBuffer.allocate(totalSize);
        concatenated.put(batchA.buffer());
        concatenated.put(batchB.buffer());
        concatenated.put(batchC.buffer());

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 5, 0, 10000, Optional.empty())
        );
        int logStartOffset = 0;
        long logAppendTimestamp = 10L;
        long maxBatchTimestamp = 20L;
        long runBaseOffset = 5L;
        long runLastOffset = 9L; // 5 records total -> last offset 9
        int highWatermark = 10;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                // ONE BatchInfo spanning the whole 3-batch run.
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(
                    partition0, 0, totalSize, runBaseOffset, runLastOffset, logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        final ByteRange range = new ByteRange(0, totalSize);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, concatenated))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files, durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.NONE);
        assertThat(data.records.sizeInBytes()).isEqualTo(totalSize);

        // Each physical record must carry its correctly relocated absolute offset.
        List<Long> offsets = new ArrayList<>();
        List<ByteBuffer> values = new ArrayList<>();
        for (Record r : data.records.records()) {
            offsets.add(r.offset());
            values.add(r.value());
        }
        assertThat(offsets).containsExactly(5L, 6L, 7L, 8L, 9L);
        assertThat(values).containsExactly(
            ByteBuffer.wrap(v0), ByteBuffer.wrap(v1), ByteBuffer.wrap(v2), ByteBuffer.wrap(v3), ByteBuffer.wrap(v4));

        // The three physical batches must keep their original boundaries (base offsets 5, 7, 8).
        List<Long> batchBaseOffsets = new ArrayList<>();
        List<Long> batchLastOffsets = new ArrayList<>();
        for (var b : data.records.batches()) {
            batchBaseOffsets.add(b.baseOffset());
            batchLastOffsets.add(b.lastOffset());
        }
        assertThat(batchBaseOffsets).containsExactly(5L, 7L, 8L);
        assertThat(batchLastOffsets).containsExactly(6L, 7L, 9L);
    }

    @Test
    public void testFetchCoalescedRunWithLogAppendTimeRelocatesOffsetsAndTimestamp() {
        // Same as testFetchCoalescedRunRelocatesOffsets but for LOG_APPEND_TIME, which exercises the
        // relocation walk's timestamp branch: every physical sub-batch must have its max timestamp
        // overwritten with the row's single logAppendTimestamp, in addition to offset relocation.
        byte[] v0 = {0};
        byte[] v1 = {1};
        byte[] v2 = {2};
        MemoryRecords batchA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v0), new SimpleRecord(v1));
        MemoryRecords batchB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v2));
        final int totalSize = batchA.sizeInBytes() + batchB.sizeInBytes();
        ByteBuffer concatenated = ByteBuffer.allocate(totalSize);
        concatenated.put(batchA.buffer());
        concatenated.put(batchB.buffer());

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 10000, Optional.empty())
        );
        long runBaseOffset = 7L;
        long runLastOffset = 9L; // 3 records -> 7,8,9
        long logAppendTimestamp = 4242L;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(
                    partition0, 0, totalSize, runBaseOffset, runLastOffset, logAppendTimestamp, 20L, TimestampType.LOG_APPEND_TIME))
            ), 0, 10)
        );

        final ByteRange range = new ByteRange(0, totalSize);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, concatenated))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files, durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.NONE);

        // Offsets relocated 7,8,9 and every physical batch reports the row's log-append timestamp.
        List<Long> offsets = new ArrayList<>();
        for (Record r : data.records.records()) {
            offsets.add(r.offset());
        }
        assertThat(offsets).containsExactly(7L, 8L, 9L);
        for (var b : data.records.batches()) {
            assertThat(b.timestampType()).isEqualTo(TimestampType.LOG_APPEND_TIME);
            assertThat(b.maxTimestamp()).isEqualTo(logAppendTimestamp);
        }
    }

    @Test
    public void testFetchCoalescedRunSplitAcrossExtentsRelocatesOffsets() {
        // A coalesced run whose byte range is split across multiple cache-aligned extents (the shape the
        // PG-backed integration test exercises with a small cache block). constructRecordsFromFile must
        // reassemble the full range from the extents before the relocation walk runs.
        final int blockSize = 16;
        byte[] v0 = {0};
        byte[] v1 = {1};
        byte[] v2 = {2};
        // Batch A: 2 records, Batch B: 1 record. Each stored with base offset 0.
        MemoryRecords batchA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v0), new SimpleRecord(v1));
        MemoryRecords batchB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(v2));
        final int totalSize = batchA.sizeInBytes() + batchB.sizeInBytes();
        ByteBuffer concatenated = ByteBuffer.allocate(totalSize);
        concatenated.put(batchA.buffer());
        concatenated.put(batchB.buffer());

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 10000, Optional.empty())
        );
        long runBaseOffset = 3L;
        long runLastOffset = 5L; // 3 records -> 3,4,5
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(
                    partition0, 0, totalSize, runBaseOffset, runLastOffset, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 6)
        );

        // Split the coalesced range into block-aligned extents.
        var ranges = new FixedBlockAlignment(blockSize).align(List.of(new ByteRange(0, totalSize)));
        var fileExtents = new ArrayList<FileExtentResult>();
        for (ByteRange range : ranges) {
            final int startOffset = Math.toIntExact(range.offset());
            final int length = Math.min(blockSize, totalSize - startOffset);
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(concatenated.duplicate().position(startOffset).limit(startOffset + length).slice());
            fileExtents.add(new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, copy)));
        }

        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, fileExtents, durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);

        assertThat(data.error).isEqualTo(Errors.NONE);
        List<Long> offsets = new ArrayList<>();
        List<ByteBuffer> values = new ArrayList<>();
        for (Record r : data.records.records()) {
            offsets.add(r.offset());
            values.add(r.value());
        }
        assertThat(offsets).containsExactly(3L, 4L, 5L);
        assertThat(values).containsExactly(ByteBuffer.wrap(v0), ByteBuffer.wrap(v1), ByteBuffer.wrap(v2));
    }

    @Test
    public void testFetchCoalescedRunWithMismatchedMetadataReturnsCorruptMessage() {
        // If the metadata's last offset disagrees with the record count in the bytes, we must not
        // serve incorrectly-offset data. The batch is dropped and the partition returns
        // CORRUPT_MESSAGE, isolated to this partition rather than aborting the whole fetch.
        MemoryRecords batchA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{0}));
        MemoryRecords batchB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{1}));
        final int totalSize = batchA.sizeInBytes() + batchB.sizeInBytes();
        ByteBuffer concatenated = ByteBuffer.allocate(totalSize);
        concatenated.put(batchA.buffer());
        concatenated.put(batchB.buffer());

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 10000, Optional.empty())
        );
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            // Says last offset 5 but the 2 records only span [0,1] -> mismatch.
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(
                    partition0, 0, totalSize, 0, 5, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 6)
        );
        final ByteRange range = new ByteRange(0, totalSize);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, concatenated))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files, durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        assertThat(result.get(partition0).error).isEqualTo(Errors.CORRUPT_MESSAGE);
        verify(metrics, times(1)).recordPartitionCorruptRecord();
        verify(metrics, never()).recordPartitionStorageError();
        verify(metrics, never()).recordPartitionPartialFetch();
    }

    @Test
    public void testCorruptedBatchReturnsCorruptMessageForPartition() {
        // A batch that passes size/range validation but fails CRC (ensureValid) must be isolated to
        // its own partition: the partition returns CORRUPT_MESSAGE (matching classic Kafka's
        // Errors.forException mapping) and the fetch as a whole does NOT throw (which would take down
        // every other partition in the same fetch).
        byte[] value = {1, 2, 3};
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(value));

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

        // Corrupt LAST_OFFSET_DELTA (covered by CRC): passes size validation, fails ensureValid CRC.
        ByteBuffer corruptedBuffer = records.buffer().duplicate();
        corruptedBuffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 999);

        final ByteRange range = new ByteRange(0, records.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, corruptedBuffer))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );

        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        assertThat(result.get(partition0).error).isEqualTo(Errors.CORRUPT_MESSAGE);
        verify(metrics, times(1)).recordPartitionCorruptRecord();
        verify(metrics, never()).recordPartitionStorageError();
        verify(metrics, never()).recordPartitionPartialFetch();
    }

    @Test
    public void testCorruptBatchInOnePartitionDoesNotAbortOthers() {
        // partition0 is valid; partition1's only batch fails CRC. The corrupt partition must not
        // abort the whole fetch: partition0 is served normally, partition1 gets CORRUPT_MESSAGE.
        TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "diskless-topic");

        MemoryRecords goodRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{1, 2, 3}));
        MemoryRecords badRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{4, 5, 6}));
        ByteBuffer corruptedBuffer = badRecords.buffer().duplicate();
        corruptedBuffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 999);

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty()),
            partition1, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, goodRecords.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 1),
            partition1, FindBatchResponse.success(List.of(
                new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, badRecords.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 1)
        );
        final ByteRange rangeA = new ByteRange(0, goodRecords.sizeInBytes());
        final ByteRange rangeB = new ByteRange(0, badRecords.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, rangeA, FileFetchJob.createFileExtent(OBJECT_KEY_A, rangeA, goodRecords.buffer())),
            new FileExtentResult.Success(OBJECT_KEY_B, rangeB, FileFetchJob.createFileExtent(OBJECT_KEY_B, rangeB, corruptedBuffer))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).error).isEqualTo(Errors.NONE);
        assertThat(result.get(partition0).records.sizeInBytes()).isEqualTo(goodRecords.sizeInBytes());
        assertThat(result.get(partition1).error).isEqualTo(Errors.CORRUPT_MESSAGE);
        verify(metrics, times(1)).recordPartitionCorruptRecord();
        verify(metrics, never()).recordPartitionStorageError();
    }

    @Test
    public void testInvalidBatchSizeReturnsCorruptMessageForPartition() {
        // A coordinate whose byteSize exceeds Integer.MAX_VALUE is a corrupt coordinate. It must be
        // classified in the extraction loop (CORRUPT_MESSAGE - "exceeds the valid size") and scoped
        // to this partition, keeping any valid prefix, while a sibling partition still serves.
        TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "diskless-topic");
        MemoryRecords goodRecords = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{1, 2, 3}));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty()),
            partition1, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        final long oversized = (long) Integer.MAX_VALUE + 1;
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, oversized, 0, 0, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 1),
            partition1, FindBatchResponse.success(List.of(
                new BatchInfo(2L, OBJECT_KEY_B.value(), BatchMetadata.of(partition1, 0, goodRecords.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 1)
        );
        final ByteRange rangeB = new ByteRange(0, goodRecords.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_B, rangeB, FileFetchJob.createFileExtent(OBJECT_KEY_B, rangeB, goodRecords.buffer()))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).error).isEqualTo(Errors.CORRUPT_MESSAGE);
        assertThat(result.get(partition1).error).isEqualTo(Errors.NONE);
        assertThat(result.get(partition1).records.sizeInBytes()).isEqualTo(goodRecords.sizeInBytes());
        verify(metrics, times(1)).recordPartitionCorruptRecord();
        verify(metrics, never()).recordPartitionStorageError();
    }

    @Test
    public void testUnexpectedExtractionErrorIsolatedBySafetyNet() {
        // An unclassified RuntimeException thrown mid-extraction (here: the ObjectKeyCreator blows up)
        // must be scoped to its partition by servePartition's safety net (KAFKA_STORAGE_ERROR),
        // never propagated to abort the whole fetch. get() must return normally.
        ObjectKeyCreator throwingCreator = mock(ObjectKeyCreator.class);
        when(throwingCreator.from(anyString())).thenThrow(new RuntimeException("boom"));

        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{1}));
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 1)
        );
        final ByteRange range = new ByteRange(0, records.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, records.buffer()))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(), throwingCreator, fetchInfos, coordinates, files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
        verify(metrics, times(1)).recordPartitionStorageError();
        verify(metrics, never()).recordPartitionCorruptRecord();
    }

    @Test
    public void testCorruptTrailingBatchServesValidPrefix() {
        // Two batches in one partition; the trailing batch fails CRC. The valid leading batch is
        // still served (partial response), and the partition is not failed wholesale.
        MemoryRecords recordsA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{1}));
        MemoryRecords recordsB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[]{2}));

        int totalSize = recordsA.sizeInBytes() + recordsB.sizeInBytes();
        ByteBuffer concatenated = ByteBuffer.allocate(totalSize);
        concatenated.put(recordsA.buffer());
        concatenated.put(recordsB.buffer());
        // Corrupt the trailing batch's CRC-covered LAST_OFFSET_DELTA (offset within batch B's region).
        concatenated.putInt(recordsA.sizeInBytes() + DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 999);

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(),
                    BatchMetadata.of(partition0, 0, recordsA.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(),
                    BatchMetadata.of(partition0, recordsA.sizeInBytes(), recordsB.sizeInBytes(), 0, 1, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 2)
        );
        final ByteRange range = new ByteRange(0, totalSize);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, concatenated))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).error).isEqualTo(Errors.NONE);
        assertThat(result.get(partition0).records.sizeInBytes()).isEqualTo(recordsA.sizeInBytes());
        verify(metrics, times(1)).recordPartitionPartialFetch();
        verify(metrics, never()).recordPartitionStorageError();
        verify(metrics, never()).recordPartitionCorruptRecord();
    }

    /**
     * With the coalesced per-partition buffer, multiple batches share one underlying byte[].
     * Each batch wraps a slice over a distinct region. setLastOffset / setMaxTimestamp mutate
     * each slice in place - this test pins that the per-batch lastOffset patches end up in
     * the correct slice and don't bleed into adjacent batches.
     */
    @Test
    public void testCoalescedBufferSliceIsolationOnLastOffsetPatch() {
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
        int highWatermark = 200;

        // Distinct, non-zero lastOffsets force setLastOffset to write a non-default value
        // into each batch's slice. If the writes leak across slices, the second batch's
        // lastOffset (101) would overwrite the first batch's lastOffset (100).
        final long firstLastOffset = 100L;
        final long secondLastOffset = 101L;

        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(),
                    BatchMetadata.of(partition0, 0, recordsA.sizeInBytes(), 0, firstLastOffset,
                        logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_A.value(),
                    BatchMetadata.of(partition0, recordsA.sizeInBytes(), recordsB.sizeInBytes(), 0, secondLastOffset,
                        logAppendTimestamp, maxBatchTimestamp, TimestampType.CREATE_TIME))
            ), logStartOffset, highWatermark)
        );

        final ByteRange range = new ByteRange(0, totalSize);
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, concatenatedBuffer))
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(),
            OBJECT_KEY_CREATOR,
            fetchInfos,
            coordinates,
            files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.NONE);

        Iterator<? extends RecordBatch> batchIterator =
            data.records.batches().iterator();
        assertThat(batchIterator.hasNext()).isTrue();
        assertThat(batchIterator.next().lastOffset())
            .as("first batch lastOffset must match its metadata, not the second batch's")
            .isEqualTo(firstLastOffset);
        assertThat(batchIterator.hasNext()).isTrue();
        assertThat(batchIterator.next().lastOffset())
            .as("second batch lastOffset must match its own metadata")
            .isEqualTo(secondLastOffset);
        assertThat(batchIterator.hasNext()).isFalse();
    }

    @Test
    public void testRecordsFetchResponseSize() {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(), BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 1)
        );
        final ByteRange range = new ByteRange(0, records.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, range, FileFetchJob.createFileExtent(OBJECT_KEY_A, range, records.buffer()))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).records.sizeInBytes()).isEqualTo(records.sizeInBytes());
        verify(metrics, times(1)).recordFetchResponseSize(records.sizeInBytes());
        verify(metrics, never()).recordPartitionPartialFetch();
        verify(metrics, never()).recordPartitionStorageError();
    }

    @Test
    public void testRecordsPartitionStorageErrorWhenMetadataMissing() {
        // No coordinates for partition0 -> servePartition returns KAFKA_STORAGE_ERROR
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, Collections.emptyMap(), Collections.emptyList(),
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
        verify(metrics, times(1)).recordPartitionStorageError();
        verify(metrics, never()).recordPartitionPartialFetch();
        // An all-error fetch returns 0 bytes and is still recorded (empty-fetch frequency is signal).
        verify(metrics, times(1)).recordFetchResponseSize(0L);
    }

    @Test
    public void testRecordsPartialPartitionFetchWhenTrailingBatchMissingExtent() {
        // First batch from OBJECT_KEY_A has its extent; second batch from OBJECT_KEY_B has none.
        // extractRecords returns the first batch, drops the trailing one. Partition response is
        // partial: foundRecords.size() < metadata.batches().size().
        byte[] firstValue = {1};
        MemoryRecords recordsA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(firstValue));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
            partition0, new FetchRequest.PartitionData(topicId, 0, 0, 1000, Optional.empty())
        );
        Map<TopicIdPartition, FindBatchResponse> coordinates = Map.of(
            partition0, FindBatchResponse.success(List.of(
                new BatchInfo(1L, OBJECT_KEY_A.value(),
                    BatchMetadata.of(partition0, 0, recordsA.sizeInBytes(), 0, 0, 10L, 20L, TimestampType.CREATE_TIME)),
                new BatchInfo(2L, OBJECT_KEY_B.value(),
                    BatchMetadata.of(partition0, 0, 50, 1, 1, 10L, 20L, TimestampType.CREATE_TIME))
            ), 0, 2)
        );
        final ByteRange rangeA = new ByteRange(0, recordsA.sizeInBytes());
        List<FileExtentResult> files = List.of(
            new FileExtentResult.Success(OBJECT_KEY_A, rangeA, FileFetchJob.createFileExtent(OBJECT_KEY_A, rangeA, recordsA.buffer()))
        );

        FetchCompleter job = new FetchCompleter(
            new MockTime(), OBJECT_KEY_CREATOR, fetchInfos, coordinates, files,
            durationMs -> {}, metrics
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();

        assertThat(result.get(partition0).error).isEqualTo(Errors.NONE);
        assertThat(result.get(partition0).records.sizeInBytes()).isEqualTo(recordsA.sizeInBytes());
        verify(metrics, times(1)).recordPartitionPartialFetch();
        verify(metrics, never()).recordPartitionStorageError();
        verify(metrics, times(1)).recordFetchResponseSize(recordsA.sizeInBytes());
    }
}
