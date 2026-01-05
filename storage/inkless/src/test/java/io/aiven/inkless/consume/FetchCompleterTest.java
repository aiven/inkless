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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
        );
        Map<TopicIdPartition, FetchPartitionData> result = job.get();
        FetchPartitionData data = result.get(partition0);
        assertThat(data.error).isEqualTo(Errors.KAFKA_STORAGE_ERROR);
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
            durationMs -> {}
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
}
