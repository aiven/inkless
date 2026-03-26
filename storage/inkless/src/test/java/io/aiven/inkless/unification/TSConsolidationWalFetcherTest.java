/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.unification;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TSConsolidationWalFetcherTest {

    private static final String OBJECT_KEY_PREFIX = "prefix";
    private static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator(OBJECT_KEY_PREFIX, false);
    private static final String OBJECT_A_MAIN = "wal-a";
    private static final String OBJECT_B_MAIN = "wal-b";
    private static final ObjectKey OBJECT_KEY_A = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_A_MAIN);
    private static final ObjectKey OBJECT_KEY_B = PlainObjectKey.create(OBJECT_KEY_PREFIX, OBJECT_B_MAIN);

    private final Uuid topicId = Uuid.randomUuid();
    private final TopicIdPartition partition0 = new TopicIdPartition(topicId, 0, "topic");
    private final TopicIdPartition partition1 = new TopicIdPartition(topicId, 1, "topic");

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private ObjectFetcher objectFetcher;

    private TSConsolidationWalFetcher fetcher;

    @BeforeEach
    void setUp() {
        fetcher = new TSConsolidationWalFetcher(objectFetcher, OBJECT_KEY_CREATOR);
    }

    @Test
    void fetchWalSegments_emptyInput_returnsEmptyMap() throws IOException, StorageBackendException {
        assertThat(fetcher.fetchWalSegments(List.of())).isEmpty();
    }

    @Test
    void fetchWalSegments_singleBatch_fetchesRangeAndReturnsRecords() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        byte[] objectBlob = copyRecordsBytes(records);
        stubFetchForBlob(objectBlob);

        long logAppendTs = 10L;
        long maxBatchTs = 20L;
        BatchInfo batch = new BatchInfo(
            1L,
            OBJECT_KEY_A.value(),
            BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, logAppendTs, maxBatchTs, TimestampType.CREATE_TIME)
        );

        Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> result = fetcher.fetchWalSegments(List.of(batch));

        assertThat(result).containsOnlyKeys(partition0);
        assertThat(result.get(partition0)).containsKey(batch);
        assertThat(result.get(partition0).get(batch).sizeInBytes()).isEqualTo(records.sizeInBytes());

        verify(objectFetcher, times(1)).fetch(OBJECT_KEY_A, new ByteRange(0, records.sizeInBytes()));
    }

    @Test
    void fetchWalSegments_twoBatchesSameObject_ordersInnerMapByBaseOffset() throws Exception {
        MemoryRecords recordsLow = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        MemoryRecords recordsHigh = MemoryRecords.withRecords(10L, Compression.NONE, new SimpleRecord(new byte[] {1}));
        byte[] objectBlob = new byte[recordsLow.sizeInBytes() + recordsHigh.sizeInBytes()];
        System.arraycopy(copyRecordsBytes(recordsLow), 0, objectBlob, 0, recordsLow.sizeInBytes());
        System.arraycopy(copyRecordsBytes(recordsHigh), 0, objectBlob, recordsLow.sizeInBytes(), recordsHigh.sizeInBytes());
        stubFetchForBlob(objectBlob);

        long logAppendTs = 1L;
        long maxBatchTs = 2L;
        BatchInfo batchHigh = new BatchInfo(
            2L,
            OBJECT_KEY_A.value(),
            BatchMetadata.of(partition0, recordsLow.sizeInBytes(), recordsHigh.sizeInBytes(), 10, 10, logAppendTs, maxBatchTs, TimestampType.CREATE_TIME
            )
        );
        BatchInfo batchLow = new BatchInfo(
            1L,
            OBJECT_KEY_A.value(),
            BatchMetadata.of(partition0,0, recordsLow.sizeInBytes(), 0, 0, logAppendTs, maxBatchTs, TimestampType.CREATE_TIME)
        );

        var result = fetcher.fetchWalSegments(List.of(batchHigh, batchLow));

        assertThat(result.get(partition0).navigableKeySet()).containsExactly(batchLow, batchHigh);
    }

    @Test
    void fetchWalSegments_twoPartitions_mergesIntoSeparateKeys() throws Exception {
        MemoryRecords recA = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        MemoryRecords recB = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord(new byte[] {2}));
        byte[] blobA = copyRecordsBytes(recA);
        byte[] blobB = copyRecordsBytes(recB);

        when(objectFetcher.fetch(any(), any())).thenAnswer(invocation -> {
            ObjectKey key = invocation.getArgument(0);
            ByteRange range = invocation.getArgument(1);
            byte[] blob = key.equals(OBJECT_KEY_A) ? blobA : blobB;
            return channelForSlice(blob, range);
        });

        long logAppendTs = 5L;
        long maxBatchTs = 6L;
        BatchInfo batchP0 = new BatchInfo(
            1L,
            OBJECT_KEY_A.value(),
            BatchMetadata.of(partition0, 0, recA.sizeInBytes(), 0, 0, logAppendTs, maxBatchTs, TimestampType.CREATE_TIME)
        );
        BatchInfo batchP1 = new BatchInfo(
            2L,
            OBJECT_KEY_B.value(),
            BatchMetadata.of(partition1, 0, recB.sizeInBytes(), 0, 0, logAppendTs, maxBatchTs, TimestampType.CREATE_TIME)
        );

        var result = fetcher.fetchWalSegments(List.of(batchP0, batchP1));

        assertThat(result).containsOnlyKeys(partition0, partition1);
        assertThat(result.get(partition0)).isNotNull().containsOnlyKeys(batchP0);
        assertThat(result.get(partition1)).isNotNull().containsOnlyKeys(batchP1);
    }

    @Test
    void fetchWalSegments_truncatedObjectBytes_omitsBatchAndReturnsEmptyMap() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        byte[] fullBlob = copyRecordsBytes(records);
        int claimedSize = records.sizeInBytes();
        assertThat(claimedSize).isGreaterThan(1);

        when(objectFetcher.fetch(any(), any())).thenAnswer(invocation -> {
            ByteRange range = invocation.getArgument(1);
            assertThat(range.offset()).isZero();
            assertThat(range.size()).isEqualTo(claimedSize);
            // Deliver one byte less than metadata.byteSize so the extent cannot cover the full batch range.
            return channelForSlice(fullBlob, new ByteRange(range.offset(), claimedSize - 1));
        });

        BatchInfo batch = new BatchInfo(
            1L,
            OBJECT_KEY_A.value(),
            BatchMetadata.of(partition0, 0, claimedSize, 0, 0, 1L, 2L, TimestampType.CREATE_TIME)
        );

        Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> result = fetcher.fetchWalSegments(List.of(batch));

        assertThat(result).isEmpty();
        verify(objectFetcher, times(1)).fetch(OBJECT_KEY_A, new ByteRange(0, claimedSize));
    }

    @Test
    void fetchWalSegments_passesResolvedObjectKeyToFetcher() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord((byte[]) null));
        stubFetchForBlob(copyRecordsBytes(records));

        BatchInfo batch = new BatchInfo(
            1L,
            OBJECT_KEY_A.value(),
            BatchMetadata.of(partition0, 0, records.sizeInBytes(), 0, 0, 1L, 2L, TimestampType.CREATE_TIME)
        );
        fetcher.fetchWalSegments(List.of(batch));

        ArgumentCaptor<ObjectKey> keyCaptor = ArgumentCaptor.forClass(ObjectKey.class);
        ArgumentCaptor<ByteRange> rangeCaptor = ArgumentCaptor.forClass(ByteRange.class);
        verify(objectFetcher).fetch(keyCaptor.capture(), rangeCaptor.capture());
        assertThat(keyCaptor.getValue()).isEqualTo(OBJECT_KEY_A);
        assertThat(rangeCaptor.getValue()).isEqualTo(new ByteRange(0, records.sizeInBytes()));
    }

    private void stubFetchForBlob(byte[] fullObjectBytes) throws Exception {
        when(objectFetcher.fetch(any(), any())).thenAnswer(invocation -> {
            ByteRange range = invocation.getArgument(1);
            return channelForSlice(fullObjectBytes, range);
        });
    }

    private static byte[] copyRecordsBytes(MemoryRecords records) {
        var buf = records.buffer().duplicate();
        byte[] copy = new byte[buf.remaining()];
        buf.get(copy);
        return copy;
    }

    private static ReadableByteChannel channelForSlice(byte[] fullBlob, ByteRange range) {
        int from = Math.toIntExact(range.offset());
        int to = from + Math.toIntExact(range.size());
        byte[] slice = Arrays.copyOfRange(fullBlob, from, to);
        return Channels.newChannel(new ByteArrayInputStream(slice));
    }
}
