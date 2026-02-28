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
package io.aiven.inkless.merge;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.FileMergeWorkItem;
import io.aiven.inkless.control_plane.MergedFileBatch;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.Storage;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileMergerMockedTest {

    static final long WORK_ITEM_ID = 1;
    static final int BROKER_ID = 1;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    static final TopicIdPartition T1P1 = new TopicIdPartition(TOPIC_ID_1, 1, TOPIC_1);
    public static final Path WORK_DIR = Path.of("/tmp/inkless/file-merge");

    @Mock
    Time time;
    @Mock
    InklessConfig inklessConfig;
    @Mock
    ControlPlane controlPlane;
    @Mock
    Storage storage;
    @Captor
    ArgumentCaptor<ObjectKey> objectKeyCaptor;
    @Captor
    ArgumentCaptor<Long> sleepCaptor;

    SharedState sharedState;

    @BeforeEach
    void setup() {
        when(inklessConfig.objectKeyPrefix()).thenReturn("prefix");
        when(inklessConfig.fileMergeWorkDir()).thenReturn(WORK_DIR);
        when(inklessConfig.cacheMaxCount()).thenReturn(10000L);

        sharedState = SharedState.initialize(time, BROKER_ID, inklessConfig, mock(MetadataView.class), controlPlane,
            mock(BrokerTopicStats.class), mock(Supplier.class));
    }

    @AfterEach
    void tearDown() {
        assertThat(WORK_DIR).isEmptyDirectory();
    }

    @Test
    void singleFileSingleBatch() throws StorageBackendException, IOException {
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);

        final String obj1 = "obj1";

        final long file1Id = 1;
        final long batch1Id = 1;

        final int file1Batch1Size = 100;
        final int file1Size = file1Batch1Size;
        final int file1UsedSize = file1Size;
        final byte[] file1Batch1 = MockInputStream.generateData(file1Batch1Size, "file1Batch1");
        when(storage.fetch(any(ObjectKey.class), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(file1Batch1)));

        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(file1Id, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file1Size, List.of(
            new BatchInfo(batch1Id, obj1, BatchMetadata.of(T1P0, 0, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME))
        ));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenAnswer(i -> {
                final ByteBuffer data = i.getArgument(1, ByteBuffer.class);
                final byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                out.write(bytes);
                return CompletableFuture.completedFuture(null);
            });

        final long expectedMergedFileSize = file1UsedSize;
        final List<MergedFileBatch> expectedMergedFileBatches = List.of(
            new MergedFileBatch(BatchMetadata.of(T1P0, 0, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME), List.of(batch1Id))
        );
        final byte[] expectedUploadBuffer = file1Batch1;

        when(controlPlane.getFileMergeWorkItem()).thenReturn(
            new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), List.of(file1InWorkItem))
        );

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        verify(storage).fetch(PlainObjectKey.create("", obj1), null);
        verify(storage).upload(objectKeyCaptor.capture(), any(ByteBuffer.class));
        assertThat(out.toByteArray()).isEqualTo(expectedUploadBuffer);

        verify(controlPlane).commitFileMergeWorkItem(eq(WORK_ITEM_ID), eq(objectKeyCaptor.getValue().value()), eq(ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT), eq(BROKER_ID), eq(expectedMergedFileSize), eq(expectedMergedFileBatches));
    }

    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    void twoFilesWithGaps(final boolean directFileOrder, final boolean directBatchOrder) throws StorageBackendException, IOException {
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);

        final String obj1 = "obj1";
        final String obj2 = "obj2";

        final long file1Id = 1;
        final long file2Id = 2;
        final long batch1Id = 1;
        final long batch2Id = 2;
        final long batch3Id = 3;
        final long batch4Id = 4;

        // File 1 layout:
        // - 1000 bytes gap
        // - 100 bytes batch, T1P0
        // - 1200 bytes gap
        // - 120 bytes batch, T1P1
        // - 1400 bytes gap
        final int file1Gap1Size = 1000;
        final int file1Batch1Size = 100;
        final int file1Gap2Size = 1200;
        final int file1Batch2Size = 120;
        final int file1Gap3Size = 1200;
        final int file1Size = file1Gap1Size + file1Batch1Size + file1Gap2Size + file1Batch2Size + file1Gap3Size;
        final int file1UsedSize = file1Batch1Size + file1Batch2Size;
        final byte[] file1Batch1 = MockInputStream.generateData(file1Batch1Size, "file1Batch1");
        final byte[] file1Batch2 = MockInputStream.generateData(file1Batch2Size, "file1Batch2");

        final BatchInfo file1Batch1InWorkItem = new BatchInfo(batch1Id, obj1, BatchMetadata.of(T1P0, file1Gap1Size, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME));
        final BatchInfo file1Batch2InWorkItem = new BatchInfo(batch2Id, obj1, BatchMetadata.of(T1P1, file1Gap1Size + file1Batch1Size + file1Gap2Size, file1Batch2Size, 100L, 123L, 100L, 200L, TimestampType.LOG_APPEND_TIME));
        final List<BatchInfo> file1Batches = directBatchOrder
            ? List.of(file1Batch1InWorkItem, file1Batch2InWorkItem)
            : List.of(file1Batch2InWorkItem, file1Batch1InWorkItem);
        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(file1Id, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file1Size, file1Batches);

        final ByteBuffer file1FullBuffer = ByteBuffer.allocate(file1Size);
        file1FullBuffer.put(new byte[file1Gap1Size]);
        file1FullBuffer.put(file1Batch1);
        file1FullBuffer.put(new byte[file1Gap2Size]);
        file1FullBuffer.put(file1Batch2);
        file1FullBuffer.put(new byte[file1Gap3Size]);
        file1FullBuffer.rewind();

        // File 2 layout:
        // - 200 bytes batch, T0P0
        // - 2000 bytes gap
        // - 210 bytes batch, T1P1
        final int file2Batch1Size = 200;
        final int file2Gap1Size = 2000;
        final int file2Batch2Size = 210;
        final int file2Size = file2Batch1Size + file2Gap1Size + file2Batch2Size;
        final int file2UsedSize = file2Batch1Size + file2Batch2Size;
        final byte[] file2Batch1 = MockInputStream.generateData(file2Batch1Size, "file2Batch1");
        final byte[] file2Batch2 = MockInputStream.generateData(file2Batch2Size, "file2Batch2");

        final BatchInfo file2Batch1InWorkItem = new BatchInfo(batch3Id, obj2, BatchMetadata.of(T0P0, 0, file2Batch1Size, 1000L, 1010L, 1000L, 2000L, TimestampType.LOG_APPEND_TIME));
        final BatchInfo file2Batch2InWorkItem = new BatchInfo(batch4Id, obj2, BatchMetadata.of(T1P1, file2Batch1Size + file2Gap1Size, file2Batch2Size, 10000L, 10100L, 10000L, 20000L, TimestampType.CREATE_TIME));
        final List<BatchInfo> file2Batches = directBatchOrder
            ? List.of(file2Batch1InWorkItem, file2Batch2InWorkItem)
            : List.of(file2Batch2InWorkItem, file2Batch1InWorkItem);
        final FileMergeWorkItem.File file2InWorkItem = new FileMergeWorkItem.File(file2Id, obj2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file2Size, file2Batches);

        final ByteBuffer file2FullBuffer = ByteBuffer.allocate(file2Size);
        file2FullBuffer.put(file2Batch1);
        file2FullBuffer.put(new byte[file2Gap1Size]);
        file2FullBuffer.put(file2Batch2);
        file2FullBuffer.rewind();

        when(storage.fetch(PlainObjectKey.create("", obj1), null))
            .thenReturn(CompletableFuture.completedFuture(file1FullBuffer));
        when(storage.fetch(PlainObjectKey.create("", obj2), null))
            .thenReturn(CompletableFuture.completedFuture(file2FullBuffer));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenAnswer(i -> {
                final ByteBuffer data = i.getArgument(1, ByteBuffer.class);
                final byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                out.write(bytes);
                return CompletableFuture.completedFuture(null);
            });

        // What we expect in the end:
        // 1. Batches are sorted by topic-partition and by their base offsets.
        // 2. No gaps, the total size equals to the batch sizes.
        // 3. The batch content matches.
        final long expectedMergedFileSize = file1UsedSize + file2UsedSize;
        final List<MergedFileBatch> expectedMergedFileBatches = List.of(
            new MergedFileBatch(BatchMetadata.of(T0P0, 0, file2Batch1Size, 1000L, 1010L, 1000L, 2000L, TimestampType.LOG_APPEND_TIME), List.of(batch3Id)),
            new MergedFileBatch(BatchMetadata.of(T1P0, file2Batch1Size, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME), List.of(batch1Id)),
            new MergedFileBatch(BatchMetadata.of(T1P1, file2Batch1Size + file1Batch1Size, file1Batch2Size, 100L, 123L, 100L, 200L, TimestampType.LOG_APPEND_TIME), List.of(batch2Id)),
            new MergedFileBatch(BatchMetadata.of(T1P1, file2Batch1Size + file1Batch1Size + file1Batch2Size, file2Batch2Size, 10000L, 10100L, 10000L, 20000L, TimestampType.CREATE_TIME), List.of(batch4Id))
        );
        // T0P0, T1P0, T1P1, T1P1
        final byte[] expectedUploadBuffer = concat(file2Batch1, file1Batch1, file1Batch2, file2Batch2);

        final List<FileMergeWorkItem.File> files = directFileOrder
            ? List.of(file1InWorkItem, file2InWorkItem)
            : List.of(file2InWorkItem, file1InWorkItem);
        when(controlPlane.getFileMergeWorkItem()).thenReturn(
            new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), files)
        );

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        verify(storage).fetch(PlainObjectKey.create("", obj1), null);
        verify(storage).fetch(PlainObjectKey.create("", obj2), null);

        assertThat(out.toByteArray()).isEqualTo(expectedUploadBuffer);
        verify(storage).upload(objectKeyCaptor.capture(), any(ByteBuffer.class));

        verify(controlPlane).commitFileMergeWorkItem(eq(WORK_ITEM_ID), eq(objectKeyCaptor.getValue().value()), eq(ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT), eq(BROKER_ID), eq(expectedMergedFileSize), eq(expectedMergedFileBatches));
    }

    @Test
    void mustSleepWhenNoWorkItem() {
        when(controlPlane.getFileMergeWorkItem()).thenReturn(null);

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();
        verify(time).sleep(sleepCaptor.capture());
        assertThat(sleepCaptor.getValue()).isBetween((long) (10_000L * 0.8), (long) (20_000L * 1.2));
        verifyNoMoreInteractions(controlPlane);
        verifyNoInteractions(storage);
    }

    @Test
    void errorInReading() throws Exception {
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);

        final String obj1 = "obj1";
        final long batch1Id = 1;
        when(storage.fetch(any(ObjectKey.class), isNull()))
            .thenReturn(CompletableFuture.failedFuture(new IOException("test")));

        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(1, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, 10, List.of(
            new BatchInfo(batch1Id, obj1, BatchMetadata.of(T1P0, 0, 10, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME))
        ));
        when(controlPlane.getFileMergeWorkItem()).thenReturn(
            new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), List.of(file1InWorkItem))
        );

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        verify(controlPlane).releaseFileMergeWorkItem(eq(WORK_ITEM_ID));
        verify(controlPlane, never()).commitFileMergeWorkItem(anyLong(), anyString(), any(), anyInt(), anyLong(), any());
        verify(time).sleep(longThat(l -> l >= 50));
        verify(storage).fetch(any(ObjectKey.class), isNull());

        verify(storage, never()).upload(any(ObjectKey.class), any(ByteBuffer.class));
        verify(storage, never()).delete(any(ObjectKey.class));
    }

    @Test
    void errorInWriting() throws Exception {
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);

        final String obj1 = "obj1";

        final long file1Id = 1;
        final long batch1Id = 1;

        final int file1Batch1Size = 100;
        final byte[] file1Batch1 = MockInputStream.generateData(file1Batch1Size, "file1Batch1");
        when(storage.fetch(any(ObjectKey.class), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(file1Batch1)));

        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.failedFuture(new StorageBackendException("test")));

        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(file1Id, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file1Batch1Size, List.of(
            new BatchInfo(batch1Id, obj1, BatchMetadata.of(T1P0, 0, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME))
        ));
        when(controlPlane.getFileMergeWorkItem()).thenReturn(
            new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), List.of(file1InWorkItem))
        );

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        verify(controlPlane).releaseFileMergeWorkItem(eq(WORK_ITEM_ID));
        verify(controlPlane, never()).commitFileMergeWorkItem(anyLong(), anyString(), any(), anyInt(), anyLong(), any());
        verify(time).sleep(longThat(l -> l >= 50));

        verify(storage).fetch(PlainObjectKey.create("", obj1), null);
        verify(storage, never()).delete(any(ObjectKey.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void errorInCommittingFromControlPlane(boolean isSafeToDelete) throws Exception {
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);

        final String obj1 = "obj1";

        final long file1Id = 1;
        final long batch1Id = 1;

        final int file1Batch1Size = 100;
        final int file1Size = file1Batch1Size;
        final byte[] file1Batch1 = MockInputStream.generateData(file1Batch1Size, "file1Batch1");
        when(storage.fetch(any(ObjectKey.class), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(file1Batch1)));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenAnswer(i -> {
                final ByteBuffer data = i.getArgument(1, ByteBuffer.class);
                final byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                out.write(bytes);
                return CompletableFuture.completedFuture(null);
            });
        if (isSafeToDelete) {
            when(storage.delete(any(ObjectKey.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        }

        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(file1Id, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file1Size, List.of(
            new BatchInfo(batch1Id, obj1, BatchMetadata.of(T1P0, 0, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME))
        ));
        when(controlPlane.getFileMergeWorkItem())
            .thenReturn(new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), List.of(file1InWorkItem)));
        doThrow(new ControlPlaneException("test"))
            .when(controlPlane).commitFileMergeWorkItem(anyLong(), anyString(), any(), anyInt(), anyLong(), any());
        when(controlPlane.isSafeToDeleteFile(anyString())).thenReturn(isSafeToDelete);

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        verify(controlPlane).releaseFileMergeWorkItem(eq(WORK_ITEM_ID));
        verify(time).sleep(longThat(l -> l >= 50));
        verify(storage).fetch(any(ObjectKey.class), isNull());

        verify(storage).upload(objectKeyCaptor.capture(), any(ByteBuffer.class));
        verify(storage, times(isSafeToDelete ? 1 : 0)).delete(objectKeyCaptor.getValue());
    }

    @Test
    void errorInCommittingNotFromControlPlane() throws Exception {
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);

        final String obj1 = "obj1";

        final long file1Id = 1;
        final long batch1Id = 1;

        final int file1Batch1Size = 100;
        final byte[] file1Batch1 = MockInputStream.generateData(file1Batch1Size, "file1Batch1");
        when(storage.fetch(any(ObjectKey.class), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(file1Batch1)));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenAnswer(i -> {
                final ByteBuffer data = i.getArgument(1, ByteBuffer.class);
                final byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                out.write(bytes);
                return CompletableFuture.completedFuture(null);
            });

        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(file1Id, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file1Batch1Size, List.of(
            new BatchInfo(batch1Id, obj1, BatchMetadata.of(T1P0, 0, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME))
        ));
        when(controlPlane.getFileMergeWorkItem())
            .thenReturn(new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), List.of(file1InWorkItem)));
        doThrow(new RuntimeException("test"))
            .when(controlPlane).commitFileMergeWorkItem(anyLong(), anyString(), any(), anyInt(), anyLong(), any());

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        verify(controlPlane).releaseFileMergeWorkItem(eq(WORK_ITEM_ID));
        verify(time).sleep(longThat(l -> l >= 50));
        verify(storage).fetch(any(ObjectKey.class), isNull());

        verify(storage).upload(objectKeyCaptor.capture(), any(ByteBuffer.class));
        verify(storage, never()).delete(objectKeyCaptor.getValue());
    }

    /**
     * Test that downloads are started in parallel before any blocking wait occurs.
     * With async storage (CRT), all fetch futures are started concurrently.
     */
    @Test
    void parallelDownloads_asyncStorage() throws Exception {
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);

        final String obj1 = "obj1";
        final String obj2 = "obj2";
        final String obj3 = "obj3";

        final int fileSize = 100;
        final byte[] fileData = MockInputStream.generateData(fileSize, "data");

        // Track the order of fetch calls and when they start/complete
        final AtomicInteger fetchCallCount = new AtomicInteger(0);
        final List<Integer> fetchStartOrder = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch allFetchesStarted = new CountDownLatch(3);
        final CountDownLatch canComplete = new CountDownLatch(1);

        // Simulate async storage - fetch returns immediately but future completes later
        when(storage.fetch(any(ObjectKey.class), isNull())).thenAnswer(invocation -> {
            final int callNumber = fetchCallCount.incrementAndGet();
            fetchStartOrder.add(callNumber);
            allFetchesStarted.countDown();

            // Return a future that completes after all fetches have started
            return CompletableFuture.supplyAsync(() -> {
                try {
                    // Wait for signal that all fetches started before completing
                    canComplete.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ByteBuffer.wrap(fileData);
            });
        });

        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        final List<FileMergeWorkItem.File> files = List.of(
            new FileMergeWorkItem.File(1, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize, List.of(
                new BatchInfo(1, obj1, BatchMetadata.of(T1P0, 0, fileSize, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME)))),
            new FileMergeWorkItem.File(2, obj2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize, List.of(
                new BatchInfo(2, obj2, BatchMetadata.of(T1P0, 0, fileSize, 12L, 22L, 3L, 4L, TimestampType.CREATE_TIME)))),
            new FileMergeWorkItem.File(3, obj3, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize, List.of(
                new BatchInfo(3, obj3, BatchMetadata.of(T1P0, 0, fileSize, 23L, 33L, 5L, 6L, TimestampType.CREATE_TIME))))
        );

        when(controlPlane.getFileMergeWorkItem())
            .thenReturn(new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), files));

        // Start the merger in a separate thread since it will block
        final Thread mergerThread = new Thread(() -> {
            final FileMerger fileMerger = new FileMerger(sharedState);
            fileMerger.run();
        });
        mergerThread.start();

        // Wait for all 3 fetches to be started - this proves they're started in parallel
        assertThat(allFetchesStarted.await(5, TimeUnit.SECONDS))
            .as("All 3 fetches should be started in parallel before any blocks")
            .isTrue();

        // Verify all 3 fetch calls were made
        assertThat(fetchCallCount.get()).isEqualTo(3);

        // Allow the fetches to complete
        canComplete.countDown();

        // Wait for merger to finish
        mergerThread.join(10_000);
        assertThat(mergerThread.isAlive()).isFalse();

        // Verify all fetches were called
        verify(storage, times(3)).fetch(any(ObjectKey.class), isNull());
    }

    /**
     * Test downloads with sync storage - futures complete immediately.
     * The parallelization logic still works, but actual I/O happens during fetch() call.
     */
    @Test
    void parallelDownloads_syncStorage() throws Exception {
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);

        final String obj1 = "obj1";
        final String obj2 = "obj2";

        final int fileSize = 100;
        final byte[] fileData1 = MockInputStream.generateData(fileSize, "data1");
        final byte[] fileData2 = MockInputStream.generateData(fileSize, "data2");

        // Sync storage returns completed futures immediately (blocking happens in fetch call)
        when(storage.fetch(eq(PlainObjectKey.create("", obj1)), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(fileData1)));
        when(storage.fetch(eq(PlainObjectKey.create("", obj2)), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(fileData2)));

        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        final List<FileMergeWorkItem.File> files = List.of(
            new FileMergeWorkItem.File(1, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize, List.of(
                new BatchInfo(1, obj1, BatchMetadata.of(T1P0, 0, fileSize, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME)))),
            new FileMergeWorkItem.File(2, obj2, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, fileSize, List.of(
                new BatchInfo(2, obj2, BatchMetadata.of(T1P0, 0, fileSize, 12L, 22L, 3L, 4L, TimestampType.CREATE_TIME))))
        );

        when(controlPlane.getFileMergeWorkItem())
            .thenReturn(new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), files));

        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();

        // Verify both fetches were called
        verify(storage).fetch(eq(PlainObjectKey.create("", obj1)), isNull());
        verify(storage).fetch(eq(PlainObjectKey.create("", obj2)), isNull());
        verify(storage).upload(any(ObjectKey.class), any(ByteBuffer.class));
    }

    /**
     * Test that delete on error path is fire-and-forget.
     */
    @Test
    void deleteOnErrorIsFireAndForget() throws Exception {
        when(inklessConfig.storageForMerge(any())).thenReturn(storage);
        when(inklessConfig.produceMaxUploadAttempts()).thenReturn(1);
        when(inklessConfig.produceUploadBackoff()).thenReturn(Duration.ZERO);

        final String obj1 = "obj1";
        final int file1Batch1Size = 100;
        final byte[] file1Batch1 = MockInputStream.generateData(file1Batch1Size, "file1Batch1");
        when(storage.fetch(any(ObjectKey.class), isNull()))
            .thenReturn(CompletableFuture.completedFuture(ByteBuffer.wrap(file1Batch1)));

        when(storage.upload(any(ObjectKey.class), any(ByteBuffer.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        // Simulate slow async delete - should not block
        final CountDownLatch deleteCanComplete = new CountDownLatch(1);
        when(storage.delete(any(ObjectKey.class))).thenAnswer(invocation -> {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    deleteCanComplete.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            });
        });

        final FileMergeWorkItem.File file1InWorkItem = new FileMergeWorkItem.File(1, obj1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, file1Batch1Size, List.of(
            new BatchInfo(1, obj1, BatchMetadata.of(T1P0, 0, file1Batch1Size, 1L, 11L, 1L, 2L, TimestampType.CREATE_TIME))
        ));
        when(controlPlane.getFileMergeWorkItem())
            .thenReturn(new FileMergeWorkItem(WORK_ITEM_ID, Instant.ofEpochMilli(1234), List.of(file1InWorkItem)));
        doThrow(new ControlPlaneException("test"))
            .when(controlPlane).commitFileMergeWorkItem(anyLong(), anyString(), any(), anyInt(), anyLong(), any());
        when(controlPlane.isSafeToDeleteFile(anyString())).thenReturn(true);

        // Run the merger - should complete without waiting for delete
        final long startTime = System.currentTimeMillis();
        final FileMerger fileMerger = new FileMerger(sharedState);
        fileMerger.run();
        final long elapsed = System.currentTimeMillis() - startTime;

        // Verify delete was initiated but method returned quickly (fire-and-forget)
        verify(storage).delete(any(ObjectKey.class));

        // Should complete quickly without waiting for the slow delete
        // (the latch wait is 5 seconds, so if we blocked, elapsed would be >= 5000)
        assertThat(elapsed).isLessThan(4000);

        // Allow delete to complete for clean test teardown
        deleteCanComplete.countDown();
    }

    private byte[] concat(final byte[] ... arrays) {
        try (final var outputStream = new ByteArrayOutputStream()) {
            for (final byte[] array : arrays) {
                outputStream.writeBytes(array);
            }
            return outputStream.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
