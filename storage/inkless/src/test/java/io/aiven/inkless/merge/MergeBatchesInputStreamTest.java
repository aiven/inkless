package io.aiven.inkless.merge;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergeBatchesInputStreamTest {
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, "topic0");
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, "topic0");
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, "topic1");

    @Test
    void readStream() throws IOException {
        // File 1 layout:
        // - 100 bytes batch 1, T0P0
        // - 23 bytes gap
        // - 87 bytes batch 2, T0P1
        final int batch1Size = 100;
        final int gapSize = 23;
        final int batch2Size = 87;
        final int file1Size = batch1Size + gapSize + batch2Size;
        final byte[] batch1Data = MockInputStream.generateData(batch1Size, "batch1");
        final byte[] batch2Data = MockInputStream.generateData(batch2Size, "batch2");
        final BatchInfo batch1Info = new BatchInfo(1, "obj", BatchMetadata.of(T0P0, 0L, batch1Size, 0L, 9999L, 1L, 2L, TimestampType.CREATE_TIME));
        final BatchInfo batch2Info = new BatchInfo(2, "obj", BatchMetadata.of(T0P1, batch1Size + gapSize, batch2Size, 10L, 9999L, 1L, 2L, TimestampType.CREATE_TIME));
        final MockInputStream file1InputStream = new MockInputStream(file1Size);
        file1InputStream.addBatch(batch1Data);
        file1InputStream.addGap(gapSize);
        file1InputStream.addBatch(batch2Data);
        file1InputStream.finishBuilding();
        final Supplier<InputStream> file1InputStreamSupplier = () -> file1InputStream;
        final var file1InputStreamWithPosition = new InputStreamWithPosition(file1InputStreamSupplier, file1Size);

        // File 2 layout:
        // - 21 bytes gap
        // - 78 bytes batch 3, T1P0
        // - 11 bytes gap
        final int gap1Size = 21;
        final int batch3Size = 78;
        final int gap2Size = 11;
        final int file2Size = gap1Size + batch3Size + gap2Size;
        final byte[] batch3Data = MockInputStream.generateData(batch3Size, "batch3");
        final int expectedMergedSize = batch1Size + batch2Size + batch3Size;

        final BatchInfo batch3Info = new BatchInfo(3, "obj", BatchMetadata.of(T1P0, gap1Size, batch3Size, 0L, 9999L, 1L, 2L, TimestampType.CREATE_TIME));

        final MockInputStream file2InputStream = new MockInputStream(file2Size);
        file2InputStream.addGap(gap1Size);
        file2InputStream.addBatch(batch3Data);
        file2InputStream.addGap(gap2Size);
        file2InputStream.finishBuilding();

        final Supplier<InputStream> file2InputStreamSupplier = () -> file2InputStream;
        final var file2InputStreamWithPosition = new InputStreamWithPosition(file2InputStreamSupplier, file2Size);

        MergeBatchesInputStream.Builder builder = new MergeBatchesInputStream.Builder()
            .addBatch(new BatchAndStream(batch3Info, file2InputStreamWithPosition))
            .addBatch(new BatchAndStream(batch2Info, file1InputStreamWithPosition))
            .addBatch(new BatchAndStream(batch1Info, file1InputStreamWithPosition));
        var mergeBatchesInputStream = builder.build();

        assertEquals(expectedMergedSize, builder.mergeMetadata().mergedFileSize());

        // Read all batches and write them into an output stream
        var outputStream = new ByteArrayOutputStream(expectedMergedSize);
        long transferred = mergeBatchesInputStream.transferTo(outputStream);
        assertEquals(expectedMergedSize, transferred);

        var expected = ByteBuffer.allocate(expectedMergedSize);
        expected.put(batch1Data);
        expected.put(batch2Data);
        expected.put(batch3Data);

        assertThat(expected.array()).isEqualTo(outputStream.toByteArray());

        // Further reads return -1
        assertEquals(-1, mergeBatchesInputStream.read(new byte[1], 0, 1));

        mergeBatchesInputStream.close();
        file1InputStream.assertClosedAndDataFullyConsumed();
        file2InputStream.assertClosedAndDataFullyConsumed();
    }


}
