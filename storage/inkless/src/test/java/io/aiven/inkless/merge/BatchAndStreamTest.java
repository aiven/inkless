package io.aiven.inkless.merge;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class BatchAndStreamTest {

    @Mock
    private InputStreamWithPosition mockInputStream;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    private BatchInfo createBatchInfo(
        int batchId,
        TopicIdPartition topicIdPartition,
        long byteOffset,
        long batchSize,
        long startingOffset
    ) {
        return new BatchInfo(batchId, "obj", BatchMetadata.of(topicIdPartition, byteOffset, batchSize, startingOffset, 9999L, 1L, 2L, TimestampType.CREATE_TIME));
    }

    @Test
    void comparatorSortsCorrectly() {
        BatchInfo batch1 = createBatchInfo(1, T0P0, 53, 10, 10);
        BatchInfo batch2 = createBatchInfo(2, T0P0, 0,  5, 1000);
        BatchInfo batch3 = createBatchInfo(3, T0P1, 10, 13, 0);
        BatchInfo batch4 = createBatchInfo(4, T1P0, 11, 9, 52);

        BatchAndStream bas1 = new BatchAndStream(batch1, mockInputStream);
        BatchAndStream bas2 = new BatchAndStream(batch2, mockInputStream);
        BatchAndStream bas3 = new BatchAndStream(batch3, mockInputStream);
        BatchAndStream bas4 = new BatchAndStream(batch4, mockInputStream);

        List<BatchAndStream> batchAndStreams = Arrays.asList(bas1, bas2, bas3, bas4);
        Collections.shuffle(batchAndStreams);

        batchAndStreams.sort(BatchAndStream.TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR);

        assertEquals(batchAndStreams, Arrays.asList(bas1, bas2, bas3, bas4));
    }

    @Test
    void twoBatchesOnSameFile() throws IOException {
        // File layout:
        // - 100 bytes batch, T0P0
        // - 23 bytes gap
        // - 87 bytes batch, T1P1
        final int batch1Size = 100;
        final int gapSize = 23;
        final int batch2Size = 87;
        final int fileSize = batch1Size + gapSize + batch2Size;
        final byte[] batch1Data = MockInputStream.generateData(batch1Size, "batch1");
        final byte[] batch2Data = MockInputStream.generateData(batch2Size, "batch2");

        final BatchInfo batch1 = createBatchInfo(1, T0P0, 0, batch1Size, 0);
        final BatchInfo batch2 = createBatchInfo(2, T0P1, batch1Size + gapSize,  batch2Size, 10);

        final MockInputStream inputStream = new MockInputStream(fileSize);
        inputStream.addBatch(batch1Data);
        inputStream.addGap(gapSize);
        inputStream.addBatch(batch2Data);
        inputStream.finishBuilding();

        final Supplier<InputStream> inputStreamSupplier = () -> inputStream;
        final var inputStreamWithPosition = new InputStreamWithPosition(inputStreamSupplier, fileSize);

        final var batch1AndStream = new BatchAndStream(batch1, inputStreamWithPosition);
        final var batch2AndStream = new BatchAndStream(batch2, inputStreamWithPosition);

        // Read first batch
        byte[] batch1ReadData = new byte[batch1Size];
        int bytesRead = batch1AndStream.read(batch1ReadData, 0, batch1Size);

        assertEquals(batch1Size, bytesRead);
        assertThat(batch1ReadData).isEqualTo(batch1Data);

        // Input stream is not closed because it hasn't reached the end
        assertFalse(inputStream.isClosed());
        // no other data to read for batch 1
        assertEquals(-1, batch1AndStream.read(batch1ReadData, 0, 1));

        // Read second batch
        byte[] batch2ReadData = new byte[batch2Size];
        bytesRead = batch2AndStream.read(batch2ReadData, 0, batch2Size);

        assertEquals(batch2Size, bytesRead);
        assertThat(batch2ReadData).isEqualTo(batch2Data);

        // Input stream is now closed because the end has been reached
        inputStream.assertClosedAndDataFullyConsumed();
        // No other data to read for batch 2
        assertEquals(-1, batch2AndStream.read(batch2ReadData, 0, 1));
    }

    @Test
    void batchWithStartAndEndGap() throws IOException {
        // File layout:
        // - 23 bytes gap
        // - 100 bytes batch, T0P0
        // - 11 bytes gap
        final int batchSize = 100;
        final int gap1Size = 23;
        final int gap2Size = 11;
        final int fileSize = gap1Size + batchSize + +gap2Size;
        final byte[] batchData = MockInputStream.generateData(batchSize, "batch1");

        final BatchInfo batchInfo = createBatchInfo(1, T0P0, gap1Size, batchSize, 0);

        final MockInputStream inputStream = new MockInputStream(fileSize);
        inputStream.addGap(gap1Size);
        inputStream.addBatch(batchData);
        inputStream.addGap(gap2Size);
        inputStream.finishBuilding();

        final Supplier<InputStream> inputStreamSupplier = () -> inputStream;
        final var inputStreamWithPosition = new InputStreamWithPosition(inputStreamSupplier, fileSize);

        final var batchAndStream = new BatchAndStream(batchInfo, inputStreamWithPosition);

        // Read batch
        byte[] batchReadData = new byte[batchSize];
        int bytesRead = batchAndStream.read(batchReadData, 0, batchSize);

        assertEquals(batchSize, bytesRead);
        assertThat(batchReadData).isEqualTo(batchData);

        // Input stream is not closed because it hasn't reached the end
        assertFalse(inputStream.isClosed());
        // no other data to read for batch
        assertEquals(-1, batchAndStream.read(batchReadData, 0, 1));

        batchAndStream.close();
        inputStream.assertClosedAndDataFullyConsumed();
    }

}
