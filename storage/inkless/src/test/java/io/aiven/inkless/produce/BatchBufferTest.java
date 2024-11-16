// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchBufferTest {
    private static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    private static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    private static final TopicPartition T1P0 = new TopicPartition("topic1", 0);
    private static final TopicPartition T1P1 = new TopicPartition("topic1", 1);

    @Test
    void totalSize() {
        final BatchBuffer buffer = new BatchBuffer();

        assertThat(buffer.totalSize()).isZero();

        final RecordBatch batch1 = createBatch(T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch1, 0);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes());

        final RecordBatch batch2 = createBatch(T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch2, 1);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes() + batch2.sizeInBytes());
    }

    @Test
    void empty() {
        final BatchBuffer buffer = new BatchBuffer();

        BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.requestIds()).isEmpty();
        assertThat(result.data()).isEmpty();

        result = buffer.close();
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.requestIds()).isEmpty();
        assertThat(result.data()).isEmpty();
    }

    @Test
    void singleBatch() {
        final RecordBatch batch = createBatch(T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        final BatchBuffer buffer = new BatchBuffer();
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, batch.sizeInBytes(), 3)
        );
        assertThat(result.requestIds()).containsExactly(0);
        assertThat(result.data()).containsExactly(batchToBytes(batch));
    }

    @Test
    void multipleTopicPartitions() {
        final BatchBuffer buffer = new BatchBuffer();

        final RecordBatch t0p0b0 = createBatch(T0P0 + "-0");
        final RecordBatch t0p0b2 = createBatch(T0P0 + "-2");
        final RecordBatch t0p0b1 = createBatch(T0P0 + "-1");

        final RecordBatch t0p1b0 = createBatch(T0P1 + "-0");
        final RecordBatch t0p1b1 = createBatch(T0P1 + "-1");
        final RecordBatch t0p1b2 = createBatch(T0P1 + "-2");

        final RecordBatch t1p0b0 = createBatch(T1P0 + "-0");
        final RecordBatch t1p0b1 = createBatch(T1P0 + "-1");
        final RecordBatch t1p0b2 = createBatch(T1P0 + "-2");

        final int batchSize = t0p0b0.sizeInBytes();  // expecting it to be same everywhere
        buffer.addBatch(T0P0, t0p0b0, 0);
        buffer.addBatch(T1P0, t1p0b0, 0);
        buffer.addBatch(T0P0, t0p0b1, 0);

        buffer.addBatch(T1P0, t1p0b1, 1);
        buffer.addBatch(T0P1, t0p1b0, 1);
        buffer.addBatch(T0P0, t0p0b2, 1);

        buffer.addBatch(T0P1, t0p1b1, 2);
        buffer.addBatch(T0P1, t0p1b2, 2);
        buffer.addBatch(T1P0, t1p0b2, 2);

        // Here batches are sorted.
        final BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, batchSize, 1),
            new CommitBatchRequest(T0P0, batchSize, batchSize, 1),
            new CommitBatchRequest(T0P0, batchSize*2, batchSize, 1),
            new CommitBatchRequest(T0P1, batchSize*3, batchSize, 1),
            new CommitBatchRequest(T0P1, batchSize*4, batchSize, 1),
            new CommitBatchRequest(T0P1, batchSize*5, batchSize, 1),
            new CommitBatchRequest(T1P0, batchSize*6, batchSize, 1),
            new CommitBatchRequest(T1P0, batchSize*7, batchSize, 1),
            new CommitBatchRequest(T1P0, batchSize*8, batchSize, 1)
        );
        assertThat(result.requestIds()).containsExactly(
            0, 0, 1,
            1, 2, 2,
            0, 1, 2
        );

        // Here batch data are sorted too.
        final ByteBuffer expectedBytes = ByteBuffer.allocate(
            t0p0b0.sizeInBytes() + t0p0b1.sizeInBytes() + t0p0b2.sizeInBytes()
            + t0p1b0.sizeInBytes() + t0p1b1.sizeInBytes() + t0p1b2.sizeInBytes()
            + t1p0b0.sizeInBytes() + t1p0b1.sizeInBytes() + t1p0b2.sizeInBytes()
        );
        t0p0b0.writeTo(expectedBytes);
        t0p0b1.writeTo(expectedBytes);
        t0p0b2.writeTo(expectedBytes);
        t0p1b0.writeTo(expectedBytes);
        t0p1b1.writeTo(expectedBytes);
        t0p1b2.writeTo(expectedBytes);
        t1p0b0.writeTo(expectedBytes);
        t1p0b1.writeTo(expectedBytes);
        t1p0b2.writeTo(expectedBytes);
        assertThat(result.data()).containsExactly(expectedBytes.array());
    }

    @Test
    void notWorksAfterClosing() {
        final BatchBuffer buffer = new BatchBuffer();

        final RecordBatch batch1 = createBatch(T0P0 + "-0");
        buffer.addBatch(T0P0, batch1, 0);
        final BatchBuffer.CloseResult result1 = buffer.close();
        assertThat(result1.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, batch1.sizeInBytes(), 1)
        );
        assertThat(result1.data()).containsExactly(batchToBytes(batch1));
        assertThat(result1.requestIds()).containsExactly(0);

        final RecordBatch batch2 = createBatch(T1P0 + "-0-longer");
        assertThatThrownBy(() -> buffer.addBatch(T1P0, batch2, 1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
    }

    RecordBatch createBatch(String ...content) {
        final SimpleRecord[] simpleRecords = Arrays.stream(content).map(c -> new SimpleRecord(c.getBytes()))
            .toArray(SimpleRecord[]::new);
        final int initialOffset = 19;  // some non-zero number
        final MemoryRecords records = MemoryRecords.withRecords(initialOffset, Compression.NONE, simpleRecords);
        return records.firstBatch();
    }

    byte[] batchToBytes(final RecordBatch batch) {
        final ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        return buf.array();
    }

    @Test
    void addBatchNulls() {
        final BatchBuffer buffer = new BatchBuffer();

        assertThatThrownBy(() -> buffer.addBatch(null, createBatch(), 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicPartition cannot be null");
        assertThatThrownBy(() -> buffer.addBatch(T0P0, null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batch cannot be null");
    }
}
