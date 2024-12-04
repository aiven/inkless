// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchBufferTest {
    private static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    private static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    private static final TopicPartition T1P0 = new TopicPartition("topic1", 0);

    private static final int initialOffset = 19;  // some non-zero number

    @Test
    void totalSize() {
        final Time time = Time.SYSTEM;
        final BatchBuffer buffer = new BatchBuffer();

        assertThat(buffer.totalSize()).isZero();

        final MutableRecordBatch batch1 = createBatch(time, initialOffset, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch1, 0);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes());

        final MutableRecordBatch batch2 = createBatch(time, initialOffset, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch2, 1);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes() + batch2.sizeInBytes());
    }

    @Test
    void empty() {
        final BatchBuffer buffer = new BatchBuffer();

        BatchBuffer.CloseResult result = buffer.close(Map.of());
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.requestIds()).isEmpty();
        assertThat(result.data()).isEmpty();

        result = buffer.close(Map.of());
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.requestIds()).isEmpty();
        assertThat(result.data()).isEmpty();
    }

    @Test
    void singleBatch() {
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch batch = createBatch(time, initialOffset, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        final byte[] beforeAdding = batchToBytes(batch);
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close(Map.of());
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, batch.sizeInBytes(), initialOffset, initialOffset + 2)
        );
        assertThat(result.requestIds()).containsExactly(0);
        assertThat(result.data()).containsExactly(batchToBytes(batch));
        assertThat(result.data()).containsExactly(beforeAdding);
    }
    @Test
    void singleBatchDuplicated() {
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch batch = createBatch(time, initialOffset, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close(Map.of(
            0,
            Map.of(
                T0P0,
                new DuplicatedBatchMetadata(initialOffset, new BatchMetadata(0, 0, 0, 0))
            )
        ));
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.requestIds()).isEmpty();
        assertThat(result.data()).isEmpty();
    }

    @Test
    void multipleTopicPartitionsWithDuplicates() {
        final Time time = Time.SYSTEM;
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch t0p0b0 = createBatch(time, initialOffset, T0P0 + "-0"); // duplicate
        final MutableRecordBatch t0p0b1 = createBatch(time, t0p0b0.nextOffset(),T0P0 + "-1");
        final MutableRecordBatch t0p0b2 = createBatch(time, t0p0b1.nextOffset(),T0P0 + "-2");

        final MutableRecordBatch t0p1b0 = createBatch(time, initialOffset,T0P1 + "-0");
        final MutableRecordBatch t0p1b1 = createBatch(time, t0p1b0.nextOffset(),T0P1 + "-1"); // duplicate
        final MutableRecordBatch t0p1b2 = createBatch(time, t0p1b1.nextOffset(),T0P1 + "-2");

        final MutableRecordBatch t1p0b0 = createBatch(time, initialOffset,T1P0 + "-0");
        final MutableRecordBatch t1p0b1 = createBatch(time, t1p0b0.nextOffset(),T1P0 + "-1");
        final MutableRecordBatch t1p0b2 = createBatch(time, t1p0b1.nextOffset(),T1P0 + "-2"); // duplicate

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
        final BatchBuffer.CloseResult result = buffer.close(Map.of(
            0,
            Map.of(
                T0P0,
                new DuplicatedBatchMetadata(initialOffset, new BatchMetadata(0, 0, 0, 0))
            ),
            2,
            Map.of(
                T0P1,
                new DuplicatedBatchMetadata(t0p1b0.nextOffset(), new BatchMetadata(0, 0, 0, 0)),
                T1P0,
                new DuplicatedBatchMetadata(t1p0b1.nextOffset(), new BatchMetadata(0, 0, 0, 0))
            )
        ));
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, batchSize, 20, 20),
            CommitBatchRequest.of(T0P0, batchSize, batchSize, 21, 21),
            CommitBatchRequest.of(T0P1, batchSize * 2, batchSize, 19, 19),
            CommitBatchRequest.of(T0P1, batchSize * 3, batchSize, 21, 21),
            CommitBatchRequest.of(T1P0, batchSize * 4, batchSize, 19, 19),
            CommitBatchRequest.of(T1P0, batchSize * 5, batchSize, 20, 20)
        );
        assertThat(result.requestIds()).containsExactly(
            0, 1,
            1, 2,
            0, 1
        );

        // Here batch data are sorted too.
        final ByteBuffer expectedBytes = ByteBuffer.allocate(
            t0p0b1.sizeInBytes() + t0p0b2.sizeInBytes()
                + t0p1b0.sizeInBytes() + t0p1b2.sizeInBytes()
                + t1p0b0.sizeInBytes() + t1p0b1.sizeInBytes()
        );
        t0p0b1.writeTo(expectedBytes);
        t0p0b2.writeTo(expectedBytes);
        t0p1b0.writeTo(expectedBytes);
        t0p1b2.writeTo(expectedBytes);
        t1p0b0.writeTo(expectedBytes);
        t1p0b1.writeTo(expectedBytes);
        assertThat(result.data()).containsExactly(expectedBytes.array());
    }

    @Test
    void multipleTopicPartitions() {
        final Time time = Time.SYSTEM;
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch t0p0b0 = createBatch(time, initialOffset, T0P0 + "-0");
        final MutableRecordBatch t0p0b1 = createBatch(time, t0p0b0.nextOffset(),T0P0 + "-1");
        final MutableRecordBatch t0p0b2 = createBatch(time, t0p0b1.nextOffset(),T0P0 + "-2");

        final MutableRecordBatch t0p1b0 = createBatch(time, initialOffset,T0P1 + "-0");
        final MutableRecordBatch t0p1b1 = createBatch(time, t0p1b0.nextOffset(),T0P1 + "-1");
        final MutableRecordBatch t0p1b2 = createBatch(time, t0p1b1.nextOffset(),T0P1 + "-2");

        final MutableRecordBatch t1p0b0 = createBatch(time, initialOffset,T1P0 + "-0");
        final MutableRecordBatch t1p0b1 = createBatch(time, t1p0b0.nextOffset(),T1P0 + "-1");
        final MutableRecordBatch t1p0b2 = createBatch(time, t1p0b1.nextOffset(),T1P0 + "-2");

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
        final BatchBuffer.CloseResult result = buffer.close(Map.of());
        assertThat(result.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, batchSize, 19, 19),
            CommitBatchRequest.of(T0P0, batchSize, batchSize, 20, 20),
            CommitBatchRequest.of(T0P0, batchSize*2, batchSize, 21, 21),
            CommitBatchRequest.of(T0P1, batchSize*3, batchSize, 19, 19),
            CommitBatchRequest.of(T0P1, batchSize*4, batchSize, 20, 20),
            CommitBatchRequest.of(T0P1, batchSize*5, batchSize, 21, 21),
            CommitBatchRequest.of(T1P0, batchSize*6, batchSize, 19, 19),
            CommitBatchRequest.of(T1P0, batchSize*7, batchSize, 20, 20),
            CommitBatchRequest.of(T1P0, batchSize*8, batchSize, 21, 21)
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
    void failAfterClosing() {
        final Time time = Time.SYSTEM;
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch batch1 = createBatch(time, initialOffset, T0P0 + "-0");
        buffer.addBatch(T0P0, batch1, 0);
        final BatchBuffer.CloseResult result1 = buffer.close(Map.of());
        assertThat(result1.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(T0P0, 0, batch1.sizeInBytes(), initialOffset, initialOffset)
        );
        assertThat(result1.data()).containsExactly(batchToBytes(batch1));
        assertThat(result1.requestIds()).containsExactly(0);

        final MutableRecordBatch batch2 = createBatch(time, initialOffset, T1P0 + "-0-longer");
        assertThatThrownBy(() -> buffer.addBatch(T1P0, batch2, 1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
    }

    @Test
    void failWhenBatchesWithSameOffset() {
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final TopicPartition topicPartition = T0P0;
        final int requestId = 0;

        final MutableRecordBatch batch1 = createBatch(time, initialOffset, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(topicPartition, batch1, requestId);

        final MutableRecordBatch batch2 = createBatch(time, initialOffset, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        assertThatThrownBy(() -> buffer.addBatch(topicPartition, batch2, requestId))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Duplicate base offset 19 for topic-partition topic0-0 and request 0");
    }


    MutableRecordBatch createBatch(Time time, long initialBatchOffset, String... content) {
        final SimpleRecord[] simpleRecords = Arrays.stream(content)
            .map(c -> new SimpleRecord(time.milliseconds(), c.getBytes()))
            .toArray(SimpleRecord[]::new);
        final MemoryRecords records = MemoryRecords.withRecords(initialBatchOffset, Compression.NONE, simpleRecords);
        final Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    byte[] batchToBytes(final RecordBatch batch) {
        final ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        return buf.array();
    }

    @Test
    void addBatchNulls() {
        final Time time = Time.SYSTEM;
        final BatchBuffer buffer = new BatchBuffer();

        assertThatThrownBy(() -> buffer.addBatch(null, createBatch(time, initialOffset), 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicPartition cannot be null");
        assertThatThrownBy(() -> buffer.addBatch(T0P0, null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batch cannot be null");
    }
}
