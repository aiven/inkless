// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchBufferTest {
    static final Uuid TOPIC_ID_0 = new Uuid(1000, 1000);
    static final Uuid TOPIC_ID_1 = new Uuid(2000, 2000);
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    private static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    private static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    private static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    @Test
    void totalSize() {
        final Time time = Time.SYSTEM;
        final BatchBuffer buffer = new BatchBuffer();

        assertThat(buffer.totalSize()).isZero();

        final MutableRecordBatch batch1 = createBatch(time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, TimestampType.CREATE_TIME, batch1, 0);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes());

        final MutableRecordBatch batch2 = createBatch(time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, TimestampType.CREATE_TIME, batch2, 1);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes() + batch2.sizeInBytes());
    }

    @Test
    void empty() {
        final Time time = new MockTime();
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
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch batch = createBatch(time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        final byte[] beforeAdding = batchToBytes(batch);
        buffer.addBatch(T0P0, TimestampType.CREATE_TIME, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, batch.sizeInBytes(), 3, time.milliseconds(), TimestampType.CREATE_TIME)
        );
        assertThat(result.requestIds()).containsExactly(0);
        assertThat(result.data()).containsExactly(batchToBytes(batch));
        assertThat(result.data()).containsExactly(beforeAdding);
    }

    @Test
    void multipleTopicPartitions() {
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch t0p0b0 = createBatch(time, T0P0 + "-0");
        final MutableRecordBatch t0p0b2 = createBatch(time, T0P0 + "-2");
        final MutableRecordBatch t0p0b1 = createBatch(time, T0P0 + "-1");

        final MutableRecordBatch t0p1b0 = createBatch(time, T0P1 + "-0");
        final MutableRecordBatch t0p1b1 = createBatch(time, T0P1 + "-1");
        final MutableRecordBatch t0p1b2 = createBatch(time, T0P1 + "-2");

        final MutableRecordBatch t1p0b0 = createBatch(time, T1P0 + "-0");
        final MutableRecordBatch t1p0b1 = createBatch(time, T1P0 + "-1");
        final MutableRecordBatch t1p0b2 = createBatch(time, T1P0 + "-2");

        final int batchSize = t0p0b0.sizeInBytes();  // expecting it to be same everywhere
        buffer.addBatch(T0P0, TimestampType.CREATE_TIME, t0p0b0, 0);
        buffer.addBatch(T1P0, TimestampType.LOG_APPEND_TIME, t1p0b0, 0);
        buffer.addBatch(T0P0, TimestampType.CREATE_TIME, t0p0b1, 0);

        buffer.addBatch(T1P0, TimestampType.LOG_APPEND_TIME, t1p0b1, 1);
        buffer.addBatch(T0P1, TimestampType.LOG_APPEND_TIME, t0p1b0, 1);
        buffer.addBatch(T0P0, TimestampType.CREATE_TIME, t0p0b2, 1);

        buffer.addBatch(T0P1, TimestampType.LOG_APPEND_TIME, t0p1b1, 2);
        buffer.addBatch(T0P1, TimestampType.LOG_APPEND_TIME, t0p1b2, 2);
        buffer.addBatch(T1P0, TimestampType.LOG_APPEND_TIME, t1p0b2, 2);

        // Here batches are sorted.
        final BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, batchSize, 1, time.milliseconds(), TimestampType.CREATE_TIME),
            new CommitBatchRequest(T0P0, batchSize, batchSize, 1, time.milliseconds(), TimestampType.CREATE_TIME),
            new CommitBatchRequest(T0P0, batchSize*2, batchSize, 1, time.milliseconds(), TimestampType.CREATE_TIME),
            new CommitBatchRequest(T0P1, batchSize*3, batchSize, 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            new CommitBatchRequest(T0P1, batchSize*4, batchSize, 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            new CommitBatchRequest(T0P1, batchSize*5, batchSize, 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            new CommitBatchRequest(T1P0, batchSize*6, batchSize, 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            new CommitBatchRequest(T1P0, batchSize*7, batchSize, 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            new CommitBatchRequest(T1P0, batchSize*8, batchSize, 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
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
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch batch1 = createBatch(time, T0P0 + "-0");
        buffer.addBatch(T0P0, TimestampType.LOG_APPEND_TIME, batch1, 0);
        final BatchBuffer.CloseResult result1 = buffer.close();
        assertThat(result1.commitBatchRequests()).containsExactly(
            new CommitBatchRequest(T0P0, 0, batch1.sizeInBytes(), 1, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
        );
        assertThat(result1.data()).containsExactly(batchToBytes(batch1));
        assertThat(result1.requestIds()).containsExactly(0);

        final MutableRecordBatch batch2 = createBatch(time, T1P0 + "-0-longer");
        assertThatThrownBy(() -> buffer.addBatch(T1P0, TimestampType.LOG_APPEND_TIME, batch2, 1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
    }

    MutableRecordBatch createBatch(Time time, String... content) {
        final SimpleRecord[] simpleRecords = Arrays.stream(content)
            .map(c -> new SimpleRecord(time.milliseconds(), c.getBytes()))
            .toArray(SimpleRecord[]::new);
        final int initialOffset = 19;  // some non-zero number
        final MemoryRecords records = MemoryRecords.withRecords(initialOffset, Compression.NONE, simpleRecords);
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

        assertThatThrownBy(() -> buffer.addBatch(null, TimestampType.LOG_APPEND_TIME, createBatch(time), 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicPartition cannot be null");
        assertThatThrownBy(() -> buffer.addBatch(T0P0, null, createBatch(time), 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("timestampType cannot be null");
        assertThatThrownBy(() -> buffer.addBatch(T0P0, TimestampType.LOG_APPEND_TIME, null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batch cannot be null");
    }
}
