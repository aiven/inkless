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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

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

        final MutableRecordBatch batch1 = createBatch(TimestampType.CREATE_TIME, time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch1, 0);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes());

        final MutableRecordBatch batch2 = createBatch(TimestampType.CREATE_TIME, time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2");
        buffer.addBatch(T0P0, batch2, 1);

        assertThat(buffer.totalSize()).isEqualTo(batch1.sizeInBytes() + batch2.sizeInBytes());
    }

    @Test
    void empty() {
        final BatchBuffer buffer = new BatchBuffer();

        BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.data()).isEmpty();

        result = buffer.close();
        assertThat(result.commitBatchRequests()).isEmpty();
        assertThat(result.data()).isEmpty();
    }

    public static Stream<Arguments> singleBatchParams() {
        final Time time = new MockTime();
        return Stream.of(
            Arguments.of(
                createBatch(TimestampType.CREATE_TIME, time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2"),
                CommitBatchRequest.of(0, T0P0, 0, 181, 19, 21, time.milliseconds(), TimestampType.CREATE_TIME)
            ),
            Arguments.of(
                createIdempotentBatch(time, T0P0 + "-0", T0P0 + "-1", T0P0 + "-2"),
                CommitBatchRequest.idempotent(0, T0P0, 0, 181, 19, 21, time.milliseconds(), TimestampType.CREATE_TIME, 1, (short) 1, 1, 3)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("singleBatchParams")
    void singleBatch(MutableRecordBatch batch, CommitBatchRequest expectedRequest) {
        final BatchBuffer buffer = new BatchBuffer();

        final byte[] beforeAdding = batchToBytes(batch);
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();
        assertThat(result.commitBatchRequests()).containsExactly(expectedRequest);
        assertThat(result.data()).containsExactly(batchToBytes(batch));
        assertThat(result.data()).containsExactly(beforeAdding);
        assertThat(batch.hasProducerId()).isEqualTo(expectedRequest.hasProducerId());
    }

    @Test
    void multipleTopicPartitions() {
        final Time time = new MockTime();
        final BatchBuffer buffer = new BatchBuffer();

        final MutableRecordBatch t0p0b0 = createBatch(TimestampType.CREATE_TIME, time, T0P0 + "-0");
        final MutableRecordBatch t0p0b2 = createBatch(TimestampType.CREATE_TIME, time, T0P0 + "-2");
        final MutableRecordBatch t0p0b1 = createBatch(TimestampType.CREATE_TIME, time, T0P0 + "-1");

        final MutableRecordBatch t0p1b0 = createBatch(TimestampType.LOG_APPEND_TIME, time, T0P1 + "-0");
        final MutableRecordBatch t0p1b1 = createBatch(TimestampType.LOG_APPEND_TIME, time, T0P1 + "-1");
        final MutableRecordBatch t0p1b2 = createBatch(TimestampType.LOG_APPEND_TIME, time, T0P1 + "-2");

        final MutableRecordBatch t1p0b0 = createBatch(TimestampType.LOG_APPEND_TIME, time, T1P0 + "-0");
        final MutableRecordBatch t1p0b1 = createBatch(TimestampType.LOG_APPEND_TIME, time, T1P0 + "-1");
        final MutableRecordBatch t1p0b2 = createBatch(TimestampType.LOG_APPEND_TIME, time, T1P0 + "-2");

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
            CommitBatchRequest.of(0, T0P0, 0, batchSize, 19, 19, time.milliseconds(), TimestampType.CREATE_TIME),
            CommitBatchRequest.of(0, T0P0, batchSize, batchSize, 19, 19, time.milliseconds(), TimestampType.CREATE_TIME),
            CommitBatchRequest.of(1, T0P0, batchSize * 2, batchSize, 19, 19, time.milliseconds(), TimestampType.CREATE_TIME),
            CommitBatchRequest.of(1, T0P1, batchSize * 3, batchSize, 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            CommitBatchRequest.of(2, T0P1, batchSize * 4, batchSize, 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            CommitBatchRequest.of(2, T0P1, batchSize * 5, batchSize, 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            CommitBatchRequest.of(0, T1P0, batchSize * 6, batchSize, 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            CommitBatchRequest.of(1, T1P0, batchSize * 7, batchSize, 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME),
            CommitBatchRequest.of(2, T1P0, batchSize * 8, batchSize, 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
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

        final MutableRecordBatch batch1 = createBatch(TimestampType.LOG_APPEND_TIME, time, T0P0 + "-0");
        buffer.addBatch(T0P0, batch1, 0);
        final BatchBuffer.CloseResult result1 = buffer.close();
        assertThat(result1.commitBatchRequests()).containsExactly(
            CommitBatchRequest.of(0, T0P0, 0, batch1.sizeInBytes(), 19, 19, time.milliseconds(), TimestampType.LOG_APPEND_TIME)
        );
        assertThat(result1.data()).containsExactly(batchToBytes(batch1));

        final MutableRecordBatch batch2 = createBatch(TimestampType.CREATE_TIME, time, T1P0 + "-0-longer");
        assertThatThrownBy(() -> buffer.addBatch(T1P0, batch2, 1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
    }

    static MutableRecordBatch createBatch(TimestampType timestampType, Time time, String... content) {
        final SimpleRecord[] simpleRecords = simpleRecords(time, content);
        final int initialOffset = 19;  // some non-zero number
        final MemoryRecords records = MemoryRecords.withRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, Compression.NONE, timestampType, simpleRecords);
        final Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        final MutableRecordBatch batch = iterator.next();
        // avoid using system clock for tests
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            batch.setMaxTimestamp(timestampType, time.milliseconds());
        }
        return batch;
    }

    static MutableRecordBatch createIdempotentBatch(Time time, String... content) {
        final SimpleRecord[] simpleRecords = simpleRecords(time, content);
        final int initialOffset = 19;  // some non-zero number
        final MemoryRecords records = MemoryRecords.withIdempotentRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, Compression.NONE, 1L, (short) 1, 1, 1, simpleRecords);
        final Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    private static SimpleRecord [] simpleRecords(Time time, String[] content) {
        return Arrays.stream(content)
            .map(c -> new SimpleRecord(time.milliseconds(), c.getBytes()))
            .toArray(SimpleRecord[]::new);
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

        assertThatThrownBy(() -> buffer.addBatch(null, createBatch(TimestampType.CREATE_TIME, time), 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicPartition cannot be null");
        assertThatThrownBy(() -> buffer.addBatch(T0P0, null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batch cannot be null");
    }
}
