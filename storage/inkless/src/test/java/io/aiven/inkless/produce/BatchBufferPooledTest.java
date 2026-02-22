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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;
import io.aiven.inkless.produce.buffer.HeapBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBatchBufferData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BatchBuffer with pooled buffers.
 */
class BatchBufferPooledTest {

    private static final Uuid TOPIC_ID = new Uuid(1000, 1000);
    private static final String TOPIC = "topic";
    private static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);

    private BufferPool pool;
    private Time time;

    @BeforeEach
    void setUp() {
        pool = new ElasticBufferPool(2);
        time = new MockTime();
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void closeWithPoolReturnsPooledBatchBufferData() {
        // Use minPoolSizeBytes=0 to test pooled behavior with small batches
        final BatchBuffer buffer = new BatchBuffer(pool, 0);
        final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-data");
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();

        assertThat(result.data()).isInstanceOf(PooledBatchBufferData.class);
        result.data().release(); // Clean up
    }

    @Test
    void closeWithoutPoolReturnsHeapBatchBufferData() {
        final BatchBuffer buffer = new BatchBuffer(); // No pool
        final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-data");
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();

        assertThat(result.data()).isInstanceOf(HeapBatchBufferData.class);
    }

    @Test
    void closeWithNullPoolReturnsHeapBatchBufferData() {
        final BatchBuffer buffer = new BatchBuffer(null);
        final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-data");
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();

        assertThat(result.data()).isInstanceOf(HeapBatchBufferData.class);
    }

    @Test
    void pooledDataContainsSameContent() {
        final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-content");

        // Create with pool (minPoolSizeBytes=0 to allow small batches)
        final BatchBuffer pooledBuffer = new BatchBuffer(pool, 0);
        pooledBuffer.addBatch(T0P0, batch, 0);
        final BatchBuffer.CloseResult pooledResult = pooledBuffer.close();

        // Create without pool (using same batch type)
        final MutableRecordBatch batch2 = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-content");
        final BatchBuffer heapBuffer = new BatchBuffer();
        heapBuffer.addBatch(T0P0, batch2, 0);
        final BatchBuffer.CloseResult heapResult = heapBuffer.close();

        // Extract bytes from both
        byte[] pooledBytes = BatchBufferTest.extractBytes(pooledResult.data());
        byte[] heapBytes = BatchBufferTest.extractBytes(heapResult.data());

        assertThat(pooledBytes).isEqualTo(heapBytes);
        pooledResult.data().release(); // Clean up pooled buffer
    }

    @Test
    void emptyBufferWithPoolReturnsHeapData() {
        final BatchBuffer buffer = new BatchBuffer(pool);

        final BatchBuffer.CloseResult result = buffer.close();

        // Empty buffers should use heap allocation (size=0, no point in pooling)
        assertThat(result.data()).isInstanceOf(HeapBatchBufferData.class);
        assertThat(result.data().size()).isZero();
    }

    @Test
    void pooledBufferAcquiresFromPool() {
        assertThat(pool.activeBufferCount()).isZero();

        // Use minPoolSizeBytes=0 to test pooled behavior with small batches
        final BatchBuffer buffer = new BatchBuffer(pool, 0);
        final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-data");
        buffer.addBatch(T0P0, batch, 0);

        final BatchBuffer.CloseResult result = buffer.close();

        // Buffer should be acquired from pool
        assertThat(pool.activeBufferCount()).isEqualTo(1);

        // Release returns to pool
        result.data().release();
        assertThat(pool.activeBufferCount()).isZero();
    }

    @Test
    void smallBuffersBelowThresholdUseHeap() {
        // Create buffer with default threshold (64KB)
        final BatchBuffer buffer = new BatchBuffer(pool);
        // Small batch will be below the threshold
        final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "small");
        buffer.addBatch(T0P0, batch, 0);

        // Pool should not be used for small buffers
        assertThat(pool.activeBufferCount()).isZero();

        final BatchBuffer.CloseResult result = buffer.close();

        // Should use heap since batch is below threshold
        assertThat(result.data()).isInstanceOf(HeapBatchBufferData.class);
        assertThat(pool.activeBufferCount()).isZero();
    }

    @Test
    void fallsBackToHeapOnPoolExhaustion() {
        // Create a small pool and exhaust it
        BufferPool smallPool = new ElasticBufferPool(1);
        var b3 = smallPool.acquire(1024 * 1024); // Exhaust it

        try {
            // Use minPoolSizeBytes=0 to test pool exhaustion behavior with small batches
            final BatchBuffer buffer = new BatchBuffer(smallPool, 0);
            final MutableRecordBatch batch = BatchBufferTest.createBatch(TimestampType.CREATE_TIME, time, "test-data");
            buffer.addBatch(T0P0, batch, 0);

            // Pool exhaustion triggers heap fallback via HeapPooledBuffer,
            // which gets wrapped in PooledBatchBufferData (with proper ref counting)
            final BatchBuffer.CloseResult result = buffer.close();

            // HeapPooledBuffer is wrapped in PooledBatchBufferData, not HeapBatchBufferData
            assertThat(result.data()).isInstanceOf(PooledBatchBufferData.class);

            // Verify heap fallback was recorded
            assertThat(smallPool.heapFallbackCount()).isEqualTo(1);

            result.data().release(); // Clean up
        } finally {
            b3.release();
            smallPool.close();
        }
    }
}
