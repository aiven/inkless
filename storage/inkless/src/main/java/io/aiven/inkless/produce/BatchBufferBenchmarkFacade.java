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
import org.apache.kafka.common.record.MutableRecordBatch;

import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.produce.buffer.BufferPool;

/**
 * Public facade for benchmarking BatchBuffer operations.
 * This class exposes package-private BatchBuffer for JMH benchmarks.
 */
public final class BatchBufferBenchmarkFacade {

    private final BatchBuffer buffer;

    /**
     * Creates a facade with optional buffer pool.
     *
     * @param bufferPool the buffer pool to use, or null for heap allocation
     */
    public BatchBufferBenchmarkFacade(BufferPool bufferPool) {
        this.buffer = new BatchBuffer(bufferPool);
    }

    /**
     * Creates a facade with optional buffer pool and custom minimum pool size.
     *
     * @param bufferPool the buffer pool to use, or null for heap allocation
     * @param minPoolSizeBytes minimum buffer size to use the pool (0 = always use pool)
     */
    public BatchBufferBenchmarkFacade(BufferPool bufferPool, int minPoolSizeBytes) {
        this.buffer = new BatchBuffer(bufferPool, minPoolSizeBytes);
    }

    public void addBatch(TopicIdPartition topicPartition, MutableRecordBatch batch, int requestId) {
        buffer.addBatch(topicPartition, batch, requestId);
    }

    public int totalSize() {
        return buffer.totalSize();
    }

    /**
     * Close the buffer and return the result.
     * @return CloseResult containing commitBatchRequests and BatchBufferData
     */
    public CloseResult close() {
        BatchBuffer.CloseResult result = buffer.close();
        return new CloseResult(result.commitBatchRequests().size(), result.data());
    }

    /**
     * Simplified close result for benchmarking.
     */
    public record CloseResult(int commitBatchRequestCount, BatchBufferData data) {
    }
}
