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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.produce.buffer.BatchBufferData;
import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.HeapBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBatchBufferData;
import io.aiven.inkless.produce.buffer.PooledBuffer;

class BatchBuffer {
    /**
     * Default minimum buffer size to use the pool (64KB).
     * Smaller buffers don't benefit much from pooling due to lower GC impact.
     */
    static final int DEFAULT_MIN_POOL_SIZE_BYTES = 64 * 1024;

    private final List<BatchHolder> batches = new ArrayList<>();
    private final BufferPool bufferPool;  // nullable - when null, use fresh allocation
    private final int minPoolSizeBytes;

    private int totalSize = 0;
    private boolean closed = false;

    BatchBuffer() {
        this(null, DEFAULT_MIN_POOL_SIZE_BYTES);
    }

    BatchBuffer(final BufferPool bufferPool) {
        this(bufferPool, DEFAULT_MIN_POOL_SIZE_BYTES);
    }

    BatchBuffer(final BufferPool bufferPool, final int minPoolSizeBytes) {
        this.bufferPool = bufferPool;
        this.minPoolSizeBytes = minPoolSizeBytes;
    }

    void addBatch(final TopicIdPartition topicPartition,
                  final MutableRecordBatch batch,
                  final int requestId) {
        Objects.requireNonNull(topicPartition, "topicPartition cannot be null");
        Objects.requireNonNull(batch, "batch cannot be null");

        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        final BatchHolder batchHolder = new BatchHolder(topicPartition, batch, requestId);
        batches.add(batchHolder);
        totalSize += batch.sizeInBytes();
    }

    CloseResult close() {
        final int dataSize = totalSize();

        // Group together by topic-partition.
        // The sort is stable so the relative order of batches of the same partition won't change.
        batches.sort(
            Comparator.comparing((BatchHolder b) -> b.topicIdPartition().topicId())
                .thenComparing(b -> b.topicIdPartition().partition())
        );

        // Try to use pooled buffer to reduce GC pressure from repeated allocations
        PooledBuffer pooledBuffer = tryAcquirePooledBuffer(dataSize);
        final ByteBuffer targetBuffer = (pooledBuffer != null)
            ? pooledBuffer.buffer()
            : ByteBuffer.allocate(dataSize);

        final List<CommitBatchRequest> commitBatchRequests = new ArrayList<>();
        for (final BatchHolder batchHolder : batches) {
            commitBatchRequests.add(
                CommitBatchRequest.of(
                    batchHolder.requestId,
                    batchHolder.topicIdPartition(),
                    targetBuffer.position(),
                    batchHolder.batch
                )
            );
            batchHolder.batch.writeTo(targetBuffer);
        }

        closed = true;

        final BatchBufferData data;
        if (pooledBuffer != null) {
            targetBuffer.flip();
            data = new PooledBatchBufferData(pooledBuffer, dataSize);
        } else {
            data = new HeapBatchBufferData(targetBuffer.array());
        }

        return new CloseResult(commitBatchRequests, data);
    }

    int totalSize() {
        return totalSize;
    }

    /**
     * Attempts to acquire a pooled buffer. Returns null if pool is not available
     * or size is below the minimum threshold.
     *
     * <p>ElasticBufferPool.acquire() is non-blocking and handles fallback internally,
     * so we don't need try-catch or manual fallback recording here.
     */
    private PooledBuffer tryAcquirePooledBuffer(final int size) {
        if (bufferPool == null || size == 0) {
            return null;
        }
        // Skip pool for small buffers - pooling overhead not worth it for small sizes
        if (size < minPoolSizeBytes) {
            return null;
        }
        return bufferPool.acquire(size);
    }

    private record BatchHolder(TopicIdPartition topicIdPartition,
                               MutableRecordBatch batch,
                               int requestId) {
    }

    /**
     * The result of closing a batch buffer.
     *
     * @param commitBatchRequests commit batch requests matching in order the batches in {@code data}.
     */
    record CloseResult(List<CommitBatchRequest> commitBatchRequests,
                       BatchBufferData data) {
    }
}
