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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

public record CommitBatchResponse(Errors errors, long assignedBaseOffset, long logAppendTime, long logStartOffset, boolean isDuplicate, CommitBatchRequest request) {
    public static CommitBatchResponse of(Errors errors, long assignedBaseOffset, long logAppendTime, long logStartOffset) {
        return new CommitBatchResponse(errors, assignedBaseOffset, logAppendTime, logStartOffset, false, null);
    }

    public static CommitBatchResponse success(final long assignedBaseOffset, final long timestamp, final long logStartOffset, final CommitBatchRequest request) {
        return new CommitBatchResponse(Errors.NONE, assignedBaseOffset, timestamp, logStartOffset, false, request);
    }

    public static CommitBatchResponse unknownTopicOrPartition() {
        return new CommitBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, RecordBatch.NO_TIMESTAMP, -1, false, null);
    }

    public static CommitBatchResponse invalidProducerEpoch() {
        return new CommitBatchResponse(Errors.INVALID_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, -1, false, null);
    }

    public static CommitBatchResponse sequenceOutOfOrder(final CommitBatchRequest request) {
        return new CommitBatchResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1, RecordBatch.NO_TIMESTAMP, -1, false, request);
    }

    public static CommitBatchResponse ofDuplicate(long assignedBaseOffset, long batchTimestamp, long logStartOffset) {
        return new CommitBatchResponse(Errors.NONE, assignedBaseOffset, batchTimestamp, logStartOffset, true, null);
    }
}
