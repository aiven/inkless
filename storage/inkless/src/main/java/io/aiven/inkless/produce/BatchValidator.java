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

import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

import java.util.Objects;

/**
 * Validates batch and records metadata.
 * Compared with file-system based {@link org.apache.kafka.storage.internals.log.LogValidator},
 * this class does not update batch offsets or log append time as that is handled by the metadata and updated on read time.
 */
public class BatchValidator {
    final Time time;

    public BatchValidator(final Time time) {
        this.time = time;
    }

    public void validateAndMaybeSetMaxTimestamp(final MutableRecordBatch batch, TimestampType timestampType) {
        Objects.requireNonNull(batch, "batch cannot be null");
        Objects.requireNonNull(timestampType, "timestampType cannot be null");

        long maxBatchTimestamp = RecordBatch.NO_TIMESTAMP;

        for (Record record : batch) {
            if (record.timestamp() > maxBatchTimestamp)
                maxBatchTimestamp = record.timestamp();
        }

        if (timestampType != TimestampType.LOG_APPEND_TIME)
            batch.setMaxTimestamp(timestampType, maxBatchTimestamp);
        else
            batch.setMaxTimestamp(timestampType, time.milliseconds());
            // the append time will be updated by the control plane and updated on read time
    }
}
