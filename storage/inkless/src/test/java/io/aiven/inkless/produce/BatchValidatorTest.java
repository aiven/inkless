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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

class BatchValidatorTest {

    @Test
    void failWhenBatchNull() {
        final BatchValidator batchValidator = new BatchValidator(Time.SYSTEM);
        Assertions.assertThatThrownBy(() -> batchValidator.validateAndMaybeSetMaxTimestamp(null, TimestampType.CREATE_TIME))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("batch cannot be null");
        final MutableRecordBatch batch = createBatchWithTimeSpreadRecords(TimestampType.CREATE_TIME, new MockTime(), 1, "b", "c");
        Assertions.assertThatThrownBy(() -> batchValidator.validateAndMaybeSetMaxTimestamp(batch, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("timestampType cannot be null");
    }

    @Test
    void validateBatchWithCreateTimeTimestampType() {
        final Time time = new MockTime();
        final BatchValidator batchValidator = new BatchValidator(time);

        // given a batch with records with increasing timestamps
        time.sleep(100);
        final long createTimeBase = time.milliseconds();
        final MutableRecordBatch batch = createBatchWithTimeSpreadRecords(TimestampType.CREATE_TIME, time, 1, "b", "c");
        final long expectedTime = createTimeBase + (batch.lastOffset() - batch.baseOffset() + 1);
        // the max time is mutated to force a new crc after update
        batch.setMaxTimestamp(TimestampType.CREATE_TIME, 0);
        final long previousChecksum = batch.checksum();

        // when we validate the batch a bit later
        time.sleep(100);
        final long validationTime = time.milliseconds();
        batchValidator.validateAndMaybeSetMaxTimestamp(batch, TimestampType.CREATE_TIME);

        // then the batch should have the max timestamp set to the last record timestamp
        assertThat(batch.timestampType()).isEqualTo(TimestampType.CREATE_TIME);
        assertThat(batch.maxTimestamp())
            .isNotEqualTo(validationTime)
            .isEqualTo(expectedTime);
        assertThat(batch.checksum()).isNotEqualTo(previousChecksum);
    }

    @Test
    void validateBatchWithAppendTimeTimestampType() {
        final Time time = new MockTime();
        final BatchValidator batchValidator = new BatchValidator(time);

        // given a batch with records with increasing timestamps
        time.sleep(100);
        final long createTimeBase = time.milliseconds();
        final MutableRecordBatch batch = createBatchWithTimeSpreadRecords(TimestampType.LOG_APPEND_TIME, time, 10, "a", "b", "c");
        final long previousChecksum = batch.checksum();

        // when we validate the batch
        time.sleep(100);
        final long validationTime = time.milliseconds();
        batchValidator.validateAndMaybeSetMaxTimestamp(batch, TimestampType.LOG_APPEND_TIME);

        // then the batch should have the max timestamp update to current time (control plane should set the _right_ value on commit)
        assertThat(batch.timestampType()).isEqualTo(TimestampType.LOG_APPEND_TIME);
        assertThat(batch.maxTimestamp())
            .isGreaterThan(createTimeBase)
            .isEqualTo(validationTime);
        assertThat(batch.checksum()).isNotEqualTo(previousChecksum);
    }

    @Test
    void batchAtNoTimestampTime() {
        final Time time = new MockTime();
        final BatchValidator batchValidator = new BatchValidator(time);

        // given a batch with no timestamp
        final SimpleRecord[] simpleRecords = new SimpleRecord[1];
        simpleRecords[0] = new SimpleRecord(RecordBatch.NO_TIMESTAMP, new byte[0]);
        final MutableRecordBatch batch = createBatchWithRecords(TimestampType.CREATE_TIME, simpleRecords);

        // when we validate the batch
        time.sleep(100);
        batchValidator.validateAndMaybeSetMaxTimestamp(batch, TimestampType.CREATE_TIME);

        // then the batch should have the max timestamp set to NO_TIMESTAMP
        assertThat(batch).isNotNull();
        assertThat(batch.maxTimestamp()).isEqualTo(RecordBatch.NO_TIMESTAMP);
    }

    static MutableRecordBatch createBatchWithTimeSpreadRecords(final TimestampType timestampType,
                                                               final Time time,
                                                               final int timeGapBetweenRecords,
                                                               final String... content) {
        final SimpleRecord[] simpleRecords = new SimpleRecord[content.length];
        for (int i = 0; i < content.length; i++) {
            time.sleep(timeGapBetweenRecords);
            simpleRecords[i] = new SimpleRecord(time.milliseconds(), content[i].getBytes());
        }
        return createBatchWithRecords(timestampType, simpleRecords);
    }

    static MutableRecordBatch createBatchWithRecords(final TimestampType timestampType,
                                                     final SimpleRecord[] simpleRecords) {
        final int initialOffset = 19;  // some non-zero number
        final MemoryRecords records = MemoryRecords.withRecords(
            RecordBatch.CURRENT_MAGIC_VALUE,
            initialOffset,
            Compression.NONE,
            timestampType,
            simpleRecords
        );
        final Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }
}