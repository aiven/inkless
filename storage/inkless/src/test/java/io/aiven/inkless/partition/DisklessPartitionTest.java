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
package io.aiven.inkless.partition;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DisklessCommitRequestData;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import io.aiven.inkless.common.ObjectFormat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class DisklessPartitionTest {
    static final TopicIdPartition TIDP = new TopicIdPartition(new Uuid(100, 200), 0, "topic");
    static final int BROKER_ID = 213;

    @Mock
    UnifiedLog log;

    MockTime time;

    @BeforeEach
    void createMockTime() {
        time = new MockTime();
    }

    @Test
    void x(@TempDir final Path tmp) throws SQLException {
        when(log.dir()).thenReturn(tmp.toFile());

        final DisklessPartition dp = new DisklessPartition(time, TIDP, BROKER_ID, log);

        final var partitionData = new DisklessCommitRequestData.PartitionDisklessCommitData();
        partitionData.setPartition(0);
        partitionData.setBatches(List.of(
            new DisklessCommitRequestData.BatchDisklessCommitData()
                .setMagic(RecordBatch.CURRENT_MAGIC_VALUE)
                .setByteOffset(0)
                .setByteSize(123)
                .setBaseOffset(0)
                .setLastOffset(99)
                .setTimestampType((short) TimestampType.CREATE_TIME.id)
                .setBatchMaxTimestamp(999L)
                .setProducerId(-1)
                .setProducerEpoch((short) -1)
                .setBaseSequence(-1)
                .setLastSequence(-1)
        ));

        dp.commit(
            "f1",
            ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
            19999,
            partitionData
        );
        System.out.println(dp);
    }
}
