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

import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;

import static org.assertj.core.api.Assertions.assertThat;


class InMemoryControlPlaneTest extends AbstractControlPlaneTest {
    @Override
    protected ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo) {
        return new ControlPlaneAndConfigs(new InMemoryControlPlane(time), BASE_CONFIG);
    }

    @Override
    protected void tearDownControlPlane() {
        // no-op
    }

    @Nested
    class Consolidation {
        @Test
        void getWalFilesEligibleForConsolidationDeletionEmptyWhenNoConsolidatedOffset() {
            controlPlane.createTopicAndPartitions(Set.of(
                new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, EXISTING_TOPIC_1_PARTITIONS)
            ));
            controlPlane.commitFile(
                "wal1",
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                BROKER_ID,
                FILE_SIZE,
                List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, 100, 0, 10, 1000, TimestampType.CREATE_TIME))
            );
            assertThat(controlPlane.getWalFilesEligibleForConsolidationDeletion()).isEmpty();
        }

        @Test
        void getWalFilesEligibleForConsolidationDeletionReturnsFileAfterConsolidatedOffsetUpdated() {
            controlPlane.createTopicAndPartitions(Set.of(
                new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, EXISTING_TOPIC_1_PARTITIONS)
            ));
            controlPlane.commitFile(
                "wal1",
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                BROKER_ID,
                FILE_SIZE,
                List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, 100, 0, 10, 1000, TimestampType.CREATE_TIME))
            );
            controlPlane.updateConsolidatedTieredEndOffset(EXISTING_TOPIC_1_ID, 0, 10);
            assertThat(controlPlane.getWalFilesEligibleForConsolidationDeletion())
                .extracting(WalFileEligibleForDeletion::objectKey)
                .containsExactlyInAnyOrder("wal1");
        }

        @Test
        void markWalFilesEligibleForConsolidationDeletionMarksFilesAndTheyAppearInGetFilesToDelete() {
            controlPlane.createTopicAndPartitions(Set.of(
                new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, EXISTING_TOPIC_1_PARTITIONS)
            ));
            controlPlane.commitFile(
                "wal1",
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                BROKER_ID,
                FILE_SIZE,
                List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, 100, 0, 10, 1000, TimestampType.CREATE_TIME))
            );
            controlPlane.updateConsolidatedTieredEndOffset(EXISTING_TOPIC_1_ID, 0, 10);
            int marked = controlPlane.markWalFilesEligibleForConsolidationDeletion();
            assertThat(marked).isEqualTo(1);
            assertThat(controlPlane.getFilesToDelete())
                .extracting(FileToDelete::objectKey)
                .containsExactlyInAnyOrder("wal1");
        }

        @Test
        void updateConsolidatedTieredEndOffsetOnlyIncreases() {
            controlPlane.createTopicAndPartitions(Set.of(
                new CreateTopicAndPartitionsRequest(EXISTING_TOPIC_1_ID, EXISTING_TOPIC_1, EXISTING_TOPIC_1_PARTITIONS)
            ));
            controlPlane.updateConsolidatedTieredEndOffset(EXISTING_TOPIC_1_ID, 0, 100);
            controlPlane.updateConsolidatedTieredEndOffset(EXISTING_TOPIC_1_ID, 0, 50);
            controlPlane.commitFile(
                "wal1",
                ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT,
                BROKER_ID,
                FILE_SIZE,
                List.of(CommitBatchRequest.of(0, EXISTING_TOPIC_1_ID_PARTITION_0, 0, 100, 0, 10, 1000, TimestampType.CREATE_TIME))
            );
            controlPlane.updateConsolidatedTieredEndOffset(EXISTING_TOPIC_1_ID, 0, 10);
            assertThat(controlPlane.getWalFilesEligibleForConsolidationDeletion())
                .extracting(WalFileEligibleForDeletion::objectKey)
                .containsExactlyInAnyOrder("wal1");
        }
    }
}
