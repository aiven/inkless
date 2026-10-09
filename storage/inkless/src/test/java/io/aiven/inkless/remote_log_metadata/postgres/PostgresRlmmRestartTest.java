/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.remote_log_metadata.postgres;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP0;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitSuccess;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.partitionDelete;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.rlmmConfigs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.startedSegment;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.toList;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.updateFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Closes the manager and reopens it against the same database. Reads see the same
 * rows after the restart, and mutations continue where they stopped.
 */
@Testcontainers
class PostgresRlmmRestartTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void metadataSurvivesRestart() throws Exception {
        final RemoteLogSegmentMetadata segA = startedSegment(TP0, 0L, 99L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata segB = startedSegment(TP0, 100L, 199L, Map.of(0, 100L));
        final RemoteLogSegmentMetadata.CustomMetadata custom =
            new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{7});
        final RemoteLogSegmentMetadataUpdate finishA =
            updateFor(segA.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        final RemoteLogSegmentMetadataUpdate finishB = updateFor(segB.remoteLogSegmentId(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 1, Optional.of(custom));

        try (PostgresRemoteLogMetadataManager first = new PostgresRemoteLogMetadataManager()) {
            first.configure(rlmmConfigs(pgContainer));
            first.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
            awaitSuccess(first.addRemoteLogSegmentMetadata(segA));
            awaitSuccess(first.addRemoteLogSegmentMetadata(segB));
            awaitSuccess(first.updateRemoteLogSegmentMetadata(finishA));
            awaitSuccess(first.updateRemoteLogSegmentMetadata(finishB));
            awaitSuccess(first.updateRemoteLogSegmentMetadata(
                updateFor(segA.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        }

        try (PostgresRemoteLogMetadataManager second = new PostgresRemoteLogMetadataManager()) {
            second.configure(rlmmConfigs(pgContainer));
            // Readiness is broker-local and resets. Reads do not need it.
            assertFalse(second.isReady(TP0));
            assertEquals(Optional.of(segB.createWithUpdates(finishB)),
                second.remoteLogSegmentMetadata(TP0, 0, 150L));
            assertEquals(Optional.empty(), second.remoteLogSegmentMetadata(TP0, 0, 50L));
            assertEquals(Optional.of(199L), second.highestOffsetForEpoch(TP0, 0));
            assertEquals(2, toList(second.listRemoteLogSegments(TP0)).size());
            assertEquals(2048L, second.remoteLogSize(TP0, 0));

            // Mutations continue: finish the pending delete, then delete the partition.
            second.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
            assertTrue(second.isReady(TP0));
            awaitSuccess(second.updateRemoteLogSegmentMetadata(
                updateFor(segA.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));
            awaitSuccess(second.putRemotePartitionDeleteMetadata(
                partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED)));
            awaitSuccess(second.putRemotePartitionDeleteMetadata(
                partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_STARTED)));
            awaitSuccess(second.putRemotePartitionDeleteMetadata(
                partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_FINISHED)));
        }

        try (PostgresRemoteLogMetadataManager third = new PostgresRemoteLogMetadataManager()) {
            third.configure(rlmmConfigs(pgContainer));
            third.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
            assertThrows(RemoteResourceNotFoundException.class,
                () -> third.remoteLogSegmentMetadata(TP0, 0, 150L));
            assertThrows(RemoteResourceNotFoundException.class,
                () -> third.listRemoteLogSegments(TP0));
        }
    }
}
