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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP0;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitSuccess;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.rlmmConfigs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.startedSegment;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.updateFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises the exact inputs the consolidation path needs: {@code DisklessLeaderEndPoint}
 * funnels through {@code RemoteLogManager.hasReadableRemoteLogCoverage}, which decides from
 * {@code isReady} plus the listed segment states. This test replicates that decision loop
 * against the PostgreSQL manager.
 */
@Testcontainers
class PostgresRlmmCoverageTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private PostgresRemoteLogMetadataManager rlmm;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        rlmm = new PostgresRemoteLogMetadataManager();
        rlmm.configure(rlmmConfigs(pgContainer));
    }

    @AfterEach
    void tearDown() throws Exception {
        rlmm.close();
        pgContainer.tearDown();
    }

    @Test
    void coverageFollowsSegmentStates() throws Exception {
        // The manager is not ready before the leadership notification. The check returns empty.
        assertEquals(Optional.empty(), coverage(TP0, 50L));

        rlmm.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
        // Ready but nothing copied: no coverage.
        assertEquals(Optional.of(false), coverage(TP0, 50L));
        assertEquals(Optional.empty(), rlmm.highestOffsetForEpoch(TP0, 0));

        // A started segment covers the offset but is unreadable: no answer yet.
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        assertEquals(Optional.empty(), coverage(TP0, 50L));

        // A finished segment covering the offset: covered.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
        assertEquals(Optional.of(true), coverage(TP0, 50L));
        assertEquals(Optional.of(false), coverage(TP0, 150L));
        assertEquals(Optional.of(100L), rlmm.highestOffsetForEpoch(TP0, 0));

        // A delete-started segment covering the offset: no answer again.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        assertEquals(Optional.empty(), coverage(TP0, 50L));
    }

    private Optional<Boolean> coverage(final TopicIdPartition tp, final long offset) throws Exception {
        // Mirrors RemoteLogManager.hasReadableRemoteLogCoverage.
        if (!rlmm.isReady(tp)) {
            return Optional.empty();
        }
        final Iterator<RemoteLogSegmentMetadata> segments = rlmm.listRemoteLogSegments(tp);
        boolean coveringUnreadable = false;
        while (segments.hasNext()) {
            final RemoteLogSegmentMetadata segment = segments.next();
            if (segment.startOffset() > offset || segment.endOffset() < offset) {
                continue;
            }
            if (segment.state() == RemoteLogSegmentState.COPY_SEGMENT_FINISHED) {
                return Optional.of(true);
            }
            if (segment.state() == RemoteLogSegmentState.COPY_SEGMENT_STARTED
                || segment.state() == RemoteLogSegmentState.DELETE_SEGMENT_STARTED) {
                coveringUnreadable = true;
            }
        }
        if (coveringUnreadable) {
            return Optional.empty();
        }
        return Optional.of(false);
    }
}
