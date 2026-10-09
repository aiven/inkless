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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP0;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP1;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.assertCauseIs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitFailureCause;
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

@Testcontainers
class PostgresRemoteLogMetadataManagerTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private PostgresRemoteLogMetadataManager rlmm;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        rlmm = new PostgresRemoteLogMetadataManager();
        rlmm.configure(rlmmConfigs(pgContainer));
        rlmm.onPartitionLeadershipChanges(Set.of(TP0, TP1), Set.of());
    }

    @AfterEach
    void tearDown() throws Exception {
        rlmm.close();
        pgContainer.tearDown();
    }

    @Test
    void addFinishLookupRoundTrip() throws Exception {
        final RemoteLogSegmentMetadata started =
            startedSegment(TP0, 0L, 100L, Map.of(0, 0L, 1, 20L, 2, 80L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));

        // A started segment is listed but not readable. The watermark query returns empty.
        assertEquals(Optional.empty(), rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));
        assertEquals(Optional.empty(), rlmm.highestOffsetForEpoch(TP0, 1));
        assertEquals(List.of(started), toList(rlmm.listRemoteLogSegments(TP0)));

        final RemoteLogSegmentMetadataUpdate finished =
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));

        final RemoteLogSegmentMetadata expected = started.createWithUpdates(finished);
        assertEquals(Optional.of(expected), rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));
        assertEquals(Optional.of(19L), rlmm.highestOffsetForEpoch(TP0, 0));
        assertEquals(Optional.of(79L), rlmm.highestOffsetForEpoch(TP0, 1));
        assertEquals(Optional.of(100L), rlmm.highestOffsetForEpoch(TP0, 2));
        assertEquals(List.of(expected), toList(rlmm.listRemoteLogSegments(TP0)));
        assertEquals(List.of(expected), toList(rlmm.listRemoteLogSegments(TP0, 1)));
        assertEquals(1024L, rlmm.remoteLogSize(TP0, 1));
    }

    @Test
    void duplicateAddWithSameContentSucceeds() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        assertEquals(List.of(started), toList(rlmm.listRemoteLogSegments(TP0)));
    }

    @Test
    void duplicateAddWithDifferentContentFails() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        final RemoteLogSegmentMetadata different = startedSegment(started.remoteLogSegmentId(),
            0L, 200L, Map.of(0, 0L), false, Optional.empty());
        assertCauseIs(awaitFailureCause(rlmm.addRemoteLogSegmentMetadata(different)),
            IllegalArgumentException.class);
        final RemoteLogSegmentMetadata differentCustomMetadata = startedSegment(started.remoteLogSegmentId(),
            0L, 100L, Map.of(0, 0L), false,
            Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{1})));
        assertCauseIs(awaitFailureCause(rlmm.addRemoteLogSegmentMetadata(differentCustomMetadata)),
            IllegalArgumentException.class);
    }

    @Test
    void addWithWrongStateThrowsSynchronously() {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata finished = started.createWithUpdates(
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED));
        assertThrows(IllegalArgumentException.class, () -> rlmm.addRemoteLogSegmentMetadata(finished));
    }

    @Test
    void updateUnknownSegmentFailsWithNotFound() throws Exception {
        final RemoteLogSegmentMetadataUpdate update = updateFor(
            new RemoteLogSegmentId(TP0, Uuid.randomUuid()), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        assertCauseIs(awaitFailureCause(rlmm.updateRemoteLogSegmentMetadata(update)),
            RemoteResourceNotFoundException.class);
    }

    @Test
    void updateWithCopyStartedStateThrowsSynchronously() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        final RemoteLogSegmentMetadataUpdate invalid =
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_STARTED);
        assertThrows(IllegalArgumentException.class, () -> rlmm.updateRemoteLogSegmentMetadata(invalid));
    }

    @Test
    void invalidSegmentTransitionsAreNoOpSuccess() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        final RemoteLogSegmentId id = started.remoteLogSegmentId();

        // Skipping DELETE_SEGMENT_STARTED is invalid. The segment remains started.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));
        assertEquals(List.of(started), toList(rlmm.listRemoteLogSegments(TP0)));

        // Finish, then retry the finish after deletion started: still a no-op success.
        final RemoteLogSegmentMetadataUpdate finished =
            updateFor(id, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));
        assertEquals(Optional.empty(), rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));

        // The terminal state rejects later transitions. Delete-started can no longer apply.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        assertEquals(List.of(), toList(rlmm.listRemoteLogSegments(TP0)));
    }

    @Test
    void sameStateRetriesSucceed() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        final RemoteLogSegmentId id = started.remoteLogSegmentId();

        final RemoteLogSegmentMetadataUpdate finished =
            updateFor(id, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));
        assertEquals(Optional.of(started.createWithUpdates(finished)),
            rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));

        final RemoteLogSegmentMetadataUpdate deleteStarted =
            updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(deleteStarted));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(deleteStarted));
        assertEquals(Optional.empty(), rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));
    }

    @Test
    void customMetadataAndTxnFlagRoundTrip() throws Exception {
        final RemoteLogSegmentMetadata.CustomMetadata addedCustom =
            new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{1, 2, 3});
        final RemoteLogSegmentMetadata started = startedSegment(
            new RemoteLogSegmentId(TP0, Uuid.randomUuid()), 0L, 100L, Map.of(0, 0L), true,
            Optional.of(addedCustom));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));

        // The finish update replaces the custom metadata, like createWithUpdates.
        final RemoteLogSegmentMetadata.CustomMetadata finishedCustom =
            new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{9, 9});
        final RemoteLogSegmentMetadataUpdate finished = updateFor(started.remoteLogSegmentId(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 1, Optional.of(finishedCustom));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));

        final RemoteLogSegmentMetadata expected = started.createWithUpdates(finished);
        assertTrue(expected.isTxnIdxEmpty());
        assertEquals(Optional.of(expected), rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));
        assertEquals(List.of(expected), toList(rlmm.listRemoteLogSegments(TP0)));
    }

    @Test
    void listSegmentsOrderingAndStateFilter() throws Exception {
        final RemoteLogSegmentMetadata segA = startedSegment(TP0, 100L, 199L, Map.of(0, 100L));
        final RemoteLogSegmentMetadata segB = startedSegment(TP0, 0L, 99L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata segC = startedSegment(TP0, 200L, 299L, Map.of(0, 200L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(segA));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(segB));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(segC));

        // Lists sort by start offset ascending regardless of insertion order.
        assertEquals(List.of(segB, segA, segC), toList(rlmm.listRemoteLogSegments(TP0, 0)));

        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(segB.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(segB.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(segC.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(segC.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(segC.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));

        // Started and delete-started segments remain listed, but finished deletes are gone.
        final List<RemoteLogSegmentMetadata> listed = toList(rlmm.listRemoteLogSegments(TP0));
        assertEquals(2, listed.size());
        assertEquals(segB.remoteLogSegmentId(), listed.get(0).remoteLogSegmentId());
        assertEquals(RemoteLogSegmentState.DELETE_SEGMENT_STARTED, listed.get(0).state());
        assertEquals(segA.remoteLogSegmentId(), listed.get(1).remoteLogSegmentId());
        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_STARTED, listed.get(1).state());
    }

    @Test
    void nextSegmentWithTxnIndexWalksForward() throws Exception {
        final RemoteLogSegmentMetadata noTxn = startedSegment(
            new RemoteLogSegmentId(TP0, Uuid.randomUuid()), 0L, 99L, Map.of(0, 0L), true, Optional.empty());
        final RemoteLogSegmentMetadata withTxn = startedSegment(
            new RemoteLogSegmentId(TP0, Uuid.randomUuid()), 100L, 199L, Map.of(0, 100L), false, Optional.empty());
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(noTxn));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(withTxn));
        final RemoteLogSegmentMetadataUpdate noTxnFinished =
            updateFor(noTxn.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        final RemoteLogSegmentMetadataUpdate withTxnFinished =
            updateFor(withTxn.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(noTxnFinished));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(withTxnFinished));

        final RemoteLogSegmentMetadata expectedTxn = withTxn.createWithUpdates(withTxnFinished);
        assertEquals(Optional.of(expectedTxn), rlmm.nextSegmentWithTxnIndex(TP0, 0, 50L));
        assertEquals(Optional.of(expectedTxn), rlmm.nextSegmentWithTxnIndex(TP0, 0, 150L));
        // Sanity check the walk start: the containing segment is the txn-empty one.
        assertEquals(Optional.of(noTxn.createWithUpdates(noTxnFinished)),
            rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));

        // Deleting the first segment leaves a gap: the walk stops instead of skipping ahead.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(
            updateFor(noTxn.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        assertEquals(Optional.empty(), rlmm.nextSegmentWithTxnIndex(TP0, 0, 50L));
        assertEquals(Optional.of(expectedTxn), rlmm.nextSegmentWithTxnIndex(TP0, 0, 150L));
    }

    @Test
    void duplicateSameStartSupersedes() throws Exception {
        final RemoteLogSegmentMetadata first = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata second = startedSegment(TP0, 0L, 200L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(first));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(second));
        final RemoteLogSegmentMetadataUpdate firstFinished =
            updateFor(first.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        final RemoteLogSegmentMetadataUpdate secondFinished =
            updateFor(second.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(firstFinished));
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(secondFinished));

        final RemoteLogSegmentMetadata expectedSecond = second.createWithUpdates(secondFinished);
        assertEquals(Optional.of(expectedSecond), rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));
        assertEquals(Optional.of(expectedSecond), rlmm.remoteLogSegmentMetadata(TP0, 0, 150L));
        assertEquals(Optional.of(200L), rlmm.highestOffsetForEpoch(TP0, 0));
        // Both segments remain listed. The superseded one is unreferenced, not removed.
        assertEquals(2, toList(rlmm.listRemoteLogSegments(TP0, 0)).size());
    }

    @Test
    void staleSameStartShadowsAndFinishRetryRestores() throws Exception {
        final RemoteLogSegmentMetadata first = startedSegment(TP0, 0L, 200L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata stale = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(first));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(stale));
        final RemoteLogSegmentMetadataUpdate firstFinished =
            updateFor(first.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(firstFinished));
        final RemoteLogSegmentMetadataUpdate staleFinished =
            updateFor(stale.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(staleFinished));

        // The stale finisher silently takes the slot without covering the watermark.
        final RemoteLogSegmentMetadata expectedStale = stale.createWithUpdates(staleFinished);
        assertEquals(Optional.of(expectedStale), rlmm.remoteLogSegmentMetadata(TP0, 0, 50L));
        assertEquals(Optional.empty(), rlmm.remoteLogSegmentMetadata(TP0, 0, 150L));
        assertEquals(Optional.of(200L), rlmm.highestOffsetForEpoch(TP0, 0));
        assertEquals(List.of(expectedStale), toList(rlmm.listRemoteLogSegments(TP0, 0)));
        // The shadowed segment keeps its lineage in the unfiltered list.
        assertEquals(2, toList(rlmm.listRemoteLogSegments(TP0)).size());

        // Retrying the original finish restores it and demotes the stale one.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(firstFinished));
        final RemoteLogSegmentMetadata expectedFirst = first.createWithUpdates(firstFinished);
        assertEquals(Optional.of(expectedFirst), rlmm.remoteLogSegmentMetadata(TP0, 0, 150L));
        assertEquals(2, toList(rlmm.listRemoteLogSegments(TP0, 0)).size());
    }

    @Test
    void staleNarrowFinisherPunchesReadHole() throws Exception {
        // A stale finisher that starts inside a covering segment takes the floor slot
        // without covering the watermark, so offsets past its end read empty upstream.
        final RemoteLogSegmentMetadata covering = startedSegment(TP0, 150L, 229L, Map.of(1, 150L));
        final RemoteLogSegmentMetadata narrow = startedSegment(TP0, 180L, 190L, Map.of(1, 180L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(covering));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(narrow));
        final RemoteLogSegmentMetadataUpdate coveringFinished =
            updateFor(covering.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(coveringFinished));
        final RemoteLogSegmentMetadataUpdate narrowFinished =
            updateFor(narrow.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(narrowFinished));

        assertEquals(Optional.of(narrow.createWithUpdates(narrowFinished)),
            rlmm.remoteLogSegmentMetadata(TP0, 1, 185L));
        assertEquals(Optional.empty(), rlmm.remoteLogSegmentMetadata(TP0, 1, 199L));
        assertEquals(Optional.of(229L), rlmm.highestOffsetForEpoch(TP0, 1));

        // Re-finishing the covering segment closes the hole again.
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(coveringFinished));
        assertEquals(Optional.of(covering.createWithUpdates(coveringFinished)),
            rlmm.remoteLogSegmentMetadata(TP0, 1, 199L));
    }

    @Test
    void emptyReadsOnUnknownPartition() throws Exception {
        final TopicIdPartition unknown = new TopicIdPartition(Uuid.randomUuid(), TP0.topicPartition());
        assertEquals(Optional.empty(), rlmm.remoteLogSegmentMetadata(unknown, 0, 0L));
        assertEquals(Optional.empty(), rlmm.highestOffsetForEpoch(unknown, 0));
        assertEquals(Optional.empty(), rlmm.nextSegmentWithTxnIndex(unknown, 0, 0L));
        assertEquals(List.of(), toList(rlmm.listRemoteLogSegments(unknown)));
        assertEquals(List.of(), toList(rlmm.listRemoteLogSegments(unknown, 0)));
        assertEquals(0L, rlmm.remoteLogSize(unknown, 0));
    }

    @Test
    void remotePartitionDeletionLifecycle() throws Exception {
        final RemoteLogSegmentMetadata started =
            startedSegment(TP0, 0L, 100L, Map.of(0, 0L, 1, 20L, 2, 50L, 3, 80L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        final RemoteLogSegmentMetadataUpdate finished =
            updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        awaitSuccess(rlmm.updateRemoteLogSegmentMetadata(finished));
        final RemoteLogSegmentMetadata expected = started.createWithUpdates(finished);
        assertEquals(Optional.of(expected), rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));

        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED)));
        assertEquals(Optional.of(expected), rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));

        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_STARTED)));
        assertEquals(Optional.of(expected), rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));

        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_FINISHED)));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.highestOffsetForEpoch(TP0, 1));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.nextSegmentWithTxnIndex(TP0, 1, 30L));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.listRemoteLogSegments(TP0));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.listRemoteLogSegments(TP0, 1));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.remoteLogSize(TP0, 1));

        // Once finished, the tombstone sticks and later deletes are no-ops.
        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED)));
        assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));

        // Other partitions are unaffected.
        final RemoteLogSegmentMetadata other = startedSegment(TP1, 0L, 10L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(other));
        assertEquals(List.of(other), toList(rlmm.listRemoteLogSegments(TP1)));
    }

    @Test
    void partitionDeleteInvalidTransitionsAreNoOpSuccess() throws Exception {
        // Starting before marking is invalid. Marking afterwards still works.
        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_STARTED)));
        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED)));
        awaitSuccess(rlmm.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED)));

        // Segments stay usable until the finished marker clears them.
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 10L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        assertEquals(List.of(started), toList(rlmm.listRemoteLogSegments(TP0)));
    }

    @Test
    void leadershipControlsReadiness() throws Exception {
        final TopicIdPartition other =
            new TopicIdPartition(Uuid.randomUuid(), TP0.topicPartition());
        assertTrue(rlmm.isReady(TP0));
        assertFalse(rlmm.isReady(other));

        rlmm.onStopPartitions(Set.of(TP0));
        assertFalse(rlmm.isReady(TP0));

        rlmm.onPartitionLeadershipChanges(Set.of(), Set.of(other));
        assertTrue(rlmm.isReady(other));
    }

    @Test
    void operationsAfterCloseFail() throws Exception {
        rlmm.close();
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 10L, Map.of(0, 0L));
        assertThrows(IllegalStateException.class, () -> rlmm.addRemoteLogSegmentMetadata(started));
        assertThrows(IllegalStateException.class, () -> rlmm.remoteLogSegmentMetadata(TP0, 0, 0L));
        assertThrows(IllegalStateException.class, () -> rlmm.configure(rlmmConfigs(pgContainer)));
        assertThrows(IllegalStateException.class, () -> rlmm.onStopPartitions(Set.of(TP0)));
        assertFalse(rlmm.isReady(TP0));
        rlmm.close();
    }

    @Test
    void forcedCloseCompletesQueuedMutations() throws Exception {
        rlmm.close();
        final Map<String, String> configs = rlmmConfigs(pgContainer);
        configs.put("mutation.threads", "1");
        rlmm = new PostgresRemoteLogMetadataManager(Time.SYSTEM, Duration.ZERO);
        rlmm.configure(configs);
        rlmm.onPartitionLeadershipChanges(Set.of(TP0), Set.of());

        final RemoteLogSegmentMetadata first = startedSegment(TP0, 0L, 10L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata queued = startedSegment(TP0, 11L, 20L, Map.of(0, 11L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(first));

        try (
            Connection connection = DriverManager.getConnection(
                pgContainer.getUserJdbcUrl(), pgContainer.getUsername(), pgContainer.getPassword());
            PreparedStatement statement = connection.prepareStatement(
                "SELECT revision FROM inkless_rlmm.rlmm_partitions "
                    + "WHERE cluster_id = ? AND topic_id = ? AND partition_id = ? FOR UPDATE")
        ) {
            connection.setAutoCommit(false);
            statement.setString(1, RlmmTestFixtures.CLUSTER_ID);
            statement.setObject(2, new UUID(
                TP0.topicId().getMostSignificantBits(), TP0.topicId().getLeastSignificantBits()));
            statement.setInt(3, TP0.partition());
            assertTrue(statement.executeQuery().next());

            final CompletableFuture<Void> blocked = rlmm.updateRemoteLogSegmentMetadata(
                updateFor(first.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED));
            final CompletableFuture<Void> notStarted = rlmm.addRemoteLogSegmentMetadata(queued);

            rlmm.close();

            assertCauseIs(awaitFailureCause(blocked), IllegalStateException.class);
            assertCauseIs(awaitFailureCause(notStarted), IllegalStateException.class);
            connection.rollback();
        }
    }

    @Test
    void configureRequiresClusterId() throws Exception {
        final Map<String, String> configs = rlmmConfigs(pgContainer);
        configs.remove("cluster.id");
        try (PostgresRemoteLogMetadataManager unconfigured = new PostgresRemoteLogMetadataManager()) {
            assertThrows(ConfigException.class, () -> unconfigured.configure(configs));
        }
    }

    @Test
    void configureFailsFastOnUnreachableDatabase() throws Exception {
        final Map<String, String> configs = rlmmConfigs(pgContainer);
        configs.put("connection.string", "jdbc:postgresql://127.0.0.1:1/nope");
        try (PostgresRemoteLogMetadataManager unconfigured = new PostgresRemoteLogMetadataManager()) {
            assertThrows(KafkaException.class, () -> unconfigured.configure(configs));
        }
    }

    @Test
    void configureRetryAfterFailure() throws Exception {
        final Map<String, String> badConfigs = rlmmConfigs(pgContainer);
        badConfigs.remove("cluster.id");
        try (PostgresRemoteLogMetadataManager retrying = new PostgresRemoteLogMetadataManager()) {
            assertThrows(ConfigException.class, () -> retrying.configure(badConfigs));
            retrying.configure(rlmmConfigs(pgContainer));
            retrying.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
            assertTrue(retrying.isReady(TP0));
        }
    }

    @Test
    void doubleConfigureIsIgnored() throws Exception {
        rlmm.configure(rlmmConfigs(pgContainer));
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 10L, Map.of(0, 0L));
        awaitSuccess(rlmm.addRemoteLogSegmentMetadata(started));
        assertEquals(List.of(started), toList(rlmm.listRemoteLogSegments(TP0)));
    }
}
