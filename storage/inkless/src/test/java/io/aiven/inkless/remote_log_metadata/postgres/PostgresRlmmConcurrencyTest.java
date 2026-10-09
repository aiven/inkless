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

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.CLUSTER_ID;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP0;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.assertCauseIs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitSuccess;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.partitionDelete;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.rlmmConfigs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.startedSegment;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.toList;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.updateFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Races two managers against the same database. Concurrent mutations serialize on the
 * partition row, so every future completes and the state converges.
 */
@Testcontainers
class PostgresRlmmConcurrencyTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private PostgresRemoteLogMetadataManager rlmmA;
    private PostgresRemoteLogMetadataManager rlmmB;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        rlmmA = new PostgresRemoteLogMetadataManager();
        rlmmA.configure(rlmmConfigs(pgContainer));
        rlmmA.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
        rlmmB = new PostgresRemoteLogMetadataManager();
        rlmmB.configure(rlmmConfigs(pgContainer));
        rlmmB.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
    }

    @AfterEach
    void tearDown() throws Exception {
        rlmmA.close();
        rlmmB.close();
        pgContainer.tearDown();
    }

    @Test
    void concurrentFinishOfSameSegment() throws Exception {
        final RemoteLogSegmentMetadata started = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmmA.addRemoteLogSegmentMetadata(started));

        final int racers = 8;
        final ExecutorService pool = Executors.newFixedThreadPool(racers);
        try {
            final CountDownLatch gate = new CountDownLatch(1);
            final List<Future<?>> runs = new ArrayList<>();
            for (int i = 0; i < racers; i++) {
                final PostgresRemoteLogMetadataManager manager = i % 2 == 0 ? rlmmA : rlmmB;
                runs.add(pool.submit(() -> {
                    gate.await(30, TimeUnit.SECONDS);
                    manager.updateRemoteLogSegmentMetadata(
                        updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED))
                        .get(30, TimeUnit.SECONDS);
                    return null;
                }));
            }
            gate.countDown();
            for (final Future<?> run : runs) {
                run.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, toList(rlmmA.listRemoteLogSegments(TP0)).size());
        assertEquals(Optional.of(100L), rlmmB.highestOffsetForEpoch(TP0, 0));
        assertTrue(rlmmA.remoteLogSegmentMetadata(TP0, 0, 50L).isPresent());
    }

    @Test
    void concurrentAddsOfDistinctSegments() throws Exception {
        final int perManager = 10;
        final ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            final CountDownLatch gate = new CountDownLatch(1);
            final List<Future<?>> runs = new ArrayList<>();
            for (int i = 0; i < perManager; i++) {
                final long baseA = i * 100L;
                final long baseB = 10_000L + i * 100L;
                runs.add(pool.submit(() -> {
                    gate.await(30, TimeUnit.SECONDS);
                    awaitSuccess(rlmmA.addRemoteLogSegmentMetadata(
                        startedSegment(TP0, baseA, baseA + 99, Map.of(0, baseA))));
                    return null;
                }));
                runs.add(pool.submit(() -> {
                    gate.await(30, TimeUnit.SECONDS);
                    awaitSuccess(rlmmB.addRemoteLogSegmentMetadata(
                        startedSegment(TP0, baseB, baseB + 99, Map.of(0, baseB))));
                    return null;
                }));
            }
            gate.countDown();
            for (final Future<?> run : runs) {
                run.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(2 * perManager, toList(rlmmA.listRemoteLogSegments(TP0)).size());
        assertEquals(2 * perManager, toList(rlmmB.listRemoteLogSegments(TP0)).size());
    }

    @Test
    void concurrentDoubleDelete() throws Exception {
        final List<RemoteLogSegmentId> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final RemoteLogSegmentMetadata started =
                startedSegment(TP0, i * 100L, i * 100L + 99, Map.of(0, (long) i * 100L));
            awaitSuccess(rlmmA.addRemoteLogSegmentMetadata(started));
            awaitSuccess(rlmmA.updateRemoteLogSegmentMetadata(
                updateFor(started.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED)));
            ids.add(started.remoteLogSegmentId());
        }

        // Both managers run the full delete flow for every segment at once.
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (final RemoteLogSegmentId id : ids) {
            futures.add(rlmmA.updateRemoteLogSegmentMetadata(
                updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
            futures.add(rlmmB.updateRemoteLogSegmentMetadata(
                updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_STARTED)));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        futures.clear();
        for (final RemoteLogSegmentId id : ids) {
            futures.add(rlmmA.updateRemoteLogSegmentMetadata(
                updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));
            futures.add(rlmmB.updateRemoteLogSegmentMetadata(
                updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);

        assertEquals(List.of(), toList(rlmmA.listRemoteLogSegments(TP0)));
        assertEquals(List.of(), toList(rlmmB.listRemoteLogSegments(TP0)));
        // Deletion does not lower the watermark.
        assertEquals(Optional.of(499L), rlmmA.highestOffsetForEpoch(TP0, 0));
    }

    @Test
    void partitionDeleteClearsAConcurrentAdd() throws Exception {
        final RemoteLogSegmentMetadata existing = startedSegment(TP0, 0L, 99L, Map.of(0, 0L));
        awaitSuccess(rlmmA.addRemoteLogSegmentMetadata(existing));
        awaitSuccess(rlmmA.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED)));
        awaitSuccess(rlmmA.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_STARTED)));

        final CompletableFuture<Void> racingAdd = rlmmB.addRemoteLogSegmentMetadata(
            startedSegment(TP0, 100L, 199L, Map.of(0, 100L)));
        final CompletableFuture<Void> finishDelete = rlmmA.putRemotePartitionDeleteMetadata(
            partitionDelete(TP0, RemotePartitionDeleteState.DELETE_PARTITION_FINISHED));

        awaitSuccess(finishDelete);
        try {
            awaitSuccess(racingAdd);
        } catch (final ExecutionException e) {
            assertCauseIs(e.getCause(), RemoteResourceNotFoundException.class);
        }

        assertThrows(RemoteResourceNotFoundException.class, () -> rlmmA.listRemoteLogSegments(TP0));
        assertThrows(RemoteResourceNotFoundException.class, () -> rlmmB.highestOffsetForEpoch(TP0, 0));
        assertEquals(0L, partitionDataRows());
    }

    @Test
    void concurrentSameStartShadowRace() throws Exception {
        final RemoteLogSegmentMetadata wide = startedSegment(TP0, 0L, 200L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata narrow = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        awaitSuccess(rlmmA.addRemoteLogSegmentMetadata(wide));
        awaitSuccess(rlmmA.addRemoteLogSegmentMetadata(narrow));

        final CompletableFuture<Void> finishWide = rlmmA.updateRemoteLogSegmentMetadata(
            updateFor(wide.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED));
        final CompletableFuture<Void> finishNarrow = rlmmB.updateRemoteLogSegmentMetadata(
            updateFor(narrow.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED));
        CompletableFuture.allOf(finishWide, finishNarrow).get(60, TimeUnit.SECONDS);

        // Serialization decides the order, but the watermark and the full listing converge.
        // The per-epoch listing contains one entry when the narrow finisher takes the slot.
        assertEquals(Optional.of(200L), rlmmA.highestOffsetForEpoch(TP0, 0));
        final int epochListed = toList(rlmmA.listRemoteLogSegments(TP0, 0)).size();
        assertTrue(epochListed == 1 || epochListed == 2, "Unexpected per-epoch listing size: " + epochListed);
        assertEquals(2, toList(rlmmA.listRemoteLogSegments(TP0)).size());
        assertTrue(rlmmA.remoteLogSegmentMetadata(TP0, 0, 50L).isPresent());
    }

    private long partitionDataRows() throws Exception {
        final UUID topicId =
            new UUID(TP0.topicId().getMostSignificantBits(), TP0.topicId().getLeastSignificantBits());
        try (
            Connection connection = DriverManager.getConnection(
                pgContainer.getUserJdbcUrl(), pgContainer.getUsername(), pgContainer.getPassword());
            PreparedStatement statement = connection.prepareStatement(
                "SELECT "
                    + "(SELECT COUNT(*) FROM inkless_rlmm.rlmm_segments "
                    + "WHERE cluster_id = ? AND topic_id = ? AND partition_id = ?) + "
                    + "(SELECT COUNT(*) FROM inkless_rlmm.rlmm_epoch_state "
                    + "WHERE cluster_id = ? AND topic_id = ? AND partition_id = ?)")
        ) {
            statement.setString(1, CLUSTER_ID);
            statement.setObject(2, topicId);
            statement.setInt(3, TP0.partition());
            statement.setString(4, CLUSTER_ID);
            statement.setObject(5, topicId);
            statement.setInt(6, TP0.partition());
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getLong(1);
            }
        }
    }
}
