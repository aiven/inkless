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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataCache;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.TP0;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.assertCauseIs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitFailureCause;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.awaitSuccess;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.rlmmConfigs;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.startedSegment;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.toList;
import static io.aiven.inkless.remote_log_metadata.postgres.RlmmTestFixtures.updateFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Compares the PostgreSQL manager against {@link RemoteLogMetadataCache}, the oracle for
 * overlap resolution and read semantics. The cache methods are package-private, so this
 * test reaches them through reflection. The one intentional divergence is a retried
 * terminal delete: the cache forgets finished segments and throws, while the tombstone
 * lets PostgreSQL complete the retry as a no-op.
 */
@Testcontainers
class PostgresRlmmDifferentialTest {
    private static final Set<Integer> EPOCHS = Set.of(0, 1, 2);
    private static final List<Long> PROBE_OFFSETS = List.of(0L, 5L, 50L, 99L, 100L, 101L, 150L, 199L, 200L, 250L, 299L, 300L);

    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    private final Method cacheAdd = cacheMethod("addCopyInProgressSegment", RemoteLogSegmentMetadata.class);
    private final Method cacheUpdate =
        cacheMethod("updateRemoteLogSegmentMetadata", RemoteLogSegmentMetadataUpdate.class);
    private final Method cacheLookup =
        cacheMethod("remoteLogSegmentMetadata", int.class, long.class);
    private final Method cacheNextTxn =
        cacheMethod("nextSegmentWithTxnIndex", int.class, long.class);
    private final Method cacheHighest =
        cacheMethod("highestOffsetForEpoch", int.class);
    private final Method cacheListEpoch =
        cacheMethod("listRemoteLogSegments", int.class);
    private final Method cacheListAll =
        cacheMethod("listAllRemoteLogSegments");
    private final Method cacheMarkInitialized =
        cacheMethod("markInitialized");

    private PostgresRemoteLogMetadataManager pg;
    private RemoteLogMetadataCache cache;
    private final Set<RemoteLogSegmentId> finishedIds = new HashSet<>();
    private final Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> originals = new HashMap<>();
    private String context = "";

    PostgresRlmmDifferentialTest() throws Exception {
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws Exception {
        pgContainer.createDatabase(testInfo);
        pg = new PostgresRemoteLogMetadataManager();
        pg.configure(rlmmConfigs(pgContainer));
        pg.onPartitionLeadershipChanges(Set.of(TP0), Set.of());
        cache = new RemoteLogMetadataCache();
        cacheMarkInitialized.invoke(cache);
        finishedIds.clear();
        originals.clear();
    }

    @AfterEach
    void tearDown() throws Exception {
        pg.close();
        pgContainer.tearDown();
    }

    @Test
    void scriptedOverlapCorners() throws Exception {
        final RemoteLogSegmentMetadata segX = startedSegment(TP0, 0L, 100L, Map.of(0, 0L));
        final RemoteLogSegmentMetadata segY = startedSegment(TP0, 50L, 150L, Map.of(0, 50L));
        final RemoteLogSegmentMetadata segZ = startedSegment(TP0, 0L, 200L, Map.of(0, 0L));
        applyAdd(segX, "add X [0,100]");
        applyAdd(segY, "add Y [50,150]");
        applyAdd(segZ, "add Z [0,200]");
        applyUpdate(updateFor(segX.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED), "finish X");
        applyUpdate(updateFor(segY.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED), "finish Y");
        applyUpdate(updateFor(segZ.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED), "finish Z");

        // The second segment starts inside the first segment's epoch 1 without covering its watermark.
        final RemoteLogSegmentMetadata segA =
            startedSegment(TP0, 0L, 100L, Map.of(0, 0L, 1, 60L));
        final RemoteLogSegmentMetadata segB =
            startedSegment(TP0, 40L, 80L, Map.of(1, 40L));
        applyAdd(segA, "add A [0,100] {0:0,1:60}");
        applyAdd(segB, "add B [40,80] {1:40}");
        applyUpdate(updateFor(segA.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED), "finish A");
        applyUpdate(updateFor(segB.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED), "finish B");

        // Deletion interleaved with a stale finish retry of the shadowed segment.
        applyUpdate(updateFor(segZ.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED), "delete-start Z");
        applyUpdate(updateFor(segX.remoteLogSegmentId(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED), "re-finish X");
        applyUpdate(updateFor(segZ.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED), "delete-finish Z");
        applyUpdate(updateFor(segZ.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED),
            "re-delete-finish Z (known divergence: cache throws, pg succeeds)");
        applyUpdate(updateFor(segB.remoteLogSegmentId(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED), "delete-start B");

        compareAll("scripted end");
    }

    @Test
    void randomizedScriptSeed1() throws Exception {
        runRandomizedScript(1L, 150);
    }

    @Test
    void randomizedScriptSeed2() throws Exception {
        runRandomizedScript(2L, 150);
    }

    private void runRandomizedScript(final long seed, final int ops) throws Exception {
        final Random random = new Random(seed);
        final List<RemoteLogSegmentId> knownIds = new ArrayList<>();
        final List<Long> knownStarts = new ArrayList<>();
        for (int i = 0; i < ops; i++) {
            context = "seed=" + seed + " op=" + i;
            final List<RemoteLogSegmentId> liveIds = new ArrayList<>(knownIds);
            liveIds.removeAll(finishedIds);
            final int pick = random.nextInt(100);
            if (pick < 40 || knownIds.isEmpty()) {
                applyFreshAdd(random, knownIds, knownStarts);
            } else if (pick < 55 && !liveIds.isEmpty()) {
                // Re-adds of deleted ids stay out: the cache resurrects them while the
                // tombstone keeps them deleted, and RLM never reuses a segment id.
                final RemoteLogSegmentId id = liveIds.get(random.nextInt(liveIds.size()));
                applyAdd(originals.get(id), context + " re-add identical " + id.id());
            } else if (pick < 55) {
                applyFreshAdd(random, knownIds, knownStarts);
            } else if (pick < 75) {
                final RemoteLogSegmentId id = knownIds.get(random.nextInt(knownIds.size()));
                applyUpdate(updateFor(id, RemoteLogSegmentState.COPY_SEGMENT_FINISHED),
                    context + " finish " + id.id());
            } else if (pick < 85) {
                final RemoteLogSegmentId id = knownIds.get(random.nextInt(knownIds.size()));
                applyUpdate(updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_STARTED),
                    context + " delete-start " + id.id());
            } else if (pick < 95) {
                final RemoteLogSegmentId id = knownIds.get(random.nextInt(knownIds.size()));
                applyUpdate(updateFor(id, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED),
                    context + " delete-finish " + id.id());
            } else if (pick < 98) {
                final RemoteLogSegmentMetadataUpdate unknown = updateFor(
                    new RemoteLogSegmentId(TP0, Uuid.randomUuid()), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
                applyUpdate(unknown, context + " finish unknown");
            } else {
                final RemoteLogSegmentId id = knownIds.get(random.nextInt(knownIds.size()));
                applyInvalidStartedUpdate(updateFor(id, RemoteLogSegmentState.COPY_SEGMENT_STARTED),
                    context + " invalid started-update " + id.id());
            }
            compareLight(context);
        }
        compareAll("seed=" + seed + " end");
    }

    private void applyFreshAdd(final Random random,
                                 final List<RemoteLogSegmentId> knownIds,
                                 final List<Long> knownStarts) throws Exception {
        final RemoteLogSegmentMetadata added = randomSegment(random, knownStarts);
        knownIds.add(added.remoteLogSegmentId());
        knownStarts.add(added.startOffset());
        originals.put(added.remoteLogSegmentId(), added);
        applyAdd(added, context + " add " + added.remoteLogSegmentId().id());
    }

    private RemoteLogSegmentMetadata randomSegment(final Random random, final List<Long> knownStarts) {
        final long start;
        if (!knownStarts.isEmpty() && random.nextInt(100) < 30) {
            start = knownStarts.get(random.nextInt(knownStarts.size()));
        } else {
            start = random.nextInt(31) * 10L;
        }
        final long end = start + 10 + random.nextInt(111);
        final TreeMap<Integer, Long> epochs = new TreeMap<>();
        epochs.put(random.nextInt(3), start);
        if (random.nextBoolean()) {
            epochs.put(random.nextInt(3), start + random.nextInt((int) (end - start) + 1));
        }
        if (random.nextInt(100) < 25) {
            epochs.put(random.nextInt(3), start + random.nextInt((int) (end - start) + 1));
        }
        final boolean txnEmpty = random.nextInt(100) < 30;
        Optional<RemoteLogSegmentMetadata.CustomMetadata> custom = Optional.empty();
        if (random.nextInt(100) < 30) {
            final byte[] bytes = new byte[4];
            random.nextBytes(bytes);
            custom = Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(bytes));
        }
        return startedSegment(new RemoteLogSegmentId(TP0, Uuid.randomUuid()), start, end, epochs, txnEmpty, custom);
    }

    private void applyAdd(final RemoteLogSegmentMetadata metadata, final String step) throws Exception {
        context = step;
        awaitSuccess(pg.addRemoteLogSegmentMetadata(metadata));
        try {
            cacheAdd.invoke(cache, metadata);
        } catch (final InvocationTargetException e) {
            throw new IllegalStateException("Cache rejected add at " + step, e.getCause());
        }
    }

    private void applyUpdate(final RemoteLogSegmentMetadataUpdate update, final String step) throws Exception {
        context = step;
        final CompletableFuture<Void> pgFuture = pg.updateRemoteLogSegmentMetadata(update);
        Throwable outcome = null;
        try {
            cacheUpdate.invoke(cache, update);
        } catch (final InvocationTargetException e) {
            outcome = e.getCause();
        }
        final Throwable cacheOutcome = outcome;
        if (cacheOutcome == null) {
            awaitSuccess(pgFuture);
        } else if (cacheOutcome instanceof RemoteResourceNotFoundException
            && finishedIds.contains(update.remoteLogSegmentId())) {
            // Known divergence: the cache forgot this finished segment, the tombstone remembers it.
            awaitSuccess(pgFuture);
        } else if (cacheOutcome instanceof RemoteResourceNotFoundException) {
            assertCauseIs(awaitFailureCause(pgFuture), RemoteResourceNotFoundException.class);
        } else if (cacheOutcome instanceof IllegalArgumentException) {
            assertCauseIs(awaitFailureCause(pgFuture), IllegalArgumentException.class);
        } else {
            throw new IllegalStateException("Unexpected cache outcome at " + step + ": " + cacheOutcome);
        }
        if (cacheOutcome == null && update.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
            finishedIds.add(update.remoteLogSegmentId());
        }
    }

    private void applyInvalidStartedUpdate(final RemoteLogSegmentMetadataUpdate update, final String step)
        throws Exception {
        context = step;
        assertThrows(IllegalArgumentException.class, () -> pg.updateRemoteLogSegmentMetadata(update));
        try {
            cacheUpdate.invoke(cache, update);
        } catch (final InvocationTargetException e) {
            // The cache throws, drops, or misses depending on stored state. None of those change state.
            final Throwable cause = e.getCause();
            if (!(cause instanceof IllegalArgumentException)
                && !(cause instanceof RemoteResourceNotFoundException)) {
                throw new IllegalStateException("Unexpected cache outcome at " + step + ": " + cause);
            }
        }
    }

    private void compareLight(final String step) throws Exception {
        assertEquals(new HashSet<>(toList(cacheListAll())), new HashSet<>(toList(pg.listRemoteLogSegments(TP0))),
            "listAll diverged at " + step);
        for (final int epoch : EPOCHS) {
            assertEquals(cacheHighest(epoch), pg.highestOffsetForEpoch(TP0, epoch),
                "highest diverged at " + step + " epoch=" + epoch);
        }
    }

    private void compareAll(final String step) throws Exception {
        compareLight(step);
        for (final int epoch : EPOCHS) {
            assertEquals(new HashSet<>(toList(cacheListEpoch(epoch))),
                new HashSet<>(toList(pg.listRemoteLogSegments(TP0, epoch))),
                "list(epoch) diverged at " + step + " epoch=" + epoch);
            assertEquals(cacheSize(epoch), pg.remoteLogSize(TP0, epoch),
                "size diverged at " + step + " epoch=" + epoch);
            for (final long offset : PROBE_OFFSETS) {
                assertEquals(cacheLookup(epoch, offset), pg.remoteLogSegmentMetadata(TP0, epoch, offset),
                    "lookup diverged at " + step + " epoch=" + epoch + " offset=" + offset);
                assertEquals(cacheNextTxn(epoch, offset), pg.nextSegmentWithTxnIndex(TP0, epoch, offset),
                    "nextTxn diverged at " + step + " epoch=" + epoch + " offset=" + offset);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<RemoteLogSegmentMetadata> cacheLookup(final int epoch, final long offset) throws Exception {
        try {
            return (Optional<RemoteLogSegmentMetadata>) cacheLookup.invoke(cache, epoch, offset);
        } catch (final InvocationTargetException e) {
            throw new IllegalStateException("Cache lookup failed", e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<RemoteLogSegmentMetadata> cacheNextTxn(final int epoch, final long offset) throws Exception {
        try {
            return (Optional<RemoteLogSegmentMetadata>) cacheNextTxn.invoke(cache, epoch, offset);
        } catch (final InvocationTargetException e) {
            throw new IllegalStateException("Cache nextTxn failed", e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<Long> cacheHighest(final int epoch) throws Exception {
        try {
            return (Optional<Long>) cacheHighest.invoke(cache, epoch);
        } catch (final InvocationTargetException e) {
            throw new IllegalStateException("Cache highest failed", e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    private Iterator<RemoteLogSegmentMetadata> cacheListEpoch(final int epoch) throws Exception {
        try {
            return (Iterator<RemoteLogSegmentMetadata>) cacheListEpoch.invoke(cache, epoch);
        } catch (final InvocationTargetException e) {
            throw new IllegalStateException("Cache list(epoch) failed", e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    private Iterator<RemoteLogSegmentMetadata> cacheListAll() throws Exception {
        try {
            return (Iterator<RemoteLogSegmentMetadata>) cacheListAll.invoke(cache);
        } catch (final InvocationTargetException e) {
            throw new IllegalStateException("Cache listAll failed", e.getCause());
        }
    }

    private long cacheSize(final int epoch) throws Exception {
        long total = 0L;
        final Iterator<RemoteLogSegmentMetadata> listed = cacheListEpoch(epoch);
        while (listed.hasNext()) {
            total += listed.next().segmentSizeInBytes();
        }
        return total;
    }

    private static Method cacheMethod(final String name, final Class<?>... params) {
        try {
            final Method method = RemoteLogMetadataCache.class.getDeclaredMethod(name, params);
            method.setAccessible(true);
            return method;
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException("Cache method not found: " + name, e);
        }
    }
}
