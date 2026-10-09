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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

final class RlmmTestFixtures {
    static final String CLUSTER_ID = "test-cluster";
    static final TopicIdPartition TP0 =
        new TopicIdPartition(Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ"), new TopicPartition("topic", 0));
    static final TopicIdPartition TP1 =
        new TopicIdPartition(Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ"), new TopicPartition("topic", 1));

    private RlmmTestFixtures() {
    }

    static Map<String, String> rlmmConfigs(final InklessPostgreSQLContainer pgContainer) {
        final Map<String, String> configs = new HashMap<>();
        configs.put("connection.string", pgContainer.getUserJdbcUrl());
        configs.put("username", pgContainer.getUsername());
        configs.put("password", pgContainer.getPassword());
        configs.put("cluster.id", CLUSTER_ID);
        return configs;
    }

    static RemoteLogSegmentMetadata startedSegment(final TopicIdPartition tp,
                                                   final long startOffset,
                                                   final long endOffset,
                                                   final Map<Integer, Long> segmentLeaderEpochs) {
        return startedSegment(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
            startOffset, endOffset, segmentLeaderEpochs, false, Optional.empty());
    }

    static RemoteLogSegmentMetadata startedSegment(final RemoteLogSegmentId id,
                                                   final long startOffset,
                                                   final long endOffset,
                                                   final Map<Integer, Long> segmentLeaderEpochs,
                                                   final boolean txnIdxEmpty,
                                                   final Optional<RemoteLogSegmentMetadata.CustomMetadata> customMetadata) {
        return new RemoteLogSegmentMetadata(id,
            startOffset,
            endOffset,
            1_700_000_000_000L,
            0,
            1_700_000_000_100L,
            1024,
            customMetadata,
            RemoteLogSegmentState.COPY_SEGMENT_STARTED,
            segmentLeaderEpochs,
            txnIdxEmpty);
    }

    static RemoteLogSegmentMetadataUpdate updateFor(final RemoteLogSegmentId id,
                                                    final RemoteLogSegmentState state) {
        return updateFor(id, state, 1, Optional.empty());
    }

    static RemoteLogSegmentMetadataUpdate updateFor(final RemoteLogSegmentId id,
                                                    final RemoteLogSegmentState state,
                                                    final int brokerId,
                                                    final Optional<RemoteLogSegmentMetadata.CustomMetadata> customMetadata) {
        return new RemoteLogSegmentMetadataUpdate(id, 1_700_000_000_200L, customMetadata, state, brokerId);
    }

    static RemotePartitionDeleteMetadata partitionDelete(final TopicIdPartition tp,
                                                         final RemotePartitionDeleteState state) {
        return new RemotePartitionDeleteMetadata(tp, state, 1_700_000_000_300L, 0);
    }

    static void awaitSuccess(final CompletableFuture<Void> future)
        throws InterruptedException, ExecutionException, TimeoutException {
        future.get(30, TimeUnit.SECONDS);
    }

    static Throwable awaitFailureCause(final CompletableFuture<?> future) throws InterruptedException {
        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
            return e.getCause();
        } catch (final TimeoutException e) {
            return e;
        }
        fail("Expected the future to fail, but it completed successfully");
        throw new IllegalStateException("unreachable");
    }

    static List<RemoteLogSegmentMetadata> toList(final Iterator<RemoteLogSegmentMetadata> iterator) {
        final List<RemoteLogSegmentMetadata> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    static void assertInfraFailure(final Throwable cause) {
        fail("Unexpected infrastructure failure: " + cause);
    }

    static void assertCauseIs(final Throwable cause, final Class<? extends Throwable> expected) {
        assertEquals(expected, cause.getClass(), "Unexpected failure cause: " + cause);
    }
}
