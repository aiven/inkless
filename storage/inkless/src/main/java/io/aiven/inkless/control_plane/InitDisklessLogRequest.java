/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Request to initialize the diskless start offset for a partition.
 *
 * @param topicId             The topic ID
 * @param topicName           The topic name
 * @param partition           The partition number
 * @param logStartOffset      The log start offset
 * @param disklessStartOffset The diskless start offset (same as high watermark when transitioning)
 * @param leaderEpoch         The leader epoch of the broker making the request. Used to reject stale requests.
 * @param producerStateEntries The producer state entries to store during initialization
 */
public record InitDisklessLogRequest(Uuid topicId,
                                     String topicName,
                                     int partition,
                                     long logStartOffset,
                                     long disklessStartOffset,
                                     int leaderEpoch,
                                     List<ProducerStateSnapshot> producerStateEntries) {

    /**
     * Creates an InitDisklessLogRequest from a UnifiedLog.
     * @param log         The UnifiedLog to extract information from
     * @param leaderEpoch The current leader epoch for this partition
     * @throws IllegalStateException if the log does not have a topic ID
     */
    public static InitDisklessLogRequest fromUnifiedLog(final UnifiedLog log, final int leaderEpoch) {
        final Uuid topicId = log.topicId()
            .orElseThrow(() -> new IllegalStateException("Topic ID is required for diskless initialization"));

        final String topicName = log.topicPartition().topic();
        final int partition = log.topicPartition().partition();
        final long logStartOffset = log.logStartOffset();
        final long highWatermark = log.highWatermark();

        // Extract producer state entries
        final List<ProducerStateSnapshot> producerStateEntries = extractProducerState(log.producerStateManager());

        return new InitDisklessLogRequest(
            topicId,
            topicName,
            partition,
            logStartOffset,
            highWatermark,
            leaderEpoch,
            producerStateEntries
        );
    }

    /**
     * Extracts the producer state from the ProducerStateManager.
     * <p>
     * For each active producer, this method extracts all retained batch metadata entries
     * (up to {@link ProducerStateEntry#NUM_BATCHES_TO_RETAIN} per producer) to support
     * duplicate detection after the transition to diskless.
     *
     * @return A list of ProducerStateSnapshot entries for all active producers
     */
    private static List<ProducerStateSnapshot> extractProducerState(final ProducerStateManager producerStateManager) {
        final Map<Long, ProducerStateEntry> activeProducers = producerStateManager.activeProducers();
        final List<ProducerStateSnapshot> snapshots = new ArrayList<>();

        for (final Map.Entry<Long, ProducerStateEntry> entry : activeProducers.entrySet()) {
            final long producerId = entry.getKey();
            final ProducerStateEntry state = entry.getValue();

            for (final BatchMetadata batch : state.batchMetadata()) {
                snapshots.add(new ProducerStateSnapshot(
                    producerId,
                    state.producerEpoch(),
                    batch.firstSeq(),
                    batch.lastSeq,
                    batch.firstOffset(),  // assigned offset is the first offset of the batch
                    batch.timestamp
                ));
            }
        }

        return snapshots;
    }
}
