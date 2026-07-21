/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
package io.aiven.inkless.delete;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.random.RandomGenerator;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.MetadataView;

/**
 * The class responsible for scheduling per partition retention enforcement.
 *
 * <p>The scheduler tries to ensure that retention enforcement is performed for each partition
 * every {@code enforcementInterval} approximately across all brokers.
 * "Approximately" means that there's a  randomization component. The number of milliseconds to wait is selected randomly from {@code [0..2*enforcementInterval)}.
 * As there is no coordination across brokers, each scheduler just multiply the interval to randomly choose from by the number of brokers,
 * keeping the global frequency on average the same.
 *
 * <p>The global coordination is not needed because the control plane must do the appropriate locking, so we're not trying to avoid collisions.</p>
 */
class RetentionEnforcementScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionEnforcementScheduler.class);

    /**
     * How ofter we update the known partitions.
     * <p>This may be a bit heavy to update this info on each call.
     */
    private static final Duration KNOWN_PARTITION_UPDATE_INTERVAL = Duration.ofMinutes(5);

    private final Time time;
    private final MetadataView metadataView;
    private final Duration enforcementInterval;
    private final RandomGenerator random;

    private Instant lastKnownPartitionsUpdate = Instant.MIN;
    private Set<TopicIdPartition> knownPartitions = new HashSet<>();

    private final PriorityQueue<TopicIdPartitionWithNextEnforcementTime> partitionsByNextEnforcementTime = new PriorityQueue<>(
        TopicIdPartitionWithNextEnforcementTime.timeComparator()
    );

    public RetentionEnforcementScheduler(final Time time,
                                         final MetadataView metadataView,
                                         final Duration enforcementInterval,
                                         final RandomGenerator random) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.enforcementInterval = Objects.requireNonNull(enforcementInterval, "enforcementInterval cannot be null");
        this.random = Objects.requireNonNull(random, "random cannot be null");
    }

    List<TopicIdPartition> getReadyPartitions() {
        final Instant now = TimeUtils.now(time);

        updateKnownPartitionsIfNeeded(now);

        final List<TopicIdPartition> result = new ArrayList<>();
        TopicIdPartitionWithNextEnforcementTime tidpwt;
        // These peek() and poll() are guaranteed to work with the same item
        // if there's no concurrent modification (which should be the case).
        while ((tidpwt = partitionsByNextEnforcementTime.peek()) != null && tidpwt.nextEnforcementTime().isBefore(now)) {
            partitionsByNextEnforcementTime.poll();
            final TopicIdPartition partition = tidpwt.topicIdPartition();
            // Filter out previously deleted partitions.
            if (knownPartitions.contains(partition)) {
                result.add(partition);
            } else {
                LOGGER.debug("Partition removed: {}", partition);
            }
        }

        // We need to reschedule the taken partitions.
        for (final TopicIdPartition partition : result) {
            schedulePartition(now, partition);
        }

        return result;
    }

    private void updateKnownPartitionsIfNeeded(final Instant now) {
        if (lastKnownPartitionsUpdate.plus(KNOWN_PARTITION_UPDATE_INTERVAL).isBefore(now)) {
            final Set<TopicIdPartition> newKnownPartitions = metadataView.getDisklessTopicPartitions();
            for (final TopicIdPartition partition : newKnownPartitions) {
                // Schedule the new partitions.
                if (!this.knownPartitions.contains(partition)) {
                    LOGGER.debug("Partition added: {}", partition);
                    schedulePartition(now, partition);
                }
            }
            this.knownPartitions = newKnownPartitions;
            lastKnownPartitionsUpdate = now;
        }
    }

    private void schedulePartition(final Instant now, final TopicIdPartition partition) {
        partitionsByNextEnforcementTime.add(
            new TopicIdPartitionWithNextEnforcementTime(partition, nextCheck(now)
        ));
    }

    private Instant nextCheck(final Instant now) {
        // TODO consider adaptive checking:
        // If a partition is infrequently written to, we can proportionally decrease the enforcing frequency for it.

        // brokerCount may be 0, for example when the first broker is just starting.
        // Defaulting to 1 in this case.
        final int effectiveBrokerCount = Math.max(1, metadataView.getBrokerCount());

        // Use a centered distribution around the Mean with a controlled jitter.
        // Range: [(1 - jitterPercentage) * Mean, (1 + jitterPercentage) * Mean]
        final long targetMeanDelay = enforcementInterval.toMillis() * effectiveBrokerCount;
        final double jitterPercentage = 0.25;

        final long jitterMs = (long) (targetMeanDelay * jitterPercentage);

        final long baseDelay = targetMeanDelay - jitterMs;
        return now.plusMillis(baseDelay + random.nextLong(jitterMs * 2));
    }

    // Visible for testing.
    record TopicIdPartitionWithNextEnforcementTime(TopicIdPartition topicIdPartition,
                                                   Instant nextEnforcementTime) {
        private static Comparator<TopicIdPartitionWithNextEnforcementTime> timeComparator() {
            return Comparator.comparing(TopicIdPartitionWithNextEnforcementTime::nextEnforcementTime);
        }
    }

    // Visible for testing.
    List<TopicIdPartitionWithNextEnforcementTime> dumpQueue() {
        return partitionsByNextEnforcementTime.stream().toList();
    }
}
