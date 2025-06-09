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
import java.util.stream.StreamSupport;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.MetadataView;

class RetentionEnforcementScheduler {
    private static final Duration BROKER_COUNT_UPDATE_INTERVAL = Duration.ofMinutes(1);
    private static final Duration KNOWN_PARTITION_UPDATE_INTERVAL = Duration.ofMinutes(5);

    private final Time time;
    private final MetadataView metadataView;
    private final Duration enforcementInterval;
    private final RandomGenerator random;

    private Instant lastBrokerCountUpdate = Instant.MIN;
    private int brokerCount = 1;
    private Instant lastKnownPartitionsUpdate = Instant.MIN;
    private Set<TopicIdPartition> knownPartitions = new HashSet<>();

    private final PriorityQueue<TopicIdPartitionWithLastEnforcementTime> partitionsByLastEnforcementTime = new PriorityQueue<>(
        TopicIdPartitionWithLastEnforcementTime.timeComparator()
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

        updateBrokerCountIfNeeded(now);
        updateKnownPartitionsIfNeeded(now);

        final List<TopicIdPartition> result = new ArrayList<>();
        TopicIdPartitionWithLastEnforcementTime tidpwt;
        while ((tidpwt = partitionsByLastEnforcementTime.peek()) != null && tidpwt.nextEnforcementTime().isBefore(now)) {
            partitionsByLastEnforcementTime.poll();
            final TopicIdPartition partition = tidpwt.topicIdPartition();
            // Filter out previously deleted partitions.
            if (knownPartitions.contains(partition)) {
                result.add(partition);
            }
        }

        // We need to reschedule the taken partitions.
        for (final TopicIdPartition partition : result) {
            schedulePartition(now, partition);
        }

        return result;
    }

    private void updateBrokerCountIfNeeded(final Instant now) {
        if (lastBrokerCountUpdate.plus(BROKER_COUNT_UPDATE_INTERVAL).isBefore(now)) {
            final int newBrokerCount = (int) StreamSupport.stream(metadataView.getAliveBrokers().spliterator(), false).count();
            if (this.brokerCount != newBrokerCount) {
                this.brokerCount = newBrokerCount;
            }
            lastBrokerCountUpdate = now;
        }
    }

    private void updateKnownPartitionsIfNeeded(final Instant now) {
        if (lastKnownPartitionsUpdate.plus(KNOWN_PARTITION_UPDATE_INTERVAL).isBefore(now)) {
            final Set<TopicIdPartition> newKnownPartitions = metadataView.getInklessTopicPartitions();
            for (final TopicIdPartition partition : newKnownPartitions) {
                // Schedule the new partitions.
                if (!this.knownPartitions.contains(partition)) {
                    schedulePartition(now, partition);
                }
            }
            this.knownPartitions = newKnownPartitions;
            lastKnownPartitionsUpdate = now;
        }
    }

    private void schedulePartition(final Instant now, final TopicIdPartition partition) {
        partitionsByLastEnforcementTime.add(
            new TopicIdPartitionWithLastEnforcementTime(partition, nextCheck(now)
        ));
    }

    private Instant nextCheck(final Instant now) {
        // TODO consider adaptive checking:
        // If a partition is infrequently written to, we can proportionally decrease the enforcing frequency for it.

        // brokerCount may be 0, for example when the first broker is just starting.
        // Defaulting to 1 in this case.
        final int effectiveBrokerCount = Math.max(1, brokerCount);
        final long limit =
            // Multiply by 2 because on average we'll get enforcementInterval
            2 * enforcementInterval.toMillis()
            // The more brokers we have, the less frequently we should actually check.
            * effectiveBrokerCount;
        return now.plusMillis(random.nextLong(limit));
    }

    // Visible for testing.
    record TopicIdPartitionWithLastEnforcementTime(TopicIdPartition topicIdPartition,
                                                   Instant nextEnforcementTime) {
        private static Comparator<TopicIdPartitionWithLastEnforcementTime> timeComparator() {
            return Comparator.comparing(TopicIdPartitionWithLastEnforcementTime::nextEnforcementTime);
        }
    }

    // Visible for testing.
    List<TopicIdPartitionWithLastEnforcementTime> dumpQueue() {
        return partitionsByLastEnforcementTime.stream().toList();
    }
}
