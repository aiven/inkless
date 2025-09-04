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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.random.RandomGenerator;

import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class RetentionEnforcementSchedulerTest {
    static final Duration ENFORCEMENT_INTERVAL = Duration.ofSeconds(1);
    static final Long ENFORCEMENT_INTERVAL_MS = ENFORCEMENT_INTERVAL.toMillis();

    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);

    Time time = new MockTime();
    @Mock
    MetadataView metadataView;
    @Mock
    RandomGenerator random;

    @BeforeEach
    void setUp() {
        // Make it deterministic. As `bound` is equals to the expected interval * 2, we divide by 2.
        when(random.nextLong(anyLong())).thenAnswer(inv -> (long) inv.getArguments()[0] / 2);
    }

    /**
     * Test that each individual broker must wait longer when the cluster is bigger.
     */
    @Test
    void intervalsDependOnBrokerCount() {
        final var scheduler = new RetentionEnforcementScheduler(time, metadataView, ENFORCEMENT_INTERVAL, random);

        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0, T0P1));
        when(metadataView.getBrokerCount()).thenReturn(1);

        // The first run is the initial scheduling.
        scheduler.getReadyPartitions();

        final Instant expectedNextTime1 = Instant.ofEpochMilli(time.milliseconds() + ENFORCEMENT_INTERVAL_MS);
        assertThat(scheduler.dumpQueue())
            .containsExactlyInAnyOrder(
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P0, expectedNextTime1),
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P1, expectedNextTime1)
            );

        // Add the second broker.
        when(metadataView.getBrokerCount()).thenReturn(2);
        // Sleep enough for scheduling to happen.
        time.sleep(Duration.ofMinutes(10).toMillis());

        assertThat(scheduler.getReadyPartitions()).containsExactlyInAnyOrder(T0P0, T0P1);

        final Instant expectedNextTime2 = Instant.ofEpochMilli(time.milliseconds() + ENFORCEMENT_INTERVAL_MS * 2);
        assertThat(scheduler.dumpQueue())
            .containsExactlyInAnyOrder(
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P0, expectedNextTime2),
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P1, expectedNextTime2)
            );

        // Add the third broker.
        when(metadataView.getBrokerCount()).thenReturn(3);
        // Sleep enough for scheduling to happen.
        time.sleep(Duration.ofMinutes(10).toMillis());

        assertThat(scheduler.getReadyPartitions()).containsExactlyInAnyOrder(T0P0, T0P1);

        final Instant expectedNextTime3 = Instant.ofEpochMilli(time.milliseconds() + ENFORCEMENT_INTERVAL_MS * 3);
        assertThat(scheduler.dumpQueue())
            .containsExactlyInAnyOrder(
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P0, expectedNextTime3),
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P1, expectedNextTime3)
            );

        // Reduce back to the single broker.
        when(metadataView.getBrokerCount()).thenReturn(1);
        // Sleep enough for scheduling to happen.
        time.sleep(Duration.ofMinutes(10).toMillis());

        assertThat(scheduler.getReadyPartitions()).containsExactlyInAnyOrder(T0P0, T0P1);

        final Instant expectedNextTime4 = Instant.ofEpochMilli(time.milliseconds() + ENFORCEMENT_INTERVAL_MS);
        assertThat(scheduler.dumpQueue())
            .containsExactlyInAnyOrder(
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P0, expectedNextTime4),
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P1, expectedNextTime4)
            );
    }

    @Test
    void newPartitionsAreScheduled() {
        final var scheduler = new RetentionEnforcementScheduler(time, metadataView, ENFORCEMENT_INTERVAL, random);

        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0));
        when(metadataView.getBrokerCount()).thenReturn(1);

        // The first run is the initial scheduling.
        scheduler.getReadyPartitions();

        // Sleep enough for known partitions to be invalidated.
        time.sleep(Duration.ofMinutes(10).toMillis());
        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0, T0P1));

        assertThat(scheduler.getReadyPartitions()).containsExactlyInAnyOrder(T0P0);

        final Instant expectedNextTime = Instant.ofEpochMilli(time.milliseconds() + ENFORCEMENT_INTERVAL_MS);
        assertThat(scheduler.dumpQueue())
            .containsExactly(
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P1, expectedNextTime),
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P0, expectedNextTime)
            );
    }

    @Test
    void removedPartitionsAreForgotten() {
        final var scheduler = new RetentionEnforcementScheduler(time, metadataView, ENFORCEMENT_INTERVAL, random);

        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P0, T0P1));
        when(metadataView.getBrokerCount()).thenReturn(1);

        // The first run is the initial scheduling.
        scheduler.getReadyPartitions();

        // Sleep enough for known partitions to be invalidated.
        time.sleep(Duration.ofMinutes(10).toMillis());
        when(metadataView.getDisklessTopicPartitions()).thenReturn(Set.of(T0P1));

        assertThat(scheduler.getReadyPartitions()).containsExactlyInAnyOrder(T0P1);
        
        final Instant expectedNextTime = Instant.ofEpochMilli(time.milliseconds() + ENFORCEMENT_INTERVAL_MS);
        assertThat(scheduler.dumpQueue())
            .containsExactly(
                new RetentionEnforcementScheduler.TopicIdPartitionWithNextEnforcementTime(T0P1, expectedNextTime)
            );
    }
}
