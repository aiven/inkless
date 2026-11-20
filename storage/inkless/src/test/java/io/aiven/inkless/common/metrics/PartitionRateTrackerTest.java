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
package io.aiven.inkless.common.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;

import org.HdrHistogram.DoubleHistogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import io.aiven.inkless.common.metrics.PartitionRateTracker.Direction;
import io.aiven.inkless.common.metrics.PartitionRateTracker.GroupStats;
import io.aiven.inkless.common.metrics.PartitionRateTracker.Placement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

class PartitionRateTrackerTest {
    private static final long WINDOW_MS = 60_000L;

    private final MockTime time = new MockTime();
    private final TopicPartition a = new TopicPartition("a", 0);
    private final TopicPartition b = new TopicPartition("b", 0);
    private final TopicPartition c = new TopicPartition("c", 0);
    private final TopicPartition diskless = new TopicPartition("diskless", 0);

    private final Map<TopicPartition, Placement> placements = new HashMap<>();
    private PartitionRateTracker tracker;

    @BeforeEach
    void setUp() {
        placements.put(a, Placement.CLASSIC_LEADER);
        placements.put(b, Placement.CLASSIC_LEADER);
        placements.put(c, Placement.CLASSIC_LEADER);
        placements.put(diskless, Placement.DISKLESS);
        tracker = new PartitionRateTracker(time,
            () -> new PartitionRateTracker.Hosted(3, 1, tp -> placements.getOrDefault(tp, Placement.NOT_HOSTED)));
    }

    @AfterEach
    void tearDown() {
        tracker.close();
    }

    @Test
    void rateEqualToABoundCountsInThatBound() {
        final GroupStats stats = PartitionRateTracker.computeStats(histogramOf(1024, 1536, 67_108_864), 0.0, 0, 0);

        assertThat(stats.cumulativeBuckets()[0]).isEqualTo(1);
        assertThat(stats.cumulativeBuckets()[1]).isEqualTo(2);
        assertThat(stats.cumulativeBuckets()[PartitionRateTracker.BUCKET_BOUNDS.length - 1]).isEqualTo(3);
    }

    @Test
    void rateMoreThanOnePercentAboveABoundCountsInTheNextBound() {
        final GroupStats stats = PartitionRateTracker.computeStats(histogramOf(1060, 1580), 0.0, 0, 0);

        assertThat(stats.cumulativeBuckets()[0]).isZero();
        assertThat(stats.cumulativeBuckets()[1]).isEqualTo(1);
        assertThat(stats.cumulativeBuckets()[2]).isEqualTo(2);
    }

    @Test
    void bucketBoundsAreAscendingAndIncludeTheRollThreshold() {
        final long[] bounds = PartitionRateTracker.BUCKET_BOUNDS;
        assertThat(bounds).hasSize(18).isSorted().contains(1024L, 1536L, 1_048_576L, 67_108_864L);
    }

    @Test
    void quantilesUseNearestRank() {
        final double[] values = new double[100];
        for (int i = 0; i < values.length; i++) {
            values[i] = i + 1;
        }

        final GroupStats stats = PartitionRateTracker.computeStats(histogramOf(values), 0.0, 0, 0);

        final double[] expected = {50.0, 95.0, 99.0, 100.0, 100.0};
        for (int i = 0; i < expected.length; i++) {
            assertThat(stats.quantiles()[i]).isCloseTo(expected[i], withinPercentage(1));
        }
    }

    @Test
    void computeStatsGivesEqualQuantilesForOneValue() {
        final GroupStats stats = PartitionRateTracker.computeStats(histogramOf(2000), 2000.0, 0, 0);

        assertThat(stats.active()).isEqualTo(1);
        assertThat(stats.quantiles()).containsOnly(stats.quantiles()[0]);
        assertThat(stats.quantiles()[0]).isCloseTo(2000.0, withinPercentage(1));
        assertThat(stats.total()).isEqualTo(2000.0);
    }

    @Test
    void computeStatsGivesCumulativeBucketsMaxAndTotal() {
        final GroupStats stats = PartitionRateTracker.computeStats(histogramOf(1024, 1500, 1500, 5000, 1_000_000_000), 1234.5, 0, 0);

        assertThat(stats.active()).isEqualTo(5);
        assertThat(stats.quantiles()[4]).isCloseTo(1.0e9, withinPercentage(1));
        assertThat(stats.total()).isEqualTo(1234.5);
        assertThat(stats.cumulativeBuckets()[0]).isEqualTo(1);
        assertThat(stats.cumulativeBuckets()[1]).isEqualTo(3);
        assertThat(stats.cumulativeBuckets()[2]).isEqualTo(3);
        assertThat(stats.cumulativeBuckets()[3]).isEqualTo(3);
        assertThat(stats.cumulativeBuckets()[4]).isEqualTo(4);
        assertThat(stats.cumulativeBuckets()[PartitionRateTracker.BUCKET_BOUNDS.length]).isEqualTo(5);
    }

    @Test
    void computeStatsOfNoRatesIsZero() {
        final GroupStats stats = PartitionRateTracker.computeStats(histogramOf(), 0.0, 0, 0);

        assertThat(stats.active()).isZero();
        assertThat(stats.quantiles()).containsOnly(0.0);
        assertThat(stats.cumulativeBuckets()).containsOnly(0L);
    }

    @Test
    void firstIntervalOfAnEntryCountsItsBytes() {
        tracker.record(Direction.IN, a, 6000);
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        final GroupStats stats = stats(Direction.IN, false);
        assertThat(stats.active()).isEqualTo(1);
        assertThat(stats.quantiles()[0]).isCloseTo(100.0, withinPercentage(1));
    }

    @Test
    void rateBelowOneBytePerSecondKeepsItsPrecision() {
        tracker.record(Direction.IN, a, 1);
        tracker.snapshot();
        tracker.record(Direction.IN, a, 90);
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        final GroupStats stats = stats(Direction.IN, false);
        assertThat(stats.quantiles()[0]).isCloseTo(1.5, withinPercentage(1));
        assertThat(stats.quantiles()[4]).isCloseTo(1.5, withinPercentage(1));
        assertThat(stats.total()).isEqualTo(1.5);
        assertThat(stats.cumulativeBuckets()[0]).isEqualTo(1);
    }

    @Test
    void wallClockStepBackDoesNotChangeTheElapsedTime() {
        final long[] wallClockMs = {time.milliseconds()};
        final Time steppingTime = new Time() {
            @Override
            public long milliseconds() {
                return wallClockMs[0];
            }

            @Override
            public long nanoseconds() {
                return time.nanoseconds();
            }

            @Override
            public void sleep(final long ms) {
                time.sleep(ms);
            }

            @Override
            public void waitObject(final Object obj, final Supplier<Boolean> condition, final long deadlineMs) {
                throw new UnsupportedOperationException();
            }
        };
        final PartitionRateTracker stepping = new PartitionRateTracker(steppingTime,
            () -> new PartitionRateTracker.Hosted(1, 0, tp -> Placement.CLASSIC_LEADER));
        stepping.record(Direction.IN, a, 1);
        stepping.snapshot();
        stepping.record(Direction.IN, a, 60_000);
        wallClockMs[0] -= 10 * WINDOW_MS;
        steppingTime.sleep(WINDOW_MS);
        stepping.snapshot();

        assertThat(stepping.currentSnapshot().of(Direction.IN, false).total()).isEqualTo(1000.0);
    }

    @Test
    void twoRunsGiveTheRateOverTheElapsedTime() {
        tracker.record(Direction.IN, a, 1);
        tracker.record(Direction.IN, b, 1);
        tracker.record(Direction.IN, c, 1);
        tracker.snapshot();

        tracker.record(Direction.IN, a, 60_000);
        tracker.record(Direction.IN, b, 120_000);
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        final GroupStats stats = stats(Direction.IN, false);
        assertThat(stats.active()).isEqualTo(2);
        assertThat(stats.quantiles()[0]).isCloseTo(1000.0, withinPercentage(1));
        assertThat(stats.quantiles()[1]).isCloseTo(2000.0, withinPercentage(1));
        assertThat(stats.quantiles()[4]).isCloseTo(2000.0, withinPercentage(1));
        assertThat(stats.total()).isEqualTo(3000.0);
    }

    @Test
    void idlePartitionsAreNotActiveButStayHosted() {
        tracker.record(Direction.IN, a, 1);
        tracker.snapshot();
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        assertThat(stats(Direction.IN, false).active()).isZero();
        assertThat(tracker.currentSnapshot().hosted(false)).isEqualTo(3);
        assertThat(tracker.currentSnapshot().hosted(true)).isEqualTo(1);
    }

    @Test
    void groupsByDirectionTopicTypeAndRole() {
        placements.put(b, Placement.CLASSIC_FOLLOWER);
        for (final Direction direction : Direction.values()) {
            tracker.record(direction, a, 1);
            tracker.record(direction, b, 1);
            tracker.record(direction, diskless, 1);
        }
        tracker.snapshot();
        for (final Direction direction : Direction.values()) {
            tracker.record(direction, a, 60_000);
            tracker.record(direction, b, 120_000);
            tracker.record(direction, diskless, 180_000);
        }
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        for (final Direction direction : Direction.values()) {
            final GroupStats classic = stats(direction, false);
            assertThat(classic.active()).isEqualTo(2);
            assertThat(classic.activeLeader()).isEqualTo(1);
            assertThat(classic.activeFollower()).isEqualTo(1);
            assertThat(stats(direction, true).quantiles()[4]).isCloseTo(3000.0, withinPercentage(1));
        }
    }

    @Test
    void snapshotDropsEntriesOfPartitionsNotHosted() {
        tracker.record(Direction.IN, a, 1);
        tracker.record(Direction.OUT, diskless, 1);
        tracker.snapshot();
        placements.put(a, Placement.NOT_HOSTED);
        placements.put(diskless, Placement.NOT_HOSTED);
        tracker.record(Direction.IN, a, 60_000);
        tracker.record(Direction.OUT, diskless, 60_000);
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        assertThat(stats(Direction.IN, false).active()).isZero();
        assertThat(stats(Direction.OUT, true).active()).isZero();
        placements.put(a, Placement.CLASSIC_LEADER);
        placements.put(diskless, Placement.DISKLESS);
        tracker.record(Direction.IN, a, 60_000);
        tracker.record(Direction.OUT, diskless, 60_000);
        time.sleep(WINDOW_MS);
        tracker.snapshot();
        // The snapshot dropped the entries. Bytes recorded while this broker didn't host them don't count.
        assertThat(stats(Direction.IN, false).total()).isEqualTo(1000.0);
        assertThat(stats(Direction.OUT, true).total()).isEqualTo(1000.0);
    }

    @Test
    void snapshotExpiresAnEntryAfterAnIntervalWithoutBytes() {
        final List<TopicPartition> placed = new ArrayList<>();
        final PartitionRateTracker counting = new PartitionRateTracker(time, () -> new PartitionRateTracker.Hosted(0, 1, tp -> {
            placed.add(tp);
            return Placement.DISKLESS;
        }));
        counting.record(Direction.IN, diskless, 1);
        counting.snapshot();
        placed.clear();
        time.sleep(WINDOW_MS);
        counting.snapshot();

        assertThat(placed).isEmpty();
        assertThat(counting.trackedPartitionCount()).isZero();
        counting.record(Direction.IN, diskless, 60_000);
        time.sleep(WINDOW_MS);
        counting.snapshot();
        assertThat(counting.trackedPartitionCount()).isEqualTo(1);
        assertThat(counting.currentSnapshot().of(Direction.IN, true).total()).isEqualTo(1000.0);
    }

    @Test
    void zeroByteRecordDoesNotCreateAnEntry() {
        final List<TopicPartition> placed = new ArrayList<>();
        final PartitionRateTracker counting = new PartitionRateTracker(time, () -> new PartitionRateTracker.Hosted(0, 0, tp -> {
            placed.add(tp);
            return Placement.CLASSIC_LEADER;
        }));
        counting.record(Direction.OUT, a, 0);
        time.sleep(WINDOW_MS);
        counting.snapshot();

        assertThat(placed).isEmpty();
    }

    @Test
    void placementAtSnapshotTimeApplies() {
        tracker.record(Direction.OUT, a, 1);
        tracker.snapshot();
        tracker.record(Direction.OUT, a, 60_000);
        placements.put(a, Placement.CLASSIC_FOLLOWER);
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        final GroupStats stats = stats(Direction.OUT, false);
        assertThat(stats.activeLeader()).isZero();
        assertThat(stats.activeFollower()).isEqualTo(1);
    }

    @Test
    void registersGaugesThatReadTheLastSnapshotAndCloseRemovesThem() {
        tracker.registerMetrics(new KafkaMetricsGroup("kafka.server", "ReplicaManager"));
        tracker.record(Direction.IN, a, 1);
        tracker.snapshot();
        tracker.record(Direction.IN, a, 120_000);
        time.sleep(WINDOW_MS);
        tracker.snapshot();

        final Map<String, String> classic = tags("topicType", "classic");
        assertThat((double) gaugeValue("PartitionBytesInRateP50", classic)).isCloseTo(2000.0, withinPercentage(1));
        assertThat((double) gaugeValue("PartitionBytesInRateMax", classic)).isCloseTo(2000.0, withinPercentage(1));
        assertThat(gaugeValue("PartitionsByBytesInRate", with(classic, "le", "1024"))).isEqualTo(0L);
        assertThat(gaugeValue("PartitionsByBytesInRate", with(classic, "le", "2048"))).isEqualTo(1L);
        assertThat(gaugeValue("PartitionsByBytesInRate", with(classic, "le", "Inf"))).isEqualTo(1L);
        assertThat(gaugeValue("ActivePartitionsBytesIn", classic)).isEqualTo(1L);
        assertThat(gaugeValue("PartitionBytesInRateSum", classic)).isEqualTo(2000.0);
        assertThat(gaugeValue("HostedPartitions", classic)).isEqualTo(3);
        assertThat(gaugeValue("HostedPartitions", tags("topicType", "diskless"))).isEqualTo(1);

        // Classic bytes out have a role tag and no untagged count.
        assertThat(registered("ActivePartitionsBytesOut", with(classic, "role", "leader"))).isNotNull();
        assertThat(registered("ActivePartitionsBytesOut", with(classic, "role", "follower"))).isNotNull();
        assertThat(registered("ActivePartitionsBytesOut", classic)).isNull();
        assertThat(registered("ActivePartitionsBytesOut", tags("topicType", "diskless"))).isNotNull();
        assertThat(registered("ActivePartitionsBytesOut", with(tags("topicType", "diskless"), "role", "leader"))).isNull();

        tracker.close();

        assertThat(registered("PartitionBytesInRateP50", classic)).isNull();
        assertThat(registered("PartitionsByBytesInRate", with(classic, "le", "Inf"))).isNull();
        assertThat(registered("HostedPartitions", classic)).isNull();
    }

    @Test
    void documentsEveryRegisteredMetricName() {
        final Set<String> documented = new HashSet<>();
        PartitionRateTracker.all().forEach(template -> documented.add(template.name()));

        final Set<String> expected = new HashSet<>();
        for (final String direction : List.of("In", "Out")) {
            for (final String quantile : List.of("P50", "P95", "P99", "P999", "Max")) {
                expected.add("PartitionBytes" + direction + "Rate" + quantile);
            }
            expected.add("PartitionsByBytes" + direction + "Rate");
            expected.add("PartitionBytes" + direction + "RateSum");
            expected.add("ActivePartitionsBytes" + direction);
        }
        expected.add("HostedPartitions");
        assertThat(documented).isEqualTo(expected);
    }

    private static DoubleHistogram histogramOf(final double... values) {
        final DoubleHistogram histogram = new DoubleHistogram(PartitionRateTracker.SIGNIFICANT_DIGITS);
        for (final double value : values) {
            histogram.recordValue(value);
        }
        return histogram;
    }

    private GroupStats stats(final Direction direction, final boolean diskless) {
        return tracker.currentSnapshot().of(direction, diskless);
    }

    private static Map<String, String> tags(final String... keyValues) {
        final Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            tags.put(keyValues[i], keyValues[i + 1]);
        }
        return tags;
    }

    private static Map<String, String> with(final Map<String, String> tags, final String key, final String value) {
        final Map<String, String> result = new LinkedHashMap<>(tags);
        result.put(key, value);
        return result;
    }

    private static Metric registered(final String name, final Map<String, String> tags) {
        final KafkaMetricsGroup group = new KafkaMetricsGroup("kafka.server", "ReplicaManager");
        return KafkaYammerMetrics.defaultRegistry().allMetrics().get(group.metricName(name, tags));
    }

    private static Object gaugeValue(final String name, final Map<String, String> tags) {
        return ((Gauge<?>) registered(name, tags)).value();
    }
}
