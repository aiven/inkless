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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import org.HdrHistogram.DoubleHistogram;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Tracks the client bytes in (produce) and out (consumer fetch) of each partition on this broker and publishes the
 * distribution of the rates as gauges: percentiles, bucket counts, and totals.
 *
 * <p>Callers only {@link #record} bytes. A periodic {@link #snapshot()} converts the byte counts into rates
 * and publishes them. The snapshot omits idle partitions. The gauges read the last snapshot, so a JMX read
 * never iterates partitions.
 *
 * <p>Costs and threading:
 * <ul>
 *   <li>{@link #record}: request threads call it concurrently.
 *   It does one map lookup and one {@link LongAdder} add.
 *   It allocates only for the first bytes of a partition.
 *   The snapshot resolves the topic type and the role, so this method doesn't.</li>
 *   <li>{@link #snapshot()}: one thread calls it, once per window.
 *   For each partition with bytes, it calls the placement function once and records the rates in reused histograms.
 *   It allocates a constant amount, whatever the number of partitions, plus what the placement function allocates.</li>
 *   <li>Gauges: a JMX read gets one field of the last snapshot. A read doesn't take a lock: each snapshot is immutable.</li>
 *   <li>Memory: one counter for both directions of each partition with client bytes, kept until the partition is
 *   no longer hosted.
 *   Each of the four histograms grows to fit the largest rate it records, and a reset keeps that size.
 *   At 1% precision, a histogram that recorded rates from 1 byte per minute to 64 MiB/s takes about 35 KB.</li>
 * </ul>
 */
public final class PartitionRateTracker implements Closeable {
    public enum Direction {
        IN("In", "client bytes in, from produce requests"),
        OUT("Out", "client bytes out, to consumer fetch requests");

        private final String name;
        private final String doc;

        Direction(final String name, final String doc) {
            this.name = name;
            this.doc = doc;
        }

        String rateName() {
            return "PartitionBytes" + name + "Rate";
        }

        String bucketsName() {
            return "PartitionsByBytes" + name + "Rate";
        }

        String sumName() {
            return rateName() + "Sum";
        }

        String activeName() {
            return "ActivePartitionsBytes" + name;
        }
    }

    /**
     * Where a partition with traffic is on this broker at snapshot time.
     */
    public enum Placement {
        DISKLESS,
        CLASSIC_LEADER,
        CLASSIC_FOLLOWER,
        // The broker no longer hosts the classic partition, or a deletion removed the topic. The snapshot drops the entry.
        NOT_HOSTED
    }

    /**
     * The state of this broker that one snapshot reads.
     *
     * @param classicLeaders     the number of classic partitions this broker leads
     * @param disklessPartitions the number of diskless partitions in the cluster
     * @param placement          the placement of each partition with traffic
     */
    public record Hosted(int classicLeaders, int disklessPartitions, Function<TopicPartition, Placement> placement) {
    }

    // Powers of 2 from 1 KiB/s to 64 MiB/s, plus 1.5 KiB/s: 128 MiB per 24 h,
    // where a 128 MiB segment fills before a 24 h time roll.
    static final long[] BUCKET_BOUNDS;

    static {
        final List<Long> bounds = new ArrayList<>();
        for (int exponent = 10; exponent <= 26; exponent++) {
            bounds.add(1L << exponent);
        }
        bounds.add(1536L);
        bounds.sort(Long::compare);
        BUCKET_BOUNDS = bounds.stream().mapToLong(Long::longValue).toArray();
    }

    // Name suffixes, as in KafkaMetricHistogram. The last one is the max.
    static final String[] QUANTILES = {"P50", "P95", "P99", "P999", "Max"};
    private static final double[] QUANTILE_PERCENTS = {50.0, 95.0, 99.0, 99.9, 100.0};
    // Keeps each recorded rate within 1% of its value.
    static final int SIGNIFICANT_DIGITS = 2;
    private static final String INF = "Inf";

    private static final String GROUP = "ReplicaManager";

    static final String HOSTED_PARTITIONS = "HostedPartitions";
    private static final String HOSTED_PARTITIONS_DOC =
        "Number of partitions that this broker hosts: the classic partitions it leads, or all diskless partitions. "
            + "Kafka internal topics are not counted. "
            + "For classic bytes out, compare it with the role=leader series of ActivePartitionsBytesOut only: "
            + "the role=follower series counts partitions that this broker follows, and this count leaves them out.";

    /**
     * Returns the metric name templates of the tracker, for documentation only.
     */
    public static List<MetricNameTemplate> all() {
        final String topicType = "topicType";
        final List<MetricNameTemplate> templates = new ArrayList<>();
        for (final Direction direction : Direction.values()) {
            for (final String quantile : QUANTILES) {
                templates.add(new MetricNameTemplate(direction.rateName() + quantile, GROUP,
                    "Rate in bytes per second at quantile " + quantile +
                        " of the active partitions on this broker, for "
                        + direction.doc +
                        ". A partition is active when it had client traffic on this broker in the last "
                        + "snapshot interval. The value covers all active partitions of this broker, within 1%, "
                        + "and doesn't combine across brokers.",
                    topicType));
            }
            templates.add(new MetricNameTemplate(direction.bucketsName(), GROUP,
                "Number of active partitions on this broker with a rate at or below the le bound in bytes per second, for "
                    + direction.doc + ". The sum over brokers counts each broker's slice of a partition separately.",
                topicType, "le"));
            templates.add(new MetricNameTemplate(direction.sumName(), GROUP,
                "Sum of the rates of the active partitions on this broker in bytes per second, for " + direction.doc +
                    ".",
                topicType));
            final String activeDoc = "Number of active partitions on this broker, for " + direction.doc + ". "
                +
                "A diskless partition, and a classic partition fetched through followers, can be active on several brokers.";
            if (direction == Direction.OUT) {
                templates.add(new MetricNameTemplate(direction.activeName(), GROUP,
                    activeDoc + " Classic partitions have a series for each role of this broker for the partition.",
                    topicType, "role"));
            }
            templates.add(new MetricNameTemplate(direction.activeName(), GROUP, activeDoc, topicType));
        }
        templates.add(new MetricNameTemplate(HOSTED_PARTITIONS, GROUP, HOSTED_PARTITIONS_DOC, topicType));
        return templates;
    }

    private static final class Counter {
        final LongAdder bytesIn = new LongAdder();
        final LongAdder bytesOut = new LongAdder();
        // Only the snapshot thread touches them.
        long lastBytesIn;
        long lastBytesOut;
    }

    /**
     * The rates of one direction and topic type during a snapshot run. Only the snapshot thread touches it.
     */
    private static final class Accumulator {
        final DoubleHistogram rates = new DoubleHistogram(SIGNIFICANT_DIGITS);
        double total;
        long activeLeader;
        long activeFollower;

        void reset() {
            rates.reset();
            total = 0.0;
            activeLeader = 0;
            activeFollower = 0;
        }

        GroupStats stats() {
            return computeStats(rates, total, activeLeader, activeFollower);
        }
    }

    /**
     * The result of one snapshot for one direction and topic type.
     *
     * @param cumulativeBuckets Cumulative. The last index is the {@code Inf} bound.
     */
    record GroupStats(long active, long activeLeader, long activeFollower, double[] quantiles, double total,
                      long[] cumulativeBuckets) {
        static final GroupStats EMPTY =
            new GroupStats(0, 0, 0, new double[QUANTILES.length], 0.0, new long[BUCKET_BOUNDS.length + 1]);

    }

    /**
     * The published result of one snapshot run.
     */
    record Snapshot(GroupStats classicIn, GroupStats classicOut, GroupStats disklessIn, GroupStats disklessOut,
                    int hostedClassic, int hostedDiskless) {
        static final Snapshot EMPTY =
            new Snapshot(GroupStats.EMPTY, GroupStats.EMPTY, GroupStats.EMPTY, GroupStats.EMPTY, 0, 0);

        GroupStats of(final Direction direction, final boolean diskless) {
            if (direction == Direction.IN) {
                return diskless ? disklessIn : classicIn;
            }
            return diskless ? disklessOut : classicOut;
        }

        int hosted(final boolean diskless) {
            return diskless ? hostedDiskless : hostedClassic;
        }
    }

    private final Time time;
    private final Supplier<Hosted> hostedSupplier;
    private final Map<TopicPartition, Counter> counters = new ConcurrentHashMap<>();
    private final Accumulator classicIn = new Accumulator();
    private final Accumulator classicOut = new Accumulator();
    private final Accumulator disklessIn = new Accumulator();
    private final Accumulator disklessOut = new Accumulator();
    private final List<Runnable> unregisterActions = new ArrayList<>();
    private long lastRunMs;
    private volatile Snapshot snapshot = Snapshot.EMPTY;

    public PartitionRateTracker(final Time time, final Supplier<Hosted> hostedSupplier) {
        this.time = time;
        this.hostedSupplier = hostedSupplier;
        this.lastRunMs = time.hiResClockMs();
    }

    /**
     * Adds bytes that the broker committed (in) or sent (out) for a partition.
     * The snapshot assigns the topic type and the role of the partition.
     */
    public void record(final Direction direction, final TopicPartition tp, final long bytes) {
        if (bytes <= 0) {
            return;
        }
        Counter counter = counters.get(tp);
        if (counter == null) {
            counter = counters.computeIfAbsent(tp, k -> new Counter());
        }
        (direction == Direction.IN ? counter.bytesIn : counter.bytesOut).add(bytes);
    }

    /**
     * Turns the byte counts into rates and publishes them.
     * A partition that gets its first bytes during an interval counts them over the whole interval,
     * so its first rate is lower than its real rate.
     */
    public void snapshot() {
        final Hosted hosted = hostedSupplier.get();
        final long nowMs = time.hiResClockMs();
        final long elapsedMs = nowMs - lastRunMs;
        lastRunMs = nowMs;

        classicIn.reset();
        classicOut.reset();
        disklessIn.reset();
        disklessOut.reset();
        final Iterator<Map.Entry<TopicPartition, Counter>> iterator = counters.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<TopicPartition, Counter> entry = iterator.next();
            final Placement placement = hosted.placement().apply(entry.getKey());
            if (placement == Placement.NOT_HOSTED) {
                iterator.remove();
                continue;
            }
            final Counter counter = entry.getValue();
            final boolean diskless = placement == Placement.DISKLESS;
            final long in = counter.bytesIn.sum();
            addRate(diskless ? disklessIn : classicIn, placement, in - counter.lastBytesIn, elapsedMs);
            counter.lastBytesIn = in;
            final long out = counter.bytesOut.sum();
            addRate(diskless ? disklessOut : classicOut, placement, out - counter.lastBytesOut, elapsedMs);
            counter.lastBytesOut = out;
        }
        snapshot = new Snapshot(classicIn.stats(), classicOut.stats(), disklessIn.stats(), disklessOut.stats(),
            hosted.classicLeaders(), hosted.disklessPartitions());
    }

    /**
     * Adds the rate of {@code delta} bytes over {@code elapsedMs} to {@code accumulator}, if both are positive.
     */
    private static void addRate(final Accumulator accumulator, final Placement placement, final long delta,
                                final long elapsedMs) {
        if (elapsedMs <= 0 || delta <= 0) {
            return;
        }
        final double rate = delta * 1000.0 / elapsedMs;
        accumulator.rates.recordValue(rate);
        accumulator.total += rate;
        if (placement == Placement.CLASSIC_LEADER) {
            accumulator.activeLeader++;
        } else if (placement == Placement.CLASSIC_FOLLOWER) {
            accumulator.activeFollower++;
        }
    }

    /**
     * Returns the stats of one group from its histogram of rates in bytes per second.
     * The quantiles, the max, and the bucket counts are within 1% of the recorded rates.
     */
    static GroupStats computeStats(final DoubleHistogram rates, final double total, final long activeLeader,
                                   final long activeFollower) {
        final long n = rates.getTotalCount();
        final double[] quantiles = new double[QUANTILES.length];
        final long[] buckets = new long[BUCKET_BOUNDS.length + 1];
        if (n > 0) {
            for (int i = 0; i < QUANTILE_PERCENTS.length; i++) {
                quantiles[i] = rates.getValueAtPercentile(QUANTILE_PERCENTS[i]);
            }
            for (int i = 0; i < BUCKET_BOUNDS.length; i++) {
                buckets[i] = (long) rates.getCountBetweenValues(0, BUCKET_BOUNDS[i]);
            }
            buckets[BUCKET_BOUNDS.length] = n;
        }
        return new GroupStats(n, activeLeader, activeFollower, quantiles, total, buckets);
    }

    Snapshot currentSnapshot() {
        return snapshot;
    }

    /**
     * Registers the gauges in {@code metricsGroup}. Call {@link #close()} to remove them.
     */
    public void registerMetrics(final KafkaMetricsGroup metricsGroup) {
        for (final Direction direction : Direction.values()) {
            for (final boolean diskless : new boolean[] {false, true}) {
                final Map<String, String> tags = withTag(new LinkedHashMap<>(), "topicType", topicTypeTag(diskless));
                for (int i = 0; i < QUANTILES.length; i++) {
                    final int index = i;
                    register(metricsGroup, direction.rateName() + QUANTILES[i], tags,
                        () -> snapshot.of(direction, diskless).quantiles[index]);
                }
                for (int i = 0; i <= BUCKET_BOUNDS.length; i++) {
                    final int index = i;
                    final String le = i < BUCKET_BOUNDS.length ? Long.toString(BUCKET_BOUNDS[i]) : INF;
                    register(metricsGroup, direction.bucketsName(), withTag(tags, "le", le),
                        () -> snapshot.of(direction, diskless).cumulativeBuckets[index]);
                }
                if (direction == Direction.OUT && !diskless) {
                    register(metricsGroup, direction.activeName(), withTag(tags, "role", "leader"),
                        () -> snapshot.classicOut().activeLeader);
                    register(metricsGroup, direction.activeName(), withTag(tags, "role", "follower"),
                        () -> snapshot.classicOut().activeFollower);
                } else {
                    register(metricsGroup, direction.activeName(), tags, () -> snapshot.of(direction, diskless).active);
                }
                register(metricsGroup, direction.sumName(), tags, () -> snapshot.of(direction, diskless).total);
            }
        }
        for (final boolean diskless : new boolean[] {false, true}) {
            register(metricsGroup, HOSTED_PARTITIONS,
                withTag(new LinkedHashMap<>(), "topicType", topicTypeTag(diskless)),
                () -> snapshot.hosted(diskless));
        }
    }

    private static String topicTypeTag(final boolean diskless) {
        return diskless ? "diskless" : "classic";
    }

    private <T> void register(final KafkaMetricsGroup metricsGroup, final String name, final Map<String, String> tags,
                              final Supplier<T> value) {
        metricsGroup.newGauge(name, value, tags);
        unregisterActions.add(() -> metricsGroup.removeMetric(name, tags));
    }

    private static Map<String, String> withTag(final Map<String, String> tags, final String key, final String value) {
        final Map<String, String> result = new LinkedHashMap<>(tags);
        result.put(key, value);
        return result;
    }

    @Override
    public void close() {
        unregisterActions.forEach(Runnable::run);
        unregisterActions.clear();
    }
}
