/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh.metrics;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.common.metrics.PartitionRateTracker;

/**
 * Measures the partition rate hooks of the produce and fetch responses, per request, in conditions close to a broker.
 *
 * <p>Each request takes its partitions at random from a working set, which is a random subset of the stored entries.
 * Each request has its own copy of each topic name, as a parsed request does. The map lookup then compares the bytes
 * of the names. A thread takes a snapshot every second, which is 60 times the default rate. Before each snapshot,
 * it records bytes for every stored entry, so that no entry expires. Other threads
 * stream through memory to evict the caches, as the record payloads of a broker do. The JVM flags are those of
 * {@code kafka-run-class.sh}. The baseline methods build the same keys without the hooks; subtract them to get the
 * cost of the hooks.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=20",
    "-XX:InitiatingHeapOccupancyPercent=35", "-XX:+ExplicitGCInvokesConcurrent", "-XX:MaxInlineLevel=15"})
// 8 threads, as the default num.io.threads.
@Threads(8)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class InklessPartitionRateTrackerBenchmark {
    private static final int PARTITIONS_PER_TOPIC = 100;
    private static final int MIN_REQUESTS_PER_THREAD = 4096;
    private static final long POLLUTION_BYTES = 256L * 1024 * 1024;
    private static final AtomicInteger SEEDS = new AtomicInteger();

    @Param({"100000", "1000000"})
    private int storedPartitions;

    @Param({"1000", "100000"})
    private int workingSet;

    @Param({"1", "10", "100"})
    private int partitionsPerRequest;

    @Param({"2"})
    private int pollutionThreads;

    private PartitionRateTracker tracker;
    private int[] workingSetIndexes;
    private String[] topicNames;
    private Uuid[] topicIds;
    private final List<Thread> background = new ArrayList<>();
    private volatile boolean running;

    @Setup(Level.Trial)
    public void setup() {
        if (workingSet > storedPartitions) {
            throw new IllegalArgumentException("workingSet > storedPartitions");
        }
        final int topics = storedPartitions / PARTITIONS_PER_TOPIC;
        topicNames = new String[topics];
        topicIds = new Uuid[topics];
        final Map<String, Boolean> diskless = new HashMap<>();
        for (int t = 0; t < topics; t++) {
            topicNames[t] = "benchmark-topic-of-typical-length-" + t;
            topicIds[t] = Uuid.randomUuid();
            diskless.put(topicNames[t], t % 2 == 0);
        }
        // Stands for ReplicaManager.allPartitions, which the placement function reads for classic partitions.
        final Map<TopicPartition, Boolean> online = new ConcurrentHashMap<>();
        final PartitionRateTracker.Hosted hosted = new PartitionRateTracker.Hosted(storedPartitions / 2, storedPartitions / 2,
            tp -> diskless.get(tp.topic())
                ? PartitionRateTracker.Placement.DISKLESS
                : online.get(tp) ? PartitionRateTracker.Placement.CLASSIC_LEADER : PartitionRateTracker.Placement.CLASSIC_FOLLOWER);
        tracker = new PartitionRateTracker(Time.SYSTEM, () -> hosted);

        final TopicPartition[] stored = new TopicPartition[storedPartitions];
        final List<Integer> order = new ArrayList<>(storedPartitions);
        for (int i = 0; i < storedPartitions; i++) {
            stored[i] = key(i, copyOf(topicNames[i / PARTITIONS_PER_TOPIC]));
            online.put(stored[i], true);
            order.add(i);
        }
        Collections.shuffle(order, new Random(7));
        workingSetIndexes = new int[workingSet];
        for (int i = 0; i < workingSet; i++) {
            workingSetIndexes[i] = order.get(i);
        }

        running = true;
        final Thread snapshots = new Thread(() -> {
            while (running) {
                for (final TopicPartition tp : stored) {
                    tracker.record(PartitionRateTracker.Direction.IN, tp, 1);
                    tracker.record(PartitionRateTracker.Direction.OUT, tp, 1);
                }
                tracker.snapshot();
                sleep(1000);
            }
        }, "benchmark-snapshot");
        background.add(snapshots);
        for (int p = 0; p < pollutionThreads; p++) {
            background.add(new Thread(this::pollute, "benchmark-pollution-" + p));
        }
        background.forEach(thread -> {
            thread.setDaemon(true);
            thread.start();
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        running = false;
        for (final Thread thread : background) {
            thread.join();
        }
        background.clear();
    }

    // A new String with the same bytes, as parsing a request gives.
    private static String copyOf(final String name) {
        return new String(name.toCharArray());
    }

    private static TopicPartition key(final int index, final String topic) {
        return new TopicPartition(topic, index % PARTITIONS_PER_TOPIC);
    }

    private void pollute() {
        final long[] memory = new long[(int) (POLLUTION_BYTES / Long.BYTES)];
        long sum = 0;
        while (running) {
            for (int i = 0; i < memory.length; i += 8) {
                sum += memory[i]++;
            }
        }
        if (sum == 42) {
            System.out.println(sum);
        }
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * The partitions of the requests of one thread, built before the measurement, with one copy of each topic name per
     * request.
     */
    @State(Scope.Thread)
    public static class Requests {
        String[][] topics;
        int[][] partitions;
        Uuid[][] ids;
        int next;

        @Setup(Level.Trial)
        public void setup(final InklessPartitionRateTrackerBenchmark benchmark) {
            final SplittableRandom random = new SplittableRandom(SEEDS.incrementAndGet());
            final int k = benchmark.partitionsPerRequest;
            // Enough requests to record each partition of the working set about twice.
            final int requests = Math.max(MIN_REQUESTS_PER_THREAD, 2 * benchmark.workingSet / k);
            topics = new String[requests][k];
            partitions = new int[requests][k];
            ids = new Uuid[requests][k];
            for (int r = 0; r < requests; r++) {
                final Map<Integer, String> namesOfRequest = new HashMap<>();
                for (int i = 0; i < k; i++) {
                    final int index = benchmark.workingSetIndexes[random.nextInt(benchmark.workingSetIndexes.length)];
                    final int topic = index / PARTITIONS_PER_TOPIC;
                    topics[r][i] = namesOfRequest.computeIfAbsent(topic, t -> copyOf(benchmark.topicNames[t]));
                    partitions[r][i] = index % PARTITIONS_PER_TOPIC;
                    ids[r][i] = benchmark.topicIds[topic];
                }
            }
        }

        int nextRequest() {
            next = (next + 1) % topics.length;
            return next;
        }
    }

    /**
     * Runs the fetch hook. {@code KafkaApis.recordBytesOutMetric} builds the key, then
     * {@code ReplicaManager.recordClientBytesOut} records the bytes of a topic that isn't internal.
     */
    @Benchmark
    public void fetchHook(final Requests requests, final Blackhole blackhole) {
        final int r = requests.nextRequest();
        final String[] topics = requests.topics[r];
        final int[] partitions = requests.partitions[r];
        final Uuid[] ids = requests.ids[r];
        for (int i = 0; i < topics.length; i++) {
            final TopicIdPartition tp = new TopicIdPartition(ids[i], new TopicPartition(topics[i], partitions[i]));
            if (!Topic.isInternal(tp.topicPartition().topic())) {
                tracker.record(PartitionRateTracker.Direction.OUT, tp.topicPartition(), 1000);
            }
            blackhole.consume(tp);
        }
    }

    @Benchmark
    public void fetchBaseline(final Requests requests, final Blackhole blackhole) {
        final int r = requests.nextRequest();
        final String[] topics = requests.topics[r];
        final int[] partitions = requests.partitions[r];
        final Uuid[] ids = requests.ids[r];
        for (int i = 0; i < topics.length; i++) {
            blackhole.consume(new TopicIdPartition(ids[i], new TopicPartition(topics[i], partitions[i])));
        }
    }

    /**
     * Runs the produce hook. The handler keeps the record sizes in a map. For each partition, the response callback
     * reads the size and records the bytes of a topic that isn't internal.
     */
    @Benchmark
    public void produceHook(final Requests requests, final Blackhole blackhole) {
        final int r = requests.nextRequest();
        final String[] topics = requests.topics[r];
        final int[] partitions = requests.partitions[r];
        final Uuid[] ids = requests.ids[r];
        final TopicIdPartition[] request = new TopicIdPartition[topics.length];
        final Map<TopicIdPartition, Integer> sizes = new HashMap<>();
        for (int i = 0; i < topics.length; i++) {
            request[i] = new TopicIdPartition(ids[i], new TopicPartition(topics[i], partitions[i]));
            sizes.put(request[i], 1000);
        }
        for (final TopicIdPartition tp : request) {
            final Integer size = sizes.get(tp);
            if (size != null && !Topic.isInternal(tp.topicPartition().topic())) {
                tracker.record(PartitionRateTracker.Direction.IN, tp.topicPartition(), size);
            }
        }
        blackhole.consume(request);
    }

    @Benchmark
    public void produceBaseline(final Requests requests, final Blackhole blackhole) {
        final int r = requests.nextRequest();
        final String[] topics = requests.topics[r];
        final int[] partitions = requests.partitions[r];
        final Uuid[] ids = requests.ids[r];
        final TopicIdPartition[] request = new TopicIdPartition[topics.length];
        for (int i = 0; i < topics.length; i++) {
            request[i] = new TopicIdPartition(ids[i], new TopicPartition(topics[i], partitions[i]));
        }
        blackhole.consume(request);
    }
}
