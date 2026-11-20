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
package org.apache.kafka.jmh.metadata;

import kafka.server.metadata.InklessMetadataView;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.common.KRaftVersion;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Measures the per-call cost of the topic type checks that run for each partition on the produce and fetch paths.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class InklessMetadataViewTopicFlagBenchmark {
    private static final int TOPIC_COUNT = 1000;

    @Param({"0", "10", "30"})
    private int additionalOverridesPerTopic;

    private InklessMetadataView view;
    private String[] topics;
    private int next;

    @Setup
    public void setup() {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        topics = new String[TOPIC_COUNT];
        IntStream.range(0, TOPIC_COUNT).forEach(i -> {
            String topic = "topic-" + i;
            topics[i] = topic;
            delta.replay(new TopicRecord().setName(topic).setTopicId(Uuid.randomUuid()));
            // Half of the topics are diskless with remote storage, as for a consolidating topic.
            if (i % 2 == 0) {
                replayConfig(delta, topic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
                replayConfig(delta, topic, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            }
            IntStream.range(0, additionalOverridesPerTopic).forEach(k ->
                replayConfig(delta, topic, "benchmark.override." + k, "value-" + k));
        });
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(1, () -> KRaftVersion.KRAFT_VERSION_1);
        metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY));
        view = new InklessMetadataView(metadataCache, Map::of);
    }

    private static void replayConfig(MetadataDelta delta, String topic, String key, String value) {
        delta.replay(new ConfigRecord()
            .setResourceType(ConfigResource.Type.TOPIC.id())
            .setResourceName(topic)
            .setName(key)
            .setValue(value));
    }

    private String nextTopic() {
        next = (next + 1) % TOPIC_COUNT;
        return topics[next];
    }

    @Benchmark
    public boolean isDisklessTopic() {
        return view.isDisklessTopic(nextTopic());
    }

    @Benchmark
    public boolean isConsolidatingDisklessTopic() {
        return view.isConsolidatingDisklessTopic(nextTopic());
    }
}
