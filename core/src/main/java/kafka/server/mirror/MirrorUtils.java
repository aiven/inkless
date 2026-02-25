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
package kafka.server.mirror;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.controller.ConfigurationControlManager.PAUSED_TOPIC_SUFFIX;
import static org.apache.kafka.controller.ConfigurationControlManager.REMOVED_TOPIC_SUFFIX;

/**
 * Shared data types and utility methods for cluster mirroring components.
 */
public final class MirrorUtils {
    private MirrorUtils() {}

    public static Map<String, Set<Integer>> groupPartitionsByTopic(Set<TopicPartition> topicPartitions) {
        Map<String, Set<Integer>> topicToPartitions = new HashMap<>();
        topicPartitions.forEach(tp -> {
            topicToPartitions.compute(tp.topic(), (k, preV) -> {
                if (preV == null) {
                    return Set.of(tp.partition());
                }
                Set<Integer> results = new HashSet<>(preV);
                results.add(tp.partition());
                return results;
            });
        });
        return topicToPartitions;
    }

    /**
     * Removes the REMOVED_TOPIC_SUFFIX from a mirror name if present.
     * This suffix is appended to mirror names when topics are being removed from mirroring.
     */
    public static String originalMirrorName(String mirrorName) {
        if (mirrorName == null) {
            return "";
        }
        if (mirrorName.endsWith(REMOVED_TOPIC_SUFFIX)) {
            return mirrorName.substring(0, mirrorName.length() - REMOVED_TOPIC_SUFFIX.length());
        }
        if (mirrorName.endsWith(PAUSED_TOPIC_SUFFIX)) {
            return mirrorName.substring(0, mirrorName.length() - PAUSED_TOPIC_SUFFIX.length());
        }
        return mirrorName;
    }

    public record PartitionStateInfo(int partition, MirrorPartitionState state, Long offset) { }

    public record PartitionStateLogEntry(String topic, int partition, MirrorPartitionState state) { }

    public record PartitionKey(String mirrorName, String topic, int partition) { }

    interface StateTransitioner {
        void transitionTo(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state);
    }

    interface StateTransitionCallback {
        void onStateLoaded(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState state);
    }
}
