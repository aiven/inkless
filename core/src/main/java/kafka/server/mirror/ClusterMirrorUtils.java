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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ClusterMirrorVersion;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.config.ClusterMirrorConfig;
import org.apache.kafka.server.network.BrokerEndPoint;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Shared data types and utility methods for cluster mirroring components.
 */
public final class ClusterMirrorUtils {
    public static final int LEADER_EPOCH_BUMP_THRESHOLD = 3;
    public static final int LEADER_EPOCH_BUMP_INCREMENT = 10;

    private ClusterMirrorUtils() {}

    public static boolean isClusterMirroringEnabled(Map<String, Short> finalizedFeatures) {
        short featureLevel = finalizedFeatures.getOrDefault(ClusterMirrorVersion.FEATURE_NAME, (short) 0);
        return ClusterMirrorVersion.fromFeatureLevel(featureLevel).isClusterMirroringSupported();
    }

    public static MirrorSourceSender createSender(BrokerEndPoint brokerEndpoint,
                                                  ClusterMirrorConfig mirrorConfig,
                                                  Metrics metrics,
                                                  Time time,
                                                  String clientId,
                                                  LogContext logContext) {
        return new MirrorSourceSender(brokerEndpoint, mirrorConfig,
                metrics, time, brokerEndpoint.id(), clientId, logContext);
    }

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

    public record PartitionStateInfo(int partition, MirrorPartitionState state, Integer leaderEpoch) { }

    public record PartitionStateLogEntry(String topic, int partition, MirrorPartitionState state,
                                         MirrorPartitionState previousState, short retryAttempt,
                                         String errorMessage) { }

    public record PartitionKey(String mirrorName, String topic, int partition) { }

    public record LeaderEpochBump(CompletableFuture<Void> future, Map<TopicPartition, Integer> partitionToEpoch) { }

    public record LeaderInfo(Node node, int leaderEpoch) { }

    public record FailedPartitionInfo(short retryAttempt, String errorMessage, MirrorPartitionState previousState) { }

    interface StateTransitioner {
        void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state, String errorMessage);
    }

    interface StateTransitionCallback {
        void onStateLoaded(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState state);
    }
}
