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
import org.apache.kafka.server.common.ClusterMirrorVersion;
import org.apache.kafka.server.common.MirrorPartitionState;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Shared data types and utility methods for cluster mirroring components.
 */
public final class ClusterMirrorUtils {
    public static final int LEADER_EPOCH_BUMP_THRESHOLD = 3;
    public static final int LEADER_EPOCH_BUMP_INCREMENT = 10;
    public static final int NON_RETRYABLE_ATTEMPT = -1;

    private ClusterMirrorUtils() {}

    /** Returns true if the finalized feature level supports cluster mirroring. */
    public static boolean isClusterMirroringEnabled(Map<String, Short> finalizedFeatures) {
        short featureLevel = finalizedFeatures.getOrDefault(ClusterMirrorVersion.FEATURE_NAME, (short) 0);
        return ClusterMirrorVersion.fromFeatureLevel(featureLevel).isClusterMirroringSupported();
    }

    /** Cached source cluster leader node and epoch for a mirror partition. */
    public record LeaderInfo(Node node, int leaderEpoch) { }

    /** Combined cache entry holding partition state, last mirror epoch, and failure metadata. */
    public record PartitionCacheEntry(MirrorPartitionState state, int lastMirrorEpoch, FailedPartitionInfo failedInfo) { }

    /** Tracks retry state for a partition in FAILED state. */
    public record FailedPartitionInfo(int retryAttempt, String errorMessage, MirrorPartitionState previousState) { }

    /** Partition state and leader epoch carried in WriteMirrorStates / ReadMirrorStates RPCs. */
    public record PartitionStateInfo(int partition, MirrorPartitionState state, Integer leaderEpoch) { }

    /** Pending leader epoch bump request with the future that completes when the bump is observed in metadata. */
    public record LeaderEpochBump(CompletableFuture<Void> future, Map<TopicPartition, Integer> partitionToEpoch) { }

    /** Callback for partition state transitions triggered by the coordinator. */
    interface StateTransitioner {
        void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state, String errorMessage, boolean nonRetryable);
    }

    /** Callback invoked after replaying partition states from the coordinator log. */
    interface StateTransitionCallback {
        void onStateReplayed(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState state);
    }
}
