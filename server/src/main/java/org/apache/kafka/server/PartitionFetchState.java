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

package org.apache.kafka.server;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.util.Optional;

/**
 * Class to keep partition offset and its state (truncatingLog, delayed)
 * This represents a partition as being either:
 * (1) Truncating its log, for example, having recently become a follower
 * (2) Delayed, for example, due to an error, where we subsequently back off a bit
 * (3) ReadyForFetch, the active state where the thread is actively fetching data.
 *
 * @param topicId The topic ID
 * @param fetchOffset The offset to fetch from
 * @param lag The lag behind the leader
 * @param currentLeaderEpoch The current leader epoch for this partition in the local cluster.
 *                           Used for leader/follower epoch validation and synchronization.
 * @param delay Optional delay before next fetch
 * @param state The current replica state (TRUNCATING, FETCHING, etc.)
 * @param lastFetchedEpoch The last fetched epoch from the log
 * @param dueMs The time when this partition is due for fetch (if delayed)
 * @param mirrorName non-empty if this is a mirroring fetch
 * @param mirrorLeaderEpoch The current mirror leader epoch known for this partition in the cluster.
 *                          Used for validation for the batch leader epoch from the fetch response.
 */
public record PartitionFetchState(
        Optional<Uuid> topicId,
        long fetchOffset,
        Optional<Long> lag,
        int currentLeaderEpoch,
        Optional<Long> delay,
        ReplicaState state,
        Optional<Integer> lastFetchedEpoch,
        Optional<Long> dueMs,
        String mirrorName,
        Optional<Integer> mirrorLeaderEpoch
) {
    public PartitionFetchState(
            Optional<Uuid> topicId,
            long fetchOffset,
            Optional<Long> lag,
            int currentLeaderEpoch,
            ReplicaState state,
            Optional<Integer> lastFetchedEpoch) {
        this(topicId, fetchOffset, lag, currentLeaderEpoch,
                Optional.empty(), state, lastFetchedEpoch);
    }

    public PartitionFetchState(
            Optional<Uuid> topicId,
            long fetchOffset,
            Optional<Long> lag,
            int currentLeaderEpoch,
            ReplicaState state,
            Optional<Integer> lastFetchedEpoch,
            String mirrorName,
            Optional<Integer> mirrorLeaderEpoch) {
        this(topicId, fetchOffset, lag, currentLeaderEpoch,
                Optional.empty(), state, lastFetchedEpoch, Optional.empty(), mirrorName, mirrorLeaderEpoch);
    }

    public PartitionFetchState(
            Optional<Uuid> topicId,
            long fetchOffset,
            Optional<Long> lag,
            int currentLeaderEpoch,
            Optional<Long> delay,
            ReplicaState state,
            Optional<Integer> lastFetchedEpoch) {
        this(topicId, fetchOffset, lag, currentLeaderEpoch,
                delay, state, lastFetchedEpoch,
                delay.map(aLong -> aLong + Time.SYSTEM.milliseconds()), "", Optional.empty());
    }

    public boolean isReadyForFetch() {
        return state == ReplicaState.FETCHING && !isDelayed();
    }

    public boolean isReplicaInSync() {
        return lag.isPresent() && lag.get() <= 0;
    }

    public boolean isTruncating() {
        return state == ReplicaState.TRUNCATING && !isDelayed();
    }

    public boolean isDelayed() {
        return dueMs.filter(aLong -> aLong > Time.SYSTEM.milliseconds()).isPresent();
    }

    public boolean isMirrorFetch() {
        return mirrorName != null && !mirrorName.isBlank();
    }

    @Override
    public String toString() {
        return "FetchState(topicId=" + topicId +
                ", fetchOffset=" + fetchOffset +
                ", currentLeaderEpoch=" + currentLeaderEpoch +
                ", lastFetchedEpoch=" + lastFetchedEpoch +
                ", state=" + state +
                ", lag=" + lag +
                ", delay=" + delay.orElse(0L) + "ms)" +
                ", mirrorName=" + mirrorName +
                ", mirrorLeaderEpoch=" + mirrorLeaderEpoch +")";
    }

    public PartitionFetchState updateTopicId(Optional<Uuid> newTopicId) {
        return new PartitionFetchState(newTopicId, this.fetchOffset, this.lag,
                this.currentLeaderEpoch, this.delay,
                this.state, this.lastFetchedEpoch, this.dueMs, this.mirrorName, this.mirrorLeaderEpoch);
    }
}