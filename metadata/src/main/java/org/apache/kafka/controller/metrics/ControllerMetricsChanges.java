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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Map.Entry;
import java.util.function.Function;


/**
 * The ControllerMetricsChanges class is used inside ControllerMetricsPublisher to track the
 * metrics changes triggered by a series of deltas.
 */
@SuppressWarnings("NPathComplexity")
class ControllerMetricsChanges {

    private final Function<String, Boolean> isInklessTopic;

    ControllerMetricsChanges() {
        this.isInklessTopic = topicName -> false; // Default implementation, can be overridden
    }

    ControllerMetricsChanges(Function<String, Boolean> isInklessTopic) {
        this.isInklessTopic = isInklessTopic;
    }

    /**
     * Calculates the change between two boolean values, expressed as an integer.
     */
    static int delta(boolean prev, boolean next) {
        if (prev) {
            return next ? 0 : -1;
        } else {
            return next ? 1 : 0;
        }
    }

    private int fencedBrokersChange = 0;
    private int activeBrokersChange = 0;
    private int controlledShutdownBrokersChange = 0;
    private int globalTopicsChange = 0;
    private int globalPartitionsChange = 0;
    private int offlinePartitionsChange = 0;
    private int partitionsWithoutPreferredLeaderChange = 0;
    private int uncleanLeaderElection = 0;
    private int electionFromElr = 0;

    public int fencedBrokersChange() {
        return fencedBrokersChange;
    }

    public int activeBrokersChange() {
        return activeBrokersChange;
    }

    public int controlledShutdownBrokersChange() {
        return controlledShutdownBrokersChange;
    }

    public int globalTopicsChange() {
        return globalTopicsChange;
    }

    public int globalPartitionsChange() {
        return globalPartitionsChange;
    }

    public int offlinePartitionsChange() {
        return offlinePartitionsChange;
    }

    public int uncleanLeaderElection() {
        return uncleanLeaderElection;
    }

    public int electionFromElr() {
        return electionFromElr;
    }

    public int partitionsWithoutPreferredLeaderChange() {
        return partitionsWithoutPreferredLeaderChange;
    }

    void handleBrokerChange(BrokerRegistration prev, BrokerRegistration next, ControllerMetadataMetrics metrics) {
        boolean wasFenced = false;
        boolean wasActive = false;
        boolean wasInControlledShutdown = false;
        if (prev != null) {
            wasFenced = prev.fenced();
            wasActive = !prev.fenced();
            wasInControlledShutdown = prev.inControlledShutdown();
        } else {
            metrics.addBrokerRegistrationStateMetric(next.id());
        }
        boolean isFenced = false;
        boolean isActive = false;
        boolean isInControlledShutdown = false;
        final int brokerId;
        if (next != null) {
            isFenced = next.fenced();
            isActive = !next.fenced();
            isInControlledShutdown = next.inControlledShutdown();
            brokerId = next.id();
        } else {
            brokerId = prev.id();
        }
        metrics.setBrokerRegistrationState(brokerId, next);
        fencedBrokersChange += delta(wasFenced, isFenced);
        activeBrokersChange += delta(wasActive, isActive);
        controlledShutdownBrokersChange += delta(wasInControlledShutdown, isInControlledShutdown);
    }

    void handleDeletedTopic(TopicImage deletedTopic) {
        deletedTopic.partitions().values().forEach(prev -> handlePartitionChange(prev, null, isInklessTopic.apply(deletedTopic.name())));
        globalTopicsChange--;
    }

    void handleTopicChange(TopicImage prev, TopicDelta topicDelta) {
        final Boolean isInkless = isInklessTopic.apply(topicDelta.name());
        if (prev == null) {
            globalTopicsChange++;
            for (PartitionRegistration nextPartition : topicDelta.partitionChanges().values()) {
                handlePartitionChange(null, nextPartition, isInkless);
            }
        } else {
            for (Entry<Integer, PartitionRegistration> entry : topicDelta.partitionChanges().entrySet()) {
                int partitionId = entry.getKey();
                PartitionRegistration prevPartition = prev.partitions().get(partitionId);
                PartitionRegistration nextPartition = entry.getValue();
                handlePartitionChange(prevPartition, nextPartition, isInkless);
            }
        }
        if (!isInkless) {
            topicDelta.partitionToUncleanLeaderElectionCount().forEach((partitionId, count) -> uncleanLeaderElection += count);
            topicDelta.partitionToElrElectionCount().forEach((partitionId, count) -> electionFromElr += count);
        }
    }

    void handlePartitionChange(PartitionRegistration prev, PartitionRegistration next, boolean isInkless) {
        boolean wasPresent = false;
        boolean wasOffline = false;
        boolean wasWithoutPreferredLeader = false;
        if (prev != null) {
            wasPresent = true;
            wasOffline = !prev.hasLeader();
            wasWithoutPreferredLeader = !prev.hasPreferredLeader();
        }
        if (isInkless) {
            wasPresent = true;
            wasOffline = false; // Inkless partitions are always considered online
            wasWithoutPreferredLeader = false; // Inkless partitions are always considered to have a preferred leader
        }
        boolean isPresent = false;
        boolean isOffline = false;
        boolean isWithoutPreferredLeader = false;
        if (next != null) {
            isPresent = true;
            isOffline = !next.hasLeader();
            isWithoutPreferredLeader = !next.hasPreferredLeader();
        }
        if (isInkless) {
            isPresent = true;
            isOffline = false; // Inkless partitions are always considered online
            isWithoutPreferredLeader = false; // Inkless partitions are always considered to have a preferred leader
        }
        globalPartitionsChange += delta(wasPresent, isPresent);
        offlinePartitionsChange += delta(wasOffline, isOffline);
        partitionsWithoutPreferredLeaderChange += delta(wasWithoutPreferredLeader, isWithoutPreferredLeader);
    }

    /**
     * Apply these changes to the metrics object.
     */
    void apply(ControllerMetadataMetrics metrics) {
        if (fencedBrokersChange != 0) {
            metrics.addToFencedBrokerCount(fencedBrokersChange);
        }
        if (activeBrokersChange != 0) {
            metrics.addToActiveBrokerCount(activeBrokersChange);
        }
        if (controlledShutdownBrokersChange != 0) {
            metrics.addToControlledShutdownBrokerCount(controlledShutdownBrokersChange);
        }
        if (globalTopicsChange != 0) {
            metrics.addToGlobalTopicCount(globalTopicsChange);
        }
        if (globalPartitionsChange != 0) {
            metrics.addToGlobalPartitionCount(globalPartitionsChange);
        }
        if (offlinePartitionsChange != 0) {
            metrics.addToOfflinePartitionCount(offlinePartitionsChange);
        }
        if (partitionsWithoutPreferredLeaderChange != 0) {
            metrics.addToPreferredReplicaImbalanceCount(partitionsWithoutPreferredLeaderChange);
        }
        if (uncleanLeaderElection > 0) {
            metrics.updateUncleanLeaderElection(uncleanLeaderElection);
            uncleanLeaderElection = 0;
        }
        if (electionFromElr > 0) {
            metrics.updateElectionFromEligibleLeaderReplicasCount(electionFromElr);
            electionFromElr = 0;
        }
    }
}
