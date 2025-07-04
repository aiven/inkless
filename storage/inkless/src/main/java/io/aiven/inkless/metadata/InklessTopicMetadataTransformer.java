/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
package io.aiven.inkless.metadata;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.LeaderAndIsr;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import io.aiven.inkless.control_plane.MetadataView;

public class InklessTopicMetadataTransformer {
    private final MetadataView metadataView;

    private final AtomicInteger roundRobinCounter = new AtomicInteger();

    public InklessTopicMetadataTransformer(final MetadataView metadataView) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
    }

    /**
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformClusterMetadata(
        final String clientId,
        final Iterable<MetadataResponseData.MetadataResponseTopic> topicMetadata
    ) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");

        final int leaderForInklessPartitions = selectLeaderForInklessPartitions(clientId);
        for (final var topic : topicMetadata) {
            if (!metadataView.isInklessTopic(topic.name())) {
                continue;
            }
            for (final var partition : topic.partitions()) {
                partition.setLeaderId(leaderForInklessPartitions);
                final List<Integer> list = List.of(leaderForInklessPartitions);
                partition.setReplicaNodes(list);
                partition.setIsrNodes(list);
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
            }
        }
    }

    public void transformClusterMetadataV2(
        final String clientId,
        final Iterable<MetadataResponseData.MetadataResponseTopic> topicMetadata
    ) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");

        int topicCount = metadataView.getTopicsCount();
        final var brokers = azAwareBrokers(clientId);
        for (final var topic : topicMetadata) {
            if (!metadataView.isInklessTopic(topic.name())) {
                continue;
            }
            for (final var partition : topic.partitions()) {
                final int partitionHash = topicCount + topicHash(topic.name()) + partition.partitionIndex();
                int leaderIndex = partitionHash % brokers.size();
                int leaderId = brokers.get(leaderIndex).id;
                partition.setLeaderId(leaderId);
                final List<Integer> list = List.of(leaderId);
                partition.setReplicaNodes(list);
                partition.setIsrNodes(list);
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
            }
        }
    }

    static int topicHash(String topicName) {
        return Utils.toPositive(Utils.murmur2(topicName.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformDescribeTopicResponse(
        final String clientId,
        final DescribeTopicPartitionsResponseData responseData
    ) {
        Objects.requireNonNull(responseData, "responseData cannot be null");

        final int leaderForInklessPartitions = selectLeaderForInklessPartitions(clientId);
        for (final var topic : responseData.topics()) {
            if (!metadataView.isInklessTopic(topic.name())) {
                continue;
            }

            for (final var partition : topic.partitions()) {
                partition.setLeaderId(leaderForInklessPartitions);
                final List<Integer> list = List.of(leaderForInklessPartitions);
                partition.setReplicaNodes(list);
                partition.setIsrNodes(list);
                partition.setEligibleLeaderReplicas(Collections.emptyList());
                partition.setLastKnownElr(Collections.emptyList());
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
            }
        }
    }

    public void transformDescribeTopicResponseV2(
        final String clientId,
        final DescribeTopicPartitionsResponseData responseData
    ) {
        Objects.requireNonNull(responseData, "responseData cannot be null");

        int topicCount = metadataView.getTopicsCount();
        final var brokers = azAwareBrokers(clientId);
        for (final var topic : responseData.topics()) {
            if (!metadataView.isInklessTopic(topic.name())) {
                continue;
            }

            for (final var partition : topic.partitions()) {
                final int partitionHash = topicCount + topicHash(topic.name()) + partition.partitionIndex();
                int leaderIndex = partitionHash % brokers.size();
                int leaderId = brokers.get(leaderIndex).id;
                partition.setLeaderId(leaderId);
                final List<Integer> list = List.of(leaderId);
                partition.setReplicaNodes(list);
                partition.setIsrNodes(list);
                partition.setEligibleLeaderReplicas(Collections.emptyList());
                partition.setLastKnownElr(Collections.emptyList());
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
            }
        }
    }

    /**
     * Select the broker ID to be the leader of all Inkless partitions.
     *
     * <p>The selection happens from brokers in the client AZ or from all brokers
     * (if brokers in the client AZ not found or the client AZ is not set).
     *
     * @return the selected broker ID.
     */
    private int selectLeaderForInklessPartitions(final String clientId) {
        final List<BrokerMetadata> brokersToPickFrom = azAwareBrokers(clientId);

        final int c = roundRobinCounter.getAndUpdate(v -> Math.max(v + 1, 0));
        final int idx = c % brokersToPickFrom.size();
        return brokersToPickFrom.get(idx).id;
    }

    private List<BrokerMetadata> azAwareBrokers(String clientId) {
        final String clientAZ = ClientAZExtractor.getClientAZ(clientId);
        // This gracefully handles the null client AZ, no need for a special check.
        final List<BrokerMetadata> brokersInClientAZ = brokersInAZ(clientAZ);
        // Fall back on all brokers if no broker in the client AZ.
        final List<BrokerMetadata> brokersToPickFrom = brokersInClientAZ.isEmpty()
            ? allAliveBrokers()
            : brokersInClientAZ;

        // This cannot happen in a normal broker run. This will serve as a guard in tests.
        if (brokersToPickFrom.isEmpty()) {
            throw new RuntimeException("No broker found, unexpected");
        }
        return brokersToPickFrom;
    }

    private List<BrokerMetadata> allAliveBrokers() {
        return StreamSupport.stream(metadataView.getAliveBrokers().spliterator(), false)
            .sorted(Comparator.comparing(bm -> bm.id))
            .toList();
    }

    /**
     * Get brokers in the specified AZ.
     *
     * @param az the AZ to look for, can be {@code null}.
     */
    private List<BrokerMetadata> brokersInAZ(final String az) {
        return StreamSupport.stream(metadataView.getAliveBrokers().spliterator(), false)
            .filter(bm -> Objects.equals(bm.rack.orElse(null), az))
            .sorted(Comparator.comparing(bm -> bm.id))
            .toList();
    }
}
