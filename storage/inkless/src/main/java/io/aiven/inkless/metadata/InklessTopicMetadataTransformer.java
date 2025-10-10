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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.LeaderAndIsr;

import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

import io.aiven.inkless.control_plane.MetadataView;

public class InklessTopicMetadataTransformer implements Closeable {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(InklessTopicMetadataTransformer.class);

    private final MetadataView metadataView;
    private final ClientAzAwarenessMetrics metrics;

    public InklessTopicMetadataTransformer(
        final MetadataView metadataView
    ) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        metrics = new ClientAzAwarenessMetrics();
    }

    /**
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformClusterMetadata(
        final ListenerName listenerName,
        final String clientId,
        final Iterable<MetadataResponseData.MetadataResponseTopic> topicMetadata
    ) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");

        for (final var topic : topicMetadata) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }
            for (final var partition : topic.partitions()) {
                final int leaderForInklessPartitions = selectLeaderForInklessPartitions(listenerName, clientId, topic.topicId(), partition.partitionIndex());
                partition.setLeaderId(leaderForInklessPartitions);
                final List<Integer> list = List.of(leaderForInklessPartitions);
                partition.setErrorCode(Errors.NONE.code());
                partition.setReplicaNodes(list);
                partition.setIsrNodes(list);
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
            }
        }
    }

    /**
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformDescribeTopicResponse(
        final ListenerName listenerName,
        final String clientId,
        final DescribeTopicPartitionsResponseData responseData
    ) {
        Objects.requireNonNull(responseData, "responseData cannot be null");

        for (final var topic : responseData.topics()) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }

            for (final var partition : topic.partitions()) {
                final int leaderForInklessPartitions = selectLeaderForInklessPartitions(listenerName, clientId, topic.topicId(), partition.partitionIndex());
                partition.setLeaderId(leaderForInklessPartitions);
                final List<Integer> list = List.of(leaderForInklessPartitions);
                partition.setErrorCode(Errors.NONE.code());
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
     * Select the broker ID to be the leader of all diskless partitions.
     *
     * <p>The selection happens from brokers in the client AZ or from all brokers
     * (if brokers in the client AZ not found or the client AZ is not set).
     *
     * @return the selected broker ID.
     */
    private int selectLeaderForInklessPartitions(final ListenerName listenerName, final String clientId,
                                                 final Uuid topicId,
                                                 final int partitionIndex) {
        final String clientAZ = ClientAZExtractor.getClientAZ(clientId);
        // This gracefully handles the null client AZ, no need for a special check.
        final List<Node> brokersInClientAZ = brokersInAZ(listenerName, clientAZ);
        // Fall back on all brokers if no broker in the client AZ.
        final List<Node> brokersToPickFrom = brokersInClientAZ.isEmpty()
            ? allAliveBrokers(listenerName)
            : brokersInClientAZ;

        if (clientAZ != null && brokersToPickFrom.isEmpty()) {
            LOG.warn("No alive brokers found from clientId {}, clientAZ {}; falling back to all brokers",
                clientId, clientAZ);
        }

        metrics.recordClientAz(clientAZ, !brokersInClientAZ.isEmpty());

        // This cannot happen in a normal broker run. This will serve as a guard in tests.
        if (brokersToPickFrom.isEmpty()) {
            throw new RuntimeException("No broker found, unexpected");
        }

        final byte[] input = String.format("%s-%s-%s", clientId, topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final long hash = Utils.murmur2(input);
        final int idx = Math.abs(Math.toIntExact(hash % brokersToPickFrom.size()));

        return brokersToPickFrom.get(idx).id();
    }

    private List<Node> allAliveBrokers(final ListenerName listenerName) {
        return StreamSupport.stream(metadataView.getAliveBrokerNodes(listenerName).spliterator(), false)
            .sorted(Comparator.comparing(Node::id))
            .toList();
    }

    /**
     * Get brokers in the specified AZ.
     *
     * @param az the AZ to look for, can be {@code null}.
     */
    private List<Node> brokersInAZ(final ListenerName listenerName, final String az) {
        return StreamSupport.stream(metadataView.getAliveBrokerNodes(listenerName).spliterator(), false)
            .filter(bm -> Objects.equals(bm.rack(), az))
            .sorted(Comparator.comparing(Node::id))
            .toList();
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }
}
