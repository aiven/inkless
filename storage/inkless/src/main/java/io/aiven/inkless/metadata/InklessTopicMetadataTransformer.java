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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.aiven.inkless.control_plane.MetadataView;

/**
 * Transforms metadata responses for diskless topics.
 *
 * <p>For managed replicas (RF > 1), routing priority depends on remote storage config:
 * <ul>
 *   <li>Diskless and Tiered topic: same-AZ replica > cross-AZ replica > unavailable</li>
 *   <li>Only Diskless (remote storage disabled): same-AZ replica > same-AZ any broker > cross-AZ replica > cross-AZ any broker</li>
 * </ul>
 *
 * <p>For unmanaged replicas (RF = 1), uses legacy behavior: hash-based selection from all alive brokers.
 */
public class InklessTopicMetadataTransformer implements Closeable {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(InklessTopicMetadataTransformer.class);

    private final MetadataView metadataView;
    private final ClientAzAwarenessMetrics metrics;

    public InklessTopicMetadataTransformer(final MetadataView metadataView) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        metrics = new ClientAzAwarenessMetrics();
    }

    /**
     * Transform cluster metadata response for diskless topics.
     *
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformClusterMetadata(
        final ListenerName listenerName,
        final String clientId,
        final Iterable<MetadataResponseData.MetadataResponseTopic> topicMetadata
    ) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");

        for (final var topic : topicMetadata) {
            final String topicName = topic.name();
            if (!metadataView.isDisklessTopic(topicName)) {
                continue;
            }

            // Check if remote storage is enabled (affects routing constraints)
            final boolean isRemoteStorageEnabled = metadataView.isRemoteStorageEnabled(topicName);

            for (final var partition : topic.partitions()) {
                final List<Integer> assignedReplicas = partition.replicaNodes();
                final boolean isManagedReplicas = assignedReplicas.size() > 1;

                if (isManagedReplicas) {
                    transformManagedReplicasPartitionMetadata(
                        listenerName,
                        clientId,
                        topic.topicId(),
                        partition,
                        assignedReplicas,
                        isRemoteStorageEnabled
                    );
                } else {
                    // RF=1 (unmanaged): use legacy behavior
                    transformUnmanagedPartitionMetadata(listenerName, clientId, topic.topicId(), partition);
                }
            }
        }
    }

    /**
     * Transform describe topic partitions response for diskless topics.
     *
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformDescribeTopicResponse(
        final ListenerName listenerName,
        final String clientId,
        final DescribeTopicPartitionsResponseData responseData
    ) {
        Objects.requireNonNull(responseData, "responseData cannot be null");

        for (final var topic : responseData.topics()) {
            final String topicName = topic.name();
            if (!metadataView.isDisklessTopic(topicName)) {
                continue;
            }

            // Check if remote storage is enabled (affects routing constraints)
            final boolean isRemoteStorageEnabled = metadataView.isRemoteStorageEnabled(topicName);

            for (final var partition : topic.partitions()) {
                final List<Integer> assignedReplicas = partition.replicaNodes();
                final boolean isManagedReplicas = assignedReplicas.size() > 1;

                if (isManagedReplicas) {
                    transformManagedReplicasPartitionDescribe(
                        listenerName, clientId, topic.topicId(),
                        partition, assignedReplicas, isRemoteStorageEnabled
                    );
                } else {
                    // RF=1 (unmanaged): use legacy behavior
                    transformUnmanagedPartitionDescribe(
                        listenerName, clientId, topic.topicId(), partition
                    );
                }
            }
        }
    }

    /**
     * Transform partition with managed replicas (RF > 1) for Metadata response.
     */
    private void transformManagedReplicasPartitionMetadata(
        final ListenerName listenerName,
        final String clientId,
        final Uuid topicId,
        final MetadataResponseData.MetadataResponsePartition partition,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled
    ) {
        final LeaderSelectionResult result = selectLeaderForManagedReplicas(
            listenerName, clientId, topicId, partition.partitionIndex(), assignedReplicas, isRemoteStorageEnabled
        );

        partition.setLeaderId(result.leaderId());
        if (result.available()) {
            partition.setErrorCode(Errors.NONE.code());
        }
        // Keep original replicaNodes - show real RF to clients
        // ISR: show only alive replicas to avoid confusing tooling
        partition.setIsrNodes(result.aliveReplicas());
        partition.setOfflineReplicas(Collections.emptyList());
        // Keep INITIAL_LEADER_EPOCH: diskless doesn't have real leader elections
        partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
    }

    /**
     * Transform partition with managed replicas (RF > 1) for DescribeTopicPartitions response.
     */
    private void transformManagedReplicasPartitionDescribe(
        final ListenerName listenerName,
        final String clientId,
        final Uuid topicId,
        final DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition partition,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled
    ) {
        final LeaderSelectionResult result = selectLeaderForManagedReplicas(
            listenerName, clientId, topicId, partition.partitionIndex(), assignedReplicas, isRemoteStorageEnabled
        );

        partition.setLeaderId(result.leaderId());
        if (result.available()) {
            partition.setErrorCode(Errors.NONE.code());
        }
        // Keep original replicaNodes - show real RF to clients
        // ISR: show only alive replicas to avoid confusing tooling
        partition.setIsrNodes(result.aliveReplicas());
        partition.setEligibleLeaderReplicas(Collections.emptyList());
        partition.setLastKnownElr(Collections.emptyList());
        partition.setOfflineReplicas(Collections.emptyList());
        partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
    }

    /**
     * Transform partition with unmanaged replicas (RF=1) for Metadata response.
     * Uses legacy behavior: hash-based selection from all alive brokers.
     */
    private void transformUnmanagedPartitionMetadata(
        final ListenerName listenerName,
        final String clientId,
        final Uuid topicId,
        final MetadataResponseData.MetadataResponsePartition partition
    ) {
        final int selectedLeader = selectLeaderLegacy(listenerName, clientId, topicId, partition.partitionIndex());
        final List<Integer> singleReplica = List.of(selectedLeader);

        partition.setLeaderId(selectedLeader);
        partition.setErrorCode(Errors.NONE.code());
        partition.setReplicaNodes(singleReplica);
        partition.setIsrNodes(singleReplica);
        partition.setOfflineReplicas(Collections.emptyList());
        partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
    }

    /**
     * Transform partition with unmanaged replicas (RF=1) for DescribeTopicPartitions response.
     */
    private void transformUnmanagedPartitionDescribe(
        final ListenerName listenerName,
        final String clientId,
        final Uuid topicId,
        final DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition partition
    ) {
        final int selectedLeader = selectLeaderLegacy(listenerName, clientId, topicId, partition.partitionIndex());
        final List<Integer> singleReplica = List.of(selectedLeader);

        partition.setLeaderId(selectedLeader);
        partition.setErrorCode(Errors.NONE.code());
        partition.setReplicaNodes(singleReplica);
        partition.setIsrNodes(singleReplica);
        partition.setEligibleLeaderReplicas(Collections.emptyList());
        partition.setLastKnownElr(Collections.emptyList());
        partition.setOfflineReplicas(Collections.emptyList());
        partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
    }

    /**
     * Select leader for managed replicas.
     *
     * <p>When remote storage is enabled: same-AZ replica > cross-AZ replica > unavailable
     * <p>When remote storage is disabled: same-AZ replica > same-AZ any broker > cross-AZ replica > cross-AZ any broker
     */
    private LeaderSelectionResult selectLeaderForManagedReplicas(
        final ListenerName listenerName,
        final String clientId,
        final Uuid topicId,
        final int partitionIndex,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled
    ) {
        final String clientAZ = ClientAZExtractor.getClientAZ(clientId);
        final Set<Integer> aliveBrokerIds = getAliveBrokerIds(listenerName);

        // Compute broker sets for routing decisions
        final List<Integer> aliveReplicas = assignedReplicas.stream()
            .filter(aliveBrokerIds::contains)
            .toList();
        final List<Integer> aliveReplicasInClientAZ = filterByAZ(listenerName, aliveReplicas, clientAZ);
        final List<Integer> allAliveBrokersInClientAZ = filterByAZ(listenerName, new ArrayList<>(aliveBrokerIds), clientAZ);

        final int selectedLeader;
        final boolean partitionAvailable;

        // Track if any replicas are offline but we can still route
        final boolean hasOfflineReplicas = aliveReplicas.size() < assignedReplicas.size();

        if (isRemoteStorageEnabled) {
            // Remote storage enabled: must stay on replicas (RLM requires UnifiedLog)
            // Priority: same-AZ replica > cross-AZ replica > unavailable

            if (!aliveReplicasInClientAZ.isEmpty()) {
                // Best: replica in same AZ
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicasInClientAZ);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, true);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else if (!aliveReplicas.isEmpty()) {
                // Fallback: replica in other AZ (cross-AZ)
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicas);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, false);
                // Only record cross-AZ routing if we know the client's AZ
                if (clientAZ != null) {
                    metrics.recordCrossAzRouting();
                }
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
                LOG.debug("Tiered partition {}-{}: no replica in client AZ {}, using cross-AZ replica",
                    topicId, partitionIndex, clientAZ);
            } else {
                // All replicas offline — partition unavailable (Kafka semantics)
                // Cannot fall back to non-replica brokers even in same AZ
                // because tiered reads REQUIRE replica brokers with UnifiedLog/RLM state
                selectedLeader = assignedReplicas.isEmpty() ? -1 : assignedReplicas.get(0);
                partitionAvailable = false;
                LOG.warn("Tiered partition {}-{}: all replicas offline, partition unavailable",
                    topicId, partitionIndex);
            }
        } else {
            // Remote storage disabled: can fall back to any broker for availability
            // Priority: same-AZ replica > same-AZ any broker > cross-AZ replica > cross-AZ any broker

            if (!aliveReplicasInClientAZ.isEmpty()) {
                // Best: assigned replica in same AZ
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicasInClientAZ);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, true);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else if (!allAliveBrokersInClientAZ.isEmpty()) {
                // Assigned replica in client AZ is offline, but other brokers in AZ are alive
                selectedLeader = selectByHash(topicId, partitionIndex, allAliveBrokersInClientAZ);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, true);
                metrics.recordFallback();
                metrics.recordOfflineReplicasRoutedAround();
                LOG.debug("Diskless partition {}-{}: no replica in client AZ {}, using same-AZ non-replica broker",
                    topicId, partitionIndex, clientAZ);
            } else if (!aliveReplicas.isEmpty()) {
                // No brokers in client AZ available; use replica in other AZ (cross-AZ)
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicas);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, false);
                // Only record cross-AZ routing if we know the client's AZ
                if (clientAZ != null) {
                    metrics.recordCrossAzRouting();
                }
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
                LOG.debug("Diskless partition {}-{}: no brokers in client AZ {}, using cross-AZ replica",
                    topicId, partitionIndex, clientAZ);
            } else {
                // Absolute last resort: any alive broker (cross-AZ, non-replica)
                final List<Integer> allAliveBrokers = new ArrayList<>(aliveBrokerIds);
                if (!allAliveBrokers.isEmpty()) {
                    selectedLeader = selectByHash(topicId, partitionIndex, allAliveBrokers);
                    partitionAvailable = true;
                    metrics.recordClientAz(clientAZ, false);
                    metrics.recordFallback();
                    // Only record cross-AZ routing if we know the client's AZ
                    if (clientAZ != null) {
                        metrics.recordCrossAzRouting();
                    }
                    metrics.recordOfflineReplicasRoutedAround();
                    LOG.warn("Diskless partition {}-{}: all replicas offline, falling back to non-replica broker {}",
                        topicId, partitionIndex, selectedLeader);
                } else {
                    // No brokers at all (shouldn't happen in normal operation)
                    selectedLeader = assignedReplicas.isEmpty() ? -1 : assignedReplicas.get(0);
                    partitionAvailable = false;
                }
            }
        }

        return new LeaderSelectionResult(selectedLeader, partitionAvailable, aliveReplicas);
    }

    /**
     * Legacy leader selection for unmanaged (RF=1) diskless partitions.
     * Selects from all alive brokers, preferring those in the client's AZ.
     */
    private int selectLeaderLegacy(
        final ListenerName listenerName,
        final String clientId,
        final Uuid topicId,
        final int partitionIndex
    ) {
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

        final byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final int hash = Utils.murmur2(input);
        final int idx = Math.abs(hash % brokersToPickFrom.size());

        return brokersToPickFrom.get(idx).id();
    }

    // Helper methods

    private Set<Integer> getAliveBrokerIds(final ListenerName listenerName) {
        return StreamSupport.stream(metadataView.getAliveBrokerNodes(listenerName).spliterator(), false)
            .map(Node::id)
            .collect(Collectors.toSet());
    }

    private List<Integer> filterByAZ(
        final ListenerName listenerName,
        final List<Integer> brokerIds,
        final String az
    ) {
        if (az == null) {
            return Collections.emptyList();
        }
        final Map<Integer, String> brokerRacks = StreamSupport
            .stream(metadataView.getAliveBrokerNodes(listenerName).spliterator(), false)
            .collect(Collectors.toMap(Node::id, n -> n.rack() != null ? n.rack() : ""));

        return brokerIds.stream()
            .filter(id -> Objects.equals(brokerRacks.get(id), az))
            .sorted()
            .toList();
    }

    private int selectByHash(final Uuid topicId, final int partitionIndex, final List<Integer> candidates) {
        final List<Integer> sorted = candidates.stream().sorted().toList();
        final byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final int hash = Utils.murmur2(input);
        final int idx = Math.abs(hash % sorted.size());
        return sorted.get(idx);
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

    /**
     * Result of leader selection for managed replicas.
     */
    private record LeaderSelectionResult(int leaderId, boolean available, List<Integer> aliveReplicas) {}
}
