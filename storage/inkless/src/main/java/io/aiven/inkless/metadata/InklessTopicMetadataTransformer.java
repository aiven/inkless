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

        final String clientAZ = normalizeAZ(ClientAZExtractor.getClientAZ(clientId));

        // Lazy-init: computed on first diskless topic, reused across all topics in the response.
        AliveBrokerSnapshot snapshot = null;

        for (final var topic : topicMetadata) {
            final String topicName = topic.name();
            if (!metadataView.isDisklessTopic(topicName)) {
                continue;
            }

            if (snapshot == null) {
                snapshot = AliveBrokerSnapshot.create(metadataView, listenerName);
            }

            // Check if remote storage is enabled (affects routing constraints)
            final boolean isRemoteStorageEnabled = metadataView.isRemoteStorageEnabled(topicName);

            for (final var partition : topic.partitions()) {
                final List<Integer> assignedReplicas = partition.replicaNodes();
                final boolean isManagedReplicas = assignedReplicas.size() > 1;

                if (isManagedReplicas) {
                    transformManagedReplicasPartitionMetadata(
                        clientAZ, topic.topicId(), partition,
                        assignedReplicas, isRemoteStorageEnabled,
                        snapshot.ids(), snapshot.racks()
                    );
                } else {
                    // RF=1 (unmanaged): use legacy behavior
                    transformUnmanagedPartitionMetadata(clientAZ, topic.topicId(), partition, snapshot.nodes());
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

        final String clientAZ = normalizeAZ(ClientAZExtractor.getClientAZ(clientId));

        // Lazy-init: computed on first diskless topic, reused across all topics in the response.
        AliveBrokerSnapshot snapshot = null;

        for (final var topic : responseData.topics()) {
            final String topicName = topic.name();
            if (!metadataView.isDisklessTopic(topicName)) {
                continue;
            }

            if (snapshot == null) {
                snapshot = AliveBrokerSnapshot.create(metadataView, listenerName);
            }

            // Check if remote storage is enabled (affects routing constraints)
            final boolean isRemoteStorageEnabled = metadataView.isRemoteStorageEnabled(topicName);

            for (final var partition : topic.partitions()) {
                final List<Integer> assignedReplicas = partition.replicaNodes();
                final boolean isManagedReplicas = assignedReplicas.size() > 1;

                if (isManagedReplicas) {
                    transformManagedReplicasPartitionDescribe(
                        clientAZ, topic.topicId(), partition,
                        assignedReplicas, isRemoteStorageEnabled,
                        snapshot.ids(), snapshot.racks()
                    );
                } else {
                    // RF=1 (unmanaged): use legacy behavior
                    transformUnmanagedPartitionDescribe(clientAZ, topic.topicId(), partition, snapshot.nodes());
                }
            }
        }
    }

    /**
     * Transform partition with managed replicas (RF > 1) for Metadata response.
     */
    private void transformManagedReplicasPartitionMetadata(
        final String clientAZ,
        final Uuid topicId,
        final MetadataResponseData.MetadataResponsePartition partition,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled,
        final Set<Integer> aliveBrokerIds,
        final Map<Integer, String> brokerRacks
    ) {
        final LeaderSelectionResult result = selectLeaderForManagedReplicas(
            clientAZ, topicId, partition.partitionIndex(), assignedReplicas, isRemoteStorageEnabled,
            aliveBrokerIds, brokerRacks
        );

        partition.setLeaderId(result.leaderId());
        // Only clear the error code when the transformer found a routable leader.
        // When unavailable (e.g. tiered mode with all replicas offline), preserve
        // the original error code from KRaft (e.g. LEADER_NOT_AVAILABLE).
        if (result.available()) {
            partition.setErrorCode(Errors.NONE.code());
        }
        // Keep original replicaNodes - show real RF to clients
        partition.setIsrNodes(result.aliveReplicas());
        partition.setOfflineReplicas(result.offlineReplicas());
        partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
    }

    /**
     * Transform partition with managed replicas (RF > 1) for DescribeTopicPartitions response.
     */
    private void transformManagedReplicasPartitionDescribe(
        final String clientAZ,
        final Uuid topicId,
        final DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition partition,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled,
        final Set<Integer> aliveBrokerIds,
        final Map<Integer, String> brokerRacks
    ) {
        final LeaderSelectionResult result = selectLeaderForManagedReplicas(
            clientAZ, topicId, partition.partitionIndex(), assignedReplicas, isRemoteStorageEnabled,
            aliveBrokerIds, brokerRacks
        );

        partition.setLeaderId(result.leaderId());
        // Only clear the error code when the transformer found a routable leader.
        // When unavailable (e.g. tiered mode with all replicas offline), preserve
        // the original error code from KRaft (e.g. LEADER_NOT_AVAILABLE).
        if (result.available()) {
            partition.setErrorCode(Errors.NONE.code());
        }
        // Keep original replicaNodes - show real RF to clients
        partition.setIsrNodes(result.aliveReplicas());
        partition.setEligibleLeaderReplicas(Collections.emptyList());
        partition.setLastKnownElr(Collections.emptyList());
        partition.setOfflineReplicas(result.offlineReplicas());
        partition.setLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH);
    }

    /**
     * Transform partition with unmanaged replicas (RF=1) for Metadata response.
     * Uses legacy behavior: hash-based selection from all alive brokers.
     */
    private void transformUnmanagedPartitionMetadata(
        final String clientAZ,
        final Uuid topicId,
        final MetadataResponseData.MetadataResponsePartition partition,
        final List<Node> aliveNodes
    ) {
        final int selectedLeader = selectLeaderLegacy(clientAZ, topicId, partition.partitionIndex(), aliveNodes);
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
        final String clientAZ,
        final Uuid topicId,
        final DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition partition,
        final List<Node> aliveNodes
    ) {
        final int selectedLeader = selectLeaderLegacy(clientAZ, topicId, partition.partitionIndex(), aliveNodes);
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
        final String clientAZ,
        final Uuid topicId,
        final int partitionIndex,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled,
        final Set<Integer> aliveBrokerIds,
        final Map<Integer, String> brokerRacks
    ) {
        // Compute broker sets for routing decisions
        final List<Integer> aliveReplicas = assignedReplicas.stream()
            .filter(aliveBrokerIds::contains)
            .toList();
        final List<Integer> offlineReplicas = assignedReplicas.stream()
            .filter(id -> !aliveBrokerIds.contains(id))
            .toList();
        final List<Integer> aliveReplicasInClientAZ = filterByAZ(brokerRacks, aliveReplicas, clientAZ);
        final List<Integer> allAliveBrokersInClientAZ = filterByAZ(brokerRacks, new ArrayList<>(aliveBrokerIds), clientAZ);

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
                recordCrossAzIfConfirmed(clientAZ, selectedLeader, brokerRacks);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else {
                // All replicas offline — partition unavailable (Kafka semantics)
                // Cannot fall back to non-replica brokers even in same AZ
                // because tiered reads REQUIRE replica brokers with UnifiedLog/RLM state.
                // Use -1 to denote no leader; do not advertise an offline broker.
                selectedLeader = -1;
                partitionAvailable = false;
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
                // No alive replicas in client AZ, but other brokers in AZ are alive; use same-AZ non-replica
                selectedLeader = selectByHash(topicId, partitionIndex, allAliveBrokersInClientAZ);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, true);
                metrics.recordFallback();
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else if (!aliveReplicas.isEmpty()) {
                // No brokers in client AZ available; use replica in other AZ (cross-AZ)
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicas);
                partitionAvailable = true;
                metrics.recordClientAz(clientAZ, false);
                recordCrossAzIfConfirmed(clientAZ, selectedLeader, brokerRacks);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else {
                // Absolute last resort: any alive broker (cross-AZ, non-replica)
                final List<Integer> allAliveBrokers = new ArrayList<>(aliveBrokerIds);
                if (!allAliveBrokers.isEmpty()) {
                    selectedLeader = selectByHash(topicId, partitionIndex, allAliveBrokers);
                    partitionAvailable = true;
                    metrics.recordClientAz(clientAZ, false);
                    metrics.recordFallback();
                    recordCrossAzIfConfirmed(clientAZ, selectedLeader, brokerRacks);
                    metrics.recordOfflineReplicasRoutedAround();
                } else {
                    // No brokers at all (shouldn't happen in normal operation)
                    selectedLeader = -1;
                    partitionAvailable = false;
                }
            }
        }

        return new LeaderSelectionResult(selectedLeader, partitionAvailable, aliveReplicas, offlineReplicas);
    }

    /**
     * Legacy leader selection for unmanaged (RF=1) diskless partitions.
     * Selects from all alive brokers, preferring those in the client's AZ.
     */
    private int selectLeaderLegacy(
        final String clientAZ,
        final Uuid topicId,
        final int partitionIndex,
        final List<Node> aliveNodes
    ) {
        final List<Node> sortedAlive = aliveNodes.stream()
            .sorted(Comparator.comparing(Node::id))
            .toList();

        // When clientAZ is null (unknown), skip AZ filtering — use all brokers equally
        final List<Node> brokersInClientAZ = clientAZ == null
            ? Collections.emptyList()
            : sortedAlive.stream()
                .filter(n -> clientAZ.equals(n.rack()))
                .toList();

        // Fall back on all brokers if no broker in the client AZ
        final List<Node> brokersToPickFrom = brokersInClientAZ.isEmpty()
            ? sortedAlive
            : brokersInClientAZ;

        metrics.recordClientAz(clientAZ, !brokersInClientAZ.isEmpty());

        // This cannot happen in a normal broker run. This will serve as a guard in tests.
        if (brokersToPickFrom.isEmpty()) {
            throw new RuntimeException("No broker found, unexpected");
        }

        final byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final int hash = Utils.murmur2(input);

        return brokersToPickFrom.get(Utils.toPositive(hash) % brokersToPickFrom.size()).id();
    }

    private static String normalizeAZ(final String az) {
        return (az == null || az.isBlank()) ? null : az;
    }

    /**
     * Record cross-AZ routing only when we can confirm the leader is in a different AZ.
     * If client AZ is unknown or the leader's rack is unset, we cannot confirm cross-AZ.
     */
    private void recordCrossAzIfConfirmed(final String clientAZ, final int leaderId, final Map<Integer, String> brokerRacks) {
        if (clientAZ == null) return;
        final String leaderRack = brokerRacks.get(leaderId);
        if (leaderRack != null && !leaderRack.isBlank() && !leaderRack.equals(clientAZ)) {
            metrics.recordCrossAzRouting();
        }
    }

    private static List<Integer> filterByAZ(
        final Map<Integer, String> brokerRacks,
        final List<Integer> brokerIds,
        final String az
    ) {
        if (az == null) {
            return Collections.emptyList();
        }
        return brokerIds.stream()
            .filter(id -> Objects.equals(brokerRacks.get(id), az))
            .toList();
    }

    private static int selectByHash(final Uuid topicId, final int partitionIndex, final List<Integer> candidates) {
        final List<Integer> sorted = candidates.stream().sorted().toList();
        final byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final int hash = Utils.murmur2(input);
        return sorted.get(Utils.toPositive(hash) % sorted.size());
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }

    /**
     * Snapshot of alive broker state, computed once per request and reused across all topics.
     */
    private record AliveBrokerSnapshot(List<Node> nodes, Set<Integer> ids, Map<Integer, String> racks) {
        static AliveBrokerSnapshot create(final MetadataView metadataView, final ListenerName listenerName) {
            final List<Node> nodes = metadataView.getAliveBrokerNodes(listenerName);
            final Set<Integer> ids = nodes.stream()
                .map(Node::id)
                .collect(Collectors.toSet());
            final Map<Integer, String> racks = nodes.stream()
                .collect(Collectors.toMap(Node::id, n -> n.rack() != null ? n.rack() : ""));
            return new AliveBrokerSnapshot(nodes, ids, racks);
        }
    }

    /**
     * Result of leader selection for managed replicas.
     */
    private record LeaderSelectionResult(int leaderId, boolean available, List<Integer> aliveReplicas, List<Integer> offlineReplicas) {}
}
