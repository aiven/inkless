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

import kafka.server.KafkaConfig;
import kafka.server.NetworkUtils;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.LastMirroredOffsetsRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.LastMirroredOffsetsRequest;
import org.apache.kafka.common.requests.LastMirroredOffsetsResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinator;
import org.apache.kafka.coordinator.mirror.MirrorRecordKey;
import org.apache.kafka.image.ConfigurationDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.config.MirrorConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.network.BrokerEndPoint;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.controller.ConfigurationControlManager.REMOVED_TOPIC_SUFFIX;

/**
 * Bridges the local destination cluster and remote source clusters for Cluster Mirroring.
 *
 * Implements {@link MetadataPublisher} to detect leadership and config changes, triggering
 * partition state transitions (PREPARING, MIRRORING, STOPPING, STOPPED) via the
 * {@link MirrorCoordinator}. Manages remote cluster connections using {@link MirrorBlockingSender}
 * and periodically syncs topic metadata, configs, consumer group offsets, and ACLs from source
 * clusters.
 *
 * Maintains in-memory caches of partition states and last mirrored offsets, populated from
 * the {@code __cluster_mirror_state} topic on coordinator election and cleared on resignation
 * or leadership loss. Routes state reads/writes to the appropriate coordinator broker, batching
 * requests per coordinator node to reduce network overhead.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataPublisher, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MirrorMetadataManager.class);
    private static final ResourcePatternFilter ANY_RESOURCE = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
    private static final AclBindingFilter ANY_RESOURCE_ACL = new AclBindingFilter(ANY_RESOURCE, AccessControlEntryFilter.ANY);

    private final KafkaConfig brokerConfig;
    private final int nodeId;
    private final Metrics metrics;
    private final Time time;
    private final Random random;
    private final NodeToControllerChannelManager channelManager;
    private final Supplier<GroupCoordinator> groupCoordinatorSupplier;
    private InterBrokerSender interBrokerSender;
    private MetadataImage metadataImage;
    private MetadataCache metadataCache;

    private Optional<StateTransitioner> stateTransitioner = Optional.empty();
    private Optional<Function<MirrorRecordKey, Integer>> coordinatingPartFinder = Optional.empty();
    private Optional<Function<String, Integer>> coordinatingPartByNameFinder = Optional.empty();

    // local cache
    private Map<String, List<MirrorBlockingSender>> sourceSenders;
    private Map<String, Map<TopicPartition, Node>> sourcePartitionLeaders;
    private Map<MirrorPartitionKey, Long> lastMirroredOffsets;
    private Map<MirrorPartitionKey, MirrorPartitionState> partitionStates;
    private Map<MirrorPartitionState, AtomicLong> partitionStateCounts;
    private Map<MirrorRecordKey, Node> coordinatorNodes;

    private KafkaMetricsGroup metricsGroup;
    private AtomicLong metadataRefreshError;
    private AtomicLong topicConfigSyncError;
    private AtomicLong consumerGroupOffsetSyncError;
    private AtomicLong aclSyncError;

    public MirrorMetadataManager(
        KafkaConfig config,
        Metrics metrics,
        Time time,
        MetadataCache metadataCache,
        NodeToControllerChannelManager channelManager,
        Supplier<GroupCoordinator> groupCoordinatorSupplier
    ) {
        this.brokerConfig = config;
        this.nodeId = config.nodeId();
        this.metrics = metrics;
        this.time = time;
        this.random = new Random();
        this.channelManager = channelManager;
        this.groupCoordinatorSupplier = groupCoordinatorSupplier;

        this.metadataImage = MetadataImage.EMPTY;
        this.metadataCache = metadataCache;
        this.sourceSenders = new HashMap<>();
        this.sourcePartitionLeaders = new HashMap<>();
        this.lastMirroredOffsets = new ConcurrentHashMap<>();
        this.partitionStates = new ConcurrentHashMap<>();
        this.partitionStateCounts = new ConcurrentHashMap<>();
        this.coordinatorNodes = new ConcurrentHashMap<>();

        this.metricsGroup = new KafkaMetricsGroup(this.getClass());
        this.metadataRefreshError = new AtomicLong();
        this.topicConfigSyncError = new AtomicLong();
        this.consumerGroupOffsetSyncError = new AtomicLong();
        this.aclSyncError = new AtomicLong();

        metricsGroup.newGauge("TopicConfigSyncError", topicConfigSyncError::get);
        metricsGroup.newGauge("ConsumerGroupOffsetSyncError", consumerGroupOffsetSyncError::get);
        metricsGroup.newGauge("AclSyncError", aclSyncError::get);
        metricsGroup.newGauge("TopicMetadataRefreshError", metadataRefreshError::get);
        metricsGroup.newGauge("PreparingPartitionState", () -> partitionStateCount(MirrorPartitionState.PREPARING));
        metricsGroup.newGauge("MirroringPartitionState", () -> partitionStateCount(MirrorPartitionState.MIRRORING));
        metricsGroup.newGauge("StoppingPartitionState", () -> partitionStateCount(MirrorPartitionState.STOPPING));
        metricsGroup.newGauge("StoppedPartitionState", () -> partitionStateCount(MirrorPartitionState.STOPPED));
        metricsGroup.newGauge("FailedPartitionState", () -> partitionStateCount(MirrorPartitionState.FAILED));
    }

    /**
     * Returns the name identifier for this metadata publisher.
     *
     * @return the metadata publisher name including broker node ID
     */
    @Override
    public String name() {
        return "MirrorMetadataManager(id=" + brokerConfig.nodeId() + ")";
    }

    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        if (coordinatingPartFinder.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordinatingPartFinder.get().apply(
                            new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), partition))).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    private boolean isLocalCoordinator(String mirrorName) {
        if (coordinatingPartByNameFinder.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordinatingPartByNameFinder.get().apply(mirrorName)).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    /**
     * Called when cluster metadata is updated.
     * Updates the local metadata image to reflect the latest cluster state.
     *
     * The metadata cache can't be used here because it is updated concurrently.
     *
     * This must be called after ReplicaManager#applyDelta because:
     * - ReplicaManager determines which partitions are local leaders/followers for this broker
     * - MirrorMetadataManager needs that information to know which mirror partitions to start managing
     *
     * For partitions whose coordinator is on a remote broker, this method collects all such partitions
     * grouped by mirror name and issues batched {@code ReadMirrorStatesRequest} calls (one per mirror)
     * rather than individual requests per partition. The per-node batching within each mirror is handled
     * by {@link #readStatesFromRemoteCoordinator}.
     *
     * @param delta the metadata delta containing changes
     * @param newImage the new complete metadata image
     * @param manifest the loader manifest with provenance information
     */
    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (interBrokerSender == null) {
            KafkaClient networkClient = NetworkUtils.buildNetworkClient("MirrorMetadataManager", brokerConfig, metrics, time, new LogContext(name()));
            interBrokerSender = new InterBrokerSender("mirror-inter-broker-sender", networkClient, brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            interBrokerSender.start();
        }

        // caching the image for query purpose
        this.metadataImage = newImage;

        // get all mirror partition leaders on this node based on the delta
        Set<TopicPartition> mirrorLeaders = getMirrorLeaders(delta, newImage);
        if (mirrorLeaders.isEmpty()) {
            return;
        }

        LOG.info("onMetadataUpdate: {}", mirrorLeaders);

        // Collect remote coordinator partitions grouped by mirrorName for batched reads
        Map<String, Map<String, Set<Integer>>> remotePartitionsByMirror = new HashMap<>();
        Map<String, Map<TopicPartition, Boolean>> remoteStopFlags = new HashMap<>();

        mirrorLeaders.forEach(tp -> {
            String rawMirrorName = (String) newImage.configs().configProperties(
                    new ConfigResource(ConfigResource.Type.TOPIC, tp.topic())).get(TopicConfig.MIRROR_NAME_CONFIG);
            boolean stopRequested = rawMirrorName.endsWith(REMOVED_TOPIC_SUFFIX);
            String mirrorName = stopRequested
                    ? rawMirrorName.substring(0, rawMirrorName.length() - REMOVED_TOPIC_SUFFIX.length())
                    : rawMirrorName;

            MirrorPartitionKey key = new MirrorPartitionKey(mirrorName, tp.topic(), tp.partition());
            if (isLocalCoordinator(key.mirrorName, key.topic, key.partition())) {
                // Handle local coordinator partitions inline (no network call needed)
                MirrorPartitionState curState = partitionStates.getOrDefault(key, MirrorPartitionState.UNKNOWN);
                applyMirrorStateTransition(key.mirrorName, tp, curState, null, stopRequested);
            } else {
                // Collect for batched remote read
                remotePartitionsByMirror
                    .computeIfAbsent(mirrorName, k -> new HashMap<>())
                    .computeIfAbsent(tp.topic(), k -> new HashSet<>())
                    .add(tp.partition());
                remoteStopFlags
                    .computeIfAbsent(mirrorName, k -> new HashMap<>())
                    .put(tp, stopRequested);
            }
        });

        // Send one batched read per mirrorName (readStatesFromRemoteCoordinator handles per-node batching internally)
        remotePartitionsByMirror.forEach((mirrorName, partitions) -> {
            Map<TopicPartition, Boolean> stopFlags = remoteStopFlags.get(mirrorName);
            readStatesFromRemoteCoordinator(mirrorName, partitions, res ->
                res.data().topics().forEach(topic ->
                    topic.partitions().forEach(partition -> {
                        if (partition.state() != -1) {
                            TopicPartition resTp = new TopicPartition(topic.name(), partition.partitionIndex());
                            MirrorPartitionState state = MirrorPartitionState.fromValue(partition.state());
                            boolean stopRequested = stopFlags.getOrDefault(resTp, false);
                            MirrorPartitionKey mpk = new MirrorPartitionKey(mirrorName, resTp.topic(), resTp.partition());
                            MirrorPartitionState curState = partitionStates.getOrDefault(mpk, MirrorPartitionState.UNKNOWN);
                            applyMirrorStateTransition(mirrorName, resTp, curState, state, stopRequested);
                        }
                    })));
        });

        if (delta.topicsDelta() != null) {
            clearFollowersState(delta.topicsDelta().localChanges(nodeId).followers().keySet(), newImage);
        }
    }

    /**
     * Applies the appropriate state transition for a mirror partition based on current state and stop flag.
     *
     * If stopRequested is true and the partition is not already STOPPED, transitions to STOPPING.
     * Otherwise, transitions to PREPARING if the partition is in UNKNOWN or STOPPED state,
     * or maintains the current/fetched state for partitions already in progress.
     *
     * @param mirrorName the name of the cluster mirror
     * @param tp the topic partition to transition
     * @param curState the current cached state of the partition
     * @param fetchedState the state fetched from the remote coordinator, or null for local coordinator partitions
     * @param stopRequested whether the mirror removal has been requested for this partition
     */
    private void applyMirrorStateTransition(String mirrorName, TopicPartition tp,
                                            MirrorPartitionState curState, MirrorPartitionState fetchedState,
                                            boolean stopRequested) {
        stateTransitioner.ifPresent(t -> {
            if (stopRequested && curState != MirrorPartitionState.STOPPED) {
                t.transitionTo(mirrorName, tp, MirrorPartitionState.STOPPING);
            } else if (curState == MirrorPartitionState.UNKNOWN || curState == MirrorPartitionState.STOPPED) {
                t.transitionTo(mirrorName, tp, MirrorPartitionState.PREPARING);
            } else {
                t.transitionTo(mirrorName, tp, fetchedState != null ? fetchedState : curState);
            }
        });
    }

    /**
     * Identifies partitions that this broker leads which belong to a mirror (have non-empty mirror.name configured).
     *
     * This method detects two distinct scenarios:
     * 1. Leadership changes: Partitions where leadership transitioned to this broker and the topic already has
     *    a non-empty mirror.name configuration. Detected via topicsDelta.
     * 2. Configuration changes: Partitions where this broker already leads and the mirror.name configuration
     *    was added or changed from empty to non-empty. Detected via configsDelta.
     * Both checks are necessary because leadership changes and configuration changes are independent events.
     *
     * @param delta the metadata delta containing leadership and configuration changes
     * @param image the current metadata image for querying topic configurations
     * @return set of topic partitions led by this broker that belong to a mirror
     */
    private Set<TopicPartition> getMirrorLeaders(MetadataDelta delta, MetadataImage image) {
        Set<TopicPartition> mirrorLeaderPartitions = new HashSet<>();

        // new partition leader in topicsDelta that has mirror.name not empty
        if (delta.topicsDelta() != null) {
            delta.topicsDelta().localChanges(nodeId).leaders().keySet().forEach(tp -> {
                Properties props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()));
                if (props.containsKey(TopicConfig.MIRROR_NAME_CONFIG)) {
                    String mirrorName = (String) props.get(TopicConfig.MIRROR_NAME_CONFIG);
                    if (mirrorName != null && !mirrorName.isBlank()) {
                        mirrorLeaderPartitions.add(tp);
                    }
                }
            });
        }

        // the config change in configsDelta contains the mirror.name setting from empty to non-empty
        if (delta.configsDelta() != null) {
            // get all resources containing the non-empty mirror name change
            Map<ConfigResource, ConfigurationDelta> mirrorNameChanged = delta.configsDelta().changes().entrySet().stream().filter(entry ->
                            entry.getValue().changes().containsKey(TopicConfig.MIRROR_NAME_CONFIG) &&
                                    !entry.getValue().changes().get(TopicConfig.MIRROR_NAME_CONFIG).isEmpty())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // get all topics from the resources
            Set<String> topicsWithMirrorNameChanged = mirrorNameChanged.keySet().stream()
                    .filter(configResource -> configResource.type().equals(ConfigResource.Type.TOPIC))
                    .map(configResource -> configResource.name()).collect(Collectors.toSet());

            // get the partition leader is the local node
            topicsWithMirrorNameChanged.stream().forEach(topic -> {
                TopicImage topicImage = image.topics().getTopic(topic);
                if (topicImage != null) {
                    topicImage.partitions().forEach((partitionId, partition) -> {
                        if (partition.leader == nodeId) {
                            mirrorLeaderPartitions.add(new TopicPartition(topic, partitionId));
                        }
                    });
                }
            });
        }

        return mirrorLeaderPartitions;
    }

    /**
     * Clears cached mirror state for partitions where this broker transitioned from leader to follower.
     *
     * When leadership moves away from this broker, mirror state (partitionStates and lastMirroredOffsets)
     * must be cleaned up to prevent memory leaks and stale state tracking. However, if this broker is the
     * active coordinator for the mirror, it retains the state even when not leading partitions, as coordinators
     * need to maintain global state for the mirrors they manage.
     *
     * @param followerDelta set of partitions where this broker became a follower
     * @param newImage the current metadata image for querying topic configurations
     */
    private void clearFollowersState(Set<TopicPartition> followerDelta, MetadataImage newImage) {
        followerDelta.forEach(followerTp -> {
            String mirrorName = (String) newImage.configs()
                    .configProperties(new ConfigResource(ConfigResource.Type.TOPIC, followerTp.topic()))
                    .get(TopicConfig.MIRROR_NAME_CONFIG);
            if (mirrorName != null && !mirrorName.isEmpty() && !isLocalCoordinator(mirrorName, followerTp.topic(), followerTp.partition())) {
                String updatedMirrorName = originalMirrorName(mirrorName);
                MirrorPartitionKey key = new MirrorPartitionKey(updatedMirrorName, followerTp.topic(), followerTp.partition());
                removePartitionState(key);
                lastMirroredOffsets.remove(key);
            }
        });
    }

    /**
     * Closes the metadata manager and releases resources.
     */
    @Override
    public void close() throws Exception {
        interBrokerSender.shutdown();
    }

    /**
     * Finds the coordinator node for a given mirror record key.
     * The coordinator is the broker that leads the partition in the __cluster_mirror_state topic
     * responsible for this mirror's metadata.
     *
     * @param key The mirror record key to find the coordinator for
     * @return The coordinator Node, or Node.noNode() if the coordinator cannot be found
     */
    private Node findMirrorCoordinatorNode(MirrorRecordKey key) {
        try {
            if (metadataCache.contains(MIRROR_STATE_TOPIC_NAME)) {
                Set<String> topicSet = new HashSet<>();
                topicSet.add(MIRROR_STATE_TOPIC_NAME);

                var interBrokerListenerName = brokerConfig.interBrokerListenerName();

                List<MetadataResponseData.MetadataResponseTopic> topicMetadata = metadataCache.getTopicMetadata(
                        topicSet,
                        interBrokerListenerName,
                        false,
                        false
                );

                if (topicMetadata == null || topicMetadata.isEmpty() || topicMetadata.get(0).errorCode() != Errors.NONE.code()) {
                    return Node.noNode();
                } else {
                    if (coordinatingPartFinder.isEmpty()) {
                        return Node.noNode();
                    }
                    int partition = coordinatingPartFinder.get().apply(key);
                    Optional<MetadataResponseData.MetadataResponsePartition> response = topicMetadata.get(0).partitions().stream()
                            .filter(responsePart -> responsePart.partitionIndex() == partition
                                    && responsePart.leaderId() != MetadataResponse.NO_LEADER_ID)
                            .findFirst();

                    if (response.isPresent()) {
                        return metadataCache.getAliveBrokerNode(response.get().leaderId(), interBrokerListenerName)
                                .orElse(Node.noNode());
                    } else {
                        return Node.noNode();
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Exception while getting share coordinator", e);
        }
        return Node.noNode();
    }

    /**
     * Configure a function for transitioning a mirror partition to a new state.
     *
     * @param function function
     */
    public void setStateTransitioner(StateTransitioner function) {
        this.stateTransitioner = Optional.of(function);
    }

    interface StateTransitioner {
        void transitionTo(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state);
    }

    /**
     * Configure a function for getting the internal topic coordinating partition by mirror key.
     *
     * @param function function
     */
    public void setCoordinatingPartitionFinder(Function<MirrorRecordKey, Integer> function) {
        this.coordinatingPartFinder = Optional.of(function);
    }

    /**
     * Configure a function for getting the internal topic coordinating partition by mirror name.
     *
     * @param function function
     */
    public void setCoordinatingPartitionByNameFinder(Function<String, Integer> function) {
        this.coordinatingPartByNameFinder = Optional.of(function);
    }

    /**
     * Asynchronously writes mirror partition states to remote coordinators.
     *
     * This method sends partition state updates (including state and last mirrored offset) to the
     * coordinator brokers responsible for this mirror. The coordinators persist these states to the
     * {@code __cluster_mirror_state} internal topic. This enables failover scenarios where another
     * broker can resume mirroring from the last known state.
     *
     * Partitions are grouped by their coordinator node so that all partitions managed by the same
     * coordinator are sent in a single batched {@code WriteMirrorStatesRequest}, reducing network
     * overhead compared to sending individual requests per partition.
     *
     * The method performs the following:
     * - Resolves the coordinator node for each partition (caching the result)
     * - Groups partitions by coordinator node
     * - Constructs one WriteMirrorStatesRequest per coordinator node with all its partitions
     * - Sends each request asynchronously via inter-broker sender
     * - Invokes the response callback for each coordinator response
     *
     * @param mirrorName the name of the cluster mirror
     * @param topicMetadata map of topic names to sets of partition metadata (state and offset)
     * @param removedTopics set of topic names that have been removed from the mirror
     * @param callback callback invoked with each coordinator's response
     */
    public void writeStatesToRemoteCoordinator(String mirrorName,
                                               Map<String, Set<MirrorPartitionMetadata>> topicMetadata,
                                               Set<String> removedTopics,
                                               Consumer<WriteMirrorStatesResponse> callback) {
        LOG.info("!!! writeStatesToRemoteCoordinator: {} {} {}", mirrorName, topicMetadata, removedTopics);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<WriteMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        topicMetadata.forEach((topic, metadata) -> {
            metadata.forEach(m -> {
                MirrorRecordKey key = new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), m.partition());
                Node coordinatorNode = coordinatorNodes.get(key);
                if (coordinatorNode == null) {
                    coordinatorNode = findMirrorCoordinatorNode(key);
                    if (coordinatorNode.equals(Node.noNode())) {
                        LOG.error("!!! Coordinator is not available for mirror {} partition {}-{}", mirrorName, topic, m.partition());
                        return;
                    }
                    coordinatorNodes.put(key, coordinatorNode);
                }

                WriteMirrorStatesRequestData.PartitionData partitionData = new WriteMirrorStatesRequestData.PartitionData();
                partitionData.setState(m.state() == null ? MirrorPartitionState.UNKNOWN.value() : m.state().value());
                partitionData.setLastMirroredOffset(m.offset());
                partitionData.setPartitionIndex(m.partition());

                nodeToTopicPartitions
                    .computeIfAbsent(coordinatorNode, k -> new HashMap<>())
                    .computeIfAbsent(topic, k -> new ArrayList<>())
                    .add(partitionData);
            });
        });

        // Send one batched request per coordinator node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            WriteMirrorStatesRequestData data = new WriteMirrorStatesRequestData().setMirrorName(mirrorName);
            List<WriteMirrorStatesRequestData.TopicData> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                topicDataList.add(new WriteMirrorStatesRequestData.TopicData()
                    .setName(topic)
                    .setPartitions(partitionDataList)));

            data.setTopics(topicDataList);
            data.setRemovedTopics(new ArrayList<>(removedTopics));

            interBrokerSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                node,
                new WriteMirrorStatesRequest.Builder(data),
                response -> {
                    LOG.info("!!! writeStatesToRemoteCoordinator completed: {}", response.responseBody());
                    if (response.responseBody() instanceof WriteMirrorStatesResponse writeMirrorStatesResponse) {
                        callback.accept(writeMirrorStatesResponse);
                    }
                }
            ));
        });
    }

    /**
     * Asynchronously reads mirror partition states from remote coordinators.
     *
     * This method queries the coordinator brokers responsible for this mirror to retrieve the
     * current state and last mirrored offset for the specified partitions. Each coordinator reads
     * this data from its partition of the {@code __cluster_mirror_state} internal topic.
     *
     * Partitions are grouped by their coordinator node so that all partitions managed by the same
     * coordinator are queried in a single batched {@code ReadMirrorStatesRequest}, reducing network
     * overhead compared to sending individual requests per partition.
     *
     * The method performs the following:
     * - Resolves the coordinator node for each partition (caching the result)
     * - Groups partitions by coordinator node
     * - Constructs one ReadMirrorStatesRequest per coordinator node with all its partitions
     * - Sends each request asynchronously via inter-broker sender
     * - Updates local cache (lastMirroredOffsets, partitionStates) with each response
     * - Invokes the callback for each coordinator response
     *
     * @param mirrorName the name of the cluster mirror
     * @param partitions map of topic names to their partition indices to query
     * @param callback callback invoked with each coordinator's response containing partition states
     */
    public void readStatesFromRemoteCoordinator(String mirrorName,
                                                Map<String, Set<Integer>> partitions,
                                                Consumer<ReadMirrorStatesResponse> callback) {
        LOG.info("!!! readStatesFromRemoteCoordinator: {} {}", mirrorName, partitions);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<ReadMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                MirrorRecordKey key = new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), part);
                Node coordinatorNode = coordinatorNodes.get(key);
                if (coordinatorNode == null) {
                    coordinatorNode = findMirrorCoordinatorNode(key);
                    if (coordinatorNode.equals(Node.noNode())) {
                        LOG.error("!!! Coordinator is not available for mirror {} partition {}:{}", mirrorName, topic, part);
                        return;
                    }
                    coordinatorNodes.put(key, coordinatorNode);
                }

                ReadMirrorStatesRequestData.PartitionData partitionData = new ReadMirrorStatesRequestData.PartitionData();
                partitionData.setPartitionIndex(part);

                nodeToTopicPartitions
                    .computeIfAbsent(coordinatorNode, k -> new HashMap<>())
                    .computeIfAbsent(topic, k -> new ArrayList<>())
                    .add(partitionData);
            });
        });

        // Send one batched request per coordinator node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            ReadMirrorStatesRequestData data = new ReadMirrorStatesRequestData().setMirrorName(mirrorName);
            List<ReadMirrorStatesRequestData.TopicData> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                topicDataList.add(new ReadMirrorStatesRequestData.TopicData()
                    .setName(topic)
                    .setPartitions(partitionDataList)));

            data.setTopics(topicDataList);

            interBrokerSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                node,
                new ReadMirrorStatesRequest.Builder(data),
                response -> {
                    if (response.responseBody() instanceof ReadMirrorStatesResponse readMirrorStatesResponse) {
                        LOG.info("!!! readMirrorStatesResponse completed: {}", response.responseBody());

                        // Update cache
                        readMirrorStatesResponse.data().topics().forEach(topic -> {
                            topic.partitions().forEach(partition -> {
                                if (partition.lastMirroredOffset() != -1) {
                                    lastMirroredOffsets.put(new MirrorPartitionKey(mirrorName, topic.name(), partition.partitionIndex()), partition.lastMirroredOffset());
                                }
                                if (partition.state() != -1) {
                                    partitionStates.put(new MirrorPartitionKey(mirrorName, topic.name(), partition.partitionIndex()), MirrorPartitionState.fromValue(partition.state()));
                                }
                            });
                        });

                        callback.accept(readMirrorStatesResponse);
                    }
                }
            ));
        });
    }

    /**
     * Reads mirror partition states from the local in-memory cache.
     *
     * This method retrieves partition state and last mirrored offset information from the local
     * cache ({@code lastMirroredOffsets} and {@code partitionStates} maps) without making
     * any network requests. It's used when the current broker is the coordinator for this mirror
     * and already has the authoritative state in memory.
     *
     * The method performs the following:
     * - Looks up state and offset for each requested partition in local cache
     * - Uses {@code -1} for lastMirroredOffset if not found in cache
     * - Uses {@code UNKNOWN} state if not found in cache
     * - Constructs a ReadMirrorStatesResponse from the cached data
     * - Invokes the callback immediately with the response (no async operation)
     *
     * This is typically called when handling ReadMirrorStatesRequest on the coordinator broker,
     * where the local cache contains the authoritative state from the {@code __cluster_mirror_state} topic.
     *
     * @param mirrorName the name of the cluster mirror
     * @param partitions map of topic names to their partition indices to query
     * @param responseCallback callback invoked immediately with cached partition states
     */
    public void readMirrorPartitionMetadataFromCache(String mirrorName,
                                                     Map<String, Set<Integer>> partitions,
                                                     Consumer<ReadMirrorStatesResponse> responseCallback) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        List<ReadMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
        partitions.forEach((tp, parts) -> {
            ReadMirrorStatesResponseData.TopicResult topicResult = new ReadMirrorStatesResponseData.TopicResult().setName(tp);
            List<ReadMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
            parts.forEach(part -> {
                ReadMirrorStatesResponseData.PartitionResult partitionResult = new ReadMirrorStatesResponseData.PartitionResult();
                partitionResult.setPartitionIndex(part);
                partitionResult.setLastMirroredOffset(lastMirroredOffsets.getOrDefault(new MirrorPartitionKey(mirrorName, tp, part), -1L));
                partitionResult.setState(partitionStates.getOrDefault(new MirrorPartitionKey(mirrorName, tp, part), MirrorPartitionState.UNKNOWN).value());
                partitionResults.add(partitionResult);
            });
            topicResult.setPartitions(partitionResults);
            topicResults.add(topicResult);
        });
        data.setTopics(topicResults);

        responseCallback.accept(new ReadMirrorStatesResponse(data));
    }

    /**
     * Converts a set of TopicPartition objects into a map of topic names to partition indices.
     * This is a utility method for transforming data structures used throughout the mirroring system.
     *
     * @param topicPartitions The set of topic partitions to convert
     * @return A map of topic names to sets of partition indices
     */
    public Map<String, Set<Integer>> convertToTopicToPartitions(Set<TopicPartition> topicPartitions) {
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
     * Initiates truncation of topic partitions to align with last mirrored offsets from source cluster.
     *
     * This method:
     * 1. Establishes a connection to the remote cluster if not already connected
     * 2. Fetches the last successfully mirrored offsets from the source cluster
     * 3. Requests the ReplicaManager to truncate local replicas to these offsets
     * 4. Invokes the callback when truncation completes for all ISR members
     *
     * Truncation ensures data consistency when resuming mirroring after a failover or restart.
     *
     * @param replicaManager the replica manager to perform truncation
     * @param mirrorName the name of the cluster mirror
     * @param topicPartitionSet the topic partitions to truncate
     * @param callback callback invoked for each partition when truncation completes
     */
    public void maybeTruncateToLastMirroredOffsets(ReplicaManager replicaManager,
                                                   String mirrorName,
                                                   Set<TopicPartition> topicPartitionSet,
                                                   Consumer<TopicPartition> callback) {
        LOG.info("!!! maybeTruncateToLastMirroredOffsets: {} {}", mirrorName, topicPartitionSet);
        createMirrorConnection(mirrorName);

        // get api versions of the source cluster
        var apiResponse = getRandomSender(sourceSenders.get(mirrorName)).sendRequest(new ApiVersionsRequest.Builder());

        if (apiResponse.responseBody() instanceof ApiVersionsResponse apiVersionsResponse) {
            if (apiVersionsResponse.apiVersion(ApiKeys.LAST_MIRRORED_OFFSETS.id) == null) {
                LOG.info("!!! LAST_MIRRORED_OFFSETS is not supported in the source cluster, truncate to offset 0 instead.");
                Map<TopicPartition, Long> offsets = new HashMap<>();
                topicPartitionSet.forEach(tp -> offsets.put(tp, 0L));
                replicaManager.maybeTruncate(offsets, callback);
                return;
            }
        }

        List<LastMirroredOffsetsRequestData.TopicData> topicDataList = new ArrayList<>();
        convertToTopicToPartitions(topicPartitionSet).forEach((topic, partitions) -> {
            List<LastMirroredOffsetsRequestData.PartitionData> partitionDataList = new ArrayList<>();
            partitions.forEach(partition -> {
                LastMirroredOffsetsRequestData.PartitionData partitionData = new LastMirroredOffsetsRequestData.PartitionData();
                partitionData.setPartitionIndex(partition);
                partitionDataList.add(partitionData);
            });
            LastMirroredOffsetsRequestData.TopicData topicData = new LastMirroredOffsetsRequestData.TopicData().setName(topic).setPartitions(partitionDataList);
            topicDataList.add(topicData);
        });

        var response = getRandomSender(sourceSenders.get(mirrorName)).sendRequest(
                new LastMirroredOffsetsRequest.Builder(
                        new LastMirroredOffsetsRequestData().setMirrorName(mirrorName).setTopics(topicDataList))
        );

        if (response.responseBody() instanceof LastMirroredOffsetsResponse lastMirroredOffsetResponse) {
            LOG.info("!!! lastMirroredOffsetResponse: {}", lastMirroredOffsetResponse);
            Map<TopicPartition, Long> offsets = new HashMap<>();
            lastMirroredOffsetResponse.data().topics().forEach(topic -> {
                String name = topic.name();
                topic.partitions().forEach(partition -> {
                    offsets.put(new TopicPartition(name, partition.partitionIndex()), partition.lastMirroredOffset());
                });
            });
            replicaManager.maybeTruncate(offsets, callback);
        }
    }

    /**
     * Gets the current leader node for a partition in a remote cluster.
     * Refreshes metadata synchronously if no cached information is available.
     *
     * @param mirrorName the name of the cluster mirror
     * @param tp the topic partition
     * @return the leader node for the partition
     */
    public Node getRemotePartitionLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourcePartitionLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            Node leader = partitionLeaders.get(tp);
            if (leader != null) {
                return leader;
            }
        }

        // No cached metadata.
        // Refresh synchronously to get correct broker IDs before creating fetcher threads.
        LOG.info("No cached metadata for mirror {} partition {}. Refreshing metadata from remote cluster.", mirrorName, tp);
        createMirrorConnection(mirrorName);
        syncTopicMetadata(mirrorName, sourceSenders.get(mirrorName));

        // Try again after refreshing metadata.
        partitionLeaders = sourcePartitionLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            Node leader = partitionLeaders.get(tp);
            if (leader != null) {
                LOG.info("Successfully fetched leader for {} from mirror {}: broker {}", tp, mirrorName, leader.id());
                return leader;
            }
        }

        // Metadata refresh failed or partition not found.
        // Fall back to random bootstrap server.
        // This should rarely happen and indicates a configuration or connectivity issue.
        LOG.warn("Unable to fetch metadata for mirror {} partition {} after refresh. Falling back to random bootstrap server.", mirrorName, tp);
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("Remote bootstrap server not found for mirror " + mirrorName));

        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        int rand = random.nextInt(addresses.size());

        // Use random node id here because we don't know node id of remote brokers.
        return new Node(random.nextInt(), addresses.get(rand).getHostString(), addresses.get(rand).getPort());
    }


    /**
     * Retrieves the last successfully mirrored offset for a specific partition from the local cache.
     *
     * @param clusterName The name of the cluster mirror
     * @param topicPartition The topic partition to query
     * @return The last mirrored offset, or 0L if no offset is cached
     */
    public long getLastMirroredOffset(String clusterName, TopicPartition topicPartition) {
        MirrorPartitionKey key = new MirrorPartitionKey(clusterName, topicPartition.topic(), topicPartition.partition());
        if (lastMirroredOffsets.containsKey(key)) {
            return lastMirroredOffsets.get(key);
        }
        return 0L;
    }

    public void updateMirrorPartitionState(String clusterName, TopicPartition topicPartition, MirrorPartitionState mirrorState) {
        putPartitionState(new MirrorPartitionKey(clusterName, topicPartition.topic(), topicPartition.partition()), mirrorState);
    }

    private void putPartitionState(MirrorPartitionKey key, MirrorPartitionState newState) {
        MirrorPartitionState oldState = partitionStates.put(key, newState);
        if (oldState != null && oldState != newState) {
            partitionStateCounts.computeIfAbsent(oldState, s -> new AtomicLong()).decrementAndGet();
        }
        if (oldState != newState) {
            partitionStateCounts.computeIfAbsent(newState, s -> new AtomicLong()).incrementAndGet();
        }
    }

    private void removePartitionState(MirrorPartitionKey key) {
        MirrorPartitionState oldState = partitionStates.remove(key);
        if (oldState != null) {
            partitionStateCounts.computeIfAbsent(oldState, s -> new AtomicLong()).decrementAndGet();
        }
    }

    private long partitionStateCount(MirrorPartitionState state) {
        return partitionStateCounts.computeIfAbsent(state, s -> new AtomicLong()).get();
    }

    /**
     * Retrieves the current state of a mirror partition from the local cache.
     * Handles mirror names with REMOVED_TOPIC_SUFFIX by stripping the suffix before lookup.
     *
     * @param mirrorName The name of the mirror (may include REMOVED_TOPIC_SUFFIX)
     * @param topicPartition The topic partition to query
     * @return The mirror partition state, or null if not found in cache
     */
    public MirrorPartitionState getMirrorPartitionState(String mirrorName, TopicPartition topicPartition) {
        String updatedMirrorName = originalMirrorName(mirrorName);
        return partitionStates.get(new MirrorPartitionKey(updatedMirrorName, topicPartition.topic(), topicPartition.partition()));
    }

    /**
     * Applies state transitions for all loaded mirror partition states where this broker is the leader.
     * After loading metadata from the internal topic, this method triggers appropriate actions
     * (e.g., starting fetchers for MIRRORING state) by grouping partitions by mirror name and state,
     * then invoking the callback for each group.
     *
     * @param callback The callback to invoke for each (mirror, state, partitions) group
     */
    public void applyLoadedPartitionStates(MirrorCoordinator.StateTransitionCallback callback) {
        Map<String, Map<MirrorPartitionState, Set<TopicPartition>>> statesToPartitionsToOperate = new HashMap<>();
        partitionStates.forEach((key, value) -> {
            LOG.info("!!! operateAll: {} {}", key, value);
            metadataCache.getLeaderAndIsr(key.topic(), key.partition()).ifPresent(metadata -> {
                // only operate when this node is the leader of the partition
                if (metadata.leader() == nodeId) {
                    statesToPartitionsToOperate.compute(key.mirrorName, (k, v) -> {
                        if (v == null) {
                            Map<MirrorPartitionState, Set<TopicPartition>> map = new HashMap<>();
                            map.put(value, Set.of(new TopicPartition(key.topic(), key.partition())));
                            return map;
                        }
                        v.compute(value, (state, prevTps) -> {
                            if (prevTps == null) {
                                Set<TopicPartition> set = new HashSet<>();
                                set.add(new TopicPartition(key.topic(), key.partition()));
                                return set;
                            } else {
                                Set<TopicPartition> result = new HashSet<>(prevTps);
                                result.add(new TopicPartition(key.topic(), key.partition()));
                                return result;
                            }
                        });
                        return v;
                    });
                }
            });
        });

        statesToPartitionsToOperate.forEach((mirrorName, statesToPartitionsMap) -> {
            statesToPartitionsMap.forEach((state, tps) -> {
                callback.onStateLoaded(mirrorName, tps, state);
            });
        });
    }

    /**
     * Updates the cached last mirrored offsets for a cluster mirror.
     * Adds and removes partition offsets from the cache and returns the updated set.
     *
     * @param clusterName the name of the cluster mirror
     * @param addedOffsets map of topic names to partition offsets to add
     * @param removedOffsets map of topic names to partition offsets to remove
     * @return the updated set of last mirrored offsets
     */
    public Map<MirrorPartitionKey, Long> updateLastMirroredOffsets(String clusterName,
                                                                   Map<String, Map<Integer, Long>> addedOffsets,
                                                                   Map<String, Map<Integer, Long>> removedOffsets) {
        removedOffsets.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirroredOffsets.remove(new MirrorPartitionKey(clusterName, topic, partition));
            });
        });
        addedOffsets.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirroredOffsets.put(new MirrorPartitionKey(clusterName, topic, partition), offset);
            });
        });
        return lastMirroredOffsets;
    }

    /**
     * Syncs topic metadata (partition leaders, partition counts) from all source clusters.
     * Runs on every broker so that all brokers can route mirror fetch requests to the correct partition leaders.
     */
    public void syncTopicMetadataFromSourceClusters() {
        Set<String> mirrors = getConfiguredMirrors();
        if (!mirrors.isEmpty()) {
            LOG.debug("Refreshing mirror metadata for mirrors: {}", mirrors);
        }

        mirrors.forEach(this::createMirrorConnection);

        sourceSenders.forEach((mirror, senders) -> syncTopicMetadata(mirror, senders));

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        metadataRefreshError.incrementAndGet();
    }

    /**
     * Syncs mirror metadata (configurations, consumer group offsets, ACLs) from source clusters.
     * Only the coordinator for each mirror name handles this, distributing load across brokers.
     */
    public void syncMirrorMetadataFromSourceClusters() {
        getConfiguredMirrors().forEach(mirrorName -> {
            if (isLocalCoordinator(mirrorName)) {
                createMirrorConnection(mirrorName);
                List<MirrorBlockingSender> senders = sourceSenders.get(mirrorName);
                MirrorConfig mirrorConfig = MirrorConfig.fromProperties(
                        metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName)));
                syncTopicConfigurations(mirrorName, senders, mirrorConfig);
                syncConsumerGroupOffsets(mirrorName, senders, mirrorConfig);
                syncAccessControlLists(mirrorName, senders, mirrorConfig);
            }
        });
    }

    private void createMirrorConnection(String mirrorName) {
        if (sourceSenders.containsKey(mirrorName)) {
            return;
        }
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("Remote bootstrap server not found in Cluster Mirror config: " + mirrorName));

        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        // Use random node id here because we don't know node id of remote brokers
        int rand = random.nextInt(addresses.size());
        var brokerEndpoint = new BrokerEndPoint(random.nextInt(), addresses.get(rand).getHostString(), addresses.get(rand).getPort());
        var logContext = new LogContext("[" + MirrorMetadataManager.class.getName() + " replicaId=" + nodeId + ", mirrorName=" + mirrorName + "] ");

        MirrorBlockingSender sender = new MirrorBlockingSender(
                brokerEndpoint,
                MirrorConfig.fromProperties(props),
                metrics,
                time,
                brokerEndpoint.id(),
                "broker-" + nodeId + "-mirror-metadata-manager-" + mirrorName,
                logContext
        );
        sourceSenders.put(mirrorName, List.of(sender));
    }

    private void syncTopicMetadata(String mirrorName, List<MirrorBlockingSender> senders) {
        Set<String> topics = getConfiguredTopics(mirrorName);
        if (topics.isEmpty()) {
            return;
        }
        var response = getRandomSender(senders).sendRequest(
            MetadataRequest.Builder.forTopicNames(topics.stream().toList(), false)
        );

        if (response.responseBody() instanceof MetadataResponse metadataResponse) {
            LOG.debug("!!! Periodic metadataResponse: {}", metadataResponse);
            Map<Integer, Node> brokerNodes = new HashMap<>();
            metadataResponse.brokers().forEach(broker -> brokerNodes.put(broker.id(), broker));
            var createPartitionsTopics = processTopicMetadata(mirrorName, metadataResponse.topicMetadata(), brokerNodes);
            maybeStopDeletedTopics(mirrorName, metadataResponse.topicMetadata());
            handlePartitionScaling(createPartitionsTopics);
        }
    }

    private void syncTopicConfigurations(String mirrorName, List<MirrorBlockingSender> senders, MirrorConfig mirrorConfig) {
        Set<String> topics = getConfiguredTopics(mirrorName);
        LOG.info("!!! Describing topic configs for topics: {}", topics);
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        topicConfigSyncError.incrementAndGet();

        List<DescribeConfigsRequestData.DescribeConfigsResource> describeConfigsResources =
            topics.stream()
                .map(topic -> new DescribeConfigsRequestData.DescribeConfigsResource()
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setResourceName(topic))
                .toList();

        DescribeConfigsRequest.Builder describeConfigsRequest =
            new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(describeConfigsResources));

        var describeConfigResponse = getRandomSender(senders).sendRequest(describeConfigsRequest);
        if (describeConfigResponse.responseBody() instanceof DescribeConfigsResponse describeConfigsRes) {
            LOG.debug("!!! Periodic describeConfigResponse: {}", describeConfigsRes);
            checkUncleanLeaderElection(mirrorName, describeConfigsRes);
            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(mirrorName, describeConfigsRes, mirrorConfig);
            applyConfigurationChanges(configsToChange);
        }
    }

    // Processes topic metadata and returns topics that need partition scaling
    private CreatePartitionsRequestData.CreatePartitionsTopicCollection processTopicMetadata(
            String mirrorName, Collection<MetadataResponse.TopicMetadata> topicMetadata, Map<Integer, Node> brokerNodes) {
        var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();

        topicMetadata.forEach(tm -> {
            var partitionLeaders = sourcePartitionLeaders.computeIfAbsent(mirrorName, k -> new HashMap<>());

            // Count partitions for this specific topic only
            int sourcePartitionCount = tm.partitionMetadata().size();

            tm.partitionMetadata().forEach(partitionMetadata -> partitionLeaders.put(
                partitionMetadata.topicPartition,
                brokerNodes.get(partitionMetadata.leaderId.get())
            ));

            if (metadataImage.topics().getTopic(tm.topicId()) != null &&
                    metadataImage.topics().getTopic(tm.topicId()).partitions().size() < sourcePartitionCount) {
                createPartitionsTopics.add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                    .setName(tm.topic())
                    .setCount(sourcePartitionCount)
                    .setAssignments(null)
                );
            }
        });

        return createPartitionsTopics;
    }

    // Handles partition scaling by sending create partitions requests
    private void handlePartitionScaling(CreatePartitionsRequestData.CreatePartitionsTopicCollection createPartitionsTopics) {
        if (!createPartitionsTopics.isEmpty()) {
            LOG.debug("!!! Detected partition count change, sending CreatePartitionsRequest: {}", createPartitionsTopics);
            channelManager.sendRequest(new CreatePartitionsRequest.Builder(
                new CreatePartitionsRequestData()
                    .setTopics(createPartitionsTopics)
                    .setValidateOnly(false)
                    .setTimeoutMs(3000)
            ), new TimeoutHandler());
        }
    }

    private void checkUncleanLeaderElection(String mirrorName, DescribeConfigsResponse describeConfigsRes) {
        describeConfigsRes.data().results().forEach(describeConfigResult -> {
            if (describeConfigResult.resourceType() == ConfigResource.Type.TOPIC.id()) {
                describeConfigResult.configs().stream()
                    .filter(con -> con.name().equals(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG) && "true".equals(con.value()))
                    .findFirst()
                    .ifPresent(con -> {
                        if (con.configSource() == DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG.id()) {
                            LOG.warn("Mirror topic '{}' has unclean.leader.election.enable=true set at topic level. " +
                                    "This is not supported as log divergence cannot be reconciled across clusters.",
                                    describeConfigResult.resourceName());
                        } else {
                            LOG.warn("Mirror topic '{}' has unclean.leader.election.enable=true (inherited from broker or cluster default). " +
                                    "This is not supported as log divergence cannot be reconciled across clusters.",
                                    describeConfigResult.resourceName());
                        }
                    });
            }
        });
    }

    private Map<String, Map<String, String>> detectConfigurationChanges(
            String mirrorName, DescribeConfigsResponse describeConfigsRes, MirrorConfig mirrorConfig) {
        Map<String, Map<String, String>> configsToChange = new HashMap<>();
        Pattern excludePattern = mirrorConfig.topicPropertiesExcludePattern();

        describeConfigsRes.data().results().forEach(describeConfigResult -> {
            if (describeConfigResult.resourceType() == ConfigResource.Type.TOPIC.id()) {
                Properties props = metadataCache.topicConfig(describeConfigResult.resourceName());
                Map<String, String> conChange = new HashMap<>();

                describeConfigResult.configs().forEach(con -> {
                    // Ensures the destination cluster's mirror.name setting is never overwritten
                    // by source cluster configs (which wouldn't have this config set)
                    if (con.configSource() == DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG.id()
                            && !con.name().equals(TopicConfig.MIRROR_NAME_CONFIG)
                            && !excludePattern.matcher(con.name()).matches()) {
                        if (props.containsKey(con.name())) {
                            if (!props.get(con.name()).equals(con.value())) {
                                conChange.put(con.name(), con.value());
                            }
                        } else {
                            conChange.put(con.name(), con.value());
                        }
                    }
                });

                if (!conChange.isEmpty()) {
                    configsToChange.put(describeConfigResult.resourceName(), conChange);
                }
            }
        });

        return configsToChange;
    }

    private void applyConfigurationChanges(Map<String, Map<String, String>> configsToChange) {
        LOG.debug("!!! Applying config change: {}", configsToChange);

        Map<ConfigResource, Collection<AlterConfigOp>> configOps = new HashMap<>();
        configsToChange.forEach((name, changes) -> {
            var changeList = changes.entrySet().stream()
                .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
                .toList();
            configOps.put(new ConfigResource(ConfigResource.Type.TOPIC, name), changeList);
        });

        if (!configOps.isEmpty()) {
            IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData().setValidateOnly(false);
            for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : configOps.entrySet()) {
                ConfigResource resource = entry.getKey();
                IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigSet =
                        new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
                for (AlterConfigOp configEntry : configOps.get(resource))
                    alterableConfigSet.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                            .setName(configEntry.configEntry().name())
                            .setValue(configEntry.configEntry().value())
                            .setConfigOperation(configEntry.opType().id()));
                IncrementalAlterConfigsRequestData.AlterConfigsResource alterConfigsResource =
                    new IncrementalAlterConfigsRequestData.AlterConfigsResource();
                alterConfigsResource.setResourceType(resource.type().id())
                        .setResourceName(resource.name()).setConfigs(alterableConfigSet);
                data.resources().add(alterConfigsResource);
            }
            channelManager.sendRequest(new IncrementalAlterConfigsRequest.Builder(data), new TimeoutHandler());
        }
    }

    private MirrorBlockingSender getRandomSender(List<MirrorBlockingSender> senders) {
        return senders.get(random.nextInt(senders.size()));
    }

    /**
     * Detects topics deleted on the source cluster and transitions their mirror partitions
     * to STOPPING, rather than propagating the deletion to the destination cluster.
     * If the topic is later recreated on the source and re-added to the mirror, the operator
     * must first delete the stale destination topic to avoid topic ID conflicts.
     *
     * @param mirrorName Mirror name
     * @param topicMetadata Topic metadata
     */
    private void maybeStopDeletedTopics(String mirrorName, Collection<MetadataResponse.TopicMetadata> topicMetadata) {
        List<String> remoteTopicNamesDeleted = topicMetadata.stream()
                .filter(tm -> tm.error() == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                .map(MetadataResponse.TopicMetadata::topic).toList();
        getConfiguredTopics(mirrorName).forEach(name -> {
            if (remoteTopicNamesDeleted.contains(name)) {
                LOG.info("!!! Detected topic {} deleted in remote cluster {}, stopping mirror partitions", name, mirrorName);
                partitionStates.keySet().stream()
                        .filter(key -> key.mirrorName().equals(mirrorName) && key.topic().equals(name))
                        .forEach(key -> stateTransitioner.ifPresent(t ->
                                t.transitionTo(mirrorName, new TopicPartition(key.topic(), key.partition()), MirrorPartitionState.STOPPING)));
            }
        });
    }

    private void syncConsumerGroupOffsets(String mirrorName, List<MirrorBlockingSender> senders, MirrorConfig mirrorConfig) {
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        consumerGroupOffsetSyncError.incrementAndGet();
        Pattern groupsIncludePattern = mirrorConfig.groupsIncludePattern();
        // 1. list group
        ListGroupsRequest.Builder builder = new ListGroupsRequest.Builder(new ListGroupsRequestData()
                // TODO: if the source cluster is in old version, it won't support types filter
                .setTypesFilter(List.of(Group.GroupType.CLASSIC.name(), Group.GroupType.CONSUMER.name()))
                .setStatesFilter(singletonList(GroupState.STABLE.name())));
        var listGroupResponse = getRandomSender(senders).sendRequest(builder);
        if (listGroupResponse.responseBody() instanceof ListGroupsResponse listGroupsRes) {
            LOG.debug("!!! listGroupsRes for mirror {}: {}", mirrorName, listGroupsRes);

            // Filter groups by include pattern
            var matchingGroups = listGroupsRes.data().groups().stream()
                    .filter(group -> groupsIncludePattern.matcher(group.groupId()).matches())
                    .toList();

            if (matchingGroups.isEmpty()) {
                return;
            }

            // 2. get committed offsets for each group
            OffsetFetchRequest.Builder offsetFetchBuilder = OffsetFetchRequest.Builder.forTopicNames(
                    new OffsetFetchRequestData()
                            .setRequireStable(false)
                            .setGroups(matchingGroups.stream().map(group -> new OffsetFetchRequestData.OffsetFetchRequestGroup()
                                    .setGroupId(group.groupId())
                                    .setTopics(null)).toList()), false);
            var offsetFetchResponse = getRandomSender(senders).sendRequest(offsetFetchBuilder);
            if (offsetFetchResponse.responseBody() instanceof OffsetFetchResponse offsetFetchRes) {
                LOG.debug("!!! Periodic offset fetch: {}", offsetFetchRes);

                // 3. commit offsets to consumer group coordinator
                // TODO: need to find the current group coordinator for each group
                offsetFetchRes.data().groups().forEach(group -> {
                    List<OffsetCommitRequestData.OffsetCommitRequestTopic> topicList = buildOffsetCommitTopicList(group);
                    commitOffsetsToGroupCoordinator(group.groupId(), topicList);
                });
            }
        }
    }

    private List<OffsetCommitRequestData.OffsetCommitRequestTopic> buildOffsetCommitTopicList(OffsetFetchResponseData.OffsetFetchResponseGroup group) {
        List<OffsetCommitRequestData.OffsetCommitRequestTopic> topicList = new ArrayList<>();
        group.topics().forEach(t -> {
            List<OffsetCommitRequestData.OffsetCommitRequestPartition> parList = new ArrayList<>();
            t.partitions().forEach(par -> {
                parList.add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                        .setPartitionIndex(par.partitionIndex())
                        .setCommittedOffset(par.committedOffset())
                        .setCommittedLeaderEpoch(par.committedLeaderEpoch())
                        .setCommittedMetadata(par.metadata()));
            });
            topicList.add(new OffsetCommitRequestData.OffsetCommitRequestTopic()
                    .setTopicId(t.topicId())
                    .setName(t.name())
                    .setPartitions(parList));
        });
        return topicList;
    }

    private void commitOffsetsToGroupCoordinator(String groupId, List<OffsetCommitRequestData.OffsetCommitRequestTopic> topicList) {
        groupCoordinatorSupplier.get().commitOffsets(
                new RequestContext(
                        new RequestHeader(
                                ApiKeys.OFFSET_COMMIT,
                                ApiKeys.OFFSET_COMMIT.latestVersion(),
                                "client",
                                0
                        ),
                        "1",
                        InetAddress.getLoopbackAddress(),
                        KafkaPrincipal.ANONYMOUS,
                        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                        SecurityProtocol.PLAINTEXT,
                        ClientInformation.EMPTY,
                        true
                ),
                new OffsetCommitRequestData()
                        .setGroupId(groupId)
                        .setMemberId("")
                        .setGenerationIdOrMemberEpoch(-1)
                        .setRetentionTimeMs(-1)
                        .setGroupInstanceId("")
                        .setTopics(topicList),
                RequestLocal.noCaching().bufferSupplier()
        ).handle((data, exception) -> {
            LOG.info("!!! Periodic offset commit: {} ;; {}", data, exception);
            return null;
        });
    }

    private void syncAccessControlLists(String mirrorName, List<MirrorBlockingSender> senders, MirrorConfig mirrorConfig) {
        // TODO: We currently mirror all ACLs from the source to the target.
        //       Any ACLs added/removed directly on the target will be overwritten
        //       on the next sync to match the source.
        //
        // TODO: How do we disambiguate ACLs that reference the same resource name
        //       when multiple cluster mirrors exist?

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        aclSyncError.incrementAndGet();

        // list remote acls
        var describeAclsRequest = new DescribeAclsRequest.Builder(ANY_RESOURCE_ACL);
        var describeAclsResponse = getRandomSender(senders).sendRequest(describeAclsRequest);
        if (!(describeAclsResponse.responseBody() instanceof DescribeAclsResponse aclsResponse)) {
            LOG.warn("!!! describeAclsResponse is not DescribeAclsResponse: {}", describeAclsResponse);
            return;
        }

        LOG.debug("!!! describeAclsResponse from remote cluster {}: {}", mirrorName, aclsResponse);

        // Filter ACLs by include rules
        List<MirrorConfig.AclRule> aclIncludeRules = mirrorConfig.aclIncludeRules();
        var allRemoteAcls = DescribeAclsResponse.aclBindings(aclsResponse.acls()).stream()
                .filter(acl -> aclIncludeRules.stream().anyMatch(rule -> rule.matches(acl)))
                .toList();
        var aclChanges = detectACLChanges(allRemoteAcls);
        applyAccessControlListChanges(mirrorName, aclChanges);
    }

    private ACLChanges detectACLChanges(List<AclBinding> allRemoteAcls) {
        var addACLsList = new ArrayList<AclBinding>();
        var deleteACLsList = new ArrayList<AclBinding>();
        var current = metadataImage.acls().acls().values();

        // collect missing acls list
        allRemoteAcls.forEach(acl -> {
            if (current.stream().map(StandardAcl::toBinding).noneMatch(a -> a.equals(acl))) {
                addACLsList.add(acl);
            }
        });

        // collect remove acls list
        metadataImage.acls().acls().values().forEach(acl -> {
            if (!allRemoteAcls.contains(acl.toBinding())) {
                deleteACLsList.add(acl.toBinding());
            }
        });

        return new ACLChanges(addACLsList, deleteACLsList);
    }

    private void applyAccessControlListChanges(String mirrorName, ACLChanges aclChanges) {
        // send createAcls request
        if (!aclChanges.aclsToAdd().isEmpty()) {
            LOG.info("!!! Adding {} ACLs from remote cluster {}", aclChanges.aclsToAdd().size(), mirrorName);
            var requestData = aclChanges.aclsToAdd().stream().map(
                    aclBinding -> new CreateAclsRequestData.AclCreation()
                            .setResourceType(aclBinding.pattern().resourceType().code())
                            .setResourceName(aclBinding.pattern().name())
                            .setResourcePatternType(aclBinding.pattern().patternType().code())
                            .setPrincipal(aclBinding.entry().principal())
                            .setHost(aclBinding.entry().host())
                            .setOperation(aclBinding.entry().operation().code())
                            .setPermissionType(aclBinding.entry().permissionType().code()))
                    .toList();
            channelManager.sendRequest(
                    new CreateAclsRequest.Builder(new CreateAclsRequestData().setCreations(requestData)),
                    new TimeoutHandler()
            );
        }

        // send deleteAcls request
        if (!aclChanges.aclsToDelete().isEmpty()) {
            LOG.info("!!! Removing {} ACLs from remote cluster {}", aclChanges.aclsToDelete().size(), mirrorName);
            var requestData = aclChanges.aclsToDelete().stream().map(
                    aclBinding -> new DeleteAclsRequestData.DeleteAclsFilter()
                            .setResourceTypeFilter(aclBinding.pattern().resourceType().code())
                            .setResourceNameFilter(aclBinding.pattern().name())
                            .setPatternTypeFilter(aclBinding.pattern().patternType().code())
                            .setPrincipalFilter(aclBinding.entry().principal())
                            .setHostFilter(aclBinding.entry().host())
                            .setOperation(aclBinding.entry().operation().code())
                            .setPermissionType(aclBinding.entry().permissionType().code()))
                    .toList();
            channelManager.sendRequest(
                    new DeleteAclsRequest.Builder(new DeleteAclsRequestData().setFilters(requestData)),
                    new TimeoutHandler()
            );
        }

    }

    private record ACLChanges(List<AclBinding> aclsToAdd, List<AclBinding> aclsToDelete) { }

    /**
     * Returns all configured mirrors from the metadata cache.
     *
     * @return the set of all configured mirror names
     */
    public Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.MIRROR)
                .map(ConfigResource::name)
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Returns the configured topics for a specific mirror from the metadata image.
     *
     * @param mirrorName the name of the cluster mirror
     * @return the set of configured topic names for this mirror
     */
    public Set<String> getConfiguredTopics(String mirrorName) {
        return metadataCache.getAllTopics().stream()
                .filter(topic -> mirrorName.equals(metadataCache.topicConfig(topic).get(TopicConfig.MIRROR_NAME_CONFIG)))
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Returns the source cluster bootstrap servers for a given mirror.
     *
     * @param mirrorName the name of the cluster mirror
     * @return the bootstrap servers string, or null if not found
     */
    public String getSourceBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        return Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElse(null);
    }

    /**
     * Get mirror partition states this broker is fetching from.
     *
     * @param mirrorName the name of the cluster mirror
     * @return partition state map
     */
    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> result = new HashMap<>();
        partitionStates.forEach((key, state) -> {
            if (key.mirrorName().equals(mirrorName)) {
                result.put(new TopicPartition(key.topic(), key.partition()), state);
            }
        });
        return result;
    }

    /**
     * Clears all cached mirror metadata including partition state and remote broker connections.
     * Called when the broker resigns as leader for mirror state topic partitions.
     */
    public void clear() {
        partitionStates.clear();
        partitionStateCounts.clear();
        lastMirroredOffsets.clear();
        coordinatorNodes.clear();
        sourceSenders.clear();
        sourcePartitionLeaders.clear();
    }

    /**
     * Removes the REMOVED_TOPIC_SUFFIX from a mirror name if present.
     * This suffix is appended to mirror names when topics are being removed from mirroring.
     *
     * @param mirrorName The mirror name, potentially with REMOVED_TOPIC_SUFFIX
     * @return The original mirror name without the suffix, or empty string if input is null
     */
    public static String originalMirrorName(String mirrorName) {
        if (mirrorName == null) {
            return "";
        }
        if (mirrorName.endsWith(REMOVED_TOPIC_SUFFIX)) {
            return mirrorName.substring(0, mirrorName.length() - REMOVED_TOPIC_SUFFIX.length());
        }
        return mirrorName;
    }

    private static class TimeoutHandler implements ControllerRequestCompletionHandler {
        @Override
        public void onTimeout() {
            LOG.info("!!! Timed out");
        }

        @Override
        public void onComplete(ClientResponse response) {
            LOG.info("!!! Update topics: {}", response);
        }
    }

    public record MirrorPartitionMetadata(int partition, MirrorPartitionState state, Long offset) { }
    public record MirrorPartitionStateRecordValue(String topic, int partition, MirrorPartitionState state) { }
    public record MirrorPartitionKey(String mirrorName, String topic, int partition) { }
}
