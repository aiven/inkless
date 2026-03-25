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

import kafka.log.LogManager;
import kafka.server.KafkaConfig;
import kafka.server.NetworkUtils;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.BumpLeaderEpochRequestData;
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
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.BumpLeaderEpochRequest;
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
import static kafka.server.mirror.MirrorUtils.groupPartitionsByTopic;
import static kafka.server.mirror.MirrorUtils.originalMirrorName;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.controller.ConfigurationControlManager.PAUSED_TOPIC_SUFFIX;
import static org.apache.kafka.controller.ConfigurationControlManager.REMOVED_TOPIC_SUFFIX;

/**
 * Bridges the local destination cluster and remote source clusters for Cluster Mirroring.
 *
 * Implements {@link MetadataPublisher} to detect leadership and config changes, triggering
 * partition state transitions (PREPARING, MIRRORING, STOPPING, STOPPED) via the
 * {@link MirrorCoordinator}. Manages remote cluster connections using {@link MirrorSourceSender}
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
    private static final ResourcePatternFilter ANY_RESOURCE = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
    private static final AclBindingFilter ANY_RESOURCE_ACL = new AclBindingFilter(ANY_RESOURCE, AccessControlEntryFilter.ANY);

    private final String name;
    private final Logger log;
    private final KafkaConfig brokerConfig;
    private final int nodeId;
    private final Metrics metrics;
    private final Time time;
    private final Random random;
    // volatile for cross-thread visibility (written by KRaft thread, read by scheduler and fetcher threads)
    private volatile MetadataImage metadataImage;
    private final MetadataCache metadataCache;
    private volatile MirrorStateSender mirrorStateSender;
    private final NodeToControllerChannelManager channelManager;
    private final Supplier<GroupCoordinator> groupCoordinatorSupplier;
    private final Supplier<MirrorFetcherManager> mirrorFetcherManagerSupplier;
    private Optional<MirrorUtils.StateTransitioner> stateTransitioner = Optional.empty();
    private Optional<Function<MirrorRecordKey, Integer>> coordinatorPartitionFinder = Optional.empty();
    private Optional<Function<String, Integer>> coordinatorPartitionByNameFinder = Optional.empty();

    // cache
    // thread-safe maps for concurrent access from metadata, scheduler, and fetcher threads
    private final Map<String, List<MirrorSourceSender>> sourceSenders = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, Node>> sourceLeaders = new ConcurrentHashMap<>();
    private final Map<MirrorUtils.PartitionKey, MirrorPartitionState> partitionStates = new ConcurrentHashMap<>();
    private final Map<MirrorPartitionState, AtomicLong> partitionStateCounts = new ConcurrentHashMap<>();
    private final Map<MirrorUtils.PartitionKey, Integer> lastMirroredOffsets = new ConcurrentHashMap<>();

    // metrics
    private KafkaMetricsGroup metricsGroup;
    private AtomicLong metadataRefreshError;
    private AtomicLong topicConfigSyncError;
    private AtomicLong consumerGroupOffsetSyncError;
    private AtomicLong aclSyncError;

    public MirrorMetadataManager(
        KafkaConfig brokerConfig,
        Metrics metrics,
        Time time,
        MetadataCache metadataCache,
        NodeToControllerChannelManager channelManager,
        Supplier<GroupCoordinator> groupCoordinatorSupplier,
        Supplier<MirrorFetcherManager> mirrorFetcherManagerSupplier
    ) {
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        this.metrics = metrics;
        this.time = time;
        this.random = new Random();
        this.channelManager = channelManager;
        this.groupCoordinatorSupplier = groupCoordinatorSupplier;
        this.mirrorFetcherManagerSupplier = mirrorFetcherManagerSupplier;
        this.metadataImage = MetadataImage.EMPTY;
        this.metadataCache = metadataCache;

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
        metricsGroup.newGauge("PausingPartitionState", () -> partitionStateCount(MirrorPartitionState.PAUSING));
        metricsGroup.newGauge("PausedPartitionState", () -> partitionStateCount(MirrorPartitionState.PAUSED));
        metricsGroup.newGauge("StoppingPartitionState", () -> partitionStateCount(MirrorPartitionState.STOPPING));
        metricsGroup.newGauge("StoppedPartitionState", () -> partitionStateCount(MirrorPartitionState.STOPPED));
        metricsGroup.newGauge("FailedPartitionState", () -> partitionStateCount(MirrorPartitionState.FAILED));
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Called when cluster metadata is updated.
     * Detects mirror partition leadership changes and triggers state transitions via batched coordinator reads.
     *
     * This is executed in the KRaft metadata publisher thread.
     * Must be called after ReplicaManager#applyDelta.
     * The metadata cache can't be used here because it is updated concurrently.
     */
    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (mirrorStateSender == null) {
            mirrorStateSender = new MirrorStateSender(MirrorStateSender.class.getSimpleName(),
                    NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                    brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            mirrorStateSender.start();
        }

        // caching the image for query purpose
        this.metadataImage = newImage;

        // detect config changes and close stale connections and fetchers to trigger reconnection
        if (delta.configsDelta() != null) {
            delta.configsDelta().changes().entrySet().stream()
                .filter(e -> e.getKey().type() == ConfigResource.Type.MIRROR)
                .forEach(e -> {
                    String mirrorName = e.getKey().name();
                    sourceLeaders.remove(mirrorName);
                    List<MirrorSourceSender> senders = sourceSenders.remove(mirrorName);
                    if (senders != null) {
                        log.info("Mirror config changed for '{}'. Closing existing connections "
                            + "to trigger reconnection with updated configuration.", mirrorName);
                        senders.forEach(MirrorSourceSender::close);
                    }
                    mirrorFetcherManagerSupplier.get().removeFetchersForMirror(mirrorName);
                });
        }

        // get all mirror partition leaders on this node based on the delta
        Set<TopicPartition> mirrorLeaders = getMirrorLeaders(delta, newImage);
        if (mirrorLeaders.isEmpty()) {
            return;
        }

        log.info("onMetadataUpdate: {}", mirrorLeaders);

        // Collect remote coordinator partitions grouped by mirrorName for batched reads
        Map<String, Map<String, Set<Integer>>> remotePartitionsByMirror = new HashMap<>();
        Map<String, Map<TopicPartition, Boolean>> remoteStopFlags = new HashMap<>();
        Map<String, Map<TopicPartition, Boolean>> remotePauseFlags = new HashMap<>();

        mirrorLeaders.forEach(tp -> {
            String rawMirrorName = (String) newImage.configs().configProperties(
                    new ConfigResource(ConfigResource.Type.TOPIC, tp.topic())).get(TopicConfig.MIRROR_NAME_CONFIG);
            boolean stopRequested = rawMirrorName.endsWith(REMOVED_TOPIC_SUFFIX);
            boolean pauseRequested = rawMirrorName.endsWith(PAUSED_TOPIC_SUFFIX);
            String mirrorName = MirrorUtils.originalMirrorName(rawMirrorName);

            MirrorUtils.PartitionKey key = new MirrorUtils.PartitionKey(mirrorName, tp.topic(), tp.partition());
            if (isLocalCoordinator(key.mirrorName(), key.topic(), key.partition())) {
                // Handle local coordinator partitions inline (no network call needed)
                MirrorPartitionState curState = partitionStates.getOrDefault(key, MirrorPartitionState.UNKNOWN);
                applyMirrorStateTransition(key.mirrorName(), tp, curState, null, stopRequested, pauseRequested);
            } else {
                // Collect for batched remote read
                remotePartitionsByMirror
                    .computeIfAbsent(mirrorName, k -> new HashMap<>())
                    .computeIfAbsent(tp.topic(), k -> new HashSet<>())
                    .add(tp.partition());
                remoteStopFlags
                    .computeIfAbsent(mirrorName, k -> new HashMap<>())
                    .put(tp, stopRequested);
                remotePauseFlags
                    .computeIfAbsent(mirrorName, k -> new HashMap<>())
                    .put(tp, pauseRequested);
            }
        });

        // Send one batched read per mirrorName (readStatesFromRemoteCoordinator handles per-node batching internally)
        remotePartitionsByMirror.forEach((mirrorName, partitions) -> {
            Map<TopicPartition, Boolean> stopFlags = remoteStopFlags.get(mirrorName);
            Map<TopicPartition, Boolean> pauseFlags = remotePauseFlags.get(mirrorName);
            readStatesFromRemoteCoordinator(mirrorName, partitions, res ->
                res.data().topics().forEach(topic ->
                    topic.partitions().forEach(partition -> {
                        TopicPartition resTp = new TopicPartition(topic.name(), partition.partitionIndex());
                        // treat unrecorded state (-1) as UNKNOWN so the partition can transition to PREPARING
                        MirrorPartitionState state = partition.state() != -1
                                ? MirrorPartitionState.fromValue(partition.state())
                                : MirrorPartitionState.UNKNOWN;
                        boolean stopRequested = stopFlags.getOrDefault(resTp, false);
                        boolean pauseRequested = pauseFlags.getOrDefault(resTp, false);
                        MirrorUtils.PartitionKey mpk = new MirrorUtils.PartitionKey(mirrorName, resTp.topic(), resTp.partition());
                        MirrorPartitionState curState = partitionStates.getOrDefault(mpk, MirrorPartitionState.UNKNOWN);
                        applyMirrorStateTransition(mirrorName, resTp, curState, state, stopRequested, pauseRequested);
                    })));
        });

        if (delta.topicsDelta() != null) {
            clearFollowersState(delta.topicsDelta().localChanges(nodeId).followers().keySet(), newImage);
        }
    }

    @Override
    public void close() throws Exception {
        mirrorStateSender.shutdown();
        closeSourceSenders();
    }

    /** Returns mirror partitions led by this broker, detecting both leadership and config changes */
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
     * Applies the appropriate state transition based on current state and stop flag.
     *
     * Possible cases:
     * stopRequested: it means the partition should head to STOPPED state. When it is true (i.e. users removeTopicsFromMirror):
     *   1. if it's already in STOPPED state, then keep the state
     *   2. else, move the state to STOPPING state
     * pauseRequested: it means the partition should head to PAUSED state. When it is true (i.e. users pause it):
     *   1. if it's already in PAUSED state, then keep the state
     *   2. else, move the state to PAUSING state
     * When stopRequested=false and pauseRequested=false:
     *   1. if it's in PAUSED state, we should move it to MIRRORING state. It will happen when users resume mirroring
     *   2. if it's in UNKNOWN or STOPPED state, we should move it to PREPARING state. It will happen when users addTopicsToMirror.
     *   3. else, keep the same state as is. This could happen like leadership change, and the new leader should continue to complete the process in previous leader.
     */
    private void applyMirrorStateTransition(String mirrorName, TopicPartition tp,
                                            MirrorPartitionState curState, MirrorPartitionState fetchedState,
                                            boolean stopRequested, boolean pauseRequested) {
        stateTransitioner.ifPresent(t -> {
            if (stopRequested) {
                if (curState != MirrorPartitionState.STOPPED) {
                    t.transitionTo(mirrorName, tp, MirrorPartitionState.STOPPING);
                } else {
                    t.transitionTo(mirrorName, tp, MirrorPartitionState.STOPPED);
                }
            } else if (pauseRequested) {
                if (curState != MirrorPartitionState.PAUSED) {
                    t.transitionTo(mirrorName, tp, MirrorPartitionState.PAUSING);
                } else {
                    t.transitionTo(mirrorName, tp, MirrorPartitionState.PAUSED);
                }
            } else if (curState == MirrorPartitionState.PAUSED) {
                t.transitionTo(mirrorName, tp, MirrorPartitionState.MIRRORING);
            } else if (curState == MirrorPartitionState.UNKNOWN
                    || curState == MirrorPartitionState.STOPPED) {
                t.transitionTo(mirrorName, tp, MirrorPartitionState.PREPARING);
            } else {
                t.transitionTo(mirrorName, tp, fetchedState != null ? fetchedState : curState);
            }
        });
    }

    // Clears mirror state for partitions where this broker lost leadership, unless it is the coordinator
    private void clearFollowersState(Set<TopicPartition> followerDelta, MetadataImage newImage) {
        followerDelta.forEach(followerTp -> {
            String mirrorName = (String) newImage.configs()
                    .configProperties(new ConfigResource(ConfigResource.Type.TOPIC, followerTp.topic()))
                    .get(TopicConfig.MIRROR_NAME_CONFIG);
            if (mirrorName != null && !mirrorName.isEmpty() && !isLocalCoordinator(mirrorName, followerTp.topic(), followerTp.partition())) {
                String updatedMirrorName = originalMirrorName(mirrorName);
                MirrorUtils.PartitionKey key = new MirrorUtils.PartitionKey(updatedMirrorName, followerTp.topic(), followerTp.partition());
                removePartitionState(key);
                lastMirroredOffsets.remove(key);
            }
        });
    }

    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        if (coordinatorPartitionFinder.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordinatorPartitionFinder.get().apply(
                            new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), partition))).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    private boolean isLocalCoordinator(String mirrorName) {
        if (coordinatorPartitionByNameFinder.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordinatorPartitionByNameFinder.get().apply(mirrorName)).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    // Finds the coordinator node (leader of the __mirror_state partition) for a mirror record key
    private Node findCoordinatorNode(MirrorRecordKey key) {
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
                    if (coordinatorPartitionFinder.isEmpty()) {
                        return Node.noNode();
                    }
                    int partition = coordinatorPartitionFinder.get().apply(key);
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
            log.warn("Exception while getting mirror coordinator", e);
        }
        return Node.noNode();
    }

    void setStateTransitioner(MirrorUtils.StateTransitioner function) {
        this.stateTransitioner = Optional.of(function);
    }

    void setCoordinatorPartitionByKeyFinder(Function<MirrorRecordKey, Integer> function) {
        this.coordinatorPartitionFinder = Optional.of(function);
    }

    void setCoordinatorPartitionByNameFinder(Function<String, Integer> function) {
        this.coordinatorPartitionByNameFinder = Optional.of(function);
    }

    /** Writes partition states to remote coordinators, batching requests per coordinator node. */
    void writeStatesToRemoteCoordinator(String mirrorName,
                                        Map<String, Set<MirrorUtils.PartitionStateInfo>> topicMetadata,
                                        Set<String> removedTopics,
                                        Consumer<WriteMirrorStatesResponse> callback) {
        log.debug("Writing states to remote coordinator: {} {} {}", mirrorName, topicMetadata, removedTopics);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<WriteMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        topicMetadata.forEach((topic, metadata) -> {
            metadata.forEach(m -> {
                MirrorRecordKey key = new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), m.partition());
                Node coordinatorNode = findCoordinatorNode(key);
                if (coordinatorNode.equals(Node.noNode())) {
                    log.error("Coordinator is not available for mirror {} partition {}-{}", mirrorName, topic, m.partition());
                    return;
                }

                WriteMirrorStatesRequestData.PartitionData partitionData = new WriteMirrorStatesRequestData.PartitionData();
                partitionData.setState(m.state() == null ? MirrorPartitionState.UNKNOWN.value() : m.state().value());
                partitionData.setLastMirroredOffset(m.leaderEpoch());
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

            mirrorStateSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                node,
                new WriteMirrorStatesRequest.Builder(data),
                response -> {
                    log.debug("Write states to remote coordinator completed: {}", response.responseBody());
                    if (response.responseBody() instanceof WriteMirrorStatesResponse writeMirrorStatesResponse) {
                        callback.accept(writeMirrorStatesResponse);
                    }
                }
            ));
        });
    }

    /**
     * Reads partition states from remote coordinators, batching requests per coordinator node.
     * Updates local cache (lastMirroredOffsets, partitionStates) with each response.
     */
    private void readStatesFromRemoteCoordinator(String mirrorName,
                                                 Map<String, Set<Integer>> partitions,
                                                 Consumer<ReadMirrorStatesResponse> callback) {
        log.debug("Reading states from remote coordinator: {} {}", mirrorName, partitions);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<ReadMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                MirrorRecordKey key = new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), part);
                Node coordinatorNode = findCoordinatorNode(key);
                if (coordinatorNode.equals(Node.noNode())) {
                    log.warn("Coordinator is not available for mirror {} partition {}-{}", mirrorName, topic, part);
                    return;
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

            mirrorStateSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                node,
                new ReadMirrorStatesRequest.Builder(data),
                response -> {
                    if (response.responseBody() instanceof ReadMirrorStatesResponse readMirrorStatesResponse) {
                        log.debug("Read states from remote coordinator completed: {}", response.responseBody());

                        // Update cache
                        readMirrorStatesResponse.data().topics().forEach(topic -> {
                            topic.partitions().forEach(partition -> {
                                if (partition.lastMirroredOffset() != -1) {
                                    lastMirroredOffsets.put(new MirrorUtils.PartitionKey(mirrorName, topic.name(), partition.partitionIndex()),
                                            partition.lastMirroredOffset());
                                }
                                if (partition.state() != -1) {
                                    partitionStates.put(new MirrorUtils.PartitionKey(mirrorName, topic.name(), partition.partitionIndex()),
                                            MirrorPartitionState.fromValue(partition.state()));
                                }
                            });
                        });

                        callback.accept(readMirrorStatesResponse);
                    }
                }
            ));
        });
    }

    /** Reads partition states and offsets from local cache. Used when this broker is the coordinator. */
    void getCachedPartitionMetadata(String mirrorName,
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
                partitionResult.setLastMirroredOffset(lastMirroredOffsets.getOrDefault(new MirrorUtils.PartitionKey(mirrorName, tp, part), -1));
                partitionResult.setState(partitionStates.getOrDefault(
                        new MirrorUtils.PartitionKey(mirrorName, tp, part), MirrorPartitionState.UNKNOWN).value());
                partitionResults.add(partitionResult);
            });
            topicResult.setPartitions(partitionResults);
            topicResults.add(topicResult);
        });
        data.setTopics(topicResults);
        responseCallback.accept(new ReadMirrorStatesResponse(data));
    }

    /** Creates initial source senders from bootstrap addresses if not already connected. */
    private void ensureConnection(String mirrorName) {
        if (sourceSenders.containsKey(mirrorName)) {
            return;
        }
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("Remote bootstrap server not found in Cluster Mirror config: " + mirrorName));

        log.info("Mirror config for '{}': {}", mirrorName, props);

        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        MirrorConfig mirrorConfig = MirrorConfig.fromProperties(props);
        List<MirrorSourceSender> senders = new ArrayList<>();
        for (var address : addresses) {
            var brokerEndpoint = new BrokerEndPoint(random.nextInt(), address.getHostString(), address.getPort());
            var logContext = new LogContext("[" + MirrorMetadataManager.class.getName() + " replicaId=" + nodeId + ", mirrorName=" + mirrorName + "] ");
            senders.add(new MirrorSourceSender(
                    brokerEndpoint,
                    mirrorConfig,
                    brokerConfig,
                    metrics,
                    time,
                    brokerEndpoint.id(),
                    "broker-" + nodeId + "-mirror-metadata-manager-" + mirrorName,
                    logContext
            ));
        }
        sourceSenders.put(mirrorName, senders);
    }

    /** Sends a request to the source cluster, iterating available senders with fallback on failure. */
    private ClientResponse trySendRequest(String mirrorName, AbstractRequest.Builder<?> requestBuilder) {
        // snapshot sender list to avoid concurrent modification during iteration
        List<MirrorSourceSender> senders = List.copyOf(sourceSenders.getOrDefault(mirrorName, List.of()));
        if (senders.isEmpty()) {
            throw new IllegalStateException("No source senders available for mirror " + mirrorName);
        }
        Exception lastException = null;
        for (MirrorSourceSender sender : senders) {
            try {
                return sender.sendRequest(requestBuilder);
            } catch (Exception e) {
                lastException = e;
                log.warn("Failed to send request to {} for mirror {}: {}", sender.brokerEndPoint(), mirrorName, e.getMessage());
            }
        }
        throw new KafkaException("Failed to send request to any source server for mirror " + mirrorName, lastException);
    }

    /** Invalidates cached source leaders for specific partitions, leaving other partitions' cached leaders intact. */
    public void invalidateSourceLeader(String mirrorName, java.util.Set<TopicPartition> partitions) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            partitions.forEach(partitionLeaders::remove);
        }
    }

    /** Updates cached source leader for a specific partition. */
    public void updateSourceLeader(String mirrorName, TopicPartition tp, Node leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    /** Resolves source partition leader from cache, falling back to bootstrap server if not cached. */
    public Node resolveSourceLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            Node leader = partitionLeaders.get(tp);
            if (leader != null) {
                return leader;
            }
        }

        // No cached metadata. Fall back to the bootstrap server instead of blocking on a
        // synchronous metadata refresh, which can stall the metadata event queue thread when
        // the source broker is down. The periodic metadata refresh will populate the cache,
        // and the fetcher thread will handle leader rediscovery via maybeCreateMirrorFetchers.
        ensureConnection(mirrorName);
        List<MirrorSourceSender> senders = sourceSenders.get(mirrorName);
        if (senders != null && !senders.isEmpty()) {
            log.info("No cached leader for mirror {} partition {}. Using bootstrap server as initial target.", mirrorName, tp);
            BrokerEndPoint ep = senders.get(0).brokerEndPoint();
            return new Node(ep.id(), ep.host(), ep.port());
        }

        throw new IllegalStateException("No source senders available for mirror " + mirrorName);
    }

    // atomic per-key update to keep partition state counts consistent
    void updatePartitionState(MirrorUtils.PartitionKey key, MirrorPartitionState newState) {
        partitionStates.compute(key, (k, oldState) -> {
            if (oldState != null && oldState != newState) {
                partitionStateCounts.computeIfAbsent(oldState, s -> new AtomicLong()).decrementAndGet();
            }
            if (oldState != newState) {
                partitionStateCounts.computeIfAbsent(newState, s -> new AtomicLong()).incrementAndGet();
            }
            return newState;
        });
    }

    // atomic remove + counter decrement to keep partition state counts consistent
    private void removePartitionState(MirrorUtils.PartitionKey key) {
        partitionStates.computeIfPresent(key, (k, oldState) -> {
            partitionStateCounts.computeIfAbsent(oldState, s -> new AtomicLong()).decrementAndGet();
            return null;
        });
    }

    private long partitionStateCount(MirrorPartitionState state) {
        return partitionStateCounts.computeIfAbsent(state, s -> new AtomicLong()).get();
    }

    /** Strips REMOVED_TOPIC_SUFFIX before lookup. */
    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
        String updatedMirrorName = originalMirrorName(mirrorName);
        return partitionStates.get(new MirrorUtils.PartitionKey(updatedMirrorName, topicPartition.topic(), topicPartition.partition()));
    }

    /** Groups loaded partition states by mirror and state, then invokes the callback for each group. */
    void applyLoadedPartitionStates(MirrorUtils.StateTransitionCallback callback) {
        Map<String, Map<MirrorPartitionState, Set<TopicPartition>>> statesToPartitionsToOperate = new HashMap<>();
        partitionStates.forEach((key, value) -> {
            log.debug("Applying loaded partition state: {} {}", key, value);
            metadataCache.getLeaderAndIsr(key.topic(), key.partition()).ifPresent(metadata -> {
                // only operate when this node is the leader of the partition
                if (metadata.leader() == nodeId) {
                    statesToPartitionsToOperate.compute(key.mirrorName(), (k, v) -> {
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

    int getLastMirroredOffset(String clusterName, TopicPartition topicPartition) {
        MirrorUtils.PartitionKey key = new MirrorUtils.PartitionKey(clusterName, topicPartition.topic(), topicPartition.partition());
        if (lastMirroredOffsets.containsKey(key)) {
            return lastMirroredOffsets.get(key);
        }
        return -1;
    }

    Map<MirrorUtils.PartitionKey, Integer> updateLastMirroredOffsets(String clusterName,
                                                                  Map<String, Map<Integer, Integer>> addedOffsets,
                                                                  Map<String, Map<Integer, Integer>> removedOffsets) {
        removedOffsets.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirroredOffsets.remove(new MirrorUtils.PartitionKey(clusterName, topic, partition));
            });
        });
        addedOffsets.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirroredOffsets.put(new MirrorUtils.PartitionKey(clusterName, topic, partition), offset);
            });
        });
        return lastMirroredOffsets;
    }

    /** Fetches last mirrored offsets from source cluster and truncates local replicas to those offsets. */
    void truncateToLastMirroredOffsets(ReplicaManager replicaManager,
                                       String mirrorName,
                                       Set<TopicPartition> topicPartitionSet,
                                       Consumer<TopicPartition> callback) {
        log.info("Truncating to last mirrored offsets for mirror {}: {}", mirrorName, topicPartitionSet);
        ensureConnection(mirrorName);

        // get api versions of the source cluster
        var apiResponse = trySendRequest(mirrorName, new ApiVersionsRequest.Builder());

        if (apiResponse.responseBody() instanceof ApiVersionsResponse apiVersionsResponse) {
            if (apiVersionsResponse.apiVersion(ApiKeys.LAST_MIRRORED_OFFSETS.id) == null) {
                log.debug("The LastMirroredOffsets API is not supported in source cluster, truncating to offset 0");
                Map<TopicPartition, Integer> offsets = new HashMap<>();
                topicPartitionSet.forEach(tp -> offsets.put(tp, 0));
                replicaManager.maybeTruncateForLeaderEpoch(offsets, callback);
                return;
            }
        }

        List<LastMirroredOffsetsRequestData.TopicData> topicDataList = new ArrayList<>();
        groupPartitionsByTopic(topicPartitionSet).forEach((topic, partitions) -> {
            List<LastMirroredOffsetsRequestData.PartitionData> partitionDataList = new ArrayList<>();
            partitions.forEach(partition -> {
                LastMirroredOffsetsRequestData.PartitionData partitionData = new LastMirroredOffsetsRequestData.PartitionData();
                partitionData.setPartitionIndex(partition);
                partitionDataList.add(partitionData);
            });
            LastMirroredOffsetsRequestData.TopicData topicData = new LastMirroredOffsetsRequestData.TopicData()
                    .setName(topic).setPartitions(partitionDataList);
            topicDataList.add(topicData);
        });

        var response = trySendRequest(mirrorName,
                new LastMirroredOffsetsRequest.Builder(
                        new LastMirroredOffsetsRequestData().setMirrorName(mirrorName).setTopics(topicDataList))
        );

        if (response.responseBody() instanceof LastMirroredOffsetsResponse lastMirroredOffsetResponse) {
            log.debug("Received last mirrored offset response: {}", lastMirroredOffsetResponse);
            Map<TopicPartition, Integer> offsets = new HashMap<>();
            lastMirroredOffsetResponse.data().topics().forEach(topic -> {
                String name = topic.name();
                topic.partitions().forEach(partition -> {
                    offsets.put(new TopicPartition(name, partition.partitionIndex()), partition.lastMirroredOffset());
                });
            });
            replicaManager.maybeTruncateForLeaderEpoch(offsets, callback);
        }
    }

    /** Syncs metadata from all source clusters. */
    void syncMetadata() {
        Set<String> mirrors = getConfiguredMirrors();
        if (!mirrors.isEmpty()) {
            log.debug("Refreshing mirror metadata for mirrors: {}", mirrors);
        }

        mirrors.forEach(this::ensureConnection);
        // snapshot keyset to avoid ConcurrentModificationException
        for (String mirrorName : Set.copyOf(sourceSenders.keySet())) {
            try {
                discoverSourceBrokers(mirrorName);
                syncTopicMetadata(mirrorName);
                syncMirrorMetadata(mirrorName);
            } catch (Exception e) {
                log.error("Failed to refresh metadata for mirror {}", mirrorName, e);
            }
        }

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        metadataRefreshError.incrementAndGet();
    }

    public void sendBumpLeaderEpoch(LogManager logManager, Set<TopicPartition> topicPartitions) {
        List<BumpLeaderEpochRequestData.TopicState> topicStates = new ArrayList<>();
        Map<String, Set<Integer>> partitions = new HashMap<>();
        topicPartitions.forEach(tp -> {
           partitions.computeIfAbsent(tp.topic(), key -> new HashSet<>()).add(tp.partition());
        });
        partitions.forEach((topic, parts) -> {
            BumpLeaderEpochRequestData.TopicState topicState = new BumpLeaderEpochRequestData.TopicState();
            List<BumpLeaderEpochRequestData.LeaderEpochState> topicLeaderEpoch = new ArrayList<>();
            parts.forEach(partitionId -> {
                int epoch = logManager.getLog(new TopicPartition(topic, partitionId), false).get().latestEpoch().orElse(0);
                topicLeaderEpoch.add(new BumpLeaderEpochRequestData.LeaderEpochState().setMinLeaderEpoch(epoch).setPartitionIndex(partitionId));
            });
            topicState.setTopicId(metadataCache.getTopicId(topic)).setPartitions(topicLeaderEpoch);
            topicStates.add(topicState);
        });

        channelManager.sendRequest(new BumpLeaderEpochRequest.Builder(
                new BumpLeaderEpochRequestData().setTopics(topicStates)
        ), new TimeoutHandler());
    }

    /**
     * Discovers source cluster brokers via metadata and adds senders for any newly found brokers.
     * Sender cleanup only happens via {@link #onMetadataUpdate} or {@link #clear}.
     */
    private void discoverSourceBrokers(String mirrorName) {
        var response = trySendRequest(mirrorName, MetadataRequest.Builder.allTopics());
        if (!(response.responseBody() instanceof MetadataResponse metadataResponse)) {
            return;
        }

        Collection<Node> discoveredBrokers = metadataResponse.brokers();
        if (discoveredBrokers.isEmpty()) {
            return;
        }
        List<MirrorSourceSender> currentSenders = sourceSenders.get(mirrorName);
        if (currentSenders == null) {
            return;
        }

        Set<String> currentEndpoints = currentSenders.stream()
                .map(s -> s.brokerEndPoint().host() + ":" + s.brokerEndPoint().port())
                .collect(Collectors.toSet());

        List<Node> newBrokers = discoveredBrokers.stream()
                .filter(n -> !currentEndpoints.contains(n.host() + ":" + n.port()))
                .toList();

        if (newBrokers.isEmpty()) {
            return;
        }

        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        MirrorConfig mirrorConfig = MirrorConfig.fromProperties(props);

        List<MirrorSourceSender> addedSenders = new ArrayList<>();
        for (Node broker : newBrokers) {
            try {
                BrokerEndPoint endpoint = new BrokerEndPoint(broker.id(), broker.host(), broker.port());
                String clientId = "nodeId-" + nodeId + "-" + mirrorName + "-" + broker.host() + "-" + broker.port();
                LogContext logContext = new LogContext("[" + MirrorMetadataManager.class.getSimpleName() + "Sender id=" + nodeId + " clientId=" + clientId + "] ");
                addedSenders.add(MirrorUtils.createSender(endpoint, mirrorConfig, brokerConfig, metrics, time, clientId, logContext));
            } catch (Exception e) {
                log.warn("Failed to create sender for broker {} in mirror {}", broker, mirrorName, e);
            }
        }

        if (addedSenders.isEmpty()) {
            return;
        }

        List<MirrorSourceSender> merged = new ArrayList<>(currentSenders);
        merged.addAll(addedSenders);
        log.info("Adding {} discovered broker(s) to source senders for mirror {}: {}",
                addedSenders.size(), mirrorName,
                addedSenders.stream().map(s -> s.brokerEndPoint().toString()).collect(Collectors.joining(", ")));
        sourceSenders.put(mirrorName, merged);
    }

    /** Fetches topic metadata from the source cluster and updates partition leaders, counts, and deletions. */
    private void syncTopicMetadata(String mirrorName) {
        Set<String> topics = getConfiguredTopics(mirrorName);
        if (topics.isEmpty()) {
            return;
        }
        var response = trySendRequest(mirrorName,
                MetadataRequest.Builder.forTopicNames(topics.stream().toList(), false)
        );

        if (response.responseBody() instanceof MetadataResponse metadataResponse) {
            log.debug("Periodic metadata response: {}", metadataResponse);
            Map<Integer, Node> brokerNodes = new HashMap<>();
            metadataResponse.brokers().forEach(broker -> brokerNodes.put(broker.id(), broker));
            var createPartitionsTopics = processTopicMetadata(mirrorName, metadataResponse.topicMetadata(), brokerNodes);
            maybeStopDeletedTopics(mirrorName, metadataResponse.topicMetadata());
            handlePartitionScaling(createPartitionsTopics);
        }
    }

    /** Processes topic metadata and returns topics that need partition scaling */
    private CreatePartitionsRequestData.CreatePartitionsTopicCollection processTopicMetadata(
            String mirrorName, Collection<MetadataResponse.TopicMetadata> topicMetadata, Map<Integer, Node> brokerNodes) {
        var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();

        topicMetadata.forEach(tm -> {
            // use ConcurrentHashMap for thread-safe access from scheduler and fetcher threads
            var partitionLeaders = sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>());

            // Count partitions for this specific topic only
            int sourcePartitionCount = tm.partitionMetadata().size();

            // skip partitions with no leader (source broker may be restarting)
            tm.partitionMetadata().forEach(partitionMetadata -> {
                if (partitionMetadata.leaderId.isPresent()) {
                    Node leader = brokerNodes.get(partitionMetadata.leaderId.get());
                    if (leader != null) {
                        partitionLeaders.put(partitionMetadata.topicPartition, leader);
                    }
                }
            });

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

    /** Handles partition scaling by sending create partitions requests */
    private void handlePartitionScaling(CreatePartitionsRequestData.CreatePartitionsTopicCollection createPartitionsTopics) {
        if (!createPartitionsTopics.isEmpty()) {
            log.debug("Detected partition count change, sending CreatePartitionsRequest: {}", createPartitionsTopics);
            channelManager.sendRequest(new CreatePartitionsRequest.Builder(
                    new CreatePartitionsRequestData()
                            .setTopics(createPartitionsTopics)
                            .setValidateOnly(false)
                            .setTimeoutMs(3000)
            ), new TimeoutHandler(log));
        }
    }

    /** Transitions mirror partitions to STOPPING when the topic is deleted on the source cluster. */
    private void maybeStopDeletedTopics(String mirrorName, Collection<MetadataResponse.TopicMetadata> topicMetadata) {
        List<String> deletedSourceTopicNames = topicMetadata.stream()
                .filter(tm -> tm.error() == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                .map(MetadataResponse.TopicMetadata::topic).toList();
        getConfiguredTopics(mirrorName, true).forEach(name -> {
            if (deletedSourceTopicNames.contains(name)) {
                log.info("Detected topic {} deleted in remote cluster {}, stopping mirror partitions", name, mirrorName);
                // snapshot keyset to avoid skipping entries during concurrent modification
                Set.copyOf(partitionStates.keySet()).stream()
                        .filter(key -> key.mirrorName().equals(mirrorName) && key.topic().equals(name))
                        .forEach(key -> stateTransitioner.ifPresent(t ->
                                t.transitionTo(mirrorName, new TopicPartition(key.topic(), key.partition()), MirrorPartitionState.STOPPING)));
            }
        });
    }

    /**
     * Syncs mirror metadata (configurations, consumer group offsets, ACLs) from source clusters.
     * Only the coordinator for each mirror name handles this, distributing load across brokers.
     */
    void syncMirrorMetadata(String mirrorName) {
        if (isLocalCoordinator(mirrorName)) {
            ensureConnection(mirrorName);
            try {
                MirrorConfig mirrorConfig = MirrorConfig.fromProperties(
                        metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName)));
                syncTopicConfigurations(mirrorName, mirrorConfig);
                syncConsumerGroupOffsets(mirrorName, mirrorConfig);
                syncAccessControlLists(mirrorName, mirrorConfig);
            } catch (Exception e) {
                log.error("Failed to sync mirror metadata for mirror {}", mirrorName, e);
            }
        }
    }

    private void syncTopicConfigurations(String mirrorName, MirrorConfig mirrorConfig) {
        Set<String> topics = getConfiguredTopics(mirrorName);
        log.debug("Describing topic configs for topics: {}", topics);
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

        var describeConfigResponse = trySendRequest(mirrorName, describeConfigsRequest);
        if (describeConfigResponse.responseBody() instanceof DescribeConfigsResponse describeConfigsRes) {
            log.debug("Periodic describe config response: {}", describeConfigsRes);
            warnIfUncleanLeaderElection(describeConfigsRes);
            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(describeConfigsRes, mirrorConfig);
            applyConfigurationChanges(configsToChange);
        }
    }

    private void warnIfUncleanLeaderElection(DescribeConfigsResponse describeConfigsRes) {
        describeConfigsRes.data().results().forEach(describeConfigResult -> {
            if (describeConfigResult.resourceType() == ConfigResource.Type.TOPIC.id()) {
                describeConfigResult.configs().stream()
                    .filter(con -> con.name().equals(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG) && "true".equals(con.value()))
                    .findFirst()
                    .ifPresent(con -> {
                        if (con.configSource() == DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG.id()) {
                            log.warn("Mirror topic '{}' has unclean.leader.election.enable=true set at topic level. " +
                                    "This is not supported as log divergence cannot be reconciled across clusters.",
                                    describeConfigResult.resourceName());
                        } else {
                            log.warn("Mirror topic '{}' has unclean.leader.election.enable=true (inherited from broker or cluster default). " +
                                    "This is not supported as log divergence cannot be reconciled across clusters.",
                                    describeConfigResult.resourceName());
                        }
                    });
            }
        });
    }

    private Map<String, Map<String, String>> detectConfigurationChanges(
            DescribeConfigsResponse describeConfigsRes, MirrorConfig mirrorConfig) {
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
        log.debug("Applying config change: {}", configsToChange);

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
            channelManager.sendRequest(new IncrementalAlterConfigsRequest.Builder(data), new TimeoutHandler(log));
        }
    }

    private void syncConsumerGroupOffsets(String mirrorName, MirrorConfig mirrorConfig) {
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        consumerGroupOffsetSyncError.incrementAndGet();
        Pattern groupsIncludePattern = mirrorConfig.groupsIncludePattern();
        // 1. list group
        ListGroupsRequest.Builder builder = new ListGroupsRequest.Builder(new ListGroupsRequestData()
                // TODO: if the source cluster is in old version, it won't support types filter
                .setTypesFilter(List.of(Group.GroupType.CLASSIC.name(), Group.GroupType.CONSUMER.name()))
                .setStatesFilter(singletonList(GroupState.STABLE.name())));
        var listGroupResponse = trySendRequest(mirrorName, builder);
        if (listGroupResponse.responseBody() instanceof ListGroupsResponse listGroupsRes) {
            log.debug("List groups response for mirror {}: {}", mirrorName, listGroupsRes);

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
            var offsetFetchResponse = trySendRequest(mirrorName, offsetFetchBuilder);
            if (offsetFetchResponse.responseBody() instanceof OffsetFetchResponse offsetFetchRes) {
                log.debug("Periodic offset fetch response: {}", offsetFetchRes);

                // 3. commit offsets to consumer group coordinator
                // TODO: need to find the current group coordinator for each group
                offsetFetchRes.data().groups().forEach(group -> {
                    List<OffsetCommitRequestData.OffsetCommitRequestTopic> topicList = toOffsetCommitTopics(group);
                    commitOffsetsToGroupCoordinator(group.groupId(), topicList);
                });
            }
        }
    }

    private List<OffsetCommitRequestData.OffsetCommitRequestTopic> toOffsetCommitTopics(OffsetFetchResponseData.OffsetFetchResponseGroup group) {
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
            log.debug("Periodic offset commit result: {}, exception: {}", data, exception);
            return null;
        });
    }

    private void syncAccessControlLists(String mirrorName, MirrorConfig mirrorConfig) {
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
        var describeAclsResponse = trySendRequest(mirrorName, describeAclsRequest);
        if (!(describeAclsResponse.responseBody() instanceof DescribeAclsResponse aclsResponse)) {
            log.warn("Unexpected ACL response type from remote cluster: {}", describeAclsResponse);
            return;
        }

        log.debug("Describe ACLs response from remote cluster {}: {}", mirrorName, aclsResponse);

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
            log.debug("Adding {} ACLs from remote cluster {}", aclChanges.aclsToAdd().size(), mirrorName);
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
                    new TimeoutHandler(log)
            );
        }

        // send deleteAcls request
        if (!aclChanges.aclsToDelete().isEmpty()) {
            log.debug("Removing {} ACLs from remote cluster {}", aclChanges.aclsToDelete().size(), mirrorName);
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
                    new TimeoutHandler(log)
            );
        }
    }

    private record ACLChanges(List<AclBinding> aclsToAdd, List<AclBinding> aclsToDelete) { }

    Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    /** Returns the set of topic names configured for the given mirror, excluding paused topics. */
    Set<String> getConfiguredTopics(String mirrorName) {
        return getConfiguredTopics(mirrorName, false);
    }

    /** Returns the set of topic names configured for the given mirror, optionally including paused topics. */
    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused) {
        return metadataCache.getAllTopics().stream()
                .filter(topic -> {
                    String topicMirrorName = (String) metadataCache.topicConfig(topic).get(TopicConfig.MIRROR_NAME_CONFIG);
                    if (topicMirrorName == null) return false;
                    if (!includePaused && topicMirrorName.endsWith(PAUSED_TOPIC_SUFFIX)) return false;
                    return mirrorName.equals(MirrorUtils.originalMirrorName(topicMirrorName));
                })
                .collect(Collectors.toSet());
    }

    String getSourceBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        return Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElse(null);
    }

    Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> result = new HashMap<>();
        partitionStates.forEach((key, state) -> {
            if (key.mirrorName().equals(mirrorName)) {
                result.put(new TopicPartition(key.topic(), key.partition()), state);
            }
        });
        return result;
    }

    void clear() {
        partitionStates.clear();
        partitionStateCounts.clear();
        lastMirroredOffsets.clear();
        closeSourceSenders();
        sourceLeaders.clear();
    }

    private void closeSourceSenders() {
        // snapshot and clear first, then close to avoid use-after-close races
        Map<String, List<MirrorSourceSender>> snapshot = new HashMap<>(sourceSenders);
        sourceSenders.clear();
        snapshot.values().forEach(senders -> senders.forEach(MirrorSourceSender::close));
    }

    private static class TimeoutHandler implements ControllerRequestCompletionHandler {
        private final Logger log;

        TimeoutHandler(Logger log) {
            this.log = log;
        }

        @Override
        public void onTimeout() {
            log.warn("Controller request timed out");
        }

        @Override
        public void onComplete(ClientResponse response) {
            log.debug("Controller request completed: {}", response);
        }
    }
}
