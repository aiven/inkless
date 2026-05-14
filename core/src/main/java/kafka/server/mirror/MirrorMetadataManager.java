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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeMirrorsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.MirrorDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.BumpLeaderEpochsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.BumpLeaderEpochsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.StartMirrorTopicsRequest;
import org.apache.kafka.common.requests.StopMirrorTopicsRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
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
import org.apache.kafka.server.config.MirrorConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.network.BrokerEndPoint;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static kafka.server.mirror.MirrorUtils.LEADER_EPOCH_BUMP_INCREMENT;
import static kafka.server.mirror.MirrorUtils.LEADER_EPOCH_BUMP_THRESHOLD;
import static kafka.server.mirror.MirrorUtils.originalMirrorName;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.controller.ConfigurationControlManager.PAUSED_TOPIC_SUFFIX;
import static org.apache.kafka.controller.ConfigurationControlManager.STOPPED_TOPIC_SUFFIX;

/**
 * Bridges the local destination cluster and remote source clusters for Cluster Mirroring.
 *
 * Implements {@link MetadataPublisher} to detect leadership and config changes, triggering
 * partition state transitions (LOG_TRUNCATION, MIRRORING, STOPPING, STOPPED) via the
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
    private volatile boolean initialized = false;
    private final NodeToControllerChannelManager channelManager;
    private final Supplier<MirrorFetcherManager> mirrorFetcherManagerSupplier;
    private Optional<MirrorUtils.StateTransitioner> stateTransitioner = Optional.empty();
    private Optional<Consumer<String>> mirrorDeletionHandler = Optional.empty();
    private Optional<Function<MirrorRecordKey, Integer>> coordinatorPartitionFinder = Optional.empty();
    private Optional<Function<String, Integer>> coordinatorPartitionByNameFinder = Optional.empty();
    private final KafkaScheduler scheduler;

    private final Map<String, Admin> srcAdmins = new ConcurrentHashMap<>();
    private volatile Admin dstAdmin;

    private final Map<String, Uuid> sourceClusterIds = new ConcurrentHashMap<>();
    private final Map<String, List<MirrorSourceSender>> sourceSenders = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, Node>> sourceLeaders = new ConcurrentHashMap<>();
    private final Map<MirrorUtils.PartitionKey, MirrorPartitionState> partitionStates = new ConcurrentHashMap<>();
    private final Map<MirrorUtils.PartitionKey, MirrorPartitionState> partitionPreviousStates = new ConcurrentHashMap<>();
    private final Map<MirrorPartitionState, AtomicLong> partitionStateCounts = new ConcurrentHashMap<>();
    private final Map<MirrorUtils.PartitionKey, Integer> lastMirrorEpochs = new ConcurrentHashMap<>();

    // Leader epoch bumps require a request to the controller followed by a metadata log fetch.
    // The bump must be confirmed on the broker side before we can write the PID reset record.
    private final Set<MirrorUtils.LeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();

    // lets the transition handler skip partitions that are already being processed
    private final Map<TopicPartition, MirrorPartitionState> pendingPartitionStates = new ConcurrentHashMap<>();
    private final Map<TopicPartition, MirrorPartitionState> prevStateBeforeFailure = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Integer> failedRetryAttempts = new ConcurrentHashMap<>();
    private final Set<Uuid> pendingTopicCreations = ConcurrentHashMap.newKeySet();

    // metrics
    private KafkaMetricsGroup metricsGroup;
    private AtomicLong metadataRefreshError;
    private AtomicLong topicConfigSyncError;
    private AtomicLong consumerGroupOffsetSyncError;
    private AtomicLong shareGroupOffsetSyncError;
    private AtomicLong aclSyncError;

    public MirrorMetadataManager(
        KafkaConfig brokerConfig,
        Metrics metrics,
        Time time,
        MetadataCache metadataCache,
        NodeToControllerChannelManager channelManager,
        Supplier<MirrorFetcherManager> mirrorFetcherManagerSupplier,
        KafkaScheduler scheduler
    ) {
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        this.metrics = metrics;
        this.time = time;
        this.random = new Random();
        this.channelManager = channelManager;
        this.mirrorFetcherManagerSupplier = mirrorFetcherManagerSupplier;
        this.metadataImage = MetadataImage.EMPTY;
        this.metadataCache = metadataCache;
        this.scheduler = scheduler;

        this.metricsGroup = new KafkaMetricsGroup(this.getClass());
        this.metadataRefreshError = new AtomicLong();
        this.topicConfigSyncError = new AtomicLong();
        this.consumerGroupOffsetSyncError = new AtomicLong();
        this.shareGroupOffsetSyncError = new AtomicLong();
        this.aclSyncError = new AtomicLong();

        metricsGroup.newGauge("TopicConfigSyncError", topicConfigSyncError::get);
        metricsGroup.newGauge("ConsumerGroupOffsetSyncError", consumerGroupOffsetSyncError::get);
        metricsGroup.newGauge("ShareGroupOffsetSyncError", shareGroupOffsetSyncError::get);
        metricsGroup.newGauge("AclSyncError", aclSyncError::get);
        metricsGroup.newGauge("TopicMetadataRefreshError", metadataRefreshError::get);
        metricsGroup.newGauge("LogTruncationPartitionState", () -> partitionStateCount(MirrorPartitionState.LOG_TRUNCATION));
        metricsGroup.newGauge("EpochFencingPartitionState", () -> partitionStateCount(MirrorPartitionState.EPOCH_FENCING));
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
        if (!initialized) {
            return;
        }
        // caching the image for query purpose
        this.metadataImage = newImage;

        // clear pending topic creations for topics that now exist
        if (delta.topicsDelta() != null) {
            delta.topicsDelta().createdTopicIds().forEach(pendingTopicCreations::remove);
        }

        maybeRecreateConnection(delta, newImage);

        // get all mirror partition leaders on this node based on the delta
        Set<TopicPartition> mirrorLeaders = getMirrorLeadersAndClearFollowerStates(delta, newImage);
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
            boolean stopRequested = rawMirrorName.endsWith(STOPPED_TOPIC_SUFFIX);
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
                        // treat unrecorded state (-1) as UNKNOWN so the partition can transition to LOG_TRUNCATION
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
        maybeCompletePendingEpochBumps();
    }

    @Override
    public void close() throws Exception {
        if (mirrorStateSender != null) {
            mirrorStateSender.shutdown();
        }
        srcAdmins.values().forEach(Admin::close);
        if (dstAdmin != null) {
            dstAdmin.close();
        }
        closeSourceSenders();
    }

    public Map<TopicPartition, MirrorPartitionState> pendingPartitionStates() {
        return pendingPartitionStates;
    }

    public Map<TopicPartition, MirrorPartitionState> prevStateBeforeFailure() {
        return prevStateBeforeFailure;
    }

    public Map<TopicPartition, Integer> failedRetryAttempts() {
        return failedRetryAttempts;
    }

    public Map<MirrorUtils.PartitionKey, MirrorPartitionState> partitionPreviousStates() {
        return partitionPreviousStates;
    }

    public void initialize(MirrorUtils.StateTransitioner stateTransitioner,
                           Consumer<String> tombStoneHandler,
                           Function<MirrorRecordKey, Integer> coordinatorPartitionByKeyFinder,
                           Function<String, Integer> coordinatorPartitionByNameFinder) {
        if (mirrorStateSender == null) {
            mirrorStateSender = new MirrorStateSender(MirrorStateSender.class.getSimpleName(),
                    NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                    brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            mirrorStateSender.start();
        }

        this.stateTransitioner = Optional.of(stateTransitioner);
        this.mirrorDeletionHandler = Optional.of(tombStoneHandler);
        this.coordinatorPartitionFinder = Optional.of(coordinatorPartitionByKeyFinder);
        this.coordinatorPartitionByNameFinder = Optional.of(coordinatorPartitionByNameFinder);

        initialized = true;
    }

    public void transitionTo(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        stateTransitioner.ifPresent(st -> st.transitionTo(mirrorName, topicPartition, state));
    }

    private static final Set<String> NON_CONNECTION_CONFIGS = Set.of(
            MirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, MirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG,
            MirrorConfig.MIRROR_GROUPS_INCLUDE_CONFIG, MirrorConfig.MIRROR_GROUPS_EXCLUDE_CONFIG,
            MirrorConfig.MIRROR_ACL_INCLUDE_CONFIG);

    private void maybeRecreateConnection(MetadataDelta delta, MetadataImage newImage) {
        if (delta.configsDelta() != null) {
            delta.configsDelta().changes().entrySet().stream()
                .filter(e -> e.getKey().type() == ConfigResource.Type.MIRROR)
                .forEach(e -> {
                    String mirrorName = e.getKey().name();
                    boolean mirrorDeleted = newImage.configs()
                            .configProperties(e.getKey()).isEmpty();
                    if (mirrorDeleted) {
                        log.info("Mirror '{}' has been deleted. Writing tombstone records.", mirrorName);
                        mirrorDeletionHandler.ifPresent(h -> h.accept(mirrorName));
                    }

                    boolean connectionConfigChanged = e.getValue().changes().keySet().stream()
                            .anyMatch(key -> !NON_CONNECTION_CONFIGS.contains(key));

                    if (connectionConfigChanged || mirrorDeleted) {
                        sourceLeaders.remove(mirrorName);
                        List<MirrorSourceSender> senders = sourceSenders.remove(mirrorName);
                        if (senders != null) {
                            log.info("Mirror config changed for '{}'. Closing existing connections "
                                    + "to trigger reconnection with updated configuration.", mirrorName);
                            senders.forEach(MirrorSourceSender::close);
                        }
                        mirrorFetcherManagerSupplier.get().removeFetchersForMirror(mirrorName);
                    }
                });
        }
    }

    /** Returns mirror partitions led by this broker, detecting both leadership and config changes */
    private Set<TopicPartition> getMirrorLeadersAndClearFollowerStates(MetadataDelta delta, MetadataImage image) {
        Set<TopicPartition> mirrorLeaderPartitions = new HashSet<>();

        if (delta.topicsDelta() != null) {
            // new partition leader in topicsDelta that has mirror.name not empty
            delta.topicsDelta().localChanges(nodeId).leaders().keySet().forEach(tp -> {
                Properties props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()));
                if (props.containsKey(TopicConfig.MIRROR_NAME_CONFIG)) {
                    String mirrorName = (String) props.get(TopicConfig.MIRROR_NAME_CONFIG);
                    if (mirrorName != null && !mirrorName.isBlank()) {
                        mirrorLeaderPartitions.add(tp);
                    }
                }
            });

            // remove the pending state from this node because it is not the leader anymore
            delta.topicsDelta().localChanges(nodeId).followers().keySet().forEach(tp -> {
                Properties props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()));
                if (props.containsKey(TopicConfig.MIRROR_NAME_CONFIG)) {
                    String mirrorName = (String) props.get(TopicConfig.MIRROR_NAME_CONFIG);
                    if (mirrorName != null && !mirrorName.isBlank()) {
                        pendingPartitionStates.remove(tp);
                        pendingLeaderEpochBumps.removeIf(bumpLeaderEpoch -> {
                            if (bumpLeaderEpoch.partitionToEpoch().containsKey(tp)) {
                                bumpLeaderEpoch.future().completeExceptionally(
                                        new IllegalStateException("Not leader anymore for " + tp));
                                return true;
                            }
                            return false;
                        });
                    }
                }
            });
        }

        // the config change in configsDelta contains the mirror.name setting from empty to non-empty
        if (delta.configsDelta() != null) {
            // get all resources containing the non-empty mirror name change
            Map<ConfigResource, ConfigurationDelta> mirrorNameChanged = delta.configsDelta().changes().entrySet().stream().filter(entry ->
                            entry.getValue().changes().containsKey(TopicConfig.MIRROR_NAME_CONFIG) &&
                                    !entry.getValue().changes().get(TopicConfig.MIRROR_NAME_CONFIG).isEmpty()
                                    && !entry.getValue().changes().get(TopicConfig.MIRROR_NAME_CONFIG).get().isBlank())
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
     * stopRequested: it means the partition should head to STOPPED state. When it is true (i.e. users stopMirrorTopics):
     *   1. if it's already in STOPPED state, then keep the state
     *   2. else, move the state to STOPPING state
     * pauseRequested: it means the partition should head to PAUSED state. When it is true (i.e. users pause it):
     *   1. if it's already in PAUSED state, then keep the state
     *   2. else, move the state to PAUSING state
     * When stopRequested=false and pauseRequested=false:
     *   1. if it's in PAUSED state, we should move it to MIRRORING state. It will happen when users resume mirroring
     *   2. if it's in UNKNOWN or STOPPED state, we should move it to LOG_TRUNCATION state. It will happen when users startMirrorTopics.
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
                t.transitionTo(mirrorName, tp, MirrorPartitionState.LOG_TRUNCATION);
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
                lastMirrorEpochs.remove(key);
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

    /** Writes partition states to remote coordinators, batching requests per coordinator node. */
    void writeStatesToRemoteCoordinator(String mirrorName,
                                        Map<String, Set<MirrorUtils.PartitionStateInfo>> topicMetadata,
                                        Set<String> stoppedTopics,
                                        Consumer<WriteMirrorStatesResponse> callback) {
        log.debug("Writing states to remote coordinator: {} {} {}", mirrorName, topicMetadata, stoppedTopics);

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
                partitionData.setLastMirrorEpoch(m.leaderEpoch());
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
            data.setStoppedTopics(new ArrayList<>(stoppedTopics));

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
     * Updates local cache (lastMirrorEpochs, partitionStates) with each response.
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
                                MirrorUtils.PartitionKey mpk = new MirrorUtils.PartitionKey(
                                        mirrorName, topic.name(), partition.partitionIndex());
                                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                                if (partition.lastMirrorEpoch() != -1) {
                                    lastMirrorEpochs.put(mpk, partition.lastMirrorEpoch());
                                }
                                if (partition.state() != -1) {
                                    partitionStates.put(mpk, MirrorPartitionState.fromValue(partition.state()));
                                }
                                partitionPreviousStates.put(mpk, MirrorPartitionState.fromValue(partition.previousState()));
                                failedRetryAttempts.put(tp, (int) partition.retryAttempt());
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
                MirrorUtils.PartitionKey pk = new MirrorUtils.PartitionKey(mirrorName, tp, part);
                ReadMirrorStatesResponseData.PartitionResult partitionResult = new ReadMirrorStatesResponseData.PartitionResult();
                partitionResult.setPartitionIndex(part);
                partitionResult.setLastMirrorEpoch(lastMirrorEpochs.getOrDefault(pk, -1));
                partitionResult.setState(partitionStates.getOrDefault(pk, MirrorPartitionState.UNKNOWN).value());
                partitionResult.setPreviousState(
                        partitionPreviousStates.getOrDefault(pk, MirrorPartitionState.UNKNOWN).value());
                partitionResult.setRetryAttempt(
                        failedRetryAttempts.getOrDefault(new TopicPartition(tp, part), 0).shortValue());
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

    /** Creates Admin clients for source mirrors and the local destination, if not already initialized. */
    private void ensureAdminClients(Set<String> mirrors) {
        for (String mirrorName : mirrors) {
            srcAdmins.computeIfAbsent(mirrorName, k -> {
                Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, k));
                return Admin.create(props);
            });
        }
        if (dstAdmin == null) {
            var endpoint = brokerConfig.effectiveAdvertisedBrokerListeners().head();
            Properties props = new Properties();
            props.put(BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
            dstAdmin = Admin.create(props);
        }
    }

    /** Sends a request to the source cluster, iterating available senders with fallback on failure. */
    private ClientResponse trySendSourceClusterRequest(String mirrorName, AbstractRequest.Builder<?> requestBuilder) {
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
    void removePartitionState(MirrorUtils.PartitionKey key) {
        partitionStates.computeIfPresent(key, (k, oldState) -> {
            partitionStateCounts.computeIfAbsent(oldState, s -> new AtomicLong()).decrementAndGet();
            return null;
        });
        partitionPreviousStates.remove(key);
    }

    void removeLastMirrorEpochs(String mirrorName) {
        lastMirrorEpochs.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
    }

    private long partitionStateCount(MirrorPartitionState state) {
        return partitionStateCounts.computeIfAbsent(state, s -> new AtomicLong()).get();
    }

    /** Strips STOPPED_TOPIC_SUFFIX before lookup. */
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

    Map<TopicPartition, Integer> getLastMirrorEpochs(String clusterName) {
        Map<TopicPartition, Integer> result = new HashMap<>();
        lastMirrorEpochs.forEach((key, epoch) -> {
            if (key.mirrorName().equals(clusterName)) {
                result.put(new TopicPartition(key.topic(), key.partition()), epoch);
            }
        });
        return result;
    }

    Map<MirrorUtils.PartitionKey, Integer> updateLastMirrorEpochs(String clusterName,
                                                                  Map<String, Map<Integer, Integer>> addedEpochs,
                                                                  Map<String, Map<Integer, Integer>> stoppedEpochs) {
        stoppedEpochs.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirrorEpochs.remove(new MirrorUtils.PartitionKey(clusterName, topic, partition));
            });
        });
        addedEpochs.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirrorEpochs.put(new MirrorUtils.PartitionKey(clusterName, topic, partition), offset);
            });
        });
        return lastMirrorEpochs;
    }

    /** Truncates local replicas using last mirrored leader epochs from this broker's coordinator cache. */
    CompletionStage<Map<String, MirrorDescription>> truncateToLastMirrorEpochs(String mirrorName,
                                                                               Set<TopicPartition> topicPartitionSet) {
        log.info("Truncating to last mirrored epochs from local state for mirror {}: {}", mirrorName, topicPartitionSet);
        Admin admin = srcAdmins.computeIfAbsent(mirrorName, k -> {
            Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, k));
            return Admin.create(props);
        });
        DescribeMirrorsResult result = admin.describeMirrors(List.of(mirrorName));
        return result.allDescriptions().toCompletionStage();
    }

    /** Periodic sync entry point. */
    void periodicSync() {
        Set<String> mirrors = getConfiguredMirrors();
        if (mirrors.isEmpty()) {
            return;
        }

        log.info("Syncing metadata for mirrors: {}", mirrors);
        mirrors.forEach(this::ensureConnection);
        ensureAdminClients(mirrors);

        // snapshot keyset to avoid ConcurrentModificationException
        for (String mirrorName : Set.copyOf(sourceSenders.keySet())) {
            try {
                discoverSourceBrokers(mirrorName);
                log.info("Syncing source topic state for mirror {}", mirrorName);
                var metadataResponse = syncSourceTopicState(mirrorName);
                log.info("Syncing coordinator metadata for mirror {}", mirrorName);
                syncCoordinatorMetadata(mirrorName, metadataResponse);
            } catch (Exception e) {
                log.error("Failed to sync metadata for mirror {}", mirrorName, e);
            }
        }

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        metadataRefreshError.incrementAndGet();
    }

    public CompletableFuture<Void> scheduleBumpLeaderEpochs(String mirrorName, Set<TopicPartition> topicPartitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.scheduleOnce("bump-leader-epoch", () -> {
            ensureConnection(mirrorName);
            discoverSourceBrokers(mirrorName);
            Optional<MetadataResponse> metadataResponse = syncSourceTopicState(mirrorName);
            maybeBumpLeaderEpochs(mirrorName, metadataResponse, topicPartitions)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            future.completeExceptionally(ex);
                        } else {
                            future.complete(null);
                        }
                    });
        });
        return future;
    }

    private CompletableFuture<Void> maybeBumpLeaderEpochs(String mirrorName, Optional<MetadataResponse> metadataResponse, Set<TopicPartition> topicPartitions) {
        if (metadataResponse.isPresent()) {
            return sendBumpLeaderEpochs(buildSourceEpochBumpTargets(mirrorName, metadataResponse.get(), topicPartitions))
                    .whenComplete((v, ex) -> {
                        if (ex != null) log.warn("Failed to bump leader epoch for mirror {}", mirrorName, ex);
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendBumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        if (partitionMinEpochs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.info("Sending bump leader epoch request: {}", partitionMinEpochs);
        CompletableFuture<Void> future = new CompletableFuture<>();

        List<BumpLeaderEpochsRequestData.TopicState> topicStates = new ArrayList<>();
        Map<String, Set<Integer>> partitions = new HashMap<>();
        partitionMinEpochs.keySet().forEach(tp -> {
            partitions.computeIfAbsent(tp.topic(), key -> new HashSet<>()).add(tp.partition());
        });
        partitions.forEach((topic, parts) -> {
            BumpLeaderEpochsRequestData.TopicState topicState = new BumpLeaderEpochsRequestData.TopicState();
            List<BumpLeaderEpochsRequestData.LeaderEpochState> topicLeaderEpoch = new ArrayList<>();
            parts.forEach(partitionId -> {
                TopicPartition tp = new TopicPartition(topic, partitionId);
                topicLeaderEpoch.add(new BumpLeaderEpochsRequestData.LeaderEpochState().setMinLeaderEpoch(partitionMinEpochs.get(tp)).setPartitionIndex(partitionId));
            });
            topicState.setTopicId(metadataCache.getTopicId(topic)).setPartitions(topicLeaderEpoch);
            topicStates.add(topicState);
        });

        pendingLeaderEpochBumps.add(new MirrorUtils.LeaderEpochBump(future, Collections.unmodifiableMap(partitionMinEpochs)));

        channelManager.sendRequest(new BumpLeaderEpochsRequest.Builder(
                new BumpLeaderEpochsRequestData().setTopics(topicStates)
        ), new ControllerRequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                log.debug("Bump leader epoch response: {}", response);
            }

            @Override
            public void onTimeout() {
                log.warn("BumpLeaderEpoch request timed out");
            }
        });
        return future;
    }

    private Map<TopicPartition, Integer> buildSourceEpochBumpTargets(String mirrorName, MetadataResponse metadataResponse, Set<TopicPartition> topicPartitions) {
        Set<String> mirrorTopics = new HashSet<>();
        if (topicPartitions.isEmpty()) {
            mirrorTopics = getConfiguredTopics(mirrorName);
        }
        Map<TopicPartition, Integer> leaderEpochFromMetadata = new HashMap<>();
        for (MetadataResponse.TopicMetadata topicMetadata : metadataResponse.topicMetadata()) {
            if (topicMetadata.error() != Errors.NONE) {
                continue;
            }
            if (!mirrorTopics.isEmpty() && !mirrorTopics.contains(topicMetadata.topic())) {
                continue;
            }
            for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
                TopicPartition tp = partitionMetadata.topicPartition;
                if (!topicPartitions.isEmpty() && !topicPartitions.contains(tp)) {
                    continue;
                }
                if (partitionMetadata.leaderEpoch.isEmpty()) {
                    continue;
                }
                if (metadataImage.topics().getTopic(tp.topic()) == null ||
                        metadataImage.topics().getTopic(tp.topic()).partitions().get(tp.partition()) == null) {
                    continue;
                }
                int epoch = partitionMetadata.leaderEpoch.get();
                int localEpoch = metadataImage.topics().getTopic(tp.topic()).partitions().get(tp.partition()).leaderEpoch;
                if (epoch > localEpoch - LEADER_EPOCH_BUMP_THRESHOLD) {
                    // will throw exception when overflow, but this should not happen
                    int newEpoch = Math.addExact(epoch, LEADER_EPOCH_BUMP_INCREMENT);
                    leaderEpochFromMetadata.put(tp, newEpoch);
                }
            }
        }
        log.info("Bumping leader epoch for partitions {} to {}", topicPartitions, leaderEpochFromMetadata);
        return leaderEpochFromMetadata;
    }

    public Map<TopicPartition, Integer> buildLocalEpochBumpTargets(LogManager logManager, Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Integer> partitionMinEpochs = new HashMap<>();
        topicPartitions.forEach(tp -> {
            int epoch = logManager.getLog(tp, false).get().latestEpoch().orElse(-1);
            partitionMinEpochs.put(tp, epoch);
        });
        return partitionMinEpochs;
    }

    private void maybeCompletePendingEpochBumps() {
        pendingLeaderEpochBumps.removeIf(bumpLeaderEpoch -> {
            Set<TopicPartition> pendingPartitions = bumpLeaderEpoch.partitionToEpoch().entrySet().stream().filter(entry -> {
                TopicPartition tp = entry.getKey();
                int epoch = entry.getValue();
                var topicImage = metadataImage.topics().getTopic(tp.topic());
                if (topicImage == null) return false;
                var partitionReg = topicImage.partitions().get(tp.partition());
                if (partitionReg == null) return false;
                return partitionReg.leaderEpoch <= epoch;
            }).map(Map.Entry::getKey).collect(Collectors.toSet());
            if (pendingPartitions.isEmpty()) {
                bumpLeaderEpoch.future().complete(null);
                return true;
            } else {
                log.info("bumpLeaderEpoch future is pending for partitions: {}, all: {}", pendingPartitions, bumpLeaderEpoch.partitionToEpoch().keySet());
                return false;
            }
        });
    }

    /**
     * Discovers source cluster brokers via metadata and adds senders for any newly found brokers.
     * Sender cleanup only happens via {@link #onMetadataUpdate} or {@link #clear}.
     */
    private void discoverSourceBrokers(String mirrorName) {
        var response = trySendSourceClusterRequest(mirrorName, MetadataRequest.Builder.allTopics());
        if (!(response.responseBody() instanceof MetadataResponse metadataResponse)) {
            return;
        }

        // Cross-cluster identity validation
        String clusterId = metadataResponse.clusterId();
        if (clusterId != null && !clusterId.isEmpty()) {
            Uuid newClusterId = Uuid.fromString(clusterId);
            Uuid previousClusterId = sourceClusterIds.put(mirrorName, newClusterId);
            if (previousClusterId != null && !previousClusterId.equals(newClusterId)) {
                throw new IllegalStateException("Source cluster ID changed for mirror " + mirrorName
                    + ": expected " + previousClusterId + ", got " + newClusterId
                    + ". This may indicate a misconfiguration or that the source cluster has been replaced.");
            }
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

    /**
     * Fetches topic metadata from the source cluster.
     * Runs on every broker to keep partition leaders, topic creation, deletion, and partition counts in sync.
     */
    private Optional<MetadataResponse> syncSourceTopicState(String mirrorName) {
        Set<String> topics = getConfiguredTopics(mirrorName);
        if (topics.isEmpty()) {
            return Optional.empty();
        }
        var response = trySendSourceClusterRequest(mirrorName,
                MetadataRequest.Builder.forTopicNames(topics.stream().toList(), false)
        );

        if (response.responseBody() instanceof MetadataResponse metadataResponse) {
            log.debug("Periodic metadata response: {}", metadataResponse);
            Map<Integer, Node> brokerNodes = new HashMap<>();
            metadataResponse.brokers().forEach(broker -> brokerNodes.put(broker.id(), broker));
            var createPartitionsTopics = processTopicMetadata(mirrorName, metadataResponse.topicMetadata(), brokerNodes);
            maybeStopDeletedTopics(mirrorName, metadataResponse.topicMetadata());
            handlePartitionScaling(createPartitionsTopics);
            return Optional.of(metadataResponse);
        }
        return Optional.empty();
    }

    /**
     * Creates a mirror topic on the destination with the source's TopicId,
     * preserving topic identity across clusters. Called during periodic metadata
     * sync when a topic has mirror.name config but doesn't exist on the destination yet.
     * Once created, onMetadataUpdate will detect it and start the mirror state machine.
     */
    private void createMirrorTopic(String topicName, org.apache.kafka.common.Uuid topicId, int numPartitions) {
        if (!pendingTopicCreations.add(topicId)) {
            log.debug("Skipping creation of mirror topic {} (topicId={}), request already in-flight", topicName, topicId);
            return;
        }
        log.info("Creating mirror topic {} on destination (partitions={}, topicId={})",
                topicName, numPartitions, topicId);
        var creatableTopic = new CreateTopicsRequestData.CreatableTopic()
                .setName(topicName)
                .setNumPartitions(numPartitions)
                .setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR)
                .setMirrorInfo(new CreateTopicsRequestData.MirrorInfo().setTopicId(topicId));
        var createTopicsData = new CreateTopicsRequestData().setTimeoutMs(brokerConfig.requestTimeoutMs());
        createTopicsData.topics().add(creatableTopic);
        ControllerRequestCompletionHandler requestCompletionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                pendingTopicCreations.remove(topicId);
                log.warn("Create mirror topic timed out for {} (topicId={})", topicName, topicId);
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.responseBody() instanceof CreateTopicsResponse createTopicsResponse) {
                    createTopicsResponse.data().topics().forEach(topic -> {
                        Errors error = Errors.forCode(topic.errorCode());
                        if (error != Errors.NONE) {
                            pendingTopicCreations.remove(topicId);
                            log.warn("Failed to create mirror topic {} (topicId={}): {}", topicName, topicId, error.message());
                        }
                    });
                }
            }
        };
        channelManager.sendRequest(
                new CreateTopicsRequest.Builder(createTopicsData),
                requestCompletionHandler);
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
            } else if (metadataImage.topics().getTopic(tm.topicId()) == null &&
                    metadataImage.topics().getTopic(tm.topic()) == null &&
                    tm.error() == Errors.NONE && sourcePartitionCount > 0) {
                // create topic on destination using cluster default replication factor
                this.createMirrorTopic(tm.topic(), tm.topicId(), sourcePartitionCount);
            } else if (metadataImage.topics().getTopic(tm.topicId()) == null &&
                    metadataImage.topics().getTopic(tm.topic()) != null &&
                    tm.error() == Errors.NONE) {
                log.error("Mirror topic {} exists on destination with TopicId {} but source has TopicId {}. "
                        + "Delete the topic on destination and let auto-creation recreate it with the correct TopicId.",
                        tm.topic(), metadataImage.topics().getTopic(tm.topic()).id(), tm.topicId());
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
     * Syncs configurations, consumer group offsets, ACLs, and topic patterns from source clusters.
     * Runs only on the coordinator broker for each mirror.
     */
    void syncCoordinatorMetadata(String mirrorName, Optional<MetadataResponse> metadataResponse) {
        if (!isLocalCoordinator(mirrorName)) {
            return;
        }

        ensureConnection(mirrorName);

        try {
            MirrorConfig mirrorConfig = MirrorConfig.fromProperties(
                    metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName)));
            syncTopicConfigurations(mirrorName, mirrorConfig);
            syncGroupOffsets(mirrorName, mirrorConfig);
            syncAccessControlLists(mirrorName, mirrorConfig);
            maybeBumpLeaderEpochs(mirrorName, metadataResponse, Set.of());
            discoverTopicsByPattern(mirrorName, mirrorConfig);
            enforceExcludePatterns(mirrorName, mirrorConfig);
        } catch (Exception e) {
            log.error("Failed to sync mirror metadata for mirror {}", mirrorName, e);
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

        var describeConfigResponse = trySendSourceClusterRequest(mirrorName, describeConfigsRequest);
        if (describeConfigResponse.responseBody() instanceof DescribeConfigsResponse describeConfigsRes) {
            log.debug("Periodic describe config response: {}", describeConfigsRes);
            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(describeConfigsRes, mirrorConfig);
            applyConfigurationChanges(configsToChange);
        }
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
                            && (excludePattern == null || !excludePattern.matcher(con.name()).matches())) {
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

    /**
     * Syncs group offsets from the source cluster to the destination in two phases:
     * consumer groups first, then share groups. Keeping them separate avoids cross-type
     * conflicts where a consumer group on the source could overwrite a share group with
     * the same name on the destination (or vice versa).
     */
    private void syncGroupOffsets(String mirrorName, MirrorConfig mirrorConfig) {
        Admin srcAdmin = srcAdmins.get(mirrorName);
        if (srcAdmin == null || dstAdmin == null) {
            log.error("Admin clients not initialized for mirror {}, skipping offset sync", mirrorName);
            return;
        }

        Set<String> mirrorTopics = getConfiguredTopics(mirrorName);
        if (mirrorTopics.isEmpty()) {
            return;
        }

        Pattern groupsIncludePattern = mirrorConfig.groupsIncludePattern();
        Pattern groupsExcludePattern = mirrorConfig.groupsExcludePattern();

        syncConsumerGroupOffsets(srcAdmin, mirrorName, mirrorTopics, groupsIncludePattern, groupsExcludePattern);
        syncShareGroupOffsets(srcAdmin, mirrorName, mirrorTopics, groupsIncludePattern, groupsExcludePattern);
    }

    private void syncConsumerGroupOffsets(Admin srcAdmin, String mirrorName, Set<String> mirrorTopics,
                                          Pattern groupsIncludePattern, Pattern groupsExcludePattern) {
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        consumerGroupOffsetSyncError.incrementAndGet();
        try {
            List<String> sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forConsumerGroups(),
                    groupsIncludePattern, groupsExcludePattern);
            if (sourceGroupIds.isEmpty()) {
                return;
            }
            log.info("Syncing consumer group offsets for mirror {}, groups={}", mirrorName, sourceGroupIds);

            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = sourceGroupIds.stream()
                    .collect(Collectors.toMap(id -> id, id -> new ListConsumerGroupOffsetsSpec()));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets = srcAdmin
                    .listConsumerGroupOffsets(groupSpecs).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            Optional<Set<String>> activeDestGroups = getNonSyncableDestinationGroupIds(ListGroupsOptions.forConsumerGroups());
            if (activeDestGroups.isEmpty()) {
                return;
            }

            for (var entry : allOffsets.entrySet()) {
                String groupId = entry.getKey();
                if (activeDestGroups.get().contains(groupId)) {
                    log.debug("Skipping consumer group offset sync for group {} in mirror {}: active on destination", groupId, mirrorName);
                    continue;
                }

                Map<TopicPartition, OffsetAndMetadata> filtered = entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (filtered.isEmpty()) {
                    continue;
                }

                try {
                    log.debug("Committing consumer group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    dstAdmin.alterConsumerGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.warn("Failed to commit consumer group offsets for group {} in mirror {}: {}", groupId, mirrorName, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to sync consumer group offsets for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    private void syncShareGroupOffsets(Admin srcAdmin, String mirrorName, Set<String> mirrorTopics,
                                       Pattern groupsIncludePattern, Pattern groupsExcludePattern) {
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        shareGroupOffsetSyncError.incrementAndGet();
        try {
            List<String> sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forShareGroups(),
                    groupsIncludePattern, groupsExcludePattern);
            if (sourceGroupIds.isEmpty()) {
                return;
            }
            log.info("Syncing share group offsets for mirror {}, groups={}", mirrorName, sourceGroupIds);

            Map<String, ListShareGroupOffsetsSpec> groupSpecs = sourceGroupIds.stream()
                    .collect(Collectors.toMap(id -> id, id -> new ListShareGroupOffsetsSpec()));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets = srcAdmin
                    .listShareGroupOffsets(groupSpecs).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            Optional<Set<String>> activeDestGroups = getNonSyncableDestinationGroupIds(ListGroupsOptions.forShareGroups());
            if (activeDestGroups.isEmpty()) {
                return;
            }

            for (var entry : allOffsets.entrySet()) {
                String groupId = entry.getKey();
                if (activeDestGroups.get().contains(groupId)) {
                    log.debug("Skipping share group offset sync for group {} in mirror {}: active on destination", groupId, mirrorName);
                    continue;
                }

                Map<TopicPartition, Long> filtered = entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
                if (filtered.isEmpty()) {
                    continue;
                }

                try {
                    log.debug("Committing share group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    dstAdmin.alterShareGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.warn("Failed to commit share group offsets for group {} in mirror {}: {}", groupId, mirrorName, e);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to sync share group offsets for mirror {}: {}", mirrorName, e);
        }
    }

    private List<String> listSourceGroupIds(Admin srcAdmin, ListGroupsOptions options,
                                            Pattern groupsIncludePattern, Pattern groupsExcludePattern) throws Exception {
        return srcAdmin.listGroups(options).all()
                .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS).stream()
                .map(GroupListing::groupId)
                .filter(id -> groupsIncludePattern == null || groupsIncludePattern.matcher(id).matches())
                .filter(id -> groupsExcludePattern == null || !groupsExcludePattern.matcher(id).matches())
                .toList();
    }

    /** Returns empty Optional on failure so the caller can skip the sync cycle. */
    private Optional<Set<String>> getNonSyncableDestinationGroupIds(ListGroupsOptions typeFilter) {
        try {
            var options = typeFilter.inGroupStates(Set.of(
                    GroupState.STABLE,
                    GroupState.PREPARING_REBALANCE,
                    GroupState.COMPLETING_REBALANCE,
                    GroupState.ASSIGNING,
                    GroupState.RECONCILING,
                    GroupState.EMPTY));
            return Optional.of(dstAdmin.listGroups(options).all()
                    .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS).stream()
                    .map(GroupListing::groupId)
                    .collect(Collectors.toSet()));
        } catch (Exception e) {
            log.warn("Failed to list destination groups, skipping offset sync cycle: {}", e);
            return Optional.empty();
        }
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
        var describeAclsResponse = trySendSourceClusterRequest(mirrorName, describeAclsRequest);
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

        // collect remove acls list (skip CLUSTER_MIRROR ACLs as they are destination-specific)
        metadataImage.acls().acls().values().forEach(acl -> {
            if (acl.resourceType() != ResourceType.CLUSTER_MIRROR && !allRemoteAcls.contains(acl.toBinding())) {
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

    private void discoverTopicsByPattern(String mirrorName, MirrorConfig mirrorConfig) {
        final Pattern topicsIncludePattern = mirrorConfig.topicsIncludePattern();
        if (topicsIncludePattern == null) {
            return;
        }

        var response = trySendSourceClusterRequest(mirrorName, MetadataRequest.Builder.allTopics());
        if (!(response.responseBody() instanceof MetadataResponse metadataResponse)) {
            log.warn("Unexpected metadata response type from source cluster for topic discovery: {}", response);
            return;
        }

        Set<String> configuredTopics = getConfiguredTopics(mirrorName, true);
        final Pattern topicsExcludePattern = mirrorConfig.topicsExcludePattern();

        List<StartMirrorTopicsRequestData.TopicData> newTopics = metadataResponse.topicMetadata().stream()
                .filter(tm -> tm.error() == Errors.NONE)
                .filter(tm -> topicsIncludePattern.matcher(tm.topic()).matches())
                .filter(tm -> topicsExcludePattern == null || !topicsExcludePattern.matcher(tm.topic()).matches())
                .filter(tm -> !configuredTopics.contains(tm.topic()))
                .map(tm -> new StartMirrorTopicsRequestData.TopicData()
                        .setTopicName(tm.topic())
                        .setTopicId(tm.topicId())
                        .setNumPartitions(tm.partitionMetadata().size()))
                .toList();

        if (newTopics.isEmpty()) {
            return;
        }

        log.info("Discovered {} new topic(s) matching mirror.topics.include pattern for mirror {}: {}",
                newTopics.size(), mirrorName, newTopics.stream().map(StartMirrorTopicsRequestData.TopicData::topicName).toList());

        StartMirrorTopicsRequestData data = new StartMirrorTopicsRequestData();
        data.setMirrorName(mirrorName);
        newTopics.forEach(topic -> data.topics().add(topic));

        // TODO: creation failures from auto-discovery are silently lost here (fire-and-forget).
        //  Add per-topic status tracking so describeMirror can surface failed topics to users.
        channelManager.sendRequest(
                new StartMirrorTopicsRequest.Builder(data),
                new TimeoutHandler(log)
        );
    }

    /**
     * Checks if any active mirroring topics now match the exclude pattern and sends
     * StopMirrorTopicsRequest to stop them. Catches cases where exclude was updated
     * via incrementalAlterConfigs outside of the startMirrorTopics/stopMirrorTopics flow.
     */
    private void enforceExcludePatterns(String mirrorName, MirrorConfig mirrorConfig) {
        Pattern excludePattern = mirrorConfig.topicsExcludePattern();
        if (excludePattern == null) return;

        Set<String> activeTopics = getConfiguredTopics(mirrorName, false, false);
        Set<String> excludedTopics = activeTopics.stream()
                .filter(topic -> excludePattern.matcher(topic).matches())
                .collect(Collectors.toSet());

        if (excludedTopics.isEmpty()) return;

        log.info("Stopping {} topic(s) matching mirror.topics.exclude for mirror {}: {}",
                excludedTopics.size(), mirrorName, excludedTopics);

        channelManager.sendRequest(
                new StopMirrorTopicsRequest.Builder(mirrorName, excludedTopics),
                new TimeoutHandler(log)
        );
    }

    Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    /** Returns the set of topic names configured for the given mirror, excluding paused topics. */
    Set<String> getConfiguredTopics(String mirrorName) {
        return getConfiguredTopics(mirrorName, false, true);
    }

    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused) {
        return getConfiguredTopics(mirrorName, includePaused, true);
    }

    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
        return metadataImage.configs().resourceData().entrySet().stream()
                .filter(configEntry -> {
                    if (configEntry.getKey().type() != ConfigResource.Type.TOPIC) return false;
                    String topicMirrorName = configEntry.getValue().data().get(TopicConfig.MIRROR_NAME_CONFIG);
                    if (topicMirrorName == null) return false;
                    if (!includeStopped && topicMirrorName.endsWith(STOPPED_TOPIC_SUFFIX)) return false;
                    if (!includePaused && topicMirrorName.endsWith(PAUSED_TOPIC_SUFFIX)) return false;
                    return mirrorName.equals(MirrorUtils.originalMirrorName(topicMirrorName));
                })
                .map(configEntry -> configEntry.getKey().name())
                .collect(Collectors.toSet());
    }

    int getActiveTopicCount(String mirrorName) {
        return getConfiguredTopics(mirrorName, false, false).size();
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

    Uuid getSourceClusterId(String mirrorName) {
        Uuid cached = sourceClusterIds.get(mirrorName);
        if (cached != null) {
            return cached;
        }
        // cache miss: resolve by sending a synchronous metadata request
        ensureConnection(mirrorName);
        try {
            var response = trySendSourceClusterRequest(mirrorName, MetadataRequest.Builder.allTopics());
            if (response.responseBody() instanceof MetadataResponse metadataResponse) {
                String clusterId = metadataResponse.clusterId();
                if (clusterId != null && !clusterId.isEmpty()) {
                    cached = Uuid.fromString(clusterId);
                    sourceClusterIds.put(mirrorName, cached);
                    return cached;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to resolve source cluster ID for mirror {}", mirrorName, e);
        }
        return null;
    }

    void clear() {
        partitionStates.clear();
        partitionPreviousStates.clear();
        partitionStateCounts.clear();
        lastMirrorEpochs.clear();
        sourceClusterIds.clear();
        closeSourceSenders();
        sourceLeaders.clear();
        pendingLeaderEpochBumps.clear();
        pendingPartitionStates.clear();
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
