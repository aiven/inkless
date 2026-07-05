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
import kafka.server.mirror.ClusterMirrorUtils.FailedPartitionInfo;
import kafka.server.mirror.ClusterMirrorUtils.PartitionKey;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BumpLeaderEpochsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DescribeClusterMirrorsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BumpLeaderEpochsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteClusterMirrorRequest;
import org.apache.kafka.common.requests.DeleteClusterMirrorResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.StartMirrorTopicsRequest;
import org.apache.kafka.common.requests.StopMirrorTopicsRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordKey;
import org.apache.kafka.image.LocalReplicaChanges;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.MirrorPartitionState;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.ClusterMirrorConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MirrorFilterUtils;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.Option;

import static kafka.server.mirror.ClusterMirrorUtils.LEADER_EPOCH_BUMP_INCREMENT;
import static kafka.server.mirror.ClusterMirrorUtils.LEADER_EPOCH_BUMP_THRESHOLD;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Bridges the local destination cluster and remote source clusters for Cluster Mirroring.
 *
 * Implements {@link MetadataPublisher} to detect leadership and config changes, triggering
 * partition state transitions.
 * Periodically syncs topic metadata, configs, group offsets, and ACLs from source clusters using {@link Admin}.
 * Maintains in-memory caches of partition states and last mirror epochs. Routes state reads/writes
 * to the appropriate coordinator broker.
 *
 * Source topic metadata and broker discovery run on every broker so that all brokers keep an
 * up to date view of source partition leaders. Coordinator level sync (topic configs, group
 * offsets, ACLs, pattern discovery, and exclude enforcement) runs only on the broker that
 * leads the {@code __mirror_state} partition for a given mirror name.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataPublisher, AutoCloseable {
    private static final Set<String> NON_CONNECTION_CONFIGS = Set.of(
            ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_GROUPS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_GROUPS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_ACL_INCLUDE_CONFIG);

    private final Logger log;
    private volatile boolean isInitialized = false;
    private final String name;
    private final String clusterId;
    private final KafkaConfig brokerConfig;
    private final int nodeId;

    private final NodeToControllerChannelManager channelManager;
    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private final MetadataCache metadataCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;
    private final Time time;

    private Optional<ClusterMirrorUtils.StateTransitioner> stateTransitioner = Optional.empty();
    private Optional<Consumer<String>> tombstoneWriter = Optional.empty();
    private Optional<Function<ClusterMirrorRecordKey, Integer>> coordPartitionFinderByKey = Optional.empty();
    private Optional<Function<String, Integer>> coordPartitionFinderByName = Optional.empty();

    // Network communication
    private volatile MirrorStateSender mirrorStateSender; // raw WriteMirrorStates/ReadMirrorStates RPCs to coord brokers
    private final Map<String, Admin> srcAdmins = new ConcurrentHashMap<>(); // source cluster metadata discovery (one per mirror)
    private volatile Admin dstAdmin; // group offset and ACLs sync

    // Broker cache
    private final Map<String, Map<TopicPartition, ClusterMirrorUtils.LeaderInfo>> sourceLeaders = new ConcurrentHashMap<>();
    private final Map<PartitionKey, MirrorPartitionState> partitionStates = new ConcurrentHashMap<>();
    private final Map<PartitionKey, Integer> lastMirrorEpochs = new ConcurrentHashMap<>();
    private final Map<MirrorPartitionState, AtomicLong> partitionStateCounts = new ConcurrentHashMap<>();
    private final Map<TopicPartition, FailedPartitionInfo> failedPartitionInfo = new ConcurrentHashMap<>();
    private final Map<TopicPartition, MirrorPartitionState> pendingPartitionStates = new ConcurrentHashMap<>();
    private final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();
    private final Set<ClusterMirrorUtils.LeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();

    // Metrics
    private final AtomicLong metadataRefreshError;
    private final AtomicLong topicConfigSyncError;
    private final AtomicLong consumerGroupOffsetSyncError;
    private final AtomicLong shareGroupOffsetSyncError;
    private final AtomicLong aclSyncError;

    public MirrorMetadataManager(
        String clusterId,
        KafkaConfig brokerConfig,
        NodeToControllerChannelManager channelManager,
        Supplier<ReplicaManager> replicaManagerSupplier,
        MetadataCache metadataCache,
        KafkaScheduler scheduler,
        Metrics metrics,
        Time time
    ) {
        this.clusterId = clusterId;
        this.brokerConfig = brokerConfig;
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);
        this.nodeId = brokerConfig.nodeId();

        this.channelManager = channelManager;
        this.replicaManagerSupplier = replicaManagerSupplier;
        this.metadataCache = metadataCache;

        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;

        KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());
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

    private boolean isLocalCoordinator(String mirrorName) {
        if (coordPartitionFinderByName.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartitionFinderByName.get().apply(mirrorName)).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        if (coordPartitionFinderByKey.isPresent()) {
            int activeCoordinator = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartitionFinderByKey.get().apply(
                            new ClusterMirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), partition))).leader;
            return activeCoordinator == brokerConfig.nodeId();
        }
        return false;
    }

    /**
     * Called by ClusterMirrorCoordinator on startup.
     * Creates and starts the MirrorStateSender used for WriteMirrorStates and ReadMirrorStates RPCs.
     * Wires in the state transitioner, tombstone handler, and coordinator partition finders.
     */
    public void initialize(ClusterMirrorUtils.StateTransitioner stateTransitioner,
                           Consumer<String> tombStoneHandler,
                           Function<ClusterMirrorRecordKey, Integer> coordPartitionByKeyFinder,
                           Function<String, Integer> coordPartitionByNameFinder) {
        if (mirrorStateSender == null) {
            mirrorStateSender = new MirrorStateSender(MirrorStateSender.class.getSimpleName(),
                    NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                    brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            mirrorStateSender.start();
        }

        this.stateTransitioner = Optional.of(stateTransitioner);
        this.tombstoneWriter = Optional.of(tombStoneHandler);
        this.coordPartitionFinderByKey = Optional.of(coordPartitionByKeyFinder);
        this.coordPartitionFinderByName = Optional.of(coordPartitionByNameFinder);

        this.isInitialized = true;
    }

    private Admin getOrCreateSourceAdmin(String mirrorName) {
        return srcAdmins.computeIfAbsent(mirrorName, k -> {
            Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, k));
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-src-admin-" + k + "-" + nodeId);
            return Admin.create(props);
        });
    }

    private Admin getOrCreateDestAdmin() {
        if (dstAdmin == null) {
            Properties props = buildDestAdminClientProps(brokerConfig);
            dstAdmin = Admin.create(props);
        }
        return dstAdmin;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Called when cluster metadata is updated and it is executed in the KRaft metadata publisher thread.
     * Detects mirror partition leadership changes and triggers state transitions via batched coordinator reads.
     * On connection config changes, source connections are recreated, source metadata is fetched eagerly,
     * and fetchers are re-created for affected MIRRORING partitions.
     *
     * This method must be called after ReplicaManager#applyDelta.
     * The metadataCache can't be used here because it is updated concurrently.
     */
    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (!isInitialized) {
            return;
        }

        this.metadataImage = newImage;

        Set<String> reconnectedMirrors = maybeRecreateSourceConnection(delta, newImage);
        Set<TopicPartition> mirrorLeaders = collectMirrorLeaderChanges(delta, newImage, reconnectedMirrors);

        if (mirrorLeaders.isEmpty()) {
            return;
        }

        log.info("Processing metadata update for {} mirror leader partition(s): {}", mirrorLeaders.size(), mirrorLeaders);

        processStateTransitions(mirrorLeaders, newImage);
        maybeCompletePendingEpochBumps();
    }

    /**
     * Handles mirror config changes from the metadata delta. When a connection config changes
     * (e.g. bootstrap.servers), closes the old source AdminClient, removes fetchers, and eagerly
     * fetches source metadata so fetchers can be recreated immediately. When a mirror is deleted,
     * writes tombstone records and cleans up all cached state.
     *
     * @return names of mirrors whose source connections were recreated (excludes deleted mirrors)
     */
    private Set<String> maybeRecreateSourceConnection(MetadataDelta delta, MetadataImage newImage) {
        Set<String> reconnectedMirrors = new HashSet<>();
        if (delta.configsDelta() != null) {
            delta.configsDelta().changes().entrySet().stream()
                    .filter(e -> e.getKey().type() == ConfigResource.Type.CLUSTER_MIRROR)
                    .forEach(e -> {
                        String mirrorName = e.getKey().name();
                        boolean mirrorDeleted = newImage.configs().configProperties(e.getKey()).isEmpty();
                        if (mirrorDeleted) {
                            log.info("Mirror '{}' has been deleted. Writing tombstone records.", mirrorName);
                            tombstoneWriter.ifPresent(h -> h.accept(mirrorName));
                        }

                        boolean connectionConfigChanged = e.getValue().changes().keySet().stream()
                                .anyMatch(key -> !NON_CONNECTION_CONFIGS.contains(key));
                        if (connectionConfigChanged) {
                            log.info("Mirror '{}' has connection config changed. Recreating connections.", mirrorName);
                        }
                        if (connectionConfigChanged || mirrorDeleted) {
                            sourceLeaders.remove(mirrorName);
                            Admin admin = srcAdmins.remove(mirrorName);
                            if (admin != null) {
                                admin.close(Duration.ZERO);
                            }
                            var mirrorFetcherManager = replicaManagerSupplier.get().mirrorFetcherManager();
                            mirrorFetcherManager.removeFetchersForMirror(mirrorName);
                            mirrorFetcherManager.shutdownIdleFetcherThreads();
                            if (!mirrorDeleted) {
                                reconnectedMirrors.add(mirrorName);
                            }
                        }
                    });
        }
        return reconnectedMirrors;
    }

    /**
     * Collects mirror partitions that need state transitions and cleans up state for lost leadership.
     * Three phases:
     * 1. Collect new mirror leaders from the topics delta (leadership gains and mirror state changes)
     * 2. Clean up cached state for partitions where this broker lost leadership
     * 3. Re-add MIRRORING partitions for reconnected mirrors so their fetchers get recreated
     */
    private Set<TopicPartition> collectMirrorLeaderChanges(MetadataDelta delta, MetadataImage image,
                                                           Set<String> reconnectedMirrors) {
        Set<TopicPartition> mirrorLeaderPartitions = new HashSet<>();

        if (delta.topicsDelta() != null) {
            LocalReplicaChanges localReplicaChanges = delta.topicsDelta().localChanges(nodeId);

            // Phase 1: collect partitions where this broker gained mirror leadership
            localReplicaChanges.leaders().keySet().forEach(tp -> {
                String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
                if (mirrorName != null && !mirrorName.isBlank()) {
                    mirrorLeaderPartitions.add(tp);
                }
            });
            localReplicaChanges.mirrorTopicStates().keySet().forEach(topicId -> {
                TopicImage topicImage = image.topics().getTopic(topicId);
                if (topicImage != null) {
                    topicImage.partitions().forEach((partitionId, partition) -> {
                        if (partition.leader == nodeId) {
                            mirrorLeaderPartitions.add(new TopicPartition(topicImage.name(), partitionId));
                        }
                    });
                }
            });

            // Phase 2: clean up state for partitions where this broker lost leadership
            localReplicaChanges.followers().keySet().forEach(tp -> {
                String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
                if (mirrorName == null || mirrorName.isBlank()) {
                    return;
                }
                pendingPartitionStates.remove(tp);
                pendingLeaderEpochBumps.removeIf(bump -> {
                    if (bump.partitionToEpoch().containsKey(tp)) {
                        bump.future().completeExceptionally(new IllegalStateException("Not leader anymore for " + tp));
                        return true;
                    }
                    return false;
                });
                if (!isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                    PartitionKey key = new PartitionKey(mirrorName, tp.topic(), tp.partition());
                    removePartitionState(key);
                }
            });
        }

        // Phase 3: re-add MIRRORING partitions for mirrors whose source connection was recreated
        if (!reconnectedMirrors.isEmpty()) {
            log.info("Re-evaluating MIRRORING partitions for reconnected mirrors: {}", reconnectedMirrors);
            partitionStates.forEach((key, state) -> {
                if (reconnectedMirrors.contains(key.mirrorName()) && state == MirrorPartitionState.MIRRORING) {
                    mirrorLeaderPartitions.add(new TopicPartition(key.topic(), key.partition()));
                }
            });
        }

        return mirrorLeaderPartitions;
    }

    /**
     * Applies state transitions for mirror leader partitions. Local coordinator partitions are handled
     * inline. Remote coordinator partitions are grouped by mirror for batched reads, then transitions
     * are applied from the responses.
     */
    private void processStateTransitions(Set<TopicPartition> mirrorLeaders, MetadataImage newImage) {
        Map<String, Map<String, Set<Integer>>> remotePartitionsByMirror = new HashMap<>();
        Map<String, Map<TopicPartition, Boolean>> remoteStopFlags = new HashMap<>();
        Map<String, Map<TopicPartition, Boolean>> remotePauseFlags = new HashMap<>();

        mirrorLeaders.forEach(tp -> {
            TopicImage topicImage = newImage.topics().getTopic(tp.topic());
            String mirrorName = topicImage.mirrorName();
            int desiredMirrorState = topicImage.desiredMirrorState();
            boolean stopRequested = desiredMirrorState == MirrorPartitionState.STOPPED.value();
            boolean pauseRequested = desiredMirrorState == MirrorPartitionState.PAUSED.value();

            PartitionKey key = new PartitionKey(mirrorName, tp.topic(), tp.partition());
            if (isLocalCoordinator(key.mirrorName(), key.topic(), key.partition())) {
                MirrorPartitionState curState = partitionStates.getOrDefault(key, MirrorPartitionState.UNKNOWN);
                log.debug("Local transition for {} (current: {})", tp, curState);
                applyStateTransition(key.mirrorName(), tp, curState, null, stopRequested, pauseRequested);
            } else {
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

        remotePartitionsByMirror.forEach((mirrorName, partitions) -> {
            Map<TopicPartition, Boolean> stopFlags = remoteStopFlags.get(mirrorName);
            Map<TopicPartition, Boolean> pauseFlags = remotePauseFlags.get(mirrorName);
            log.debug("Reading remote coordinator state for mirror '{}': {}", mirrorName, partitions);
            readStatesFromRemoteCoordinator(mirrorName, partitions, res ->
                    res.data().topics().forEach(topic ->
                            topic.partitions().forEach(partition -> {
                                if (partition.errorCode() != Errors.NONE.code()) {
                                    log.warn("Error reading mirror state for {}-{}: {}",
                                            topic.name(), partition.partitionIndex(), Errors.forCode(partition.errorCode()));
                                    return;
                                }
                                TopicPartition resTp = new TopicPartition(topic.name(), partition.partitionIndex());
                                MirrorPartitionState state = MirrorPartitionState.fromValue(partition.state());
                                boolean stopRequested = stopFlags.getOrDefault(resTp, false);
                                boolean pauseRequested = pauseFlags.getOrDefault(resTp, false);
                                PartitionKey mpk = new PartitionKey(mirrorName, resTp.topic(), resTp.partition());
                                MirrorPartitionState curState = partitionStates.getOrDefault(mpk, MirrorPartitionState.UNKNOWN);
                                applyStateTransition(mirrorName, resTp, curState, state, stopRequested, pauseRequested);
                            })));
        });
    }

    @Override
    public void close() throws Exception {
        if (mirrorStateSender != null) {
            mirrorStateSender.shutdown();
        }
        closeSourceAdmins();
        if (dstAdmin != null) {
            dstAdmin.close();
        }
        if (mirrorStateSender != null) {
            mirrorStateSender.shutdown();
        }
        clearCache();
    }

    // Force-close source admin clients so any in-flight requests fail immediately
    // instead of blocking until requestTimeoutMs expires.
    void closeSourceAdmins() {
        srcAdmins.values().forEach(admin -> admin.close(Duration.ZERO));
    }

    void clearCache() {
        failedPartitionInfo.clear();
        partitionStates.clear();
        partitionStateCounts.clear();
        lastMirrorEpochs.clear();
        sourceLeaders.clear();
        pendingLeaderEpochBumps.clear();
        pendingPartitionStates.clear();
    }

    /** Returns partition states pending coordinator write. */
    public Map<TopicPartition, MirrorPartitionState> pendingPartitionStates() {
        return pendingPartitionStates;
    }

    public Map<TopicPartition, FailedPartitionInfo> failedPartitionInfo() {
        return failedPartitionInfo;
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartition, MirrorPartitionState state, String errorMessage) {
        stateTransitioner.ifPresent(st -> st.transitionTo(mirrorName, topicPartition, state, errorMessage));
    }

    /**
     * Applies the appropriate state transition based on current state and stop flag.
     *
     * stopRequested: it means the partition should head to STOPPED state. When it is true (i.e. users stopMirrorTopics):
     *   1. if it's already in STOPPED state, then keep the state
     *   2. else, move the state to STOPPING state
     *
     * pauseRequested: it means the partition should head to PAUSED state. When it is true (i.e. users pause it):
     *   1. if it's already in PAUSED state, then keep the state
     *   2. else, move the state to PAUSING state
     *
     * When stopRequested=false and pauseRequested=false:
     *   1. if it's in PAUSED state, we should move it to MIRRORING state. It will happen when users resume mirroring
     *   2. if it's in UNKNOWN, STOPPED, or FAILED state, we should move it to LOG_TRUNCATION state.
     *      UNKNOWN/STOPPED happens on startMirrorTopics. FAILED happens on manual restart after retries are exhausted.
     *   3. else, keep the same state as is. This could happen like leadership change, and the new leader should
     *      continue to complete the process in previous leader
     */
    private void applyStateTransition(String mirrorName, TopicPartition tp,
                                      MirrorPartitionState curState, MirrorPartitionState fetchedState,
                                      boolean stopRequested, boolean pauseRequested) {
        stateTransitioner.ifPresent(t -> {
            if (stopRequested) {
                if (curState != MirrorPartitionState.STOPPED) {
                    t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPING, null);
                } else {
                    t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED, null);
                }
            } else if (pauseRequested) {
                if (curState != MirrorPartitionState.PAUSED) {
                    t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSING, null);
                } else {
                    t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED, null);
                }
            } else if (curState == MirrorPartitionState.PAUSED) {
                t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null);
            } else if (curState == MirrorPartitionState.UNKNOWN
                    || curState == MirrorPartitionState.STOPPED
                    || curState == MirrorPartitionState.FAILED) {
                t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.LOG_TRUNCATION, null);
            } else {
                t.transitionTo(mirrorName, Set.of(tp), fetchedState != null ? fetchedState : curState, null);
            }
        });
    }

    public void scheduleSourceMetadataSync(String mirrorName) {
        scheduler.scheduleOnce("SourceMetadataSync", () -> {
            syncTopicMetadata(mirrorName);
        });
    }

    /** Periodic metadata refresh entry point. */
    void runMetadataRefresh() {
        // retry the pending tombstone writes first to make sure they are all clean up even if no mirror existed
        retryPendingTombstoneWrites();

        Set<String> mirrors = getConfiguredMirrors();
        if (mirrors.isEmpty()) {
            return;
        }

        log.info("Refreshing metadata for mirrors: {}", mirrors);

        for (String mirrorName : mirrors) {
            try {
                validateSourceClusterId(mirrorName);
                var topicMetadata = syncTopicMetadata(mirrorName);
                syncCoordinatorMetadata(mirrorName, topicMetadata);
            } catch (Exception e) {
                log.error("Failed to refresh metadata for mirror {}", mirrorName, e);
            }
        }

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        metadataRefreshError.incrementAndGet();
    }

    private void retryPendingTombstoneWrites() {
        if (tombstoneWriter.isEmpty()) {
            log.warn("Mirror deletion handler not configured. Tombstone record writes will be skipped.");
            return;
        }
        Set<String> configuredMirrors = getConfiguredMirrors();
        Set<String> staleMirrors = partitionStates.keySet().stream()
                .map(ClusterMirrorUtils.PartitionKey::mirrorName)
                .filter(name -> !configuredMirrors.contains(name))
                .collect(Collectors.toSet());
        for (String mirrorName : staleMirrors) {
            log.info("Found stale partition states for deleted mirror '{}'. Writing tombstones.", mirrorName);
            tombstoneWriter.ifPresent(h -> h.accept(mirrorName));
        }
    }

    private void validateSourceClusterId(String mirrorName) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);
        try {
            var clusterResult = srcAdmin.describeCluster();
            String newClusterId = clusterResult.clusterId().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            if (newClusterId != null && !newClusterId.isEmpty()) {
                String previousClusterId = getSourceClusterId(mirrorName);
                if (previousClusterId != null && !previousClusterId.equals(newClusterId)) {
                    throw new IllegalStateException("Source cluster ID changed for mirror " + mirrorName
                            + ": expected " + previousClusterId + ", got " + newClusterId
                            + ". This may indicate a misconfiguration or that the source cluster has been replaced.");
                }
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Failed to describe source cluster for mirror {}", mirrorName, e);
        }
    }

    /**
     * Fetches topics metadata from the source cluster via Admin.describeTopics.
     * Runs on every broker to keep partition leaders, topic creation, topic deletion, and partition counts in sync.
     */
    private Optional<List<SourceTopicState>> syncTopicMetadata(String mirrorName) {
        log.info("Syncing source topic state for mirror {}", mirrorName);
        Set<String> topics = getConfiguredTopics(mirrorName, false);
        if (topics.isEmpty()) {
            return Optional.empty();
        }

        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        try {
            Map<String, KafkaFuture<TopicDescription>> futures = srcAdmin.describeTopics(topics).topicNameValues();
            List<SourceTopicState> result = new ArrayList<>();

            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    TopicDescription td = entry.getValue().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    List<SourcePartitionState> partitions = td.partitions().stream()
                            .map(pi -> new SourcePartitionState(
                                    new TopicPartition(td.name(), pi.partition()),
                                    pi.leader(),
                                    pi.leaderEpoch()))
                            .toList();
                    result.add(new SourceTopicState(td.name(), td.topicId(), true, partitions));
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        result.add(new SourceTopicState(entry.getKey(), Uuid.ZERO_UUID, false, List.of()));
                    } else {
                        log.warn("Failed to describe topic {} for mirror {}", entry.getKey(), mirrorName, e.getCause());
                    }
                }
            }

            processTopicMetadata(mirrorName, result);
            return Optional.of(result);
        } catch (Exception e) {
            log.warn("Failed to sync source topic state for mirror {}", mirrorName, e);
            return Optional.empty();
        }
    }

    private void processTopicMetadata(String mirrorName, List<SourceTopicState> topicInfos) {
        var creatableTopics = new ArrayList<CreateTopicsRequestData.CreatableTopic>();
        var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();

        topicInfos.forEach(ti -> {
            // use ConcurrentHashMap for thread-safe access from scheduler and fetcher threads
            var partitionLeaders = sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>());

            int sourcePartitionCount = ti.partitions().size();

            // skip partitions with no leader (source broker may be restarting)
            ti.partitions().forEach(pi -> {
                if (pi.leader() != null) {
                    partitionLeaders.put(pi.topicPartition(),
                            new ClusterMirrorUtils.LeaderInfo(pi.leader(), pi.leaderEpoch().orElse(0)));
                }
            });

            // Pre-KIP-516 sources (Kafka < 2.8) return ZERO_UUID; fall back to name-based lookup
            TopicImage destTopic = !ti.topicId().equals(Uuid.ZERO_UUID)
                    ? metadataImage.topics().getTopic(ti.topicId())
                    : metadataImage.topics().getTopic(ti.topic());

            if (destTopic != null && destTopic.partitions().size() < sourcePartitionCount) {
                createPartitionsTopics.add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                        .setName(ti.topic())
                        .setCount(sourcePartitionCount)
                        .setAssignments(null)
                );
            } else if (destTopic == null &&
                    metadataImage.topics().getTopic(ti.topic()) == null &&
                    ti.exists() && sourcePartitionCount > 0) {
                if (pendingTopicCreations.add(ti.topic())) {
                    creatableTopics.add(new CreateTopicsRequestData.CreatableTopic()
                            .setName(ti.topic())
                            .setNumPartitions(sourcePartitionCount)
                            .setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR)
                            .setMirrorInfo(new CreateTopicsRequestData.MirrorInfo().setTopicId(
                                    ti.topicId().equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : ti.topicId())));
                }
            } else if (destTopic == null &&
                    metadataImage.topics().getTopic(ti.topic()) != null &&
                    ti.exists()) {
                log.error("Mirror topic {} exists on destination with TopicId {} but source has TopicId {}. "
                                + "Delete the topic on destination and let auto-creation recreate it with the correct TopicId.",
                        ti.topic(), metadataImage.topics().getTopic(ti.topic()).id(), ti.topicId());
            }
        });

        if (!creatableTopics.isEmpty()) {
            createMirrorTopics(creatableTopics);
        }

        maybeScalePartitions(createPartitionsTopics);
        maybeStopDeletedTopics(mirrorName, topicInfos);
        maybeStartMissedPartitions(mirrorName);
    }

    /**
     * Creates mirror topics on the destination with the source's TopicId,
     * preserving topic identity across clusters. Called during periodic metadata
     * sync when topics have mirror.name config but don't exist on the destination yet.
     * Once created, onMetadataUpdate will detect them and start the mirror state machine.
     */
    private void createMirrorTopics(List<CreateTopicsRequestData.CreatableTopic> creatableTopics) {
        var topicNames = creatableTopics.stream().map(CreateTopicsRequestData.CreatableTopic::name).toList();
        creatableTopics.forEach(t -> log.info("Creating mirror topic {} on destination (partitions={}, topicId={})",
                t.name(), t.numPartitions(), t.mirrorInfo().topicId()));
        var createTopicsData = new CreateTopicsRequestData().setTimeoutMs(brokerConfig.requestTimeoutMs());
        creatableTopics.forEach(createTopicsData.topics()::add);
        ControllerRequestCompletionHandler requestCompletionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                topicNames.forEach(pendingTopicCreations::remove);
                log.warn("Create mirror topics timed out for {}", topicNames);
            }

            @Override
            public void onComplete(ClientResponse response) {
                topicNames.forEach(pendingTopicCreations::remove);
                if (response.responseBody() instanceof CreateTopicsResponse createTopicsResponse) {
                    createTopicsResponse.data().topics().forEach(topic -> {
                        Errors error = Errors.forCode(topic.errorCode());
                        if (error != Errors.NONE) {
                            log.warn("Failed to create mirror topic {}: {}", topic.name(), error.message());
                        }
                    });
                }
            }
        };
        channelManager.sendRequest(
                new CreateTopicsRequest.Builder(createTopicsData),
                requestCompletionHandler);
    }

    private void maybeScalePartitions(CreatePartitionsRequestData.CreatePartitionsTopicCollection topics) {
        if (!topics.isEmpty()) {
            log.debug("Detected partition count change, sending CreatePartitionsRequest: {}", topics);
            channelManager.sendRequest(new CreatePartitionsRequest.Builder(
                    new CreatePartitionsRequestData()
                            .setTopics(topics)
                            .setValidateOnly(false)
                            .setTimeoutMs(3000)
            ), new TimeoutHandler(log));
        }
    }

    private void maybeStopDeletedTopics(String mirrorName, List<SourceTopicState> topicInfos) {
        List<String> deletedSourceTopicNames = new ArrayList<>(topicInfos.stream()
                .filter(ti -> !ti.exists())
                .map(SourceTopicState::topic).toList());

        if (deletedSourceTopicNames.isEmpty()) {
            return;
        }

        // In old cluster, it is possible the broker metadata update in progress, and the returned metadata response is stale.
        // list topic again to make sure it is indeed deleted.
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);
        try {
            Set<String> allTopics = srcAdmin.listTopics().names().get();
            log.debug("Source topic name list: {}", allTopics);
            deletedSourceTopicNames.removeAll(allTopics);
        } catch (Exception e) {
            log.warn("Failed to describe topic for mirror {}: {}", mirrorName, e.getMessage());
        }

        getConfiguredTopics(mirrorName, true).forEach(name -> {
            if (deletedSourceTopicNames.contains(name)) {
                log.info("Detected topic {} deleted in remote cluster {}, stopping mirror partitions", name, mirrorName);
                // snapshot keyset to avoid skipping entries during concurrent modification
                Set.copyOf(partitionStates.keySet()).stream()
                        .filter(key -> key.mirrorName().equals(mirrorName) && key.topic().equals(name))
                        .forEach(key -> stateTransitioner.ifPresent(t ->
                                t.transitionTo(mirrorName, Set.of(new TopicPartition(key.topic(), key.partition())), MirrorPartitionState.STOPPING, null)));
            }
        });
    }

    /**
     * Transitions partitions stuck in UNKNOWN because their source leader was not yet known
     * when onMetadataUpdate first ran.
     *
     * The race: processTopicMetadata creates a mirror topic on the destination, which
     * triggers onMetadataUpdate. That callback needs the source leader in sourceLeaders to
     * transition the partition to LOG_TRUNCATION. If the source has not elected a leader yet
     * (or the Admin describeTopics response arrived without one), the partition stays in
     * UNKNOWN. Since onMetadataUpdate only fires on destination metadata changes, it will
     * not retry on its own.
     *
     * This is more likely against older source clusters (e.g. Kafka 2.1 with ZK) where
     * Admin.describeTopics goes through three round trips before returning topic metadata:
     * describeCluster (node discovery), DescribeTopicPartitions (rejected with
     * UnsupportedVersionException), then MetadataRequest (fallback). The extra latency
     * widens the window in which the source leader is not yet known.
     */
    private void maybeStartMissedPartitions(String mirrorName) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders == null) {
            return;
        }
        partitionLeaders.keySet().forEach(tp -> {
            PartitionKey key = new PartitionKey(mirrorName, tp.topic(), tp.partition());
            if (partitionStates.getOrDefault(key, MirrorPartitionState.UNKNOWN) != MirrorPartitionState.UNKNOWN) {
                return;
            }
            TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
            if (topicImage == null) {
                return;
            }
            // Skip stopped/paused topics: if the partition cache was cleared (e.g. coordinator
            // leadership change), the state defaults to UNKNOWN and would be restarted here.
            int desiredState = topicImage.desiredMirrorState();
            if (desiredState == MirrorPartitionState.STOPPED.value()
                    || desiredState == MirrorPartitionState.PAUSED.value()) {
                return;
            }
            var partition = topicImage.partitions().get(tp.partition());
            if (partition != null && partition.leader == nodeId) {
                log.info("Source leader for {} discovered after initial onMetadataUpdate, transitioning to LOG_TRUNCATION", tp);
                stateTransitioner.ifPresent(t -> t.transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.LOG_TRUNCATION, null));
            }
        });
    }

    /**
     * Syncs configurations, consumer group offsets, ACLs, and topic patterns from source clusters.
     * Runs only on the coordinator broker for each mirror.
     */
    private void syncCoordinatorMetadata(String mirrorName, Optional<List<SourceTopicState>> topicInfos) {
        if (!isLocalCoordinator(mirrorName)) {
            return;
        }

        log.info("Syncing coordinator metadata for mirror {}", mirrorName);

        try {
            ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(
                    metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName)));
            syncTopicConfigs(mirrorName, mirrorConfig);
            syncGroupOffsets(mirrorName, mirrorConfig);
            syncAcls(mirrorName, mirrorConfig);
            if (!getConfiguredTopics(mirrorName, false, false).isEmpty()) {
                maybeBumpLeaderEpochs(mirrorName, topicInfos, Set.of());
            }
            discoverTopicsByPattern(mirrorName, mirrorConfig);
            enforceExcludePatterns(mirrorName, mirrorConfig);
        } catch (Exception e) {
            log.error("Failed to sync mirror metadata for mirror {}", mirrorName, e);
        }
    }

    private void syncTopicConfigs(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        Set<String> topics = getConfiguredTopics(mirrorName, false);
        log.debug("Describing topic configs for topics: {}", topics);
        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        topicConfigSyncError.incrementAndGet();

        Collection<ConfigResource> resources = topics.stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                .toList();

        try {
            Map<ConfigResource, Config> sourceConfigs = srcAdmin.describeConfigs(resources)
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(sourceConfigs, mirrorConfig);
            applyConfigurationChanges(configsToChange);
        } catch (Exception e) {
            log.warn("Failed to describe topic configs for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    private Map<String, Map<String, String>> detectConfigurationChanges(
            Map<ConfigResource, Config> sourceConfigs, ClusterMirrorConfig mirrorConfig) {
        Map<String, Map<String, String>> configsToChange = new HashMap<>();
        Pattern excludePattern = mirrorConfig.topicPropertiesExcludePattern();

        sourceConfigs.forEach((resource, config) -> {
            if (resource.type() == ConfigResource.Type.TOPIC) {
                Properties props = metadataCache.topicConfig(resource.name());
                Map<String, String> conChange = new HashMap<>();

                config.entries().forEach(entry -> {
                    if (entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
                            && (excludePattern == null || !excludePattern.matcher(entry.name()).matches())) {
                        if (props.containsKey(entry.name())) {
                            if (!props.get(entry.name()).equals(entry.value())) {
                                conChange.put(entry.name(), entry.value());
                            }
                        } else {
                            conChange.put(entry.name(), entry.value());
                        }
                    }
                });

                if (!conChange.isEmpty()) {
                    configsToChange.put(resource.name(), conChange);
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
    private void syncGroupOffsets(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        Set<String> mirrorTopics = getConfiguredTopics(mirrorName, false, false);
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

                Map<TopicPartition, OffsetAndMetadata> filtered = new HashMap<>();
                entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .forEach(ent -> {
                            TopicPartition topicPartition = ent.getKey();
                            Option<Long> logStartOffset = replicaManagerSupplier.get().getLog(topicPartition).map(UnifiedLog::logStartOffset);
                            Option<Long> logEndOffset = replicaManagerSupplier.get().getLog(topicPartition).map(UnifiedLog::logEndOffset);
                            if (logStartOffset.isEmpty() ||  logEndOffset.isEmpty()) {
                                log.debug("Cannot get the log start offset or log end offset for partition {}, skip consumer group sync for it.", topicPartition);
                                return;
                            }
                            OffsetAndMetadata sourceGroupOffsetAndMetadata = ent.getValue();

                            // Committing to the range [local logStartOffset ~ local logEndOffset]
                            long finalOffset = Math.max(logStartOffset.get(), Math.min(sourceGroupOffsetAndMetadata.offset(), logEndOffset.get()));

                            if (finalOffset == sourceGroupOffsetAndMetadata.offset()) {
                                filtered.put(topicPartition, sourceGroupOffsetAndMetadata);
                            } else if (finalOffset == logEndOffset.get()) {
                                int logEndEpoch = replicaManagerSupplier.get().getLog(topicPartition).map(l -> l.leaderEpochCache().epochForOffset(logEndOffset.get())).getOrElse(() -> -1);
                                if (logEndEpoch < 0) {
                                    log.debug("Cannot get the log end epoch for partition {}, skip consumer group sync for it.", topicPartition);
                                } else {
                                    filtered.put(topicPartition, new OffsetAndMetadata(logEndOffset.get(), Optional.of(logEndEpoch), ""));
                                }
                            } else {
                                // finalOffset == logStartOffset
                                int logStartEpoch = replicaManagerSupplier.get().getLog(topicPartition).map(l -> l.leaderEpochCache().epochForOffset(logStartOffset.get())).getOrElse(() -> -1);
                                if (logStartEpoch < 0) {
                                    log.debug("Cannot get the log start epoch for partition {}, skip consumer group sync for it.", topicPartition);
                                } else {
                                    filtered.put(topicPartition, new OffsetAndMetadata(logStartOffset.get(), Optional.of(logStartEpoch), ""));
                                }
                            }
                        });

                if (filtered.isEmpty()) {
                    continue;
                }

                try {
                    log.debug("Committing consumer group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    getOrCreateDestAdmin().alterConsumerGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
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
            List<String> sourceGroupIds;
            try {
                sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forShareGroups(),
                        groupsIncludePattern, groupsExcludePattern);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnsupportedVersionException) {
                    log.debug("The source cluster doesn't support share group, skipping share group offset sync");
                    return;
                } else {
                    throw e;
                }
            }
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

                Map<TopicPartition, Long> filtered = new  HashMap<>();
                entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .forEach(ent -> {
                            TopicPartition topicPartition = ent.getKey();
                            Option<Long> logStartOffset = replicaManagerSupplier.get().getLog(topicPartition).map(UnifiedLog::logStartOffset);
                            Option<Long> logEndOffset = replicaManagerSupplier.get().getLog(topicPartition).map(UnifiedLog::logEndOffset);
                            if (logStartOffset.isEmpty() ||  logEndOffset.isEmpty()) {
                                log.debug("Cannot get the log start offset or log end offset for partition {}, skip share group offset sync for it.", topicPartition);
                                return;
                            }
                            OffsetAndMetadata sourceGroupOffsetAndMetadata = ent.getValue();
                            // Committing to the range [local logStartOffset ~ local logEndOffset]
                            long finalOffset = Math.max(logStartOffset.get(), Math.min(sourceGroupOffsetAndMetadata.offset(), logEndOffset.get()));
                            filtered.put(topicPartition, finalOffset);
                        });
                if (filtered.isEmpty()) {
                    continue;
                }

                try {
                    log.debug("Committing share group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    getOrCreateDestAdmin().alterShareGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
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
                    GroupState.RECONCILING));
            return Optional.of(getOrCreateDestAdmin().listGroups(options).all()
                    .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS).stream()
                    .map(GroupListing::groupId)
                    .collect(Collectors.toSet()));
        } catch (Exception e) {
            log.warn("Failed to list destination groups, skipping offset sync cycle.", e);
            return Optional.empty();
        }
    }

    private void syncAcls(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        // TODO: We currently mirror all ACLs from the source to the target.
        //       Any ACLs added/removed directly on the target will be overwritten
        //       on the next sync to match the source.
        //
        // TODO: How do we disambiguate ACLs that reference the same resource name
        //       when multiple cluster mirrors exist?

        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        aclSyncError.incrementAndGet();

        try {
            Collection<AclBinding> sourceAcls = srcAdmin.describeAcls(AclBindingFilter.ANY)
                    .values().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            log.debug("Describe ACLs response from remote cluster {}: {}", mirrorName, sourceAcls);

            List<MirrorFilterUtils.AclRule> aclIncludeRules = mirrorConfig.aclIncludeRules();
            var allRemoteAcls = sourceAcls.stream()
                    .filter(acl -> aclIncludeRules.stream().anyMatch(rule -> rule.matches(acl)))
                    .toList();
            var aclChanges = detectAclChanges(allRemoteAcls);
            applyAclChanges(mirrorName, aclChanges);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SecurityDisabledException) {
                log.debug("ACL sync skipped for mirror {}: {}", mirrorName, e.getCause().getMessage());
            } else {
                log.warn("Failed to describe ACLs for mirror {}: {}", mirrorName, e.getMessage());
            }
        } catch (Exception e) {
            log.warn("Failed to describe ACLs for mirror {}: {}", mirrorName, e.getMessage());
        }
    }

    private SourceAclChanges detectAclChanges(List<AclBinding> sourceAcls) {
        var addACLsList = new ArrayList<AclBinding>();
        var deleteACLsList = new ArrayList<AclBinding>();
        var current = metadataImage.acls().acls().values();

        // collect missing acls list
        sourceAcls.forEach(acl -> {
            if (current.stream().map(StandardAcl::toBinding).noneMatch(a -> a.equals(acl))) {
                addACLsList.add(acl);
            }
        });

        // collect remove acls list (skip CLUSTER_MIRROR ACLs as they are destination-specific)
        metadataImage.acls().acls().values().forEach(acl -> {
            if (acl.resourceType() != ResourceType.CLUSTER_MIRROR && !sourceAcls.contains(acl.toBinding())) {
                deleteACLsList.add(acl.toBinding());
            }
        });

        return new SourceAclChanges(addACLsList, deleteACLsList);
    }

    private void applyAclChanges(String mirrorName, SourceAclChanges aclChanges) {
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

    private void discoverTopicsByPattern(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        final Pattern topicsIncludePattern = mirrorConfig.topicsIncludePattern();
        if (topicsIncludePattern == null) {
            return;
        }

        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        Set<String> configuredTopics = getConfiguredTopics(mirrorName, true);
        final Pattern topicsExcludePattern = mirrorConfig.topicsExcludePattern();

        List<StartMirrorTopicsRequestData.TopicMetadata> newTopics;
        try {
            Set<String> allSourceTopics = srcAdmin.listTopics()
                    .names().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            List<String> candidates = allSourceTopics.stream()
                    .filter(name -> topicsIncludePattern.matcher(name).matches())
                    .filter(name -> topicsExcludePattern == null || !topicsExcludePattern.matcher(name).matches())
                    .filter(name -> !configuredTopics.contains(name))
                    .toList();

            if (candidates.isEmpty()) {
                return;
            }

            Map<String, TopicDescription> descriptions = srcAdmin.describeTopics(candidates)
                    .allTopicNames().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            cacheSourceLeaders(mirrorName, descriptions.values());

            newTopics = descriptions.values().stream()
                    .map(td -> new StartMirrorTopicsRequestData.TopicMetadata()
                            .setTopicName(td.name())
                            .setTopicId(td.topicId())
                            .setNumPartitions(td.partitions().size()))
                    .toList();
        } catch (Exception e) {
            log.warn("Failed to discover topics by pattern for mirror {}", mirrorName, e);
            return;
        }

        if (newTopics.isEmpty()) {
            return;
        }

        log.info("Discovered {} new topic(s) matching mirror.topics.include pattern for mirror {}: {}",
                newTopics.size(), mirrorName, newTopics.stream().map(StartMirrorTopicsRequestData.TopicMetadata::topicName).toList());

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
     * Checks if any active mirror topics now match the exclude pattern and sends
     * StopMirrorTopicsRequest to stop them. Catches cases where exclude was updated
     * via incrementalAlterConfigs outside of the startMirrorTopics/stopMirrorTopics flow.
     */
    private void enforceExcludePatterns(String mirrorName, ClusterMirrorConfig mirrorConfig) {
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

    /** Finds the coordinator node (leader of the __mirror_state partition) for a mirror record key */
    private Node findCoordinatorNode(ClusterMirrorRecordKey key) {
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
                    if (coordPartitionFinderByKey.isEmpty()) {
                        return Node.noNode();
                    }
                    int partition = coordPartitionFinderByKey.get().apply(key);
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
                                        Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> topicMetadata,
                                        Set<String> stoppedTopics,
                                        Consumer<WriteMirrorStatesResponse> callback) {
        log.debug("Writing states to remote coordinator: {} {} {}", mirrorName, topicMetadata, stoppedTopics);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<WriteMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        topicMetadata.forEach((topic, metadata) -> {
            metadata.forEach(m -> {
                ClusterMirrorRecordKey key = new ClusterMirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), m.partition());
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
            List<WriteMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                topicDataList.add(new WriteMirrorStatesRequestData.TopicMetadata()
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
    void readStatesFromRemoteCoordinator(String mirrorName,
                                         Map<String, Set<Integer>> partitions,
                                         Consumer<ReadMirrorStatesResponse> callback) {
        log.debug("Reading states from remote coordinator: {} {}", mirrorName, partitions);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<ReadMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                ClusterMirrorRecordKey key = new ClusterMirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), part);
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
            List<ReadMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                topicDataList.add(new ReadMirrorStatesRequestData.TopicMetadata()
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

                        readMirrorStatesResponse.data().topics().forEach(topic -> {
                            topic.partitions().forEach(partition -> {
                                PartitionKey mpk = new PartitionKey(
                                        mirrorName, topic.name(), partition.partitionIndex());
                                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                                if (partition.state() != -1) {
                                    partitionStates.put(mpk, MirrorPartitionState.fromValue(partition.state()));
                                }
                                if (partition.state() == MirrorPartitionState.FAILED.value()) {
                                    failedPartitionInfo.put(tp, new FailedPartitionInfo(
                                            partition.retryAttempt(), partition.errorMessage(),
                                            MirrorPartitionState.fromValue(partition.previousState())));
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
    void getTopicMetadata(String mirrorName,
                          Map<String, Set<Integer>> partitions,
                          Consumer<ReadMirrorStatesResponse> responseCallback) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        List<ReadMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
        partitions.forEach((tp, parts) -> {
            ReadMirrorStatesResponseData.TopicResult topicResult = new ReadMirrorStatesResponseData.TopicResult().setName(tp);
            List<ReadMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
            parts.forEach(part -> {
                PartitionKey pk = new PartitionKey(mirrorName, tp, part);
                ReadMirrorStatesResponseData.PartitionResult partitionResult = new ReadMirrorStatesResponseData.PartitionResult();
                if (!isLocalCoordinator(mirrorName, tp, part)) {
                    partitionResult.setErrorCode(Errors.NOT_COORDINATOR.code());
                    partitionResult.setErrorMessage(Errors.NOT_COORDINATOR.message());
                } else {
                    partitionResult.setPartitionIndex(part);
                    partitionResult.setLastMirrorEpoch(lastMirrorEpochs.getOrDefault(pk, -1));
                    partitionResult.setState(partitionStates.getOrDefault(pk, MirrorPartitionState.UNKNOWN).value());
                    FailedPartitionInfo fpi = failedPartitionInfo.get(new TopicPartition(tp, part));
                    partitionResult.setPreviousState(
                            fpi != null ? fpi.previousState().value() : MirrorPartitionState.UNKNOWN.value());
                    partitionResult.setRetryAttempt(fpi != null ? fpi.retryAttempt() : 0);
                    partitionResult.setErrorMessage(fpi != null ? fpi.errorMessage() : null);
                }
                partitionResults.add(partitionResult);
            });
            topicResult.setPartitions(partitionResults);
            topicResults.add(topicResult);
        });
        data.setTopics(topicResults);
        responseCallback.accept(new ReadMirrorStatesResponse(data));
    }

    // Visible for testing
    static Properties buildDestAdminClientProps(KafkaConfig brokerConfig) {
        ListenerName mirrorAdminListener = brokerConfig.mirrorAdminListenerName();
        Endpoint endpoint = (Endpoint) brokerConfig.effectiveAdvertisedBrokerListeners()
                .filter(e -> e.listener().equals(mirrorAdminListener.value()))
                .head();

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-dst-admin-" + brokerConfig.nodeId());

        SecurityProtocol securityProtocol = endpoint.securityProtocol();
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);

        Map<String, ?> configs = ChannelBuilders.channelBuilderConfigs(brokerConfig, mirrorAdminListener);

        // Get all the security configs
        ConfigDef securityConfigDef = new ConfigDef().withClientSaslSupport().withClientSslSupport();
        Set<String> securityConfigs = new HashSet<>(securityConfigDef.configKeys().keySet());

        String mirrorAdminSaslMechanism = brokerConfig.saslMechanismMirrorAdminProtocol();
        if (securityProtocol == SecurityProtocol.SASL_SSL || securityProtocol == SecurityProtocol.SASL_PLAINTEXT) {
            props.put(SaslConfigs.SASL_MECHANISM, mirrorAdminSaslMechanism);
        }

        String saslMechanismConfigPrefix = mirrorAdminListener.saslMechanismConfigPrefix(mirrorAdminSaslMechanism);
        Map<String, ?> saslMechanismConfigs = brokerConfig.originalsWithPrefix(saslMechanismConfigPrefix, true);

        securityConfigs.forEach(key -> {
            if (key.equals(SaslConfigs.SASL_MECHANISM)) return;
            if (saslMechanismConfigs.containsKey(key)) {
                props.put(key, saslMechanismConfigs.get(key));
            } else if (configs.containsKey(key)) {
                Object value = configs.get(key);
                if (value == null) {
                    return;
                }
                props.put(key, value);
            }
        });

        return props;
    }

    /** Updates cached source leader for a specific partition. */
    public void updateSourceLeader(String mirrorName, TopicPartition tp, ClusterMirrorUtils.LeaderInfo leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    public ClusterMirrorUtils.LeaderInfo resolveSourceLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            ClusterMirrorUtils.LeaderInfo leader = partitionLeaders.get(tp);
            if (leader != null) {
                return leader;
            }
        }
        throw new IllegalStateException("No source cluster metadata available for mirror " + mirrorName + " partition:" + tp);
    }

    // Atomic per-key update to keep partition state counts consistent
    void updatePartitionState(PartitionKey key, MirrorPartitionState newState) {
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

    // Atomic remove + counter decrement to keep partition state counts consistent
    void removePartitionState(PartitionKey key) {
        partitionStates.computeIfPresent(key, (k, oldState) -> {
            partitionStateCounts.computeIfAbsent(oldState, s -> new AtomicLong()).decrementAndGet();
            return null;
        });
    }

    void removeMirrorStates(String mirrorName) {
        partitionStates.keySet().stream()
            .filter(key -> key.mirrorName().equals(mirrorName))
            .toList()
            .forEach(this::removePartitionState);
    }

    void removeLastMirrorEpochs(String mirrorName) {
        lastMirrorEpochs.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
    }

    void removeStateForPartitions(Set<TopicPartition> partitions) {
        partitions.forEach(tp -> {
            pendingPartitionStates.remove(tp);
            failedPartitionInfo.remove(tp);
        });
        pendingLeaderEpochBumps.removeIf(bump -> {
            bump.partitionToEpoch().keySet().removeAll(partitions);
            if (bump.partitionToEpoch().isEmpty()) {
                bump.future().cancel(false);
                return true;
            }
            return false;
        });
    }

    private long partitionStateCount(MirrorPartitionState state) {
        return partitionStateCounts.computeIfAbsent(state, s -> new AtomicLong()).get();
    }

    public MirrorPartitionState getPartitionState(String mirrorName, TopicPartition topicPartition) {
        return partitionStates.get(new PartitionKey(mirrorName, topicPartition.topic(), topicPartition.partition()));
    }

    /** Schedules a source metadata sync followed by a leader epoch bump request. */
    public CompletableFuture<Void> scheduleBumpLeaderEpochs(String mirrorName, Set<TopicPartition> topicPartitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.scheduleOnce("bump-leader-epoch", () -> {
            Optional<List<SourceTopicState>> topics = syncTopicMetadata(mirrorName);
            maybeBumpLeaderEpochs(mirrorName, topics, topicPartitions)
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

    private CompletableFuture<Void> maybeBumpLeaderEpochs(String mirrorName, Optional<List<SourceTopicState>> topicInfos, Set<TopicPartition> topicPartitions) {
        if (topicInfos.isPresent()) {
            return sendBumpLeaderEpochs(buildSourceEpochBumpTargets(mirrorName, topicInfos.get(), topicPartitions))
                    .whenComplete((v, ex) -> {
                        if (ex != null) log.warn("Failed to bump leader epoch for mirror {}", mirrorName, ex);
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    /** Sends an AlterPartition request to bump leader epochs on the destination. */
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

        pendingLeaderEpochBumps.add(new ClusterMirrorUtils.LeaderEpochBump(future, new ConcurrentHashMap<>(partitionMinEpochs)));
        maybeCompletePendingEpochBumps(); // already-met condition is detected immediately (e.g. epoch 11 vs target 0)

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

    private Map<TopicPartition, Integer> buildSourceEpochBumpTargets(String mirrorName, List<SourceTopicState> topicInfos, Set<TopicPartition> topicPartitions) {
        Set<String> mirrorTopics = topicPartitions.isEmpty() ? getConfiguredTopics(mirrorName, false) : Set.of();
        Map<TopicPartition, Integer> leaderEpochFromMetadata = new HashMap<>();
        for (SourceTopicState ts : topicInfos) {
            if (!ts.exists()) {
                continue;
            }
            if (!mirrorTopics.isEmpty() && !mirrorTopics.contains(ts.topic())) {
                continue;
            }
            collectEpochBumpTargets(ts, topicPartitions, leaderEpochFromMetadata);
        }
        if (!leaderEpochFromMetadata.isEmpty()) {
            log.info("Bumping leader epoch for partitions {}", leaderEpochFromMetadata);
        }
        return leaderEpochFromMetadata;
    }

    private void collectEpochBumpTargets(SourceTopicState topicInfo,
                                         Set<TopicPartition> topicPartitions,
                                         Map<TopicPartition, Integer> leaderEpochFromMetadata) {
        for (SourcePartitionState ps : topicInfo.partitions()) {
            TopicPartition tp = ps.topicPartition();
            if (!topicPartitions.isEmpty() && !topicPartitions.contains(tp)) {
                continue;
            }
            if (ps.leaderEpoch().isEmpty()) {
                continue;
            }
            TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
            if (topicImage == null || topicImage.partitions().get(tp.partition()) == null) {
                continue;
            }
            int epoch = ps.leaderEpoch().get();
            int localEpoch = topicImage.partitions().get(tp.partition()).leaderEpoch;
            if (epoch > localEpoch - LEADER_EPOCH_BUMP_THRESHOLD) {
                int newEpoch = Math.addExact(epoch, LEADER_EPOCH_BUMP_INCREMENT);
                leaderEpochFromMetadata.put(tp, newEpoch);
            }
        }
    }

    /** Reads the latest epoch from local logs for each partition. */
    public Map<TopicPartition, Integer> getLatestLocalEpochs(LogManager logManager, Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Integer> partitionMinEpochs = new HashMap<>();
        topicPartitions.forEach(tp -> {
            int epoch = logManager.getLog(tp, false).get().latestEpoch().orElse(-1);
            partitionMinEpochs.put(tp, epoch);
        });
        return partitionMinEpochs;
    }

    /**
     * Pre-populates sourceLeaders for discovered topics so that when onMetadataUpdate fires
     * after the destination topic is created, the fetcher can connect to the correct source
     * broker immediately. Without this, the fetcher starts with a bootstrap broker, gets a
     * redirect it cannot resolve, and cycles through FAILED/retry until the next periodic
     * syncSourceTopicState populates the cache.
     */
    private void cacheSourceLeaders(String mirrorName, Collection<TopicDescription> descriptions) {
        var partitionLeaders = sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>());
        descriptions.forEach(td -> td.partitions().forEach(pi -> {
            if (pi.leader() != null) {
                partitionLeaders.put(new TopicPartition(td.name(), pi.partition()),
                        new ClusterMirrorUtils.LeaderInfo(pi.leader(), pi.leaderEpoch().orElse(0)));
            }
        }));
    }

    Set<String> getConfiguredMirrors() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.CLUSTER_MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    /** Returns the set of topic names configured for the given mirror. */
    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused) {
        return getConfiguredTopics(mirrorName, includePaused, true);
    }

    Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
        return metadataImage.topics().topicsById().values().stream()
                .filter(topicInfo -> {
                    String topicMirrorName = topicInfo.mirrorName();
                    if (topicMirrorName == null || topicMirrorName.isBlank()) return false;
                    if (!mirrorName.equals(topicMirrorName)) return false;
                    int state = topicInfo.desiredMirrorState();
                    if (!includeStopped && state == MirrorPartitionState.STOPPED.value()) return false;
                    if (!includePaused && state == MirrorPartitionState.PAUSED.value()) return false;
                    return true;
                })
                .map(TopicImage::name)
                .collect(Collectors.toSet());
    }

    int getActiveTopicCount(String mirrorName) {
        return getConfiguredTopics(mirrorName, false, false).size();
    }

    String getSourceBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
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

    /*
     * Optimistic locking: broker validates partitions, controller validates no concurrent changes before update.
     * Verifies that all partitions for the given mirror are in STOPPED state.
     * Invokes the callback with {@code Optional.empty()} if all partitions are STOPPED,
     * or {@code Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES)} if any are not.
     */
    void validateStatesAndForwardToController(String mirrorName, Consumer<Optional<Errors>> callback) {
        Set<String> mirroredTopics = getConfiguredTopics(mirrorName, true, true);
        Map<String, Set<Integer>> remotePartitions = new HashMap<>();

        MetadataImage curImage = metadataImage;
        long brokerMetadataOffset = curImage.offset();
        for (String topic : mirroredTopics) {
            TopicImage topicImage = curImage.topics().getTopic(topic);
            if (topicImage == null || topicImage.desiredMirrorState() != MirrorPartitionState.STOPPED.value()) {
                log.error("The desired mirror state of topic {} is not in STOPPED state.", topic);
                callback.accept(Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES));
                return;
            }
            int partitionSize = topicImage.partitions().size();

            for (int i = 0; i < partitionSize; i++) {
                if (isLocalCoordinator(mirrorName, topic, i)) {
                    MirrorPartitionState state = partitionStates.getOrDefault(new PartitionKey(mirrorName, topic, i), MirrorPartitionState.UNKNOWN);
                    if (state != MirrorPartitionState.STOPPED) {
                        log.error("Partition {}-{} is not in STOPPED state. Current state: {}.", topic, i, state);
                        callback.accept(Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES));
                        return;
                    }
                } else {
                    remotePartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(i);
                }
            }
        }

        if (remotePartitions.isEmpty()) {
            sendDeleteClusterMirror(mirrorName, brokerMetadataOffset, callback);
            return;
        }

        readStatesFromRemoteCoordinator(mirrorName, remotePartitions, response -> {
            if (response.data().errorCode() != Errors.NONE.code()) {
                log.error("Error returned from read states from remote coordinator. Error code: {} and message: {}",
                        response.data().errorCode(), response.data().errorMessage());
                callback.accept(Optional.of(Errors.forCode(response.data().errorCode())));
                return;
            }

            for (var topicResult : response.data().topics()) {
                for (var partitionResult : topicResult.partitions()) {
                    if (partitionResult.errorCode() != Errors.NONE.code()) {
                        log.error("Error returned from read states from remote coordinator for partition {}-{}. Error code: {}",
                                topicResult.name(), partitionResult.partitionIndex(), partitionResult.errorCode());
                        callback.accept(Optional.of(Errors.forCode(partitionResult.errorCode())));
                        return;
                    }
                    if (partitionResult.state() != MirrorPartitionState.STOPPED.value()) {
                        log.error("Partition {}-{} is not in STOPPED state. Current state: {}.",
                                topicResult.name(), partitionResult.partitionIndex(), MirrorPartitionState.fromValue(partitionResult.state()));
                        callback.accept(Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATES));
                        return;
                    }
                }
            }
            sendDeleteClusterMirror(mirrorName, brokerMetadataOffset, callback);
        });
    }

    private void sendDeleteClusterMirror(String mirrorName, long brokerMetadataOffset, Consumer<Optional<Errors>> callback) {
        channelManager.sendRequest(
                new DeleteClusterMirrorRequest.Builder(mirrorName, brokerMetadataOffset),
                new ControllerRequestCompletionHandler() {
                    @Override
                    public void onTimeout() {
                        log.warn("DeleteClusterMirror request timed out");
                        callback.accept(Optional.of(Errors.REQUEST_TIMED_OUT));
                    }

                    @Override
                    public void onComplete(ClientResponse response) {
                        if (response.responseBody() instanceof DeleteClusterMirrorResponse deleteClusterMirrorResponse) {
                            if (deleteClusterMirrorResponse.data().errorCode() != Errors.NONE.code()) {
                                log.error("Error returned from delete cluster mirror request. Error code: {} and message: {}",
                                        deleteClusterMirrorResponse.data().errorCode(), deleteClusterMirrorResponse.data().errorMessage());
                                callback.accept(Optional.of(Errors.forCode(deleteClusterMirrorResponse.data().errorCode())));
                                return;
                            }
                            callback.accept(Optional.empty());
                        }
                    }
                });
    }

    String getSourceClusterId(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return (String) props.get(CommonClientConfigs.MIRROR_SOURCE_CLUSTER_ID_CONFIG);
    }

    /** Groups loaded partition states by mirror and state, then invokes the callback for each group. */
    void applyLoadedPartitionStates(ClusterMirrorUtils.StateTransitionCallback callback) {
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

    /** Looks up lineage epochs from the source cluster for failback/fan-out truncation. */
    CompletionStage<Map<TopicPartition, Integer>> sendLmeLookup(
            String mirrorName, Set<TopicPartition> topicPartitionSet) {
        Admin admin = getOrCreateSourceAdmin(mirrorName);
        List<DescribeClusterMirrorsRequestData.TopicLineage> lineages = buildTopicLineages(topicPartitionSet);
        log.info("LME lookup request for mirror {}: {}", mirrorName, lineages);
        DescribeClusterMirrorsOptions options = new DescribeClusterMirrorsOptions()
                .topicLineages(lineages);
        DescribeClusterMirrorsResult result = admin.describeClusterMirrors(List.of(mirrorName), options);

        return result.lineageEpochs().toCompletionStage().toCompletableFuture()
                .thenApply(lineageEpochs -> {
                    Map<TopicPartition, Integer> epochs = new HashMap<>();
                    if (!lineageEpochs.isEmpty()) {
                        lineageEpochs.forEach((topicId, partitionEpochs) -> {
                            Optional<String> topicName = metadataCache.getTopicName(topicId);
                            topicName.ifPresent(name ->
                                    partitionEpochs.forEach((partIdx, lme) ->
                                            epochs.put(new TopicPartition(name, partIdx), lme)));
                        });
                    }
                    log.info("LME lookup response for mirror {}: {}", mirrorName, epochs);
                    return epochs;
                })
                .orTimeout(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Builds topic lineage entries for all configured source cluster IDs.
     * The source needs to match against prior mirror configs.
     */
    private List<DescribeClusterMirrorsRequestData.TopicLineage> buildTopicLineages(
            Set<TopicPartition> topicPartitionSet) {
        Map<String, String> mirrorSourceClusterIds = new HashMap<>();
        for (String mirrorName : getConfiguredMirrors()) {
            String cid = getSourceClusterId(mirrorName);
            if (cid != null && !cid.isEmpty()) {
                mirrorSourceClusterIds.put(mirrorName, cid);
            }
        }

        Map<Uuid, List<Integer>> partitionsByTopicId = new HashMap<>();
        for (TopicPartition tp : topicPartitionSet) {
            Uuid topicId = metadataCache.getTopicId(tp.topic());
            partitionsByTopicId.computeIfAbsent(topicId, k -> new ArrayList<>()).add(tp.partition());
        }

        List<String> allSrcClusterIds = new ArrayList<>(mirrorSourceClusterIds.values());
        List<DescribeClusterMirrorsRequestData.TopicLineage> lineages = new ArrayList<>();
        for (Map.Entry<Uuid, List<Integer>> entry : partitionsByTopicId.entrySet()) {
            List<Integer> partitions = entry.getValue();
            lineages.add(new DescribeClusterMirrorsRequestData.TopicLineage()
                    .setTopicId(entry.getKey())
                    .setPartitions(partitions)
                    .setSrcClusterIds(allSrcClusterIds)
                    .setDstClusterId(clusterId));
        }
        return lineages;
    }

    /**
     * Coordinator-aware LME lookup for lineage results. For each (mirror, topic, partition),
     * reads from local cache if this broker is the coordinator, otherwise forwards a
     * ReadMirrorStates request to the coordinator broker.
     *
     * @param mirrorPartitions mirrorName -> topicName -> partition indices
     * @param callback receives mirrorName -> (TopicPartition -> LME) when all reads complete
     */
    void processLmeLookup(Map<String, Map<String, Set<Integer>>> mirrorPartitions,
                          Consumer<Map<String, Map<TopicPartition, Integer>>> callback) {
        log.debug("Processing LME lookup for {}", mirrorPartitions);
        Map<String, Map<TopicPartition, Integer>> result = new ConcurrentHashMap<>();

        // Group remote partitions by (coordinator node, mirror name)
        Map<Node, Map<String, Map<String, Set<Integer>>>> remoteByNode = new HashMap<>();

        mirrorPartitions.forEach((mirrorName, topicParts) -> {
            topicParts.forEach((topic, parts) -> {
                parts.forEach(part -> {
                    if (isLocalCoordinator(mirrorName, topic, part)) {
                        PartitionKey pk = new PartitionKey(mirrorName, topic, part);
                        int lme = lastMirrorEpochs.getOrDefault(pk, -1);
                        result.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>())
                                .put(new TopicPartition(topic, part), lme);
                    } else {
                        ClusterMirrorRecordKey key = new ClusterMirrorRecordKey(
                                mirrorName, metadataCache.getTopicId(topic), part);
                        Node node = findCoordinatorNode(key);
                        if (!node.equals(Node.noNode())) {
                            remoteByNode
                                    .computeIfAbsent(node, k -> new HashMap<>())
                                    .computeIfAbsent(mirrorName, k -> new HashMap<>())
                                    .computeIfAbsent(topic, k -> new HashSet<>())
                                    .add(part);
                        }
                    }
                });
            });
        });

        if (remoteByNode.isEmpty()) {
            log.debug("LME lookup result (local): {}", result);
            callback.accept(result);
            return;
        }

        // Count total requests: one per (node, mirror) pair
        int totalRequests = remoteByNode.values().stream()
                .mapToInt(Map::size).sum();
        AtomicInteger remaining = new AtomicInteger(totalRequests);

        remoteByNode.forEach((node, mirrorMap) -> {
            mirrorMap.forEach((mirrorName, topicParts) -> {
                ReadMirrorStatesRequestData data = new ReadMirrorStatesRequestData()
                        .setMirrorName(mirrorName);
                List<ReadMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();
                topicParts.forEach((topic, parts) -> {
                    List<ReadMirrorStatesRequestData.PartitionData> partDataList = new ArrayList<>();
                    parts.forEach(part -> partDataList.add(
                            new ReadMirrorStatesRequestData.PartitionData().setPartitionIndex(part)));
                    topicDataList.add(new ReadMirrorStatesRequestData.TopicMetadata()
                            .setName(topic).setPartitions(partDataList));
                });
                data.setTopics(topicDataList);

                mirrorStateSender.enqueue(new RequestAndCompletionHandler(
                        time.milliseconds(), node,
                        new ReadMirrorStatesRequest.Builder(data),
                        response -> {
                            if (response.responseBody() instanceof ReadMirrorStatesResponse resp) {
                                resp.data().topics().forEach(topic -> {
                                    topic.partitions().forEach(partition -> {
                                        if (partition.lastMirrorEpoch() != -1) {
                                            result.computeIfAbsent(mirrorName,
                                                            k -> new ConcurrentHashMap<>())
                                                    .put(new TopicPartition(topic.name(),
                                                                    partition.partitionIndex()),
                                                            partition.lastMirrorEpoch());
                                        }
                                    });
                                });
                            }
                            if (remaining.decrementAndGet() == 0) {
                                log.debug("LME lookup result (coordinator): {}", result);
                                callback.accept(result);
                            }
                        }
                ));
            });
        });
    }

    /** Upserts added paritions, returning the full epoch map for record serialization. */
    Map<PartitionKey, Integer> updateLastMirrorEpochs(
            String clusterName, Map<String, Map<Integer, Integer>> addedEpochs) {
        addedEpochs.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirrorEpochs.put(new PartitionKey(clusterName, topic, partition), offset);
            });
        });
        return lastMirrorEpochs;
    }

    private record TimeoutHandler(Logger log) implements ControllerRequestCompletionHandler {
        @Override
        public void onTimeout() {
            log.warn("Controller request timed out");
        }

        @Override
        public void onComplete(ClientResponse response) {
            log.debug("Controller request completed: {}", response);
        }
    }

    private record SourceTopicState(String topic, Uuid topicId, boolean exists, List<SourcePartitionState> partitions) { }
    private record SourcePartitionState(TopicPartition topicPartition, Node leader, Optional<Integer> leaderEpoch) { }
    private record SourceAclChanges(List<AclBinding> aclsToAdd, List<AclBinding> aclsToDelete) { }
}
