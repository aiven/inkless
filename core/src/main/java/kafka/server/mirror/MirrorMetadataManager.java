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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BumpLeaderEpochsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BumpLeaderEpochsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.StartMirrorTopicsRequest;
import org.apache.kafka.common.requests.StopMirrorTopicsRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordKey;
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
import org.apache.kafka.server.config.ClusterMirrorConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.network.BrokerEndPoint;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;

import java.time.Duration;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.Option;

import static kafka.server.mirror.ClusterMirrorUtils.LEADER_EPOCH_BUMP_INCREMENT;
import static kafka.server.mirror.ClusterMirrorUtils.LEADER_EPOCH_BUMP_THRESHOLD;
import static kafka.server.mirror.ClusterMirrorUtils.originalMirrorName;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.controller.ConfigurationControlManager.PAUSED_TOPIC_SUFFIX;
import static org.apache.kafka.controller.ConfigurationControlManager.STOPPED_TOPIC_SUFFIX;

/**
 * Bridges the local destination cluster and remote source clusters for Cluster Mirroring.
 *
 * Implements {@link MetadataPublisher} to detect leadership and config changes, triggering
 * partition state transitions. Manages remote cluster connections using {@link MirrorSourceSender}
 * and periodically syncs topic metadata, configs, group offsets, and ACLs from source clusters.
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
    private final Logger log;
    private volatile boolean isInitialized = false;
    private final String name;
    private final int nodeId;

    private final KafkaConfig brokerConfig;
    private final NodeToControllerChannelManager channelManager;
    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private final MetadataCache metadataCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;
    private final Time time;

    private final Random random = new Random();
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;

    private volatile MirrorStateSender mirrorStateSender;
    private final Map<String, Admin> srcAdmins = new ConcurrentHashMap<>();
    private volatile Admin dstAdmin;

    private final Map<String, Uuid> sourceClusterIds = new ConcurrentHashMap<>();
    private final Map<String, List<MirrorSourceSender>> sourceSenders = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, ClusterMirrorUtils.LeaderInfo>> sourceLeaders = new ConcurrentHashMap<>();
    private final Map<ClusterMirrorUtils.PartitionKey, MirrorPartitionState> partitionStates = new ConcurrentHashMap<>();
    private final Map<ClusterMirrorUtils.PartitionKey, MirrorPartitionState> partitionPreviousStates = new ConcurrentHashMap<>();
    private final Map<ClusterMirrorUtils.PartitionKey, Integer> lastMirrorEpochs = new ConcurrentHashMap<>();
    private final Map<MirrorPartitionState, AtomicLong> partitionStateCounts = new ConcurrentHashMap<>();

    private Optional<ClusterMirrorUtils.StateTransitioner> stateTransitioner = Optional.empty();
    private Optional<Consumer<String>> mirrorDeletionHandler = Optional.empty();
    private Optional<Function<ClusterMirrorRecordKey, Integer>> coordPartitionFinderByKey = Optional.empty();
    private Optional<Function<String, Integer>> coordPartitionFinderByName = Optional.empty();

    // lets the transition handler skip partitions that are already being processed
    private final Map<TopicPartition, MirrorPartitionState> pendingPartitionStates = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Integer> failedRetryAttempts = new ConcurrentHashMap<>();
    private final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();

    // Leader epoch bumps require a request to the controller followed by a metadata log fetch.
    // The bump must be confirmed on the broker side before we can write the PID reset record.
    private final Set<ClusterMirrorUtils.LeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();

    private final AtomicLong metadataRefreshError;
    private final AtomicLong topicConfigSyncError;
    private final AtomicLong consumerGroupOffsetSyncError;
    private final AtomicLong shareGroupOffsetSyncError;
    private final AtomicLong aclSyncError;

    public MirrorMetadataManager(
        KafkaConfig brokerConfig,
        NodeToControllerChannelManager channelManager,
        Supplier<ReplicaManager> replicaManagerSupplier,
        MetadataCache metadataCache,
        KafkaScheduler scheduler,
        Metrics metrics,
        Time time
    ) {
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

    /** Wires in the state transitioner, tombstone handler, and coordinator partition finders. */
    public void initialize(ClusterMirrorUtils.StateTransitioner stateTransitioner,
                           Consumer<String> tombStoneHandler,
                           Function<ClusterMirrorRecordKey, Integer> coordinatorPartitionByKeyFinder,
                           Function<String, Integer> coordinatorPartitionByNameFinder) {
        if (mirrorStateSender == null) {
            mirrorStateSender = new MirrorStateSender(MirrorStateSender.class.getSimpleName(),
                    NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                    brokerConfig.requestTimeoutMs(), Time.SYSTEM);
            mirrorStateSender.start();
        }

        this.stateTransitioner = Optional.of(stateTransitioner);
        this.mirrorDeletionHandler = Optional.of(tombStoneHandler);
        this.coordPartitionFinderByKey = Optional.of(coordinatorPartitionByKeyFinder);
        this.coordPartitionFinderByName = Optional.of(coordinatorPartitionByNameFinder);

        this.isInitialized = true;
    }

    private Admin getOrCreateSourceAdmin(String mirrorName) {
        return srcAdmins.computeIfAbsent(mirrorName, k -> {
            Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, k));
            return Admin.create(props);
        });
    }

    private Admin getOrCreateDestAdmin() {
        if (dstAdmin == null) {
            var endpoint = brokerConfig.effectiveAdvertisedBrokerListeners().head();
            Properties props = new Properties();
            props.put(BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
            dstAdmin = Admin.create(props);
        }
        return dstAdmin;
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
        if (!isInitialized) {
            return;
        }

        // Caching the metadata image for query purpose
        this.metadataImage = newImage;

        Set<String> reconnectedMirrors = maybeRecreateSourceConnection(delta, newImage);

        // Get all mirror partition leaders on this node based on the delta
        Set<TopicPartition> mirrorLeaders = getMirrorLeadersAndClearFollowerStates(delta, newImage);

        // After a connection config change (e.g. bootstrap.servers), fetchers were removed
        // and must be recreated by re-evaluating MIRRORING partitions for the affected mirrors
        if (!reconnectedMirrors.isEmpty()) {
            partitionStates.forEach((key, state) -> {
                if (reconnectedMirrors.contains(key.mirrorName()) && state == MirrorPartitionState.MIRRORING) {
                    mirrorLeaders.add(new TopicPartition(key.topic(), key.partition()));
                }
            });
        }

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
            String mirrorName = ClusterMirrorUtils.originalMirrorName(rawMirrorName);

            ClusterMirrorUtils.PartitionKey key = new ClusterMirrorUtils.PartitionKey(mirrorName, tp.topic(), tp.partition());
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
                                ClusterMirrorUtils.PartitionKey mpk = new ClusterMirrorUtils.PartitionKey(mirrorName, resTp.topic(), resTp.partition());
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
        failedRetryAttempts.clear();
        partitionStates.clear();
        partitionPreviousStates.clear();
        partitionStateCounts.clear();
        lastMirrorEpochs.clear();
        sourceClusterIds.clear();

        // snapshot and clear first, then close to avoid use-after-close races
        Map<String, List<MirrorSourceSender>> snapshot = new HashMap<>(sourceSenders);
        sourceSenders.clear();
        snapshot.values().forEach(senders -> senders.forEach(MirrorSourceSender::close));

        sourceLeaders.clear();
        pendingLeaderEpochBumps.clear();
        pendingPartitionStates.clear();
    }

    /** Returns partition states pending coordinator write. */
    public Map<TopicPartition, MirrorPartitionState> pendingPartitionStates() {
        return pendingPartitionStates;
    }

    /** Returns the per-partition count of consecutive failed retry attempts. */
    public Map<TopicPartition, Integer> failedRetryAttempts() {
        return failedRetryAttempts;
    }

    /** Returns the previous state of each partition before its last transition. */
    public Map<ClusterMirrorUtils.PartitionKey, MirrorPartitionState> partitionPreviousStates() {
        return partitionPreviousStates;
    }

    /** Transitions a mirror partition to the given state via the configured state transitioner. */
    public void transitionTo(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        stateTransitioner.ifPresent(st -> st.transitionTo(mirrorName, topicPartition, state));
    }

    private static final Set<String> NON_CONNECTION_CONFIGS = Set.of(
            ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_GROUPS_INCLUDE_CONFIG, ClusterMirrorConfig.MIRROR_GROUPS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.MIRROR_ACL_INCLUDE_CONFIG);

    private Set<String> maybeRecreateSourceConnection(MetadataDelta delta, MetadataImage newImage) {
        Set<String> reconnectedMirrors = new HashSet<>();
        if (delta.configsDelta() != null) {
            delta.configsDelta().changes().entrySet().stream()
                    .filter(e -> e.getKey().type() == ConfigResource.Type.CLUSTER_MIRROR)
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
                            Admin admin = srcAdmins.remove(mirrorName);
                            if (admin != null) {
                                admin.close();
                            }
                            replicaManagerSupplier.get().mirrorFetcherManager().removeFetchersForMirror(mirrorName);
                            if (!mirrorDeleted) {
                                reconnectedMirrors.add(mirrorName);
                            }
                        }
                    });
        }
        return reconnectedMirrors;
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
     *   2. if it's in UNKNOWN or STOPPED state, we should move it to LOG_TRUNCATION state. It will happen when users startMirrorTopics
     *   3. else, keep the same state as is. This could happen like leadership change, and the new leader should continue to complete the process in previous leader
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


    /** Periodic metadata sync entry point. */
    void periodicSync() {
        Set<String> mirrors = getConfiguredMirrors();
        if (mirrors.isEmpty()) {
            return;
        }

        log.info("Syncing metadata for mirrors: {}", mirrors);
        mirrors.forEach(this::ensureConnection);

        // Snapshot keyset to avoid ConcurrentModificationException
        for (String mirrorName : Set.copyOf(sourceSenders.keySet())) {
            try {
                discoverSourceBrokers(mirrorName);
                var topicsState = syncSourceTopicsState(mirrorName);
                syncCoordinatorMetadata(mirrorName, topicsState);
            } catch (Exception e) {
                log.error("Failed to sync metadata for mirror {}", mirrorName, e);
            }
        }

        // TODO: This is incremented on every metadata refresh for testing purpose, as we don't have error handling at this stage
        metadataRefreshError.incrementAndGet();
    }

    /** Schedules a source cluster metadata update. */
    public void scheduleSourceClusterMetadataUpdate(String mirrorName) {
        scheduler.scheduleOnce("SourceClusterMetadataUpdate", () -> {
            discoverSourceBrokers(mirrorName);
            syncSourceTopicsState(mirrorName);
        });
    }

    /**
     * Discovers source cluster brokers via metadata and adds senders for any newly found brokers.
     * Sender cleanup only happens via {@link #onMetadataUpdate} or {@link #clearCache}.
     */
    private void discoverSourceBrokers(String mirrorName) {
        Collection<Node> discoveredBrokers = describeSourceClusterNodes(mirrorName);
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

        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(props);

        List<MirrorSourceSender> addedSenders = new ArrayList<>();
        for (Node broker : newBrokers) {
            try {
                BrokerEndPoint endpoint = new BrokerEndPoint(broker.id(), broker.host(), broker.port());
                String clientId = "nodeId-" + nodeId + "-" + mirrorName + "-" + broker.host() + "-" + broker.port();
                LogContext logContext = new LogContext("[" + MirrorMetadataManager.class.getSimpleName() + "Sender id=" + nodeId + " clientId=" + clientId + "] ");
                addedSenders.add(ClusterMirrorUtils.createSender(endpoint, mirrorConfig, brokerConfig, metrics, time, clientId, logContext));
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

    private Collection<Node> describeSourceClusterNodes(String mirrorName) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);
        try {
            var clusterResult = srcAdmin.describeCluster();
            String clusterId = clusterResult.clusterId().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            Collection<Node> nodes = clusterResult.nodes().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            validateSourceClusterId(mirrorName, clusterId);
            return nodes;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Failed to describe source cluster for mirror {}", mirrorName, e);
            return List.of();
        }
    }

    private void validateSourceClusterId(String mirrorName, String clusterId) {
        if (clusterId != null && !clusterId.isEmpty()) {
            Uuid newClusterId = Uuid.fromString(clusterId);
            Uuid previousClusterId = sourceClusterIds.put(mirrorName, newClusterId);
            if (previousClusterId != null && !previousClusterId.equals(newClusterId)) {
                throw new IllegalStateException("Source cluster ID changed for mirror " + mirrorName
                        + ": expected " + previousClusterId + ", got " + newClusterId
                        + ". This may indicate a misconfiguration or that the source cluster has been replaced.");
            }
        }
    }

    /**
     * Fetches topics metadata from the source cluster via Admin.describeTopics.
     * Runs on every broker to keep partition leaders, topic creation, topic deletion, and partition counts in sync.
     */
    private Optional<List<SourceTopicState>> syncSourceTopicsState(String mirrorName) {
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
                                t.transitionTo(mirrorName, new TopicPartition(key.topic(), key.partition()), MirrorPartitionState.STOPPING)));
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
            ClusterMirrorUtils.PartitionKey key = new ClusterMirrorUtils.PartitionKey(mirrorName, tp.topic(), tp.partition());
            if (partitionStates.getOrDefault(key, MirrorPartitionState.UNKNOWN) != MirrorPartitionState.UNKNOWN) {
                return;
            }
            TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
            if (topicImage == null) {
                return;
            }
            var partition = topicImage.partitions().get(tp.partition());
            if (partition != null && partition.leader == nodeId) {
                log.info("Source leader for {} discovered after initial onMetadataUpdate, transitioning to LOG_TRUNCATION", tp);
                stateTransitioner.ifPresent(t -> t.transitionTo(mirrorName, tp, MirrorPartitionState.LOG_TRUNCATION));
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
        ensureConnection(mirrorName);

        try {
            ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(
                    metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName)));
            syncTopicConfigs(mirrorName, mirrorConfig);
            syncGroupOffsets(mirrorName, mirrorConfig);
            syncAcls(mirrorName, mirrorConfig);
            maybeBumpLeaderEpochs(mirrorName, topicInfos, Set.of());
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
                            && !entry.name().equals(TopicConfig.MIRROR_NAME_CONFIG)
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

        Set<String> mirrorTopics = getConfiguredTopics(mirrorName, false);
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
                    GroupState.RECONCILING,
                    GroupState.EMPTY));
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

            List<ClusterMirrorConfig.AclRule> aclIncludeRules = mirrorConfig.aclIncludeRules();
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

        List<StartMirrorTopicsRequestData.TopicData> newTopics;
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
                    .map(td -> new StartMirrorTopicsRequestData.TopicData()
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
                                    entry.getValue().changes().get(TopicConfig.MIRROR_NAME_CONFIG).isPresent()
                                    && !entry.getValue().changes().get(TopicConfig.MIRROR_NAME_CONFIG).get().isBlank())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // get all topics from the resources
            Set<String> topicsWithMirrorNameChanged = mirrorNameChanged.keySet().stream()
                    .filter(configResource -> configResource.type().equals(ConfigResource.Type.TOPIC))
                    .map(ConfigResource::name).collect(Collectors.toSet());

            // get the partition leader is the local node
            topicsWithMirrorNameChanged.forEach(topic -> {
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

    /** Clears state for partitions where this broker lost leadership, unless it is the coordinator */
    private void clearFollowersState(Set<TopicPartition> followerDelta, MetadataImage newImage) {
        followerDelta.forEach(followerTp -> {
            String mirrorName = (String) newImage.configs()
                    .configProperties(new ConfigResource(ConfigResource.Type.TOPIC, followerTp.topic()))
                    .get(TopicConfig.MIRROR_NAME_CONFIG);
            if (mirrorName != null && !mirrorName.isEmpty() && !isLocalCoordinator(mirrorName, followerTp.topic(), followerTp.partition())) {
                String updatedMirrorName = originalMirrorName(mirrorName);
                ClusterMirrorUtils.PartitionKey key = new ClusterMirrorUtils.PartitionKey(updatedMirrorName, followerTp.topic(), followerTp.partition());
                removePartitionState(key);
                lastMirrorEpochs.remove(key);
            }
        });
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
                                ClusterMirrorUtils.PartitionKey mpk = new ClusterMirrorUtils.PartitionKey(
                                        mirrorName, topic.name(), partition.partitionIndex());
                                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                                if (partition.lastMirrorEpoch() != -1) {
                                    lastMirrorEpochs.put(mpk, partition.lastMirrorEpoch());
                                }
                                if (partition.state() != -1) {
                                    partitionStates.put(mpk, MirrorPartitionState.fromValue(partition.state()));
                                }
                                if (partition.state() == MirrorPartitionState.FAILED.value()) {
                                    partitionPreviousStates.put(mpk, MirrorPartitionState.fromValue(partition.previousState()));
                                    failedRetryAttempts.put(tp, (int) partition.retryAttempt());
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
                ClusterMirrorUtils.PartitionKey pk = new ClusterMirrorUtils.PartitionKey(mirrorName, tp, part);
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
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("Remote bootstrap server not found in Cluster Mirror config: " + mirrorName));

        log.info("Mirror config for '{}': {}", mirrorName, props);

        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(props);
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

    /** Updates cached source leader for a specific partition. */
    public void updateSourceLeader(String mirrorName, TopicPartition tp, ClusterMirrorUtils.LeaderInfo leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    /** Resolves source partition leader from cache, falling back to bootstrap server if not cached. */
    public ClusterMirrorUtils.LeaderInfo resolveSourceLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            ClusterMirrorUtils.LeaderInfo leader = partitionLeaders.get(tp);
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
            log.info("No cached leader for mirror {} partition {}. Using one of the bootstrap server as initial target.", mirrorName, tp);
            BrokerEndPoint ep = senders.get(random.nextInt(senders.size())).brokerEndPoint();
            // set leaderEpoch to 0 if unknown to trigger source epoch discovery through fencing
            return new ClusterMirrorUtils.LeaderInfo(new Node(ep.id(), ep.host(), ep.port()), 0);
        }

        throw new IllegalStateException("No source senders available for mirror " + mirrorName);
    }

    // Atomic per-key update to keep partition state counts consistent
    void updatePartitionState(ClusterMirrorUtils.PartitionKey key, MirrorPartitionState newState) {
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
    void removePartitionState(ClusterMirrorUtils.PartitionKey key) {
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
        return partitionStates.get(new ClusterMirrorUtils.PartitionKey(updatedMirrorName, topicPartition.topic(), topicPartition.partition()));
    }

    /** Schedules a source metadata sync followed by a leader epoch bump request. */
    public CompletableFuture<Void> scheduleBumpLeaderEpochs(String mirrorName, Set<TopicPartition> topicPartitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.scheduleOnce("bump-leader-epoch", () -> {
            ensureConnection(mirrorName);
            discoverSourceBrokers(mirrorName);
            Optional<List<SourceTopicState>> topics = syncSourceTopicsState(mirrorName);
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

        pendingLeaderEpochBumps.add(new ClusterMirrorUtils.LeaderEpochBump(future, Collections.unmodifiableMap(partitionMinEpochs)));
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
        return metadataImage.configs().resourceData().entrySet().stream()
                .filter(configEntry -> {
                    if (configEntry.getKey().type() != ConfigResource.Type.TOPIC) return false;
                    String topicMirrorName = configEntry.getValue().data().get(TopicConfig.MIRROR_NAME_CONFIG);
                    if (topicMirrorName == null) return false;
                    if (!includeStopped && topicMirrorName.endsWith(STOPPED_TOPIC_SUFFIX)) return false;
                    if (!includePaused && topicMirrorName.endsWith(PAUSED_TOPIC_SUFFIX)) return false;
                    return mirrorName.equals(ClusterMirrorUtils.originalMirrorName(topicMirrorName));
                })
                .map(configEntry -> configEntry.getKey().name())
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

    Uuid getSourceClusterId(String mirrorName) {
        Uuid cached = sourceClusterIds.get(mirrorName);
        if (cached != null) {
            return cached;
        }
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);
        try {
            String clusterId = srcAdmin.describeCluster()
                .clusterId().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            if (clusterId != null && !clusterId.isEmpty()) {
                cached = Uuid.fromString(clusterId);
                sourceClusterIds.put(mirrorName, cached);
                return cached;
            }
        } catch (Exception e) {
            log.warn("Failed to resolve source cluster ID for mirror {}", mirrorName, e);
        }
        return null;
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

    /** Truncates local replicas using last mirrored leader epochs from this broker's coordinator cache. */
    CompletionStage<Map<String, ClusterMirrorDescription>> truncateToLastMirrorEpochs(
            String mirrorName, Set<TopicPartition> topicPartitionSet) {
        log.info("Truncating to last mirrored epochs from local state for mirror {}: {}", mirrorName, topicPartitionSet);
        Admin admin = srcAdmins.computeIfAbsent(mirrorName, k -> {
            Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, k));
            return Admin.create(props);
        });
        DescribeClusterMirrorsResult result = admin.describeClusterMirrors(List.of(mirrorName));
        return result.allDescriptions().toCompletionStage();
    }

    /** Removes stopped partition epochs and upserts added ones, returning the full epoch map for record serialization. */
    Map<ClusterMirrorUtils.PartitionKey, Integer> updateLastMirrorEpochs(
            String clusterName, Map<String, Map<Integer, Integer>> addedEpochs, Map<String, Map<Integer, Integer>> stoppedEpochs) {
        stoppedEpochs.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirrorEpochs.remove(new ClusterMirrorUtils.PartitionKey(clusterName, topic, partition));
            });
        });
        addedEpochs.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirrorEpochs.put(new ClusterMirrorUtils.PartitionKey(clusterName, topic, partition), offset);
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
