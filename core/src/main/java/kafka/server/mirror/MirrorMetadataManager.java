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
import kafka.server.metadata.KRaftMetadataCache;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
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
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
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
import org.apache.kafka.coordinator.group.GroupCoordinator;
import org.apache.kafka.coordinator.mirror.MirrorRecordKey;
import org.apache.kafka.image.LocalReplicaChanges;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.config.MirrorConfig;
import org.apache.kafka.server.network.BrokerEndPoint;

import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.server.util.Scheduler;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Component that provides cluster state synchronization for cluster mirroring.
 *
 * The synchronized state include:
 * - Topic metadata (leaders, partitions, configurations)
 * - Consumer group offsets and state
 * - Access Control Lists (ACLs)
 * - Automatic partition expansion when remote clusters scale
 * - Topic deletion detection and propagation
 *
 * The manager maintains persistent connections to remote clusters via bootstrap servers
 * configured in cluster mirror settings, and periodically polls for changes to keep the
 * local cluster state synchronized with remote cluster state.
 *
 * Key responsibilities:
 * - Establishes and manages connections to remote brokers using MirrorBlockingSender
 * - Monitors remote cluster topology and partition leadership changes
 * - Synchronizes topic configurations between clusters
 * - Mirrors consumer group offsets to maintain consistency across clusters
 * - Replicates ACL policies to ensure security posture alignment
 * - Handles dynamic partition scaling by detecting remote partition count changes
 * - Propagates topic deletions from remote clusters to maintain consistency
 *
 * This component is essential for cluster mirroring scenarios where multiple Kafka clusters
 * need to maintain synchronized state for disaster recovery, cross-region replication,
 * or federated deployment architectures.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataPublisher, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MirrorMetadataManager.class);
    private static final ResourcePatternFilter ANY_RESOURCE = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
    private static final AclBindingFilter ANY_RESOURCE_ACL = new AclBindingFilter(ANY_RESOURCE, AccessControlEntryFilter.ANY);

    private final KafkaConfig brokerConfig;
    private final int nodeId;
    // Mapping from remote bootstrap servers to its corresponding broker sender and topics.
    // TODO: A better key might be a cluster id or cluster-mirror. For now, we use remote bootstrap servers for demo.
    private final Map<String, List<MirrorBlockingSender>> remoteBrokers;
    private final Map<String, Set<String>> topics;
    private final Map<String, Map<Integer, Node>> remoteClusterNodes;
    private final Map<String, Map<TopicPartition, Node>> remotePartitionLeaders;
    private final Metrics metrics;
    private final Time time;
    private final Random random;
    private MetadataImage metadataImage;
    private MetadataCache metadataCache;
    private NodeToControllerChannelManager channelManager;
    private final Supplier<GroupCoordinator> groupCoordinatorSupplier;
    private Map<MirroredPartitionKey, Long> lastMirroredOffsets = new ConcurrentHashMap<>();
    private Map<MirroredPartitionKey, MirrorState> mirroredPartitionState = new ConcurrentHashMap<>();
    private Map<MirroredPartitionKey, Runnable> onMirroringWaitingList = new ConcurrentHashMap<>();
//    private Map<String, Set<String>> onPreparingWaitingList = new ConcurrentHashMap<>();
    private Map<String, Node> coordinatorNodesCache = new ConcurrentHashMap<>();
    private InterBrokerSender interBrokerSender;
    private Optional<MirrorCoordinator.Transitioner> transitioner = Optional.empty();
    private Optional<Function<MirrorRecordKey, Integer>> keyToPartition = Optional.empty();

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
        this.remoteBrokers = new HashMap<>();
        this.topics = new HashMap<>();
        this.remoteClusterNodes = new HashMap<>();
        this.remotePartitionLeaders = new HashMap<>();
        this.metrics = metrics;
        this.time = time;
        this.metadataImage = MetadataImage.EMPTY;
        this.random = new Random();
        this.metadataCache = metadataCache;
        this.channelManager = channelManager;
        this.groupCoordinatorSupplier = groupCoordinatorSupplier;

        KafkaClient networkClient = NetworkUtils.buildNetworkClient("MirrorMetadataManager", config, metrics, time, new LogContext(name()));
        this.interBrokerSender = new InterBrokerSender("mirror-inter-sender", networkClient, config.requestTimeoutMs(), Time.SYSTEM);
        interBrokerSender.start();
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

    /**
     * Called when cluster metadata is updated.
     * Updates the local metadata image to reflect the latest cluster state.
     *
     * @param delta the metadata delta containing changes
     * @param newImage the new complete metadata image
     * @param manifest the loader manifest with provenance information
     */
    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        this.metadataImage = newImage;

//        if (delta.configsDelta() != null)
//            LOG.info("!!! onMetadataUpdate config: {}", delta.configsDelta().changes())
        if (delta.topicsDelta() != null) {
            LOG.info("!!! onMetadataUpdate: {}", delta.topicsDelta().localChanges(nodeId).readOnlyLeaders());
            delta.topicsDelta().localChanges(nodeId).readOnlyLeaders().entrySet().forEach(entry -> {
                TopicPartition tp = entry.getKey();
                LocalReplicaChanges.PartitionInfo info = entry.getValue();
                boolean shouldStopping;
                String mirrorName = info.partition().mirrorName;
                if (mirrorName.endsWith("*")) {
                    shouldStopping = true;
                    mirrorName = mirrorName.substring(0, mirrorName.length() - 1);
                } else {
                    shouldStopping = false;
                }
                MirroredPartitionKey key = new MirroredPartitionKey(mirrorName, tp.topic(), tp.partition());
                if (mirroredPartitionState.containsKey(key)) {
                    transitioner.ifPresent(t -> {
                        if (shouldStopping) {
                            t.transitionTo(key.mirrorName, tp, MirrorState.STOPPING);
                        } else {
                            // moving to preparing_mirroring if it's starting because it's waiting for metadata update.
                            // otherwise, move to what the current state is
                            if (mirroredPartitionState.get(key) == MirrorState.STARTING) {
                                t.transitionTo(key.mirrorName, tp, MirrorState.PREPARING_MIRRORING);
                            } else {
                                t.transitionTo(key.mirrorName, tp, mirroredPartitionState.get(key));
                            }
                        }
                    });
                } else {
                    // get the state from remote coordinator
                    readStatesToRemoteCoordinator(key.mirrorName, Map.of(tp.topic(), Set.of(tp.partition())), res -> {
                        res.data().topics().forEach(topic -> {
                            topic.partitions().forEach(partition -> {
                                if (partition.state() != -1) {
                                    MirrorState state = MirrorState.fromValue(partition.state());
                                    mirroredPartitionState.put(new MirroredPartitionKey(key.mirrorName, tp.topic(), tp.partition()), state);
                                    transitioner.ifPresent(t -> {
                                        if (shouldStopping) {
                                            t.transitionTo(key.mirrorName, tp, MirrorState.STOPPING);
                                        } else {
                                            if (state == MirrorState.STARTING) {
                                                t.transitionTo(key.mirrorName, tp, MirrorState.PREPARING_MIRRORING);
                                            } else {
                                                t.transitionTo(key.mirrorName, tp, state);
                                            }
                                        }
                                    });
                                }
                            });
                        });
                    });
                }
            });
        }
    }

    /**
     * Closes the metadata manager and releases resources.
     */
    @Override
    public void close() throws Exception {
    }

    public Node getMirrorCoordinator(MirrorRecordKey key) {
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
                    if (keyToPartition.isEmpty()) {
                        return Node.noNode();
                    }
                    int partition = keyToPartition.get().apply(key);
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

    public void setTransitioner(MirrorCoordinator.Transitioner transitioner) {
        this.transitioner = Optional.of(transitioner);
    }

    public void setKeyToPartition(Function<MirrorRecordKey, Integer> function) {
        this.keyToPartition = Optional.of(function);
    }

//    public Node findCoordinator(String mirrorName) {
//        LOG.info("!!! findCoordinator: {}", mirrorName);
//        return getMirrorCoordinator(new MirrorRecordKey(mirrorName));


//        FindCoordinatorRequest.Builder request = new FindCoordinatorRequest.Builder(
//                new FindCoordinatorRequestData()
//                        .setKeyType(FindCoordinatorRequest.CoordinatorType.MIRROR.id())
//                        .setKey(mirrorName)
//                        .setCoordinatorKeys(List.of())
//        );
//        CompletableFuture<Node> future = new CompletableFuture<>();
//        interBrokerSender.enqueue(new RequestAndCompletionHandler(
//                time.milliseconds(),
//                bootstrapServerNode,
//                request,
//                response -> {
//                    LOG.info("!!! ClientResponse: {}", response);
//                    if (response.responseBody() instanceof FindCoordinatorResponse findCoordinatorResponse) {
//                        LOG.info("!!! findCoordinatorResponse: {}", findCoordinatorResponse);
////                        Map<Errors, Integer> errorCounts = findCoordinatorResponse.errorCounts();
////                        if (errorCounts.containsKey(Errors.COORDINATOR_NOT_AVAILABLE) || errorCounts.containsKey(Errors.COORDINATOR_LOAD_IN_PROGRESS)) {
////                            try {
////                                Thread.sleep(500);
////                            } catch (InterruptedException e) {
////                                throw new RuntimeException(e);
////                            }
////                            findCoordinator(mirrorName);
////                            return;
////                        }
//                        coordinatorNodesCache.put(mirrorName, findCoordinatorResponse.node());
//                        future.complete(findCoordinatorResponse.node());
//                    }
//                }
//        ));



//    }

    public void writeStatesToRemoteCoordinator(String mirrorName,
                                               Map<String, Set<MirroredPartitionMetadata>> topicMetadata,
                                               Set<String> removedTopics,
                                               Consumer<WriteMirrorStatesResponse> responseCallback) {
        LOG.info("!!! writeStatesToRemoteCoordinator: {} {} {}", mirrorName, topicMetadata, removedTopics);

        Node coordinatorNode = coordinatorNodesCache.get(mirrorName);
        if (coordinatorNode == null) {
            coordinatorNode = getMirrorCoordinator(new MirrorRecordKey(mirrorName));
            if (coordinatorNode.equals(Node.noNode())) {
                // coordinator is not available, return it now.
                LOG.error("!!! coordinator is not available, return it now.");
                return;
            }
        }

        coordinatorNodesCache.put(mirrorName, coordinatorNode);

        WriteMirrorStatesRequestData data = new WriteMirrorStatesRequestData().setMirrorName(mirrorName);
        List<WriteMirrorStatesRequestData.TopicState> topicStates = new ArrayList<>();

        topicMetadata.forEach((t, metadata) -> {
            List<WriteMirrorStatesRequestData.PartitionState> partitionStates = new ArrayList<>();
            metadata.forEach(m -> {
                WriteMirrorStatesRequestData.PartitionState partitionState = new WriteMirrorStatesRequestData.PartitionState();
                partitionState.setState(m.state() == null ? (byte) -1 : m.state.value());
                partitionState.setLastMirroredOffset(m.offset);
                partitionState.setPartitionIndex(m.partition);
                partitionStates.add(partitionState);
            });
            WriteMirrorStatesRequestData.TopicState state = new WriteMirrorStatesRequestData.TopicState().setName(t).setPartitions(partitionStates);
            topicStates.add(state);
        });
        data.setTopicsUpdated(topicStates);
        data.setRemovedTopics(new ArrayList<>(removedTopics));

        // send
        interBrokerSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                coordinatorNode,
                new WriteMirrorStatesRequest.Builder(data),
                response -> {
                    LOG.info("!!! writeStatesToRemoteCoordinator onComplete: {}", response.responseBody());
                    if (response.responseBody() instanceof WriteMirrorStatesResponse writeMirrorStatesResponse) {
                        responseCallback.accept(writeMirrorStatesResponse);
                    }
                }
        ));
    }



    public void readStatesToRemoteCoordinator(String mirrorName,
                                              Map<String, Set<Integer>> partitions,
                                              Consumer<ReadMirrorStatesResponse> onWriteComplete) {
        LOG.info("!!! readStatesToRemoteCoordinator: {} {}", mirrorName, partitions);

        Node coordinatorNode = coordinatorNodesCache.get(mirrorName);
        if (coordinatorNode == null) {
            coordinatorNode = getMirrorCoordinator(new MirrorRecordKey(mirrorName));
            if (coordinatorNode.equals(Node.noNode())) {
                // coordinator is not available, return it now.
                LOG.error("!!! coordinator is not available, return it now.");
                return;
            }
        }
        coordinatorNodesCache.put(mirrorName, coordinatorNode);

        ReadMirrorStatesRequestData data = new ReadMirrorStatesRequestData().setMirrorName(mirrorName);
        List<ReadMirrorStatesRequestData.TopicState> topicStates = new ArrayList<>();

        partitions.forEach((tp, parts) -> {
            ReadMirrorStatesRequestData.TopicState state = new ReadMirrorStatesRequestData.TopicState().setName(tp);
            List<ReadMirrorStatesRequestData.PartitionState> partitionStates = new ArrayList<>();
            parts.forEach(part -> {
                ReadMirrorStatesRequestData.PartitionState partitionState = new ReadMirrorStatesRequestData.PartitionState();
                partitionState.setPartitionIndex(part);
                partitionStates.add(partitionState);
            });
            state.setPartitions(partitionStates);
            topicStates.add(state);
        });
        data.setMirrorName(mirrorName).setTopics(topicStates);

        // send
        interBrokerSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                coordinatorNode,
                new ReadMirrorStatesRequest.Builder(data),
                response -> {
                    if (response.responseBody() instanceof ReadMirrorStatesResponse readMirrorStatesResponse) {
                        LOG.info("!!! readMirrorStatesResponse onComplete: {}", response.responseBody());
                        onWriteComplete.accept(readMirrorStatesResponse);

                        // update cache
                        readMirrorStatesResponse.data().topics().forEach(topic -> {
                            topic.partitions().forEach(partition -> {
                                if (partition.lastMirroredOffset() != -1) {
                                    lastMirroredOffsets.put(new MirroredPartitionKey(mirrorName, topic.name(), partition.partitionIndex()), partition.lastMirroredOffset());
                                }
                                if (partition.state() != -1) {
                                    mirroredPartitionState.put(new MirroredPartitionKey(mirrorName, topic.name(), partition.partitionIndex()), MirrorState.fromValue(partition.state()));
                                }
                                topics.compute(mirrorName, (key, oldVal) -> {
                                    if (oldVal == null) {
                                        return Set.of(topic.name());
                                    } else {
                                        Set<String> newSet = new HashSet<>(oldVal);
                                        newSet.add(topic.name());
                                        return newSet;
                                    }
                                });
                            });
                        });
                    }
                }
        ));
    }

    public void readStatesFromCache(String mirrorName,
                                    Map<String, Set<Integer>> partitions,
                                    Consumer<ReadMirrorStatesResponse> responseCallback) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        List<ReadMirrorStatesResponseData.TopicState> topicStates = new ArrayList<>();
        partitions.forEach((tp, parts) -> {
            ReadMirrorStatesResponseData.TopicState state = new ReadMirrorStatesResponseData.TopicState().setName(tp);
            List<ReadMirrorStatesResponseData.PartitionState> partitionStates = new ArrayList<>();
            parts.forEach(part -> {
                ReadMirrorStatesResponseData.PartitionState partitionState = new ReadMirrorStatesResponseData.PartitionState();
                partitionState.setPartitionIndex(part);
                partitionState.setLastMirroredOffset(lastMirroredOffsets.getOrDefault(new MirroredPartitionKey(mirrorName, tp, part), -1L));
                partitionState.setState(mirroredPartitionState.getOrDefault(new MirroredPartitionKey(mirrorName, tp, part), MirrorState.UNKNOWN).value());
                partitionStates.add(partitionState);
            });
            state.setPartitions(partitionStates);
            topicStates.add(state);
        });
        data.setTopics(topicStates);

        responseCallback.accept(new ReadMirrorStatesResponse(data));
    }

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
     * maybe truncate the topics on the mirrorName by sending LAST_MIRRORED_OFFSET API to the source cluster
     *
     * @param replicaManager the replicaManager
     * @param mirrorName the name of the cluster mirror
     * @param topicPartitionSet the topic partitions to be truncated
     * @param onTruncateComplete the callback when truncation completed
     */
    public void maybeTruncate(ReplicaManager replicaManager, String mirrorName, Set<TopicPartition> topicPartitionSet, Consumer<TopicPartition> onTruncateComplete) {
        LOG.info("!!! maybeTruncate: {} {}", mirrorName, topicPartitionSet);
        createMirrorConnection(mirrorName);

        List<LastMirroredOffsetsRequestData.TopicState> topicStates = new ArrayList<>();
        convertToTopicToPartitions(topicPartitionSet).forEach((topic, partitions) -> {
            List<LastMirroredOffsetsRequestData.PartitionState> partitionStates = new ArrayList<>();
            partitions.forEach(partition -> {
                LastMirroredOffsetsRequestData.PartitionState partitionState = new LastMirroredOffsetsRequestData.PartitionState();
                partitionState.setPartitionIndex(partition);
                partitionStates.add(partitionState);
            });
            LastMirroredOffsetsRequestData.TopicState state = new LastMirroredOffsetsRequestData.TopicState().setName(topic).setPartitions(partitionStates);
            topicStates.add(state);
        });

        var response = getRandomSender(remoteBrokers.get(mirrorName)).sendRequest(
                new LastMirroredOffsetsRequest.Builder(
                        new LastMirroredOffsetsRequestData().setMirrorName(mirrorName).setTopics(topicStates))
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
            replicaManager.maybeTruncate(offsets, onTruncateComplete);
            // if no last mirrored offset set in the source cluster, ignore it and directly move to MIRRORING state
            if (topicPartitionSet.size() > offsets.size()) {
                Set<TopicPartition> topicPartitionSetWithoutOffsets = new HashSet<>(topicPartitionSet);
                topicPartitionSetWithoutOffsets.removeAll(offsets.keySet().stream().map(TopicPartition::topic).collect(Collectors.toSet()));
                topicPartitionSetWithoutOffsets.forEach(onTruncateComplete::accept);
            }
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
        var partitionLeaders = remotePartitionLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            Node leader = partitionLeaders.get(tp);
            if (leader != null) {
                return leader;
            }
        }

        // No cached metadata.
        // Refresh synchronously to get correct broker IDs before creating fetcher threads.
        LOG.info("No cached metadata for mirror {} partition {}. Refreshing metadata from remote cluster.", mirrorName, tp);
        topics.compute(mirrorName, (k, preV) -> {
            Set<String> sets;
            if (preV == null) {
                sets = new ConcurrentSkipListSet<>();
            } else {
                sets = new ConcurrentSkipListSet<>(preV);
            }
            sets.add(tp.topic());
            return sets;
        });
        refreshMetadata();

        // Try again after refreshing metadata.
        partitionLeaders = remotePartitionLeaders.get(mirrorName);
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
     * Updates the cached set of mirror topics for a cluster mirror.
     * Adds and removes topics from the cache and returns the updated set.
     *
     * @param clusterName the name of the cluster mirror
     * @param addedTopics topics to add to the cache
     * @param removedTopics topics to remove from the cache
     * @return the updated set of mirror topics
     */
    public Set<String> updateMirrorTopicsCache(String clusterName, Set<String> addedTopics, Set<String> removedTopics) {
        Set<String> mutableTopics = new HashSet<>(this.topics.getOrDefault(clusterName, Set.of()));
        mutableTopics.removeAll(removedTopics);
        mutableTopics.addAll(addedTopics);
        this.topics.put(clusterName, mutableTopics);
        return mutableTopics;
    }

    public MirrorState getMirrorPartitionState(String clusterName, TopicPartition topicPartition) {
        return mirroredPartitionState.get(new MirroredPartitionKey(clusterName, topicPartition.topic(), topicPartition.partition()));
    }

    public long getLastMirroredOffset(String clusterName, TopicPartition topicPartition) {
        return lastMirroredOffsets.get(new MirroredPartitionKey(clusterName, topicPartition.topic(), topicPartition.partition()));
    }

    public void updateMirrorPartitionStateCache(String clusterName, TopicPartition topicPartition, MirrorState mirrorState) {
        mirroredPartitionState.put(new MirroredPartitionKey(clusterName, topicPartition.topic(), topicPartition.partition()), mirrorState);
        topics.compute(clusterName, (key, oldVal) -> {
            if (oldVal == null) {
                return Set.of(topicPartition.topic());
            } else {
                Set<String> newSet = new HashSet<>(oldVal);
                newSet.add(topicPartition.topic());
                return newSet;
            }
        });
    }

    public void operateAll(MirrorCoordinator.Operator operator) {
        Map<String, Map<MirrorState, Set<TopicPartition>>> statesToPartitions = new HashMap<>();
        mirroredPartitionState.forEach((key, value) -> {
            LOG.info("!!! operateAll: {} {}", key, value);
            statesToPartitions.compute(key.mirrorName, (k, v) -> {
                if (v == null) {
                    return Map.of(value, Set.of(new TopicPartition(key.topic(), key.partition())));
                }
                v.compute(value, (state, prevTps) -> {
                    if (prevTps == null) {
                        return Set.of(new TopicPartition(key.topic(), key.partition()));
                    } else {
                        Set<TopicPartition> result = new HashSet<>(prevTps);
                        result.add(new TopicPartition(key.topic(), key.partition()));
                        return result;
                    }
                });
                return v;
            });
        });

        statesToPartitions.forEach((mirrorName, statesToPartitionsMap) -> {
            statesToPartitionsMap.forEach((state, tps) -> {
                operator.operateOnNewState(mirrorName, tps, state);
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
    public Map<MirroredPartitionKey, Long> updateLastMirroredOffsetsCache(String clusterName,
                                                                  Map<String, Map<Integer, Long>> addedOffsets,
                                                                  Map<String, Map<Integer, Long>> removedOffsets) {
        removedOffsets.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirroredOffsets.remove(new MirroredPartitionKey(clusterName, topic, partition));
            });
        });
        addedOffsets.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach((partition, offset) -> {
                lastMirroredOffsets.put(new MirroredPartitionKey(clusterName, topic, partition), offset);
            });
        });
        return lastMirroredOffsets;
    }

//    public void onPreparing(String clusterName, Set<String> topics) {
//        LOG.info("!!! onPreparing: {} {}", clusterName, topics);
//        onPreparingWaitingList.compute(clusterName, (key, prevVal) -> {
//            Set<String> result = new HashSet<>();
//            if (prevVal != null) {
//                result.addAll(prevVal);
//            }
//            result.addAll(topics);
//            return result;
//        });
//        LOG.info("!!! onPreparingWaitingList: {}", onPreparingWaitingList);
//    }

//    public void operateOnPreparing(String mirrorName, TopicPartition topicPartition) {
//        LOG.info("!!! operateOnPreparing: {} {}", mirrorName, topicPartition);
//        Set<String> topics = onPreparingWaitingList.get(mirrorName);
//        if (runs == null) {
//            // the coordinator is the remote node, retrieve state from remote coordinator
//            readStatesToRemoteCoordinator(mirrorName, Map.of(topicPartition.topic(),
//                    Set.of(topicPartition.partition())),
//                    readStatesResponse -> {
//
//                    });
//        } else {
//            runs.get(topicPartition.topic()).accept(topicPartition);
//            runs.remove(topicPartition.topic());
//            if (runs.isEmpty()) {
//                onPreparingWaitingList.remove(mirrorName);
//            } else {
//                onPreparingWaitingList.put(mirrorName, runs);
//            }
//        }
//
//    }

    // wait until the state entering MIRRORING state and then start fetching
    public void onMirroring(Map<TopicPartition, LocalReplicaChanges.PartitionInfo> readOnlyLeaders, Consumer<Map<TopicPartition, LocalReplicaChanges.PartitionInfo>> onMirroring) {
        readOnlyLeaders.forEach((tp, info) -> {
            MirroredPartitionKey key = new MirroredPartitionKey(info.partition().mirrorName, tp.topic(), tp.partition());
            LOG.info("!!! onMirroring: {}", key);
            if (mirroredPartitionState.get(key) == MirrorState.MIRRORING) {
                onMirroring.accept(Map.of(tp, info));
            } else {
                onMirroringWaitingList.put(key, () -> {
                    LOG.info("!!! onMirroring accept: {}", info.partition().mirrorName);
                    onMirroring.accept(Map.of(tp, info));
                });
            }
        });
    }

    // operate the onMirroring callback and remove from the list
    public void operateOnMirroring(String mirrorName, Set<TopicPartition> topicPartitions) {
        topicPartitions.forEach(tp -> {
            MirroredPartitionKey key = new MirroredPartitionKey(mirrorName, tp.topic(), tp.partition());
            Runnable runnable = onMirroringWaitingList.get(key);
            if (runnable != null) {
                runnable.run();
                onMirroringWaitingList.remove(key);
            }
        });
    }

    /**
     * Refreshes metadata from all remote clusters.
     * Synchronizes topic metadata, configurations, consumer group offsets, and ACLs.
     * Called periodically by the scheduler to keep clusters in sync.
     */
    public void refreshMetadata() {
        if (!topics.isEmpty()) {
            LOG.info("!!! Refreshing mirror metadata for topics:" + topics);
        }

        checkMirrorConnections();

//        remoteBrokers.forEach((mirrorName, senders) -> {
//            syncTopicMetadata(mirrorName, senders);
//            syncTopicConfigurations(mirrorName, senders);
//            syncConsumerGroupOffsets(senders);
//            syncACLs(mirrorName, senders);
//        });
    }

    private void checkMirrorConnections() {
        // make sure all mirror names has at least one connections ready
        // TODO: we should find another connection if it is not accessible
        topics.keySet().forEach(mirrorName -> {
            // create a connection for the mirrorName
            if (!remoteBrokers.containsKey(mirrorName)) {
                createMirrorConnection(mirrorName);
            }
        });
    }

    private void createMirrorConnection(String mirrorName) {
        // should get the cluster name from records
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("Remote bootstrap server not found in Cluster Mirror config: " + mirrorName));

        // get the 1st one bootstrap server
        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        int rand = random.nextInt(addresses.size());
        createMirrorConnection(mirrorName, addresses.get(rand).getHostString(), addresses.get(rand).getPort());
    }

    private MirrorBlockingSender createMirrorConnection(String mirrorName, String host, int port) {

        // Use random node id here because we don't know node id of remote brokers.
        var brokerEndpoint = new BrokerEndPoint(random.nextInt(), host, port);
        var logContext = new LogContext("[" + MirrorMetadataManager.class.getName() + " replicaId=" + nodeId + ", mirrorName=" + mirrorName + "] ");

        MirrorBlockingSender sender = new MirrorBlockingSender(
                brokerEndpoint,
                MirrorConfig.fromProperties(metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName))),
                metrics,
                time,
                brokerEndpoint.id(),
                "broker-" + nodeId + "-cluster-mirror-metadata-manager-" + mirrorName,
                logContext
        );
        remoteBrokers.put(mirrorName, List.of(sender));

        return sender;
    }

    private void syncTopicMetadata(String mirrorName, List<MirrorBlockingSender> senders) {
        // get a random node in source cluster
        var response = getRandomSender(senders).sendRequest(
            MetadataRequest.Builder.forTopicNames(topics.get(mirrorName).stream().toList(), false)
        );

        if (response.responseBody() instanceof MetadataResponse metadataResponse) {
            LOG.debug("!!! Periodic metadataResponse: {}", metadataResponse);
            updateRemoteClusterNodesCache(mirrorName, metadataResponse);
            var createPartitionsTopics = processTopicMetadata(mirrorName, metadataResponse.topicMetadata());
            maybeDeleteTopic(mirrorName, metadataResponse.topicMetadata());
            handlePartitionScaling(createPartitionsTopics);
        }
    }

    private void syncTopicConfigurations(String mirrorName, List<MirrorBlockingSender> senders) {
        LOG.info("!!! Describing topic configs for topics: {}", topics);

        List<DescribeConfigsRequestData.DescribeConfigsResource> describeConfigsResources =
            topics.get(mirrorName).stream()
                .map(topic -> new DescribeConfigsRequestData.DescribeConfigsResource()
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setResourceName(topic))
                .toList();

        DescribeConfigsRequest.Builder describeConfigsRequest =
            new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(describeConfigsResources));

        var describeConfigResponse = getRandomSender(senders).sendRequest(describeConfigsRequest);
        if (describeConfigResponse.responseBody() instanceof DescribeConfigsResponse describeConfigsRes) {
            LOG.debug("!!! Periodic describeConfigResponse: {}", describeConfigsRes);

            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(mirrorName, describeConfigsRes);
            applyConfigurationChanges(configsToChange);
        }
    }

    private void updateRemoteClusterNodesCache(String mirrorName, MetadataResponse metadataResponse) {
        metadataResponse.brokers().forEach(broker -> {
            remoteClusterNodes.computeIfAbsent(mirrorName, k -> new HashMap<>()).put(broker.id(), broker);
        });
    }

    // Processes topic metadata and returns topics that need partition scaling
    private CreatePartitionsRequestData.CreatePartitionsTopicCollection processTopicMetadata(
            String mirrorName, Collection<MetadataResponse.TopicMetadata> topicMetadataResp) {
        var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();

        topicMetadataResp.forEach(topicMetadata -> {
            var partitionLeaders = remotePartitionLeaders.computeIfAbsent(mirrorName, k -> new HashMap<>());

            topicMetadata.partitionMetadata().forEach(partitionMetadata -> {
                partitionLeaders.put(
                    partitionMetadata.topicPartition,
                    remoteClusterNodes.get(mirrorName).get(partitionMetadata.leaderId.get())
                );
            });

            if (metadataImage.topics().getTopic(topicMetadata.topicId()) != null &&
                    metadataImage.topics().getTopic(topicMetadata.topicId()).partitions().size() < partitionLeaders.size()) {
                createPartitionsTopics.add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                    .setName(topicMetadata.topic())
                    .setCount(partitionLeaders.size())
                    .setAssignments(null)
                );
            }
        });

        return createPartitionsTopics;
    }

    // Handles partition scaling by sending create partitions requests
    private void handlePartitionScaling(CreatePartitionsRequestData.CreatePartitionsTopicCollection createPartitionsTopics) {
        if (!createPartitionsTopics.isEmpty()) {
            LOG.info("!!! Detected partition count change, sending CreatePartitionsRequest: {}", createPartitionsTopics);
            channelManager.sendRequest(new CreatePartitionsRequest.Builder(
                new CreatePartitionsRequestData()
                    .setTopics(createPartitionsTopics)
                    .setValidateOnly(false)
                    .setTimeoutMs(3000)
            ), new TimeoutHandler());
        }
    }

    private Map<String, Map<String, String>> detectConfigurationChanges(
            String mirrorName, DescribeConfigsResponse describeConfigsRes) {
        Map<String, Map<String, String>> configsToChange = new HashMap<>();

        describeConfigsRes.data().results().forEach(describeConfigResult -> {
            if (describeConfigResult.resourceType() == ConfigResource.Type.TOPIC.id() &&
                topics.get(mirrorName).contains(describeConfigResult.resourceName())) {

                Properties props = metadataCache.topicConfig(describeConfigResult.resourceName());
                Map<String, String> conChange = new HashMap<>();

                describeConfigResult.configs().forEach(con -> {
                    if (con.configSource() == DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG.id()) {
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
        LOG.info("!!! Applying config change: {}", configsToChange);

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

    private void maybeDeleteTopic(String mirrorName, Collection<MetadataResponse.TopicMetadata> topicMetadataResp) {
        LOG.info("!!! Deleting topics from topicMetadataResp: {}", topicMetadataResp);
        List<String> deletedTopics = new ArrayList<>();
        // deleted topics if needed
        List<String> remoteTopicNamesDeleted = topicMetadataResp.stream()
                .filter(topicMetadata -> topicMetadata.error() == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                .map(MetadataResponse.TopicMetadata::topic).toList();
        topics.get(mirrorName).forEach(name -> {
            if (remoteTopicNamesDeleted.contains(name)) {
                LOG.info("!!! Detected topic {} deleted in remote cluster {}, removing it locally too", name, mirrorName);
                // send a delete topic request to the controller
                channelManager.sendRequest(new DeleteTopicsRequest.Builder(
                        new DeleteTopicsRequestData()
                                .setTopicNames(List.of(name))
                                .setTimeoutMs(10000)), new TimeoutHandler());
                LOG.info("!!! Sent delete topic request for {}", name);
                deletedTopics.add(name);
            }
        });
        deletedTopics.forEach(name -> topics.get(mirrorName).remove(name));
    }

    private void syncConsumerGroupOffsets(List<MirrorBlockingSender> senders) {
        // 1. list group
        ListGroupsRequest.Builder builder = new ListGroupsRequest.Builder(new ListGroupsRequestData()
                .setTypesFilter(List.of(GroupType.CLASSIC.name(), GroupType.CONSUMER.name()))
                .setStatesFilter(singletonList(GroupState.STABLE.name())));
        var listGroupResponse = getRandomSender(senders).sendRequest(builder);
        if (listGroupResponse.responseBody() instanceof ListGroupsResponse listGroupsRes) {
            LOG.info("!!! Periodic list group: {}", listGroupsRes);

            // 2. get committed offsets for each group
            OffsetFetchRequest.Builder offsetFetchBuilder = OffsetFetchRequest.Builder.forTopicNames(
                    new OffsetFetchRequestData()
                            .setRequireStable(false)
                            .setGroups(listGroupsRes.data().groups().stream().map(group -> new OffsetFetchRequestData.OffsetFetchRequestGroup()
                                    .setGroupId(group.groupId())
                                    .setTopics(null)).toList()), false);
            var offsetFetchResponse = getRandomSender(senders).sendRequest(offsetFetchBuilder);
            if (offsetFetchResponse.responseBody() instanceof OffsetFetchResponse offsetFetchRes) {
                LOG.info("!!! Periodic offset fetch: {}", offsetFetchRes);

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

    private void syncACLs(String mirrorName, List<MirrorBlockingSender> senders) {
        // TODO: We currently mirror all ACLs from the source to the target.
        //       Any ACLs added/removed directly on the target will be overwritten
        //       on the next sync to match the source.
        //
        // TODO: How do we disambiguate ACLs that reference the same resource name
        //       when multiple cluster mirrors exist?

        // list remote acls
        var describeAclsRequest = new DescribeAclsRequest.Builder(ANY_RESOURCE_ACL);
        var describeAclsResponse = getRandomSender(senders).sendRequest(describeAclsRequest);
        if (!(describeAclsResponse.responseBody() instanceof DescribeAclsResponse aclsResponse)) {
            LOG.warn("!!! describeAclsResponse is not DescribeAclsResponse: {}", describeAclsResponse);
            return;
        }

        LOG.info("!!! describeAclsResponse from remote cluster {}: {}", mirrorName, aclsResponse);

        var allRemoteAcls = DescribeAclsResponse.aclBindings(aclsResponse.acls());
        var aclChanges = detectACLChanges(allRemoteAcls);
        applyACLChanges(mirrorName, aclChanges);
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

    private void applyACLChanges(String mirrorName, ACLChanges aclChanges) {
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
     * Clears all cached mirror metadata including topics and remote broker connections.
     * Called when the broker resigns as leader for mirror state topic partitions.
     */
    public void clear() {
        topics.clear();
        remoteBrokers.clear();
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

    public record MirroredPartitionMetadata(int partition, MirrorState state, Long offset) { }
    public record MirroredPartitionStateRecordValue(String topic, int partition, MirrorState state) { }
    public record MirroredPartitionKey(String mirrorName, String topic, int partition) { }
}
