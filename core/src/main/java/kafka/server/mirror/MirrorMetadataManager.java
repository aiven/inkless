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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
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
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
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
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupCoordinator;
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
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Cluster Mirror component that provides synchronization of cluster state.
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
    private final LogManager logManager;
    private final Scheduler scheduler;

    public MirrorMetadataManager(
        KafkaConfig config,
        Metrics metrics,
        Time time,
        MetadataCache metadataCache,
        NodeToControllerChannelManager channelManager,
        Supplier<GroupCoordinator> groupCoordinatorSupplier,
        LogManager logManager,
        Scheduler scheduler
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
        this.logManager = logManager;
        this.scheduler = scheduler;
    }

    @Override
    public String name() {
        return "MirrorMetadataManager(id=" + brokerConfig.nodeId() + ")";
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        this.metadataImage = newImage;
    }

    @Override
    public void close() throws Exception {

    }

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
        topics.computeIfAbsent(mirrorName, k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add(tp.topic());
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

    public Set<String> updateMirroredTopics(String clusterName, Set<String> addedTopics, Set<String> removedTopics) {
        Set<String> mutableTopics = new HashSet<>(this.topics.getOrDefault(clusterName, Set.of()));
        mutableTopics.removeAll(removedTopics);
        mutableTopics.addAll(addedTopics);
        this.topics.put(clusterName, mutableTopics);

        return mutableTopics;
    }

    public void refreshMetadata() {
        if (!topics.isEmpty()) {
            LOG.info("!!! Refreshing mirror metadata for topics:" + topics);
        }

        checkMirrorConnections();

        remoteBrokers.forEach((mirrorName, senders) -> {
            syncTopicMetadata(mirrorName, senders);
            syncTopicConfigurations(mirrorName, senders);
            syncConsumerGroupOffsets(senders);
            syncACLs(mirrorName, senders);
        });
    }

    /**
     * Ensures that connections to all remote clusters are established.
     * Creates new connections for cluster mirrors that don't have existing connections.
     */
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

    /**
     * Creates a new connection to the specified remote cluster.
     */
    private void createMirrorConnection(String mirrorName) {
        // should get the cluster name from records
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("Remote bootstrap server not found in Cluster Mirror config: " + mirrorName));

        // get the 1st one bootstrap server
        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        int rand = random.nextInt(addresses.size());

        // Use random node id here because we don't know node id of remote brokers.
        var brokerEndpoint = new BrokerEndPoint(random.nextInt(), addresses.get(rand).getHostString(), addresses.get(rand).getPort());
        var logContext = new LogContext("[" + MirrorMetadataManager.class.getName() + " replicaId=" + nodeId + ", mirrorName=" + mirrorName + "] ");

        remoteBrokers.put(mirrorName, List.of(new MirrorBlockingSender(
                brokerEndpoint,
                MirrorConfig.fromProperties(props),
                metrics,
                time,
                brokerEndpoint.id(),
                "broker-" + nodeId + "-cluster-mirror-metadata-manager-" + mirrorName,
                logContext
        )));
    }

    /**
     * Synchronizes topic metadata for a specific remote cluster.
     * This includes updating broker nodes, partition leaders, and handling partition scaling.
     */
    private void syncTopicMetadata(String mirrorName, List<MirrorBlockingSender> senders) {
        // get a random node in source cluster
        var response = getRandomSender(senders).sendRequest(
            MetadataRequest.Builder.forTopicNames(topics.get(mirrorName).stream().toList(), false)
        );

        if (response.responseBody() instanceof MetadataResponse metadataResponse) {
            LOG.debug("!!! Periodic metadataResponse: {}", metadataResponse);
            updateRemoteClusterNodes(mirrorName, metadataResponse);
            var createPartitionsTopics = processTopicMetadata(mirrorName, metadataResponse.topicMetadata());
            maybeDeleteTopic(mirrorName, metadataResponse.topicMetadata());
            handlePartitionScaling(createPartitionsTopics);
        }
    }

    /**
     * Synchronizes topic configurations for a specific remote cluster.
     */
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

    /**
     * Updates the remote cluster nodes mapping from metadata response.
     */
    private void updateRemoteClusterNodes(String mirrorName, MetadataResponse metadataResponse) {
        metadataResponse.brokers().forEach(broker -> {
            remoteClusterNodes.computeIfAbsent(mirrorName, k -> new HashMap<>()).put(broker.id(), broker);
        });
    }

    /**
     * Processes topic metadata and returns topics that need partition scaling.
     */
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

    /**
     * Handles partition scaling by sending create partitions requests.
     */
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

    /**
     * Detects configuration changes between remote and local topics.
     */
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

    /**
     * Applies configuration changes to local topics.
     */
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

    /**
     * Returns a random sender from the list of available senders.
     */
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

    /**
     * Synchronizes consumer group offsets for a specific remote cluster.
     */
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

    /**
     * Builds the topic list for offset commit requests.
     */
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

    /**
     * Commits offsets to the group coordinator.
     */
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

    /**
     * Synchronizes ACLs for a specific remote cluster.
     */
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

    /**
     * Detects ACL changes between remote and local clusters.
     */
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

    /**
     * Applies ACL changes by sending create and delete requests.
     */
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
}
