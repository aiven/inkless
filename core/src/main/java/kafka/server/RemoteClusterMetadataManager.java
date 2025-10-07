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
package kafka.server;

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
import org.apache.kafka.server.network.BrokerEndPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
 * A manager to handle metadata related to remote clusters. It watches topic leader changes,
 * topic partitions changes, and topic configuration changes in remote clusters and updates
 * the local metadata accordingly.
 */
public class RemoteClusterMetadataManager implements MetadataPublisher, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RemoteClusterMetadataManager.class);
    private static final ResourcePatternFilter ANY_RESOURCE = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
    private static final AclBindingFilter ANY_RESOURCE_ACL = new AclBindingFilter(ANY_RESOURCE, AccessControlEntryFilter.ANY);

    private final KafkaConfig brokerConfig;
    private final int nodeId;
    // Mapping from remote bootstrap servers to its corresponding broker sender and topics.
    // TODO: A better key might be a cluster id or cluster-link. For now, we use remote bootstrap servers for demo.
    private final Map<String, List<RemoteBrokerBlockingSender>> remoteBrokers;
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

    public RemoteClusterMetadataManager(
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
    }

    @Override
    public String name() {
        return "RemoteClusterMetadataManager(id=" + brokerConfig.nodeId() + ")";
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        this.metadataImage = newImage;
    }

    @Override
    public void close() throws Exception {

    }

    public Node getRemotePartitionLeader(String clusterLinkName, TopicPartition tp) {
        var partitionLeaders = remotePartitionLeaders.get(clusterLinkName);
        if (partitionLeaders != null) {
            return partitionLeaders.get(tp);
        }
        // return a random node if no leader info
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_LINK, clusterLinkName));
        String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                .map(Object::toString)
                .orElseThrow(() -> new IllegalArgumentException("remote bootstrap server not found in cluster link config: " + clusterLinkName));
        // get the 1st one bootstrap server
        var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
        int rand = random.nextInt(addresses.size());
        // Use random node id here because we don't know node id of remote brokers.
        return new Node(random.nextInt(), addresses.get(rand).getHostString(), addresses.get(rand).getPort());
    }

    public void updateMirroredTopics(String clusterName, Set<String> topics) {
        this.topics.put(clusterName, topics);
    }

    public void refreshRemoteMetadata() {
        log.info("!!! Refreshing remote cluster metadata:" + topics);
        // make sure all clusterLinkNames has at least one connections ready
        // TODO: we should find another connection if it is not accessible
        topics.keySet().forEach(clusterLinkName -> {
            // create a connection for the clusterLinkName
            if (!remoteBrokers.containsKey(clusterLinkName)) {
                // should get the cluster name from records
                Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_LINK, clusterLinkName));
                String bootstrapServers = Optional.ofNullable(props.get(BOOTSTRAP_SERVERS_CONFIG))
                        .map(Object::toString)
                        .orElseThrow(() -> new IllegalArgumentException("remote bootstrap server not found in cluster link config: " + clusterLinkName));
                // get the 1st one bootstrap server
                var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
                int rand = random.nextInt(addresses.size());
                // Use random node id here because we don't know node id of remote brokers.
                var brokerEndpoint = new BrokerEndPoint(random.nextInt(), addresses.get(rand).getHostString(), addresses.get(rand).getPort());
                var logContext = new LogContext("[" + RemoteClusterMetadataManager.class.getName() + " replicaId=" + nodeId + ", remoteBootstrapServers=" + clusterLinkName + ", " +
                        "readOnly=true] ");
                remoteBrokers.put(clusterLinkName, List.of(new RemoteBrokerBlockingSender(
                        brokerEndpoint,
                        brokerConfig,
                        metrics,
                        time,
                        brokerEndpoint.id(),
                        "broker-" + nodeId + "-remote-cluster-metadata-manager-" + clusterLinkName,
                        logContext,
                        props
                )));

            }
        });
        log.info("!!! Updating remote cluster metadata for topics: {}, and maybe update the partition size", topics);
        remoteBrokers.forEach((clusterLinkName, senders) -> {
            // get a random node in source cluster
            var response = senders.get(random.nextInt(senders.size())).sendRequest(MetadataRequest.Builder.forTopicNames(topics.get(clusterLinkName).stream().toList(), false));
            if (response.responseBody() instanceof MetadataResponse metadataResponse) {
                log.debug("!!! periodic metadataResponse: {}", metadataResponse);
                metadataResponse.brokers().forEach(broker -> {
                    remoteClusterNodes.computeIfAbsent(clusterLinkName, k -> new HashMap<>()).put(broker.id(), broker);
                });

                var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();
                Collection<MetadataResponse.TopicMetadata> topicMetadataResp = metadataResponse.topicMetadata();
                topicMetadataResp.forEach(topicMetadata -> {
                    var partitionLeaders = remotePartitionLeaders.computeIfAbsent(clusterLinkName, k -> new HashMap<>());
                    topicMetadata.partitionMetadata().forEach(partitionMetadata -> {
                        partitionLeaders.put(partitionMetadata.topicPartition, remoteClusterNodes.get(clusterLinkName).get(partitionMetadata.leaderId.get()));
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

                maybeDeleteTopic(clusterLinkName, topicMetadataResp);

                if (!createPartitionsTopics.isEmpty()) {
                    log.info("!!! Detected partition count change, sending CreatePartitionsRequest: {}", createPartitionsTopics);
                    channelManager.sendRequest(new CreatePartitionsRequest.Builder(
                        new CreatePartitionsRequestData()
                            .setTopics(createPartitionsTopics)
                            .setValidateOnly(false)
                            .setTimeoutMs(3000)
                    ), new TimeoutHandler());
                }
            }

            // get the topic configs in the source cluster
            log.info("!!! describing topic configs for topics: {}", topics);
            List<DescribeConfigsRequestData.DescribeConfigsResource> describeConfigsResources = topics.get(clusterLinkName).stream().map(
                    topic -> new DescribeConfigsRequestData.DescribeConfigsResource().
                    setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(topic)).toList();
            DescribeConfigsRequest.Builder describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(describeConfigsResources));

            Map<String, Map<String, String>> configsToChange = new HashMap<>();
            Map<String, String> conChange = new HashMap<>();
            var describeConfigResponse = senders.get(random.nextInt(senders.size())).sendRequest(describeConfigsRequest);
            if (describeConfigResponse.responseBody() instanceof DescribeConfigsResponse describeConfigsRes) {
                log.debug("!!! periodic describeConfigsRes: {}", describeConfigsRes);
                describeConfigsRes.data().results().forEach(describeConfigResult -> {
                    if (describeConfigResult.resourceType() == ConfigResource.Type.TOPIC.id() && topics.get(clusterLinkName).contains(describeConfigResult.resourceName())) {
                        Properties props = metadataCache.topicConfig(describeConfigResult.resourceName());
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
                            configsToChange.put(describeConfigResult.resourceName(), new HashMap<>(conChange));
                            conChange.clear();
                        }

                    }

                });
            }

            log.info("!!! applying config change: {}", configsToChange);
            // apply the change
            Map<ConfigResource, Collection<AlterConfigOp>> configOps = new HashMap<>();
            configsToChange.forEach((name, changes) -> {
                var changeList = changes.entrySet().stream().map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()),  AlterConfigOp.OpType.SET)).toList();
                configOps.put(new ConfigResource(ConfigResource.Type.TOPIC, name), changeList);
            });

            if (!configOps.isEmpty()) {
                IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData().setValidateOnly(false);
                for (ConfigResource resource : configOps.keySet()) {
                    IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigSet =
                            new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
                    for (AlterConfigOp configEntry : configOps.get(resource))
                        alterableConfigSet.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                                .setName(configEntry.configEntry().name())
                                .setValue(configEntry.configEntry().value())
                                .setConfigOperation(configEntry.opType().id()));
                    IncrementalAlterConfigsRequestData.AlterConfigsResource alterConfigsResource = new IncrementalAlterConfigsRequestData.AlterConfigsResource();
                    alterConfigsResource.setResourceType(resource.type().id())
                            .setResourceName(resource.name()).setConfigs(alterableConfigSet);
                    data.resources().add(alterConfigsResource);
                }
                channelManager.sendRequest(new IncrementalAlterConfigsRequest.Builder(data), new TimeoutHandler());
            }

        });
        updateConsumerGroupOffsets();
        updateACLs();
    }

    private void maybeDeleteTopic(String clusterLinkName, Collection<MetadataResponse.TopicMetadata> topicMetadataResp) {
        log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        log.info("!!! periodic topicMetadataResp: {}", topicMetadataResp);
        List<String> deletedTopics = new ArrayList<>();
        // deleted topics if needed
        List<String> remoteTopicNamesDeleted = topicMetadataResp.stream()
                .filter(topicMetadata -> topicMetadata.error() == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                .map(MetadataResponse.TopicMetadata::topic).toList();
        topics.get(clusterLinkName).forEach(name -> {
            if (remoteTopicNamesDeleted.contains(name)) {
                log.info("!!! Detected topic {} deleted in remote cluster {}, removing it locally too", name, clusterLinkName);
                // send a delete topic request to the controller
                channelManager.sendRequest(new DeleteTopicsRequest.Builder(
                        new DeleteTopicsRequestData()
                                .setTopicNames(List.of(name))
                                .setTimeoutMs(10000)), new TimeoutHandler());
                log.info("!!! Sent delete topic request for {}", name);
                deletedTopics.add(name);
            }
        });
        deletedTopics.forEach(name -> topics.get(clusterLinkName).remove(name));
    }

    private void updateConsumerGroupOffsets() {
        remoteBrokers.forEach((clusterLinkName, senders) -> {

            // 1. list group
            ListGroupsRequest.Builder builder = new ListGroupsRequest.Builder(new ListGroupsRequestData()
                    .setTypesFilter(List.of(GroupType.CLASSIC.name(), GroupType.CONSUMER.name()))
                    .setStatesFilter(singletonList(GroupState.STABLE.name())));
            var listGroupResponse = senders.get(random.nextInt(senders.size())).sendRequest(builder);
            if (listGroupResponse.responseBody() instanceof ListGroupsResponse listGroupsRes) {
                log.info("!!! periodic list group: {}", listGroupsRes);

                // 2. get committed offsets for each group
                OffsetFetchRequest.Builder offsetFetchBuilder = OffsetFetchRequest.Builder.forTopicNames(
                        new OffsetFetchRequestData()
                                .setRequireStable(false)
                                .setGroups(listGroupsRes.data().groups().stream().map(group -> new OffsetFetchRequestData.OffsetFetchRequestGroup()
                                        .setGroupId(group.groupId())
                                        .setTopics(null)).toList())
                                , false);
                var offsetFetchResponse = senders.get(random.nextInt(senders.size())).sendRequest(offsetFetchBuilder);
                if (offsetFetchResponse.responseBody() instanceof OffsetFetchResponse offsetFetchRes) {
                    log.info("!!! periodic offset fetch: {}", offsetFetchRes);

                    // 3. commit offsets to consumer group coordinator
                    // TODO: need to find the current group coordinator for each group
                    offsetFetchRes.data().groups().forEach(group -> {
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
                                        .setGroupId(group.groupId())
                                        .setMemberId("")
                                        .setGenerationIdOrMemberEpoch(-1)
                                        .setRetentionTimeMs(-1)
                                        .setGroupInstanceId("")
                                        .setTopics(topicList),
                                RequestLocal.noCaching().bufferSupplier()
                        ).handle((data, exception) -> {
                            log.info("!!! periodic offset commit: {} ;; {}", data, exception);
                            return null;
                        });
                    });
                }
            }
        });
    }

    private void updateACLs() {
        remoteBrokers.forEach((clusterLinkName, senders) -> {
            // TODO: We currently mirror all ACLs from the source to the target.
            //       Any ACLs added/removed directly on the target will be overwritten
            //       on the next sync to match the source.
            //
            // TODO: How do we disambiguate ACLs that reference the same resource name
            //       when multiple ClusterLinks exist?
            // list remote acls
            var describeAclsRequest = new DescribeAclsRequest.Builder(ANY_RESOURCE_ACL);
            var describeAclsResponse = senders.get(random.nextInt(senders.size())).sendRequest(describeAclsRequest);
            if (!(describeAclsResponse.responseBody() instanceof DescribeAclsResponse aclsResponse)) {
                log.warn("!!! describeAclsResponse is not DescribeAclsResponse: {}", describeAclsResponse);
                return;
            }

            log.info("!!! describeAclsResponse from remote cluster {}: {}", clusterLinkName, aclsResponse);

            // check delta
            var allRemoteAcls = DescribeAclsResponse.aclBindings(aclsResponse.acls());
            var addACLsList = new ArrayList<AclBinding>();
            var deleteACLsList = new ArrayList<AclBinding>();
            var current = metadataImage.acls().acls().values();

            // collect missing acls list
            allRemoteAcls.forEach(acl -> {
                if (current.stream().map(StandardAcl::toBinding).noneMatch(a -> a.equals(acl))) {
                    log.info("!!! Add ACL from remote cluster {}: {}", clusterLinkName, acl);
                    addACLsList.add(acl);
                }
            });

            // collect remove acls list
            metadataImage.acls().acls().values().forEach(acl -> {
                if (!allRemoteAcls.contains(acl.toBinding())) {
                    log.info("!!! Remove ACL from remote cluster {}: {}", clusterLinkName, acl);
                    deleteACLsList.add(acl.toBinding());
                }
            });

            // send createAcls request
            if (!addACLsList.isEmpty())  {
                var requestData = addACLsList.stream().map(
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
            if (!deleteACLsList.isEmpty()) {
                var requestData = deleteACLsList.stream().map(
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
        });
    }

    public void clear() {
        topics.clear();
        remoteBrokers.clear();
    }

    private static class TimeoutHandler implements ControllerRequestCompletionHandler {
        @Override
        public void onTimeout() {
            log.info("!!! Timed out");
        }

        @Override
        public void onComplete(ClientResponse response) {
            log.info("!!! Update topics: {}", response);
        }
    }
}
