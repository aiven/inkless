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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.LocalReplicaChanges;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.network.BrokerEndPoint;
import org.apache.kafka.server.util.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * A manager to handle metadata related to remote clusters. It watches topic leader changes,
 * topic partitions changes, and topic configuration changes in remote clusters and updates
 * the local metadata accordingly.
 */
public class RemoteClusterMetadataManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RemoteClusterMetadataManager.class);
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

    public RemoteClusterMetadataManager(
        KafkaConfig config,
        Metrics metrics,
        Time time,
        MetadataCache metadataCache,
        NodeToControllerChannelManager channelManager
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
        String bootstrapServers = props.get(BOOTSTRAP_SERVERS_CONFIG).toString();
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
                String bootstrapServers = props.get(BOOTSTRAP_SERVERS_CONFIG).toString();
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
                        logContext
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
                metadataResponse.topicMetadata().forEach(topicMetadata -> {
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
