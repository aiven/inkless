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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

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
    private final Map<String, RemoteBrokerBlockingSender> remoteBrokers;
    private final Map<String, Set<String>> topics;
    private final Map<String, Map<Integer, Node>> remoteClusterNodes;
    private final Map<String, Map<TopicPartition, Node>> remotePartitionLeaders;
    private final Metrics metrics;
    private final Time time;
    private final NodeToControllerChannelManager channelManager;
    private final Random random;
    private MetadataImage metadataImage;
    private MetadataCache metadataCache;

    public RemoteClusterMetadataManager(
        KafkaConfig config,
        Metrics metrics,
        Time time,
        MetadataCache metadataCache
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
    }

    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage) {
        // TODO: Use ClusterLinkDelta to manage remote brokers / topics.
        metadataImage = newImage;
        if (delta.topicsDelta() == null) {
            return;
        }
        var localChanges = delta.topicsDelta().localChanges(nodeId);
        if (!localChanges.followers().isEmpty()) {
            handleFollowerChanges(localChanges.followers());
        }
        if (!localChanges.readOnlyLeaders().isEmpty()) {
            handleReadOnlyLeadersChanges(localChanges.readOnlyLeaders());
        }
    }

    @Override
    public void close() throws Exception {

    }

    private void handleFollowerChanges(Map<TopicPartition, LocalReplicaChanges.PartitionInfo> followers) {
        followers.forEach((tp, info) -> {
            var remoteBrokerTopics = topics.get(info.partition().remoteBootstrapServers);
            if (remoteBrokerTopics != null) {
                remoteBrokerTopics.remove(tp.topic());
                if (remoteBrokerTopics.isEmpty()) {
                    var sender = remoteBrokers.remove(info.partition().remoteBootstrapServers);
                    if (sender != null) {
                        sender.close();
                    }
                    topics.remove(info.partition().remoteBootstrapServers);
                }
            }
        });
    }

    private void handleReadOnlyLeadersChanges(Map<TopicPartition, LocalReplicaChanges.PartitionInfo> readOnlyLeaders) {
        var updateRemoteBootstrapServers = new HashSet<String>();
        readOnlyLeaders.forEach((tp, info) -> {
            remoteBrokers.computeIfAbsent(
                info.partition().remoteBootstrapServers,
                k -> {
                    var remoteBootstrapServers = Arrays.stream(k.split(",")).toList();
                    var addresses = ClientUtils.parseAndValidateAddresses(remoteBootstrapServers, "use_all_dns_ips");
                    // Use random node id here because we don't know node id of remote brokers.
                    var brokerEndpoint = new BrokerEndPoint(random.nextInt(), addresses.get(0).getHostString(), addresses.get(0).getPort());
                    var logContext = new LogContext("[" + RemoteClusterMetadataManager.class.getName() + " replicaId=" + nodeId + ", remoteBootstrapServers=" + k + ", " +
                        "readOnly=true] ");
                    return new RemoteBrokerBlockingSender(
                        brokerEndpoint,
                        brokerConfig,
                        metrics,
                        time,
                        brokerEndpoint.id(),
                        "broker-" + nodeId + "-remote-cluster-metadata-manager-" + k.replace(":", "-"),
                        logContext
                    );
                });
            updateRemoteBootstrapServers.add(info.partition().remoteBootstrapServers);
            topics.computeIfAbsent(info.partition().remoteBootstrapServers, k -> new HashSet<>()).add(tp.topic());
        });

        log.info("!!! Updating remote cluster metadata for bootstrap servers: {}", updateRemoteBootstrapServers);
        updateRemoteBootstrapServers.forEach(remoteBootstrapServers -> {
            var sender = remoteBrokers.get(remoteBootstrapServers);
            var updatedTopics = topics.get(remoteBootstrapServers);
            var response = sender.sendRequest(MetadataRequest.Builder.forTopicNames(updatedTopics.stream().toList(), false));
            if (response.responseBody() instanceof MetadataResponse metadataResponse) {
                log.info("!!! metadataResponse: {}", metadataResponse);
                metadataResponse.brokers().forEach(broker -> {
                    remoteClusterNodes.computeIfAbsent(remoteBootstrapServers, k -> new HashMap<>()).put(broker.id(), broker);
                });
                metadataResponse.topicMetadata().forEach(topicMetadata -> {
                    var partitionLeaders = remotePartitionLeaders.computeIfAbsent(remoteBootstrapServers, k -> new HashMap<>());
                    topicMetadata.partitionMetadata().forEach(partitionMetadata -> {
                        partitionLeaders.put(partitionMetadata.topicPartition, remoteClusterNodes.get(remoteBootstrapServers).get(partitionMetadata.leaderId.get()));
                    });
                });
            }
        });
    }

    public Node getRemotePartitionLeader(String remoteBootstrapServers, TopicPartition tp) {
        var partitionLeaders = remotePartitionLeaders.get(remoteBootstrapServers);
        if (partitionLeaders != null) {
            return partitionLeaders.get(tp);
        }
        return null;
    }

    public void refreshRemoteMetadata() {
        log.info("!!! Refreshing remote cluster metadata");
        if (remoteBrokers.isEmpty()) {
            // should get the cluster name from records
            Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_LINK, "my-link"));
            String bootstrapServers = props.get(BOOTSTRAP_SERVERS_CONFIG).toString();
            // get the 1st one bootstrap server
            var addresses = ClientUtils.parseAndValidateAddresses(Arrays.stream(bootstrapServers.split(",")).toList(), "use_all_dns_ips");
            // Use random node id here because we don't know node id of remote brokers.
            var brokerEndpoint = new BrokerEndPoint(random.nextInt(), addresses.get(0).getHostString(), addresses.get(0).getPort());
            var logContext = new LogContext("[" + RemoteClusterMetadataManager.class.getName() + " replicaId=" + nodeId + ", remoteBootstrapServers=" + k + ", " +
                    "readOnly=true] ");
            return new RemoteBrokerBlockingSender(
                    brokerEndpoint,
                    brokerConfig,
                    metrics,
                    time,
                    brokerEndpoint.id(),
                    "broker-" + nodeId + "-remote-cluster-metadata-manager-" + ,
                    logContext
            );

        }
        remoteBrokers.forEach((remoteBootstrapServers, sender) -> {
            var response = sender.sendRequest(MetadataRequest.Builder.forTopicNames(topics.get(remoteBootstrapServers).stream().toList(), false));
            if (response.responseBody() instanceof MetadataResponse metadataResponse) {
                log.info("!!! periodic metadataResponse: {}", metadataResponse);
                metadataResponse.brokers().forEach(broker -> {
                    remoteClusterNodes.computeIfAbsent(remoteBootstrapServers, k -> new HashMap<>()).put(broker.id(), broker);
                });

                var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();
                metadataResponse.topicMetadata().forEach(topicMetadata -> {
                    var partitionLeaders = remotePartitionLeaders.computeIfAbsent(remoteBootstrapServers, k -> new HashMap<>());
                    topicMetadata.partitionMetadata().forEach(partitionMetadata -> {
                        partitionLeaders.put(partitionMetadata.topicPartition, remoteClusterNodes.get(remoteBootstrapServers).get(partitionMetadata.leaderId.get()));
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
        });
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
