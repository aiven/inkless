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
package kafka.server.coordinator;

import kafka.server.KafkaConfig;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.util.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;

public class TopicMirrorLinkCoordinator {

    private static final Logger log = LoggerFactory.getLogger(TopicMirrorLinkCoordinator.class);
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final KafkaConfig config;
    private final ReplicaManager replicaManager;
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final MetadataCache metadataCache;
    private final Time time;
    private volatile int numPartitions = -1;

    public TopicMirrorLinkCoordinator(
            KafkaConfig config,
            ReplicaManager replicaManager,
            Scheduler scheduler,
            Metrics metrics,
            MetadataCache metadataCache,
            Time time,
            NodeToControllerChannelManager nodeToControllerChannelManager
    ) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.metadataCache = metadataCache;
        this.time = time;
    }

    public void startup() {
        isActive.set(true);
        log.info("Starting up.");
        scheduler.startup();
        scheduler.schedule("topic-mirror-link-query",
                this::querySourceCluster,
                5000,
                5000
        );
        numPartitions = config.clusterLinksConfig().clusterLinkTopicNumPartitions();
    }

    // periodically query source cluster to get the metadata
    void querySourceCluster() {

    }

    // called when onMetadataUpdate, needs to handle new leader elected
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {

    }

    // called when onMetadataUpdate, needs to handle old leader resigned
    public void onResignation(
            int partitionIndex,
            OptionalInt partitionLeaderEpoch
    ) {

    }

    public void shutdown() {
        isActive.set(false);
    }

    /**
     * Return the partition index for the given key.
     *
     */
    public int partitionFor(ClusterLinkPartitionKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
