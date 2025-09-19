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
import kafka.server.RemoteBrokerBlockingSender;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.clusterlink.generated.ClusterLinkMirrorTopicsKey;
import org.apache.kafka.coordinator.clusterlink.generated.ClusterLinkMirrorTopicsValue;
import org.apache.kafka.coordinator.clusterlink.generated.CoordinatorRecordType;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;

import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.utils.Utils.require;

public class TopicMirrorLinkCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(TopicMirrorLinkCoordinator.class);
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final KafkaConfig config;
    private final ReplicaManager replicaManager;
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final MetadataCache metadataCache;
    private final Time time;
    private volatile int numPartitions = -1;
    private final Map<String, RemoteBrokerBlockingSender> remoteBrokers = new HashMap<>();
    // cluster-link name(or id) map to all subscribed topics
    private final Map<String, Set<String>> mirroredTopicsInLink = new HashMap<>();

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
        logger.info("Starting up.");
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

    private String readClusterLinkRecordKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version != CoordinatorRecordType.CLUSTER_LINK_MIRROR_TOPICS.id()) {
            throw new IllegalArgumentException("Unknown cluster link log key version " + version);
        }
        return new ClusterLinkMirrorTopicsKey(new ByteBufferAccessor(buffer), version).clusterLinkId();
    }

    private Set<String> readClusterLinkRecordValue(ByteBuffer buffer) {
        Set<String> topics = new HashSet<>();
        short version = buffer.getShort();
        if (version >= ClusterLinkMirrorTopicsValue.LOWEST_SUPPORTED_VERSION && version <= ClusterLinkMirrorTopicsValue.HIGHEST_SUPPORTED_VERSION) {
            ClusterLinkMirrorTopicsValue value = new ClusterLinkMirrorTopicsValue(new ByteBufferAccessor(buffer), version);
            value.topics().forEach(t -> topics.add(t.name()));
        } else {
            throw new IllegalStateException("Unknown version {} from the cluster link message value");
        }
        return topics;
    }

    private void loadClusterLinkData(TopicPartition topicPartition) {
        long logEndOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(() -> -1L);

        replicaManager.getLog(topicPartition).foreach(log -> {
            ByteBuffer buffer = ByteBuffer.allocate(0);

            // loop breaks if leader changes at any time during the load, since logEndOffset is -1
            long currOffset = log.logStartOffset();

            // loop breaks if no records have been read, since the end of the log has been reached
            boolean readAtLeastOneRecord = true;
            int maxLength = 5 * 1024 * 1024;

            try {
                // might need a lock
                while (currOffset < logEndOffset && readAtLeastOneRecord && !isActive.get()) {
                    FetchDataInfo fetchDataInfo = log.read(currOffset, maxLength, FetchIsolation.LOG_END, true);

                    readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes() > 0;

                    MemoryRecords memRecords;

                    if (fetchDataInfo.records instanceof MemoryRecords) {
                        memRecords = (MemoryRecords) fetchDataInfo.records;
                    } else if (fetchDataInfo.records instanceof FileRecords) {
                        FileRecords fileRecords = (FileRecords) fetchDataInfo.records;
                        int sizeInBytes = fileRecords.sizeInBytes();
                        int bytesNeeded = Math.max(maxLength, sizeInBytes);

                        // minOneMessage = true in the above log.read means that the buffer may need to be grown to ensure progress can be made
                        if (buffer.capacity() < bytesNeeded) {
                            if (maxLength < bytesNeeded)
                                logger.warn("Loaded cluster link data from {} with buffer larger ({} bytes) than " +
                                        "{} bytes)", topicPartition, bytesNeeded, maxLength);

                            buffer = ByteBuffer.allocate(bytesNeeded);
                        }
                        buffer.clear();
                        fileRecords.readInto(buffer, 0);
                        memRecords = MemoryRecords.readableRecords(buffer);
                    } else {
                        return null;
                    }
                    Iterator<MutableRecordBatch> itr = memRecords.batches().iterator();
                    while (itr.hasNext()) {
                        MutableRecordBatch batch = itr.next();
                        batch.iterator().forEachRemaining(record -> {
                            require(record.hasKey(), "cluster link log's key should not be null");
                            String clusterName = readClusterLinkRecordKey(record.key());
                            Set<String> topics = readClusterLinkRecordValue(record.value());
                            mirroredTopicsInLink.put(clusterName, topics);
                        });
                        currOffset = batch.nextOffset();
                    };
                }
            } catch (Throwable t) {
                logger.error("Error loading transactions from transaction log P{}", topicPartition, t);
            }
            return null;
        });
    }

    // called when onMetadataUpdate, needs to handle new leader elected
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {
        // load the data in __cluster-link logs
        loadClusterLinkData(new TopicPartition(Topic.CLUSTER_LINK_TOPIC_NAME, partitionIndex));
    }

    // called when onMetadataUpdate, needs to handle old leader resigned
    public void onResignation(
            int partitionIndex,
            OptionalInt partitionLeaderEpoch
    ) {
        // clear the cache
        clear();

    }

    private void clear() {
        remoteBrokers.clear();
        mirroredTopicsInLink.clear();
    }

    public void shutdown() {
        isActive.set(false);
    }

    /**
     * Return the partition index for the given key.
     *
     */
    public int partitionFor(ClusterLinkKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
