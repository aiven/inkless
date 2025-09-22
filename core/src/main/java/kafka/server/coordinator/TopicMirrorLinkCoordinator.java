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
import kafka.server.RemoteClusterMetadataManager;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.clusterlink.generated.ClusterLinkMirrorTopicsKey;
import org.apache.kafka.coordinator.clusterlink.generated.ClusterLinkMirrorTopicsValue;
import org.apache.kafka.coordinator.clusterlink.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.jdk.javaapi.CollectionConverters;

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
    private final TopicMirrorLinkRecordSerde serde = new TopicMirrorLinkRecordSerde();
    private final RemoteClusterMetadataManager remoteClusterMetadataManager;

    public TopicMirrorLinkCoordinator(
            KafkaConfig config,
            ReplicaManager replicaManager,
            Scheduler scheduler,
            Metrics metrics,
            MetadataCache metadataCache,
            Time time,
            RemoteClusterMetadataManager remoteClusterMetadataManager
    ) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.metadataCache = metadataCache;
        this.time = time;
        this.remoteClusterMetadataManager = remoteClusterMetadataManager;
    }

    public void startup() {
        if (!isActive.compareAndSet(false, true)) {
            logger.warn("Topic mirror link coordinator is already running.");
            return;
        }

        logger.info("Starting up.");
        scheduler.startup();
        // periodically query source cluster to get the metadata
        scheduler.schedule("topic-mirror-link-query",
                remoteClusterMetadataManager::refreshRemoteMetadata,
                5000,
                5000
        );
        numPartitions = config.clusterLinksConfig().clusterLinkTopicNumPartitions();
    }

    // TODO: handle response
    public void addTopicsInCoordinator(String clusterLinkName, Set<String> topics) {
        var clusterLinkTopicPartition = new TopicPartition(Topic.CLUSTER_LINK_TOPIC_NAME, partitionFor(new ClusterLinkKey(clusterLinkName)));
        var clusterLinkTopicIdPartition = replicaManager.topicIdPartition(clusterLinkTopicPartition);

        var record = generateClusterLinkMirrorTopics(clusterLinkName, new ArrayList<>(topics) {
        });
        var keyBytes = serde.serializeKey(record);
        var valueBytes = serde.serializeValue(record);
        var timestamp = time.milliseconds();
        var memRecord = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, keyBytes, valueBytes));

        logger.info("!!! Appending record to {}: {}", clusterLinkTopicPartition, record);
        replicaManager.appendRecords(
                // TODO: replace this with topic mirror link specific timeout
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(Map.of(clusterLinkTopicIdPartition, memRecord)),
                ignored -> null,
                ignored -> null,
                RequestLocal.noCaching(),
                CollectionConverters.asScala(Map.of())
        );
        remoteClusterMetadataManager.updateMirroredTopics(clusterLinkName, topics);
    }

    private static CoordinatorRecord generateClusterLinkMirrorTopics(String clusterLinkId, List<String> topics) {
        var key = new ClusterLinkMirrorTopicsKey().setClusterLinkId(clusterLinkId);
        var val = new ClusterLinkMirrorTopicsValue().setTopics(
                topics.stream().map(topic -> new ClusterLinkMirrorTopicsValue.Topic().setName(topic)).toList());
        var apiVersion = new ApiMessageAndVersion(val, (short) 0);
        return CoordinatorRecord.record(key, apiVersion);
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
        logger.info("Loading cluster link data from {}.", topicPartition);
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
                    logger.info("Reading cluster link data from {} at offset {}.", topicPartition, currOffset);
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
                            remoteClusterMetadataManager.updateMirroredTopics(clusterName, topics);
                        });
                        currOffset = batch.nextOffset();
                    }
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
        remoteClusterMetadataManager.clear();
    }

    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            logger.warn("Topic mirror link coordinator is already shutting down.");
            return;
        }

        logger.info("Shutting down.");
        Utils.closeQuietly(metrics, "topic mirror link coordinator metrics");
        logger.info("Shutdown complete.");
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
