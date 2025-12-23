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
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.mirror.MirrorRecordKey;
import org.apache.kafka.coordinator.mirror.MirrorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.mirror.generated.LastMirroredOffsetsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirroredOffsetsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorTopicsKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorTopicsValue;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ApiMessageAndVersion;
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

/**
 * Replicated state machine that coordinates topic mirroring between Kafka clusters.
 *
 * This coordinator is responsible for:
 * - Managing topic configurations and metadata for all cluster mirrors
 * - Coordinating with remote brokers for cross-cluster replication
 * - Handling leader election and resignation for mirrored partitions
 * - Loading and persisting cluster mirror metadata in the internal topic
 * - Scheduling periodic metadata refresh from source clusters
 *
 * The coordinator maintains state about which topics are being mirrored for each cluster
 * mirror and ensures proper coordination between source and destination clusters.
 * It integrates with the ReplicaManager to handle log operations and with the
 * MirrorMetadataManager to maintain metadata about mirror topics.
 */
public class MirrorCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(MirrorCoordinator.class);
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final KafkaConfig config;
    private final ReplicaManager replicaManager;
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final MetadataCache metadataCache;
    private final Time time;
    private volatile int numPartitions = -1;
    private final Map<String, MirrorBlockingSender> remoteBrokers = new HashMap<>();
    private final MirrorRecordSerde serde = new MirrorRecordSerde();
    private final MirrorMetadataManager mirrorMetadataManager;

    public MirrorCoordinator(
            KafkaConfig config,
            ReplicaManager replicaManager,
            Scheduler scheduler,
            Metrics metrics,
            MetadataCache metadataCache,
            Time time,
            MirrorMetadataManager mirrorMetadataManager
    ) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.metadataCache = metadataCache;
        this.time = time;
        this.mirrorMetadataManager = mirrorMetadataManager;
    }

    /**
     * Starts the mirror coordinator and begins periodic metadata refresh from source clusters.
     * Schedules background tasks to synchronize topic metadata, offsets, and ACLs.
     */
    public void startup() {
        if (!isActive.compareAndSet(false, true)) {
            LOG.warn("MirrorCoordinator is already running.");
            return;
        }

        LOG.info("Starting up.");
        scheduler.startup();
        // periodically query source cluster to get the metadata
        scheduler.schedule("mirror-metadata-refresh",
                mirrorMetadataManager::refreshMetadata,
                30000,
                30000
        );
        numPartitions = config.mirrorConfig().mirrorTopicNumPartitions();
    }

    /**
     * Updates the mirror topics metadata for a given cluster mirror.
     * Adds or removes topics from the mirror configuration and persists changes to the internal topic.
     *
     * @param mirrorName the name of the cluster mirror
     * @param addedTopics topics to add to the mirror
     * @param removedTopics topics to remove from the mirror
     */
    // TODO: handle response
    public void updateMirrorTopicsMetadata(String mirrorName, Set<String> addedTopics, Set<String> removedTopics) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionIndexForKey(new MirrorRecordKey(mirrorName)));
        var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);

        var topics = mirrorMetadataManager.updateMirrorTopicsCache(mirrorName, addedTopics, removedTopics);
        var record = generateMirrorTopics(mirrorName, topics);
        var keyBytes = serde.serializeKey(record);
        var valueBytes = serde.serializeValue(record);
        var timestamp = time.milliseconds();
        var memRecord = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, keyBytes, valueBytes));

        LOG.info("!!! Appending record to {}: {}", mirrorTopicPartition, record);
        replicaManager.appendRecords(
                // TODO: replace this with Cluster Mirror specific timeout
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(Map.of(mirrorTopicIdPartition, memRecord)),
                ignored -> null,
                ignored -> null,
                RequestLocal.noCaching(),
                CollectionConverters.asScala(Map.of())
        );
    }

    private Map<String, Map<Integer, Long>> lastMirroredOffstesToCoordinatorRecords(Set<MirrorMetadataManager.LastMirroredOffset> offsets) {
        Map<String, Map<Integer, Long>> results = new HashMap<>();
        offsets.forEach(offset -> {
            Map<Integer, Long> partitionOffsets = results.getOrDefault(offset.topic(), new HashMap<Integer, Long>());
            partitionOffsets.put(offset.partition(), offset.offset());
            results.put(offset.topic(), partitionOffsets);
        });
        return results;
    }

    /**
     * Updates the last mirrored offsets metadata for a given cluster mirror.
     * Records the latest successfully mirrored offset for each partition to support failback scenarios.
     *
     * @param mirrorName the name of the cluster mirror
     * @param partitionOffsets map of topic names to partition offsets
     */
    public void updateLastMirroredOffsetsMetadata(String mirrorName, Map<String, Map<Integer, Long>> partitionOffsets) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionIndexForKey(new MirrorRecordKey(mirrorName)));
        var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);

        var updatedOffsets = mirrorMetadataManager.updateLastMirroredOffsetsCache(mirrorName, partitionOffsets, Map.of());
        var record = generateLastMirroredOffsets(mirrorName, lastMirroredOffstesToCoordinatorRecords(updatedOffsets));
        var keyBytes = serde.serializeKey(record);
        var valueBytes = serde.serializeValue(record);
        var timestamp = time.milliseconds();
        var memRecord = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, keyBytes, valueBytes));

        LOG.info("!!! Appending offset record to {}: {}", mirrorTopicPartition, record);
        replicaManager.appendRecords(
                // TODO: replace this with Cluster Mirror specific timeout
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(Map.of(mirrorTopicIdPartition, memRecord)),
                ignored -> null,
                ignored -> null,
                RequestLocal.noCaching(),
                CollectionConverters.asScala(Map.of())
        );
    }

    private static CoordinatorRecord generateMirrorTopics(String mirrorName, Set<String> topics) {
        var key = new MirrorTopicsKey().setMirrorName(mirrorName);
        var val = new MirrorTopicsValue().setTopics(
                topics.stream().map(topic -> new MirrorTopicsValue.Topic().setName(topic)).toList());
        var apiVersion = new ApiMessageAndVersion(val, MirrorTopicsValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private static CoordinatorRecord generateLastMirroredOffsets(String mirrorName, Map<String, Map<Integer, Long>> offsets) {
        var key = new LastMirroredOffsetsKey().setMirrorName(mirrorName);
        var val = new LastMirroredOffsetsValue();
        var topics = new ArrayList<LastMirroredOffsetsValue.Topic>();
        offsets.forEach((topic, partitionOffsets) -> {
            var top = new LastMirroredOffsetsValue.Topic();
            List<LastMirroredOffsetsValue.Partition> partitions = new ArrayList<>();
            partitionOffsets.forEach((partition, offset) ->
                    partitions.add(new LastMirroredOffsetsValue.Partition().setPartitionIndex(partition).setLastMirroredOffset(offset)));
            top.setPartitions(partitions);
            topics.add(top);
        });
        val.setTopics(topics);
        var apiVersion = new ApiMessageAndVersion(val, LastMirroredOffsetsValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private String readMirrorNameFromKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version == CoordinatorRecordType.MIRROR_TOPICS.id()) {
            return new MirrorTopicsKey(new ByteBufferAccessor(buffer), version).mirrorName();
        } else if (version == CoordinatorRecordType.LAST_MIRRORED_OFFSETS.id()) {
            return new LastMirroredOffsetsKey(new ByteBufferAccessor(buffer), version).mirrorName();
        } else {
            throw new IllegalArgumentException("Unknown cluster mirror log key version " + version);
        }
    }

    private Set<String> readMirrorTopicsFromValue(ByteBuffer buffer) {
        Set<String> topics = new HashSet<>();
        short version = buffer.getShort();
        if (version >= MirrorTopicsValue.LOWEST_SUPPORTED_VERSION && version <= MirrorTopicsValue.HIGHEST_SUPPORTED_VERSION) {
            MirrorTopicsValue value = new MirrorTopicsValue(new ByteBufferAccessor(buffer), version);
            value.topics().forEach(t -> topics.add(t.name()));
        } else {
            throw new IllegalStateException("Unknown version {} from the mirror topic value");
        }
        return topics;
    }

    private Map<String, Map<Integer, Long>> readLastMirroredOffsetsValue(ByteBuffer buffer) {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        short version = buffer.getShort();
        if (version <= LastMirroredOffsetsValue.HIGHEST_SUPPORTED_VERSION & version >= LastMirroredOffsetsValue.LOWEST_SUPPORTED_VERSION) {
            LastMirroredOffsetsValue value = new LastMirroredOffsetsValue(new ByteBufferAccessor(buffer), version);
            value.topics().forEach(t -> {
                Map<Integer, Long> partitions = new HashMap<>();
                t.partitions().forEach(p -> partitions.put(p.partitionIndex(), p.lastMirroredOffset()));
                offsets.put(t.name(), partitions);
            });
        } else {
            throw new IllegalStateException("Unknown version {} from last mirrored offsets value");
        }
        return offsets;
    }

    private void loadMirrorMetadata(TopicPartition topicPartition) {
        LOG.info("!!! Loading mirror metadata from {}.", topicPartition);
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
                while (currOffset < logEndOffset && readAtLeastOneRecord && isActive.get()) {
                    LOG.info("Reading mirror data from {} at offset {}.", topicPartition, currOffset);
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
                                LOG.warn("Loaded mirror data from {} with buffer larger ({} bytes) than " +
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
                            require(record.hasKey(), "Mirror log's key should not be null");
                            short version = record.key().getShort();
                            if (version ==  CoordinatorRecordType.MIRROR_TOPICS.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                Set<String> topics = readMirrorTopicsFromValue(record.value());
                                mirrorMetadataManager.updateMirrorTopicsCache(clusterName, topics, Set.of());
                            } else if (version == CoordinatorRecordType.LAST_MIRRORED_OFFSETS.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                Map<String, Map<Integer, Long>> offsets = readLastMirroredOffsetsValue(record.value());
                                mirrorMetadataManager.updateLastMirroredOffsetsCache(clusterName, offsets, Map.of());
                            } else {
                                throw new IllegalArgumentException("Unknown cluster mirror log key version " + version);
                            }

                        });
                        currOffset = batch.nextOffset();
                    }
                }
            } catch (Throwable t) {
                LOG.error("Error loading mirrors from mirror state log {}", topicPartition, t);
            }
            return null;
        });
    }

    /**
     * Handles leadership election for a mirror state topic partition.
     * Loads mirror metadata from the internal topic when this broker becomes the leader.
     *
     * @param partitionIndex the partition index of the mirror state topic
     * @param partitionLeaderEpoch the leader epoch of the partition
     */
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {
        // load the data from the internal topic
        loadMirrorMetadata(new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionIndex));
    }

    /**
     * Handles leadership resignation for a mirror state topic partition.
     * Clears cached mirror metadata when this broker is no longer the leader.
     *
     * @param partitionIndex the partition index of the mirror state topic
     * @param partitionLeaderEpoch the leader epoch of the partition
     */
    public void onResignation(
            int partitionIndex,
            OptionalInt partitionLeaderEpoch
    ) {
        // clear the cache
        clear();
    }

    private void clear() {
        remoteBrokers.clear();
        mirrorMetadataManager.clear();
    }

    /**
     * Shuts down the mirror coordinator and releases all resources.
     * Stops periodic metadata refresh and closes metrics.
     */
    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            LOG.warn("MirrorCoordinator is already shutting down.");
            return;
        }

        LOG.info("Shutting down.");
        Utils.closeQuietly(metrics, "MirrorCoordinator metrics");
        LOG.info("Shutdown complete.");
    }

    /**
     * Returns the partition index for the given mirror record key.
     * Used to determine which partition in the mirror state topic should store this key.
     *
     * @param key the mirror record key
     * @return the partition index
     */
    public int partitionIndexForKey(MirrorRecordKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
