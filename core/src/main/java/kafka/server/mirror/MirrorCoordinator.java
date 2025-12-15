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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
 * MirrorMetadataManager to maintain metadata about mirrored topics.
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

    // TODO: handle response
    public void updateTopicsToCoordinator(String mirrorName, Set<String> addedTopics, Set<String> removedTopics) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionFor(new MirrorRecordKey(mirrorName)));
        var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);

        var topics = mirrorMetadataManager.updateMirroredTopics(mirrorName, addedTopics, removedTopics);
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

    private static CoordinatorRecord generateMirrorTopics(String mirrorName, Set<String> topics) {
        var key = new MirrorTopicsKey().setMirrorName(mirrorName);
        var val = new MirrorTopicsValue().setTopics(
                topics.stream().map(topic -> new MirrorTopicsValue.Topic().setName(topic)).toList());
        var apiVersion = new ApiMessageAndVersion(val, (short) 0);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private String readMirrorNameFromKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version != CoordinatorRecordType.MIRROR_TOPICS.id()) {
            throw new IllegalArgumentException("Unknown cluster mirror log key version " + version);
        }
        return new MirrorTopicsKey(new ByteBufferAccessor(buffer), version).mirrorName();
    }

    private Set<String> readMirrorTopicsFromValue(ByteBuffer buffer) {
        Set<String> topics = new HashSet<>();
        short version = buffer.getShort();
        if (version >= MirrorTopicsValue.LOWEST_SUPPORTED_VERSION && version <= MirrorTopicsValue.HIGHEST_SUPPORTED_VERSION) {
            MirrorTopicsValue value = new MirrorTopicsValue(new ByteBufferAccessor(buffer), version);
            value.topics().forEach(t -> topics.add(t.name()));
        } else {
            throw new IllegalStateException("Unknown version {} from the mirror message value");
        }
        return topics;
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
                            String clusterName = readMirrorNameFromKey(record.key());
                            Set<String> topics = readMirrorTopicsFromValue(record.value());
                            mirrorMetadataManager.updateMirroredTopics(clusterName, topics, Set.of());
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

    // called when onMetadataUpdate, needs to handle new leader elected
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {
        // load the data from the internal topic
        loadMirrorMetadata(new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionIndex));
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
        mirrorMetadataManager.clear();
    }

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
     * Return the partition index for the given key.
     */
    public int partitionFor(MirrorRecordKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
