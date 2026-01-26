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
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
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
import java.util.stream.IntStream;

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
     * Executes the appropriate actions for a state transition.
     * <p>
     * Each state triggers specific operations:
     * <ul>
     *   <li>INITIALIZING: Registers callbacks for metadata synchronization completion</li>
     *   <li>PREPARING: Synchronizes topic metadata and schedules truncation</li>
     *   <li>MIRRORING: Initiates mirror fetcher threads</li>
     *   <li>STOPPING: Updates metadata and persists last mirrored offsets</li>
     *   <li>STOPPED: Marks topics as writable (no action required)</li>
     *   <li>FAILED: Logs failure (recovery actions to be implemented)</li>
     * </ul>
     *
     * @param mirrorName the name of the cluster mirror
     * @param topics the set of topics transitioning to the new state
     * @param newState the target state after transition
     */
    private void handleStateTransition(String mirrorName, Set<String> topics, MirrorPartitionState newState) {
        switch (newState) {
            case INITIALIZING:
                LOG.info("!!! Updating metadata for topics {}.", topics);
                mirrorMetadataManager.registerMetadataUpdateCallback(mirrorName, topics, state -> transitionTo(mirrorName, topics, state));
                break;
            case PREPARING:
                LOG.info("!!! Preparing mirror for topics {}.", topics);
                updateMirrorTopicsMetadata(mirrorName, topics, Set.of());
                scheduleTruncation(mirrorName, topics);
                break;
            case MIRRORING:
                LOG.info("!!! Mirroring topics {}.", topics);
                mirrorMetadataManager.invokeMirroringCallback(mirrorName, topics);
                break;
            case STOPPING:
                LOG.info("!!! Stopping mirror for topics {}.", topics);
                updateMirrorTopicsMetadata(mirrorName, Set.of(), topics);
                updateLastMirroredOffsets(mirrorName, topics);
                break;
            case STOPPED:
                LOG.info("!!! Stopped mirroring for topics {}.", topics);
                // topic becomes writable
                break;
            case FAILED:
                LOG.info("!!! Failed mirroring for topics {}.", topics);
        }
    }

    /**
     * Transitions all partitions of the specified topics to a new mirror state.
     * <p>
     * This method updates the state for every partition of each topic and then
     * executes the state transition actions once for all topics.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topics the set of topics to transition
     * @param newState the target mirror state
     */
    public void transitionTo(String mirrorName, Set<String> topics, MirrorPartitionState newState) {
        topics.forEach(topic -> metadataCache.numPartitions(topic).ifPresent(numPartitions -> {
            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                TopicPartition topicPartition = new TopicPartition(topic, partitionIndex);
                transitionTo(mirrorName, topicPartition, newState, false);
            }
        }));

        handleStateTransition(mirrorName, topics, newState);
    }

    /**
     * Transitions a single partition to a new mirror state.
     * <p>
     * This method updates the partition state and optionally executes the state
     * transition actions. Use this for partition-level state changes that don't
     * require triggering topic-level operations.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topicPartition the specific partition to transition
     * @param newState the target mirror state
     * @param executeActions if true, executes state transition actions immediately;
     *                       if false, only updates the partition state
     */
    public void transitionTo(String mirrorName, TopicPartition topicPartition, MirrorPartitionState newState, boolean executeActions) {
        LOG.info("!!! Transitioning partition {} from {} to {}.", topicPartition,
                mirrorMetadataManager.getMirrorPartitionState(mirrorName, topicPartition), newState);
        updateMirrorPartitionStateMetadata(mirrorName, topicPartition, newState);
        if (executeActions) {
            handleStateTransition(mirrorName, Set.of(topicPartition.topic()), newState);
        }
    }

    /**
     * Collects the current log end offsets for all partitions of the specified topics
     * and persists them as the last successfully mirrored offsets.
     * <p>
     * This method is typically called when transitioning to the STOPPING state to
     * record the point up to which data was successfully mirrored, enabling
     * potential failback scenarios.
     *
     * @param mirrorName the name of the cluster mirror
     * @param mirrorTopics the topics whose offsets should be captured
     */
    public void updateLastMirroredOffsets(String mirrorName, Set<String> mirrorTopics) {
        Map<String, Map<Integer, Long>> partitionOffsets = new HashMap<>();
        mirrorTopics.forEach(topic -> metadataCache.numPartitions(topic).ifPresent(numPartitions -> {
            Map<Integer, Long> offsets = new HashMap<>();
            IntStream.range(0, numPartitions).forEach(i -> {
                TopicPartition topicPartition = new TopicPartition(topic, i);
                replicaManager.getPartitionOrException(topicPartition).log().foreach(log -> offsets.put(i, log.lastStableOffset()));
            });
            partitionOffsets.put(topic, offsets);
        }));

        LOG.info("!!! Partition offsets for mirror topics: " + partitionOffsets);
        updateLastMirroredOffsetsMetadata(mirrorName, partitionOffsets);
    }

    public void updateMirrorPartitionStateMetadata(String mirrorName, TopicPartition topicPartition, MirrorPartitionState newState) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, getPartitionIndexForKey(new MirrorRecordKey(mirrorName)));
        var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
        var record = generateMirrorPartitionState(mirrorName, topicPartition, newState);
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

        mirrorMetadataManager.updateMirrorPartitionState(mirrorName, topicPartition, newState);
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
        long metadataRefreshIntervalMs = config.mirrorConfig().metadataRefreshIntervalMs();
        scheduler.schedule("mirror-metadata-refresh",
                mirrorMetadataManager::refreshMetadata,
                metadataRefreshIntervalMs,
                metadataRefreshIntervalMs
        );
        numPartitions = config.mirrorConfig().mirrorTopicNumPartitions();
    }

    /**
     * Schedules truncation operations for the specified topics.
     * <p>
     * Truncation ensures that partition replicas are aligned to the last successfully
     * mirrored offset from the source cluster before resuming mirroring.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topics the topics to schedule for truncation
     */
    public void scheduleTruncation(String mirrorName, Set<String> topics) {
        topics.forEach(topic -> scheduler.scheduleOnce("last-mirrored-offset-truncation",
                () -> mirrorMetadataManager.maybeTruncateToLastMirroredOffsets(replicaManager, mirrorName, topics,
                        tp -> transitionTo(mirrorName, Set.of(tp.topic()), MirrorPartitionState.MIRRORING))));
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
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, getPartitionIndexForKey(new MirrorRecordKey(mirrorName)));
        var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);

        var topics = mirrorMetadataManager.updateMirrorTopics(mirrorName, addedTopics, removedTopics);
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

    private Map<String, Map<Integer, Long>> lastMirroredOffsetsToCoordinatorRecords(Map<MirrorMetadataManager.MirroredPartitionKey, Long> offsets) {
        Map<String, Map<Integer, Long>> results = new HashMap<>();
        offsets.forEach((key, value) -> {
            Map<Integer, Long> partitionOffsets = results.getOrDefault(key.topic(), new HashMap<Integer, Long>());
            partitionOffsets.put(key.partition(), value);
            results.put(key.topic(), partitionOffsets);
        });
        return results;
    }

    /**
     * Retrieves the last successfully mirrored offset for a specific partition.
     * <p>
     * This offset represents the point up to which data was successfully mirrored
     * before mirroring was stopped or failed.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topicPartition the topic partition
     * @return the last mirrored offset, or -1 if no offset is recorded
     */
    public long getLastMirroredOffset(String mirrorName, TopicPartition topicPartition) {
        return mirrorMetadataManager.getLastMirroredOffset(mirrorName, topicPartition);
    }

    /**
     * Updates the last mirrored offsets metadata for a given cluster mirror.
     * Records the latest successfully mirrored offset for each partition to support failback scenarios.
     *
     * @param mirrorName the name of the cluster mirror
     * @param partitionOffsets map of topic names to partition offsets
     */
    public void updateLastMirroredOffsetsMetadata(String mirrorName, Map<String, Map<Integer, Long>> partitionOffsets) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, getPartitionIndexForKey(new MirrorRecordKey(mirrorName)));
        var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);

        var updatedOffsets = mirrorMetadataManager.updateLastMirroredOffsetsCache(mirrorName, partitionOffsets, Map.of());
        var record = generateLastMirroredOffsets(mirrorName, lastMirroredOffsetsToCoordinatorRecords(updatedOffsets));
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

        // transition to stopped state after last mirrored offset stored
        // TODO: now we assume all partitions work without error, we should handle error cases
        transitionTo(mirrorName, partitionOffsets.keySet(), MirrorPartitionState.STOPPED);
    }

    // luke
    private static CoordinatorRecord generateMirrorPartitionState(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        var key = new MirrorPartitionStateKey().setMirrorName(mirrorName);
        var val = new MirrorPartitionStateValue().setTopicName(topicPartition.topic()).setPartition(topicPartition.partition()).setState(state.value());
        var apiVersion = new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
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
        } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
            return new MirrorPartitionStateKey(new ByteBufferAccessor(buffer), version).mirrorName();
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

    private MirrorMetadataManager.MirroredPartitionStateRecordValue readMirroredPartitionStateValue(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version <= MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION & version >= MirrorPartitionStateValue.LOWEST_SUPPORTED_VERSION) {
            MirrorPartitionStateValue value = new MirrorPartitionStateValue(new ByteBufferAccessor(buffer), version);
            return new MirrorMetadataManager.MirroredPartitionStateRecordValue(value.topicName(), value.partition(), MirrorPartitionState.fromValue(value.state()));
        } else {
            throw new IllegalStateException("Unknown version {} from last mirrored offsets value");
        }
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
                                mirrorMetadataManager.updateMirrorTopics(clusterName, topics, Set.of());
                            } else if (version == CoordinatorRecordType.LAST_MIRRORED_OFFSETS.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                Map<String, Map<Integer, Long>> offsets = readLastMirroredOffsetsValue(record.value());
                                mirrorMetadataManager.updateLastMirroredOffsetsCache(clusterName, offsets, Map.of());
                            } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                MirrorMetadataManager.MirroredPartitionStateRecordValue value = readMirroredPartitionStateValue(record.value());
                                mirrorMetadataManager.updateMirrorPartitionState(clusterName, new TopicPartition(value.topic(), value.partition()), value.state());
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

            // we've read all the mirrored records, transition the state for each topic partitions
            StateTransitionCallback callback = (mirrorName, topics, state)
                    -> handleStateTransition(mirrorName, topics, state);
            mirrorMetadataManager.operateAll(callback);

            return null;
        });
    }

    /**
     * Callback interface for executing operations when mirror partition states are loaded
     * from the coordinator log during startup or leader election.
     */
    interface StateTransitionCallback {
        /**
         * Called for each mirror partition state that was loaded from the coordinator log.
         *
         * @param mirrorName the name of the cluster mirror
         * @param topics the topics in this state
         * @param state the mirror state to operate on
         */
        void onStateLoaded(String mirrorName, Set<String> topics, MirrorPartitionState state);
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
     * Returns mirror names managed by this node.
     */
    public Set<String> getMirrorNames() {
        return mirrorMetadataManager.getMirrorNames();
    }

    /**
     * Returns the source cluster bootstrap servers for a given mirror.
     *
     * @param mirrorName the name of the cluster mirror
     * @return the bootstrap servers string, or null if not found
     */
    public String getSourceBootstrap(String mirrorName) {
        return mirrorMetadataManager.getSourceBootstrap(mirrorName);
    }

    /**
     * Get all topic partitions for a given mirror along with their states.
     *
     * @param mirrorName the name of the cluster mirror
     * @return partition state map
     */
    public Map<TopicPartition, MirrorPartitionState> getMirrorPartitions(String mirrorName) {
        return mirrorMetadataManager.getMirrorPartitions(mirrorName);
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
    public int getPartitionIndexForKey(MirrorRecordKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
