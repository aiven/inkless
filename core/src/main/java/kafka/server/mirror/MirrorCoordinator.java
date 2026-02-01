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
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
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
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    private final KafkaConfig kafkaConfig;
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
            KafkaConfig kafkaConfig,
            ReplicaManager replicaManager,
            Scheduler scheduler,
            Metrics metrics,
            MetadataCache metadataCache,
            Time time,
            MirrorMetadataManager mirrorMetadataManager
    ) {
        this.kafkaConfig = kafkaConfig;
        this.replicaManager = replicaManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.metadataCache = metadataCache;
        this.time = time;
        this.mirrorMetadataManager = mirrorMetadataManager;
    }

    /**
     * Checks if this broker is the coordinator for the given mirror.
     *
     * @param mirrorName the name of the cluster mirror
     * @return true if this broker is the leader of the coordinator partition for this mirror
     */
    public boolean isLocalCoordinator(String mirrorName) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, getCoordinatingPartitionByKey(new MirrorRecordKey(mirrorName)));
        var optLeaderAndIsr = metadataCache.getLeaderAndIsr(mirrorTopicPartition.topic(), mirrorTopicPartition.partition());
        return optLeaderAndIsr.isPresent() && optLeaderAndIsr.get().leader() == kafkaConfig.nodeId();
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
     * @param topicPartitions the set of topics transitioning to the new state
     * @param newState the target state after transition
     */
    private void handleStateTransition(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        switch (newState) {
            case INITIALIZING:
                LOG.info("!!! INITIALIZING for topics {}.", topicPartitions);
                // waiting for metadata update
                break;
            case PREPARING:
                LOG.info("!!! PREPARING for topics {}.", topicPartitions);
                scheduleTruncation(mirrorName, topicPartitions);
                break;
            case MIRRORING:
                LOG.info("!!! MIRRORING topics {}.", topicPartitions);
                // start mirroring
                mirrorMetadataManager.invokeMirroringCallbacks(mirrorName, topicPartitions);
                break;
            case STOPPING:
                LOG.info("!!! STOPPING for topics {}.", topicPartitions);
                // register the last mirrored offsets for each partition in internal topic
                updateLastMirroredOffsets(mirrorName, topicPartitions);
                break;
            case STOPPED:
                LOG.info("!!! STOPPED for topics {}.", topicPartitions);
                // topic becomes writable
                break;
            case FAILED:
                LOG.info("!!! FAILED for topics {}.", topicPartitions);
                // TODO: implement recovery actions (i.e. exponential backoff retry)
                break;
            default:
                LOG.error("Illegal state transition to " + newState);
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    /**
     * Transitions all mirror partitions to a new state.
     * <p>
     * This method updates the state for every partition and then
     * executes the state transition actions after transition.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topicPartitions the set of topics to transition
     * @param newState the target mirror state
     */
    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        topicPartitions.forEach(tp -> {
            MirrorPartitionState currentState = mirrorMetadataManager.getMirrorPartitionState(mirrorName, tp);
            if (MirrorPartitionState.isValidTransition(currentState, newState)) {
                LOG.info("!!! Transitioning partition {} from {} to {}.", tp, currentState, newState);
                updateMirrorPartitionState(mirrorName, tp, newState)
                        .whenComplete((optTp, ex) -> {
                            if (ex != null) {
                                // TODO: a new component will handle state transitions from a shared queue, so retry means put back in the queue
                                LOG.error("Failed to update partition state for {}: {}, retrying", tp, ex.getMessage());
                                scheduler.scheduleOnce("retry-mirror-state-update", () -> updateMirrorPartitionState(mirrorName, tp, newState), 100);
                            } else {
                                // successfully writes data into internal log
                                optTp.ifPresent(tp1 -> handleStateTransition(mirrorName, Set.of(tp1), newState));
                            }
                        });
            } else {
                LOG.info("!!! Skipping transition {} to {} for partition {}.", mirrorMetadataManager.getMirrorPartitionState(mirrorName, tp), newState, tp);
            }
        });
    }

    public void writeMirrorPartitionMetadataToInternalTopic(String mirrorName,
                                                            Map<String, Set<MirrorMetadataManager.MirrorPartitionMetadata>> topicMetadata,
                                                            Consumer<WriteMirrorStatesResponse> sendResponseCallback) {
        Map<String, Map<Integer, Long>> offsets = new HashMap<>();
        Map<String, Set<Integer>> tps = new HashMap<>();
        topicMetadata.forEach((topic, partitions) -> {
            Set<Integer> partitionIndices = new HashSet<>();
            partitions.forEach(partition -> {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                partitionIndices.add(tp.partition());
                if (partition.state() != null && partition.state() != MirrorPartitionState.UNKNOWN) {
                    updateMirrorPartitionState(mirrorName, tp, partition.state());
                }
                if (partition.offset() != -1) {
                    offsets.putIfAbsent(topic, new HashMap<>());
                    offsets.get(topic).put(partition.partition(), partition.offset());
                }
            });
            tps.put(topic, partitionIndices);
        });

        updateLastMirroredOffsetsMetadata(mirrorName, offsets, false);

        WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
        List<WriteMirrorStatesResponseData.TopicState> topicStates = new ArrayList<>();
        tps.forEach((topic, indices) -> {
            List<WriteMirrorStatesResponseData.PartitionState> partitionStates = new ArrayList<>();
            indices.forEach(i -> {
                WriteMirrorStatesResponseData.PartitionState state = new WriteMirrorStatesResponseData.PartitionState();
                state.setPartitionIndex(i);
                state.setErrorCode((short) 0);
                partitionStates.add(state);
            });
            topicStates.add(new WriteMirrorStatesResponseData.TopicState().setName(topic).setPartitions(partitionStates));
        });

        data.setTopics(topicStates);
        sendResponseCallback.accept(new WriteMirrorStatesResponse(data));
    }

    public void readMirroredPartitionMetadata(String mirrorName,
                                              Map<String, Set<Integer>> partitions,
                                              Consumer<ReadMirrorStatesResponse> sendResponseCallback) {
        mirrorMetadataManager.readStatesFromCache(mirrorName, partitions, sendResponseCallback);
    }

    /**
     * Collects the current log end offsets for all partitions
     * and persists them as the last successfully mirrored offsets.
     * <p>
     * This method is typically called when transitioning to the STOPPING state to
     * record the point up to which data was successfully mirrored, enabling
     * potential failback scenarios.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topicPartitions the topic partitions whose offsets should be captured
     */
    public void updateLastMirroredOffsets(String mirrorName, Set<TopicPartition> topicPartitions) {
        Map<String, Map<Integer, Long>> partitionOffsets = new HashMap<>();
        mirrorMetadataManager.convertToTopicToPartitions(topicPartitions).forEach((topic, parts) -> {
            Map<Integer, Long> offsets = new HashMap<>();
            parts.forEach(i -> {
                replicaManager.getPartitionOrException(new TopicPartition(topic, i)).log().foreach(log -> offsets.put(i, log.lastStableOffset()));
            });
            partitionOffsets.put(topic, offsets);
        });

        updateLastMirroredOffsetsMetadata(mirrorName, partitionOffsets, true);
    }

    public CompletableFuture<Optional<TopicPartition>> updateMirrorPartitionState(String mirrorName, TopicPartition topicPartition, MirrorPartitionState newState) {
        CompletableFuture<Optional<TopicPartition>> future = new CompletableFuture<>();
        if (isLocalCoordinator(mirrorName)) {
            // this is the leader of the coordinator (async disk I/O operation)
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, getCoordinatingPartitionByKey(new MirrorRecordKey(mirrorName)));
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
            var record = generateMirrorPartitionState(mirrorName, topicPartition, newState);
            var keyBytes = serde.serializeKey(record);
            var valueBytes = serde.serializeValue(record);
            var timestamp = time.milliseconds();
            var memRecord = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, keyBytes, valueBytes));

            replicaManager.appendRecords(
                    // TODO: replace this with Cluster Mirror specific timeout
                    Duration.ofSeconds(5).toMillis(),
                    (short) -1,
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(mirrorTopicIdPartition, memRecord)),
                    partitionResponses -> {
                        partitionResponses.foreach(partitionRes -> {
                            ProduceResponse.PartitionResponse pr = partitionRes._2;
                            if (pr.error.code() != Errors.NONE.code()) {
                                LOG.error("Failed to write partition state to coordinator: {}", pr.error.message());
                                future.completeExceptionally(pr.error.exception());
                            } else {
                                mirrorMetadataManager.updateMirrorPartitionState(mirrorName, topicPartition, newState);
                                future.complete(Optional.of(topicPartition));
                            }
                            return null;
                        });
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of())
            );
        } else {
            // write state data to remote coordinator (async network operation)
            Map<String, Set<MirrorMetadataManager.MirrorPartitionMetadata>> topicMetadata =
                    Map.of(topicPartition.topic(), Set.of(new MirrorMetadataManager.MirrorPartitionMetadata(topicPartition.partition(), newState, -1L)));
            mirrorMetadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
                    res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
                        if (par.errorCode() == Errors.NONE.code()) {
                            mirrorMetadataManager.updateMirrorPartitionState(mirrorName, topicPartition, newState);
                            future.complete(Optional.of(topicPartition));
                        } else {
                            LOG.error("Failed to write partition state to remote coordinator: {}", par.errorCode());
                            future.complete(Optional.empty());
                        }
                    })));

        }
        return future;
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
        mirrorMetadataManager.setStateTransitioner((mirrorName, tp, state) -> transitionTo(mirrorName, Set.of(tp), state));
        mirrorMetadataManager.setCoordinatingPartitionFinder(key -> getCoordinatingPartitionByKey(key));
        scheduler.startup();
        // periodically query source cluster to get the metadata
        long metadataRefreshIntervalMs = kafkaConfig.mirrorConfig().metadataRefreshIntervalMs();
        scheduler.schedule("mirror-metadata-refresh",
                mirrorMetadataManager::refreshMetadata,
                metadataRefreshIntervalMs,
                metadataRefreshIntervalMs
        );
        numPartitions = kafkaConfig.mirrorConfig().mirrorTopicNumPartitions();
    }

    /**
     * Schedules truncation operations for the specified topics.
     * <p>
     * Truncation ensures that partition replicas are aligned to the last successfully
     * mirrored offset from the source cluster before resuming mirroring.
     *
     * @param mirrorName the name of the cluster mirror
     * @param topicPartitions the topics to schedule for truncation
     */
    public void scheduleTruncation(String mirrorName, Set<TopicPartition> topicPartitions) {
        scheduler.scheduleOnce("last-mirrored-offset-truncation",
            () -> mirrorMetadataManager.maybeTruncateToLastMirroredOffsets(replicaManager, mirrorName, topicPartitions,
                    partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING)));
    }

    private Map<String, Map<Integer, Long>> lastMirroredOffsetsToCoordinatorRecords(Map<MirrorMetadataManager.MirrorPartitionKey, Long> offsets) {
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
    public void updateLastMirroredOffsetsMetadata(String mirrorName, Map<String, Map<Integer, Long>> partitionOffsets, boolean stateChange) {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        partitionOffsets.forEach((topic, offsets) -> {
            topicPartitions.addAll(offsets.keySet().stream().map(partition -> new TopicPartition(topic, partition)).collect(Collectors.toSet()));
        });
        if (isLocalCoordinator(mirrorName)) {
            // this is the leader of the coordinator
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, getCoordinatingPartitionByKey(new MirrorRecordKey(mirrorName)));
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);

            var updatedOffsets = mirrorMetadataManager.updateLastMirroredOffsets(mirrorName, partitionOffsets, Map.of());
            var record = generateLastMirroredOffsets(mirrorName, lastMirroredOffsetsToCoordinatorRecords(updatedOffsets));
            var keyBytes = serde.serializeKey(record);
            var valueBytes = serde.serializeValue(record);
            var timestamp = time.milliseconds();
            var memRecord = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, keyBytes, valueBytes));

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
            if (stateChange)
                transitionTo(mirrorName, topicPartitions, MirrorPartitionState.STOPPED);
        } else {
            Map<String, Set<MirrorMetadataManager.MirrorPartitionMetadata>> topicMetadata = new HashMap<>();
            partitionOffsets.forEach((tp, partitionOffsetMap) -> {
                Set<MirrorMetadataManager.MirrorPartitionMetadata> mirroredPartitions = new HashSet<>();
                partitionOffsetMap.forEach((partition, offset) -> {
                    mirroredPartitions.add(new MirrorMetadataManager.MirrorPartitionMetadata(partition, null, offset));
                });
                topicMetadata.put(tp, mirroredPartitions);
            });
            mirrorMetadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(), res -> {
                if (stateChange)
                    transitionTo(mirrorName, topicPartitions, MirrorPartitionState.STOPPED);
            });
        }

    }

    private static CoordinatorRecord generateMirrorPartitionState(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        var key = new MirrorPartitionStateKey().setMirrorName(mirrorName);
        var val = new MirrorPartitionStateValue().setTopicName(topicPartition.topic()).setPartition(topicPartition.partition()).setState(state.value());
        var apiVersion = new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION);
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

    private MirrorMetadataManager.MirrorPartitionStateRecordValue readMirroredPartitionStateValue(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version <= MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION & version >= MirrorPartitionStateValue.LOWEST_SUPPORTED_VERSION) {
            MirrorPartitionStateValue value = new MirrorPartitionStateValue(new ByteBufferAccessor(buffer), version);
            return new MirrorMetadataManager.MirrorPartitionStateRecordValue(value.topicName(), value.partition(), MirrorPartitionState.fromValue(value.state()));
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
                                mirrorMetadataManager.updateLastMirroredOffsets(clusterName, offsets, Map.of());
                            } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                MirrorMetadataManager.MirrorPartitionStateRecordValue value = readMirroredPartitionStateValue(record.value());
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

    interface StateTransitionCallback {
        /**
         * Called for each mirror partition state that was loaded from the coordinator log.
         *
         * @param mirrorName the name of the cluster mirror
         * @param topicPartitions     the topics in this state
         * @param state      the mirror state to operate on
         */
        void onStateLoaded(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState state);
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
        clearCache();
    }

    private void clearCache() {
        remoteBrokers.clear();
        mirrorMetadataManager.clear();
    }

    /**
     * Returns all configured mirrors from the metadata cache.
     *
     * @return the set of all configured mirror names
     */
    public Set<String> getConfiguredMirrors() {
        return mirrorMetadataManager.getConfiguredMirrors();
    }

    /**
     * Returns the configured topics for a specific mirror from the metadata image.
     *
     * @param mirrorName the name of the cluster mirror
     * @return the set of configured topic names for this mirror
     */
    public Set<String> getConfiguredTopics(String mirrorName) {
        return mirrorMetadataManager.getConfiguredTopics(mirrorName);
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
     * Get mirror partitions this broker is fetching from.
     *
     * @param mirrorName mirror name
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
    public int getCoordinatingPartitionByKey(MirrorRecordKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
