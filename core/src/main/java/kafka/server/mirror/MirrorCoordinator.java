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
import org.apache.kafka.common.utils.LogContext;
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
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;

import org.slf4j.Logger;

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

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.utils.Utils.require;

/**
 * Coordinates partition-level state transitions for Cluster Mirroring.
 * Persists state and last mirrored offsets in the {@code __cluster_mirror_state} topic,
 * distributed across brokers by hashing the mirror record key to a partition.
 */
public class MirrorCoordinator {
    private final String name;
    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final KafkaConfig brokerConfig;
    private final ReplicaManager replicaManager;
    private final MirrorMetadataManager metadataManager;
    private final Map<String, MirrorSourceSender> sourceSenders;
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final MetadataCache metadataCache;
    private final Time time;
    private final MirrorRecordSerde serde;

    private volatile int numPartitions = -1;

    public MirrorCoordinator(
            KafkaConfig brokerConfig,
            ReplicaManager replicaManager,
            Scheduler scheduler,
            Metrics metrics,
            MetadataCache metadataCache,
            Time time,
            MirrorMetadataManager mirrorMetadataManager
    ) {
        this.name = "[" + MirrorCoordinator.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorCoordinator.class);
        this.brokerConfig = brokerConfig;
        this.replicaManager = replicaManager;
        this.metadataManager = mirrorMetadataManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;
        this.serde = new MirrorRecordSerde();
        this.metadataCache = metadataCache;
        this.sourceSenders = new HashMap<>();
    }

    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME,
                getCoordinatorPartitionByKey(new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), partition)));
        var optLeaderAndIsr = metadataCache.getLeaderAndIsr(mirrorTopicPartition.topic(), mirrorTopicPartition.partition());
        return optLeaderAndIsr.isPresent() && optLeaderAndIsr.get().leader() == brokerConfig.nodeId();
    }

    // Executes the appropriate actions for a state transition.
    private void handleStateTransition(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        switch (newState) {
            case PREPARING:
                log.debug("PREPARING for topics {}.", topicPartitions);
                scheduleTruncation(mirrorName, topicPartitions);
                break;
            case MIRRORING:
                log.debug("MIRRORING topics {}.", topicPartitions);
                // start mirroring
                replicaManager.maybeCreateMirrorFetchers(mirrorName, topicPartitions);
                break;
            case PAUSING:
                log.info("PAUSING mirroring for topics {}.", topicPartitions);
                replicaManager.mirrorFetcherManager().removeFetcherForPartitions(CollectionConverters.asScala(topicPartitions));
                topicPartitions.forEach(tp -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED));
                break;
            case PAUSED:
                log.info("PAUSED mirroring for topics {}.", topicPartitions);
                // topic is still read-only
                break;
            case STOPPING:
                log.debug("STOPPING for topics {}.", topicPartitions);
                replicaManager.mirrorFetcherManager().removeFetcherForPartitions(CollectionConverters.asScala(topicPartitions));
                mirrorMetadataManager.sendBumpLeaderEpoch(replicaManager.logManager(), topicPartitions);
                truncateToLastStableOffset(topicPartitions, tp -> updateLastMirroredOffsets(mirrorName, Set.of(tp)));
                break;
            case STOPPED:
                log.debug("STOPPED for topics {}.", topicPartitions);
                // topic is writable
                break;
            case FAILED:
                log.debug("FAILED for topics {}.", topicPartitions);
                // TODO: implement recovery actions (i.e. exponential backoff retry)
                break;
            default:
                log.error("Illegal state transition to " + newState);
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    /** Validates and persists partition state, then executes transition actions. */
    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        topicPartitions.forEach(tp -> {
            MirrorPartitionState currentState = metadataManager.getPartitionState(mirrorName, tp);
            if (MirrorPartitionState.isValidTransition(currentState, newState)) {
                log.debug("Transitioning partition {} from {} to {}.", tp, currentState, newState);
                updateMirrorPartitionState(mirrorName, tp, newState)
                        .whenComplete((optTp, ex) -> {
                            if (ex != null) {
                                // TODO: a new component will handle state transitions from a shared queue, so retry means put back in the queue
                                log.error("Failed to update partition state for {}: {}, retrying", tp, ex.getMessage());
                                scheduler.scheduleOnce("MirrorStateUpdateRetry", () -> updateMirrorPartitionState(mirrorName, tp, newState), 100);
                            } else {
                                // successfully writes data into internal log
                                try {
                                    optTp.ifPresent(tp1 -> handleStateTransition(mirrorName, Set.of(tp1), newState));
                                } catch (Exception e) {
                                    log.error("Failed to handle state transition to {} for partition {}", newState, tp, e);
                                }
                            }
                        });
            } else {
                log.warn("Skipping invalid transition from {} to {} for partition {}.",
                        metadataManager.getPartitionState(mirrorName, tp), newState, tp);
            }
        });
    }

    private void truncateToLastStableOffset(Set<TopicPartition> topicPartitions,
                                            Consumer<TopicPartition> callback) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        topicPartitions.forEach(tp -> {
            if (replicaManager.getLog(tp).isDefined()) {
                offsets.put(tp, replicaManager.getLog(tp).get().lastStableOffset());
            } else {
                log.warn("Cannot get the log for partition: {}. Skipping log truncation.", tp);
                callback.accept(tp);
            }
        });
        if (!offsets.isEmpty()) {
            replicaManager.maybeTruncate(offsets, callback);
        }
    }

    /** Writes partition state and offset updates to the internal topic. */
    public void writePartitionStateInfo(String mirrorName,
                                        Map<String, Set<MirrorUtils.PartitionStateInfo>> topicMetadata,
                                        Consumer<WriteMirrorStatesResponse> callback) {
        String updatedMirrorName = MirrorUtils.originalMirrorName(mirrorName);
        Map<String, Map<Integer, Integer>> offsets = new HashMap<>();
        Map<String, Set<Integer>> tps = new HashMap<>();
        topicMetadata.forEach((topic, partitions) -> {
            Set<Integer> partitionIndices = new HashSet<>();
            partitions.forEach(partition -> {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                partitionIndices.add(tp.partition());
                if (partition.state() != null && partition.state() != MirrorPartitionState.UNKNOWN) {
                    updateMirrorPartitionState(updatedMirrorName, tp, partition.state());
                }
                if (partition.leaderEpoch() != -1) {
                    offsets.putIfAbsent(topic, new HashMap<>());
                    offsets.get(topic).put(partition.partition(), partition.leaderEpoch());
                }
            });
            tps.put(topic, partitionIndices);
        });

        updateLastMirroredOffsets(updatedMirrorName, offsets, false);

        WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
        List<WriteMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
        tps.forEach((topic, indices) -> {
            List<WriteMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
            indices.forEach(i -> {
                WriteMirrorStatesResponseData.PartitionResult partitionResult = new WriteMirrorStatesResponseData.PartitionResult();
                partitionResult.setPartitionIndex(i);
                partitionResult.setErrorCode((short) 0);
                partitionResults.add(partitionResult);
            });
            topicResults.add(new WriteMirrorStatesResponseData.TopicResult().setName(topic).setPartitions(partitionResults));
        });

        data.setTopics(topicResults);
        callback.accept(new WriteMirrorStatesResponse(data));
    }

    public void getCachedPartitionMetadata(String mirrorName,
                                           Map<String, Set<Integer>> partitions,
                                           Consumer<ReadMirrorStatesResponse> callback) {
        metadataManager.getCachedPartitionMetadata(mirrorName, partitions, callback);
    }

    private void updateLastMirroredOffsets(String mirrorName, Set<TopicPartition> topicPartitions) {
        Map<String, Map<Integer, Integer>> partitionOffsets = new HashMap<>();
        MirrorUtils.groupPartitionsByTopic(topicPartitions).forEach((topic, parts) -> {
            Map<Integer, Integer> offsets = new HashMap<>();
            parts.forEach(i -> replicaManager.getPartitionOrException(
                    new TopicPartition(topic, i)).log().foreach(log -> {
                        LOG.info("!!! latstepoch for partition {} is {} ", i, log.latestEpoch());
                        // don't need to store anything if latest epoch is empty
                        log.latestEpoch().ifPresent(epoch -> offsets.put(i, epoch));
                return null;
            }));
            partitionOffsets.put(topic, offsets);
        });

        updateLastMirroredOffsets(mirrorName, partitionOffsets, true);
    }

    private CompletableFuture<Optional<TopicPartition>> updateMirrorPartitionState(String mirrorName,
                                                                                  TopicPartition topicPartition,
                                                                                  MirrorPartitionState newState) {
        CompletableFuture<Optional<TopicPartition>> future = new CompletableFuture<>();
        if (isLocalCoordinator(mirrorName, topicPartition.topic(), topicPartition.partition())) {
            // this is the leader of the coordinator (async disk I/O operation)
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME,
                    getCoordinatorPartitionByKey(new MirrorRecordKey(
                            mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition())));
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
                                log.error("Failed to write partition state to coordinator: {}", pr.error.message());
                                future.completeExceptionally(pr.error.exception());
                            } else {
                                metadataManager.updatePartitionState(new MirrorUtils.PartitionKey(mirrorName, topicPartition.topic(), topicPartition.partition()), newState);
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
            Map<String, Set<MirrorUtils.PartitionStateInfo>> topicMetadata =
                    Map.of(topicPartition.topic(), Set.of(new MirrorUtils.PartitionStateInfo(topicPartition.partition(), newState, -1L)));
            metadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
                    res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
                        if (par.errorCode() == Errors.NONE.code()) {
                            metadataManager.updatePartitionState(new MirrorUtils.PartitionKey(mirrorName, topicPartition.topic(), topicPartition.partition()), newState);
                            future.complete(Optional.of(topicPartition));
                        } else {
                            // propagate remote coordinator errors to trigger retry
                            log.error("Failed to write partition state to remote coordinator: {}", par.errorCode());
                            future.completeExceptionally(Errors.forCode(par.errorCode()).exception());
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
            log.warn("Is already running.");
            return;
        }

        log.info("Starting up.");
        numPartitions = brokerConfig.mirrorConfig().topicNumPartitions();
        metadataManager.setStateTransitioner((mirrorName, tp, state) -> transitionTo(mirrorName, Set.of(tp), state));
        metadataManager.setCoordinatorPartitionByKeyFinder(key -> getCoordinatorPartitionByKey(key));
        metadataManager.setCoordinatorPartitionByNameFinder(mirrorName -> getCoordinatorPartitionByName(mirrorName));

        scheduler.startup();

        // periodically query source cluster to get the metadata
        long metadataRefreshIntervalMs = brokerConfig.mirrorConfig().metadataRefreshIntervalMs();
        scheduler.schedule("MirrorMetadataRefresh", metadataManager::syncMetadata, metadataRefreshIntervalMs, metadataRefreshIntervalMs);

        log.info("Startup complete.");
    }

    /** Schedules truncation to align replicas with last mirrored offsets before resuming. */
    private void scheduleTruncation(String mirrorName, Set<TopicPartition> topicPartitions) {
        scheduleTruncation(mirrorName, topicPartitions, 0);
    }

    /** Schedules truncation with retry on failure, using mirror.truncation.backoff.ms as retry delay. */
    private void scheduleTruncation(String mirrorName, Set<TopicPartition> topicPartitions, long delayMs) {
        long retryDelayMs = brokerConfig.mirrorConfig().truncationBackoffMs();
        scheduler.scheduleOnce("LastMirroredOffsetTruncation",
            () -> {
                try {
                    metadataManager.truncateToLastMirroredOffsets(replicaManager, mirrorName, topicPartitions,
                            partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING));
                } catch (Exception e) {
                    log.warn("Failed to truncate to last mirrored offsets for mirror {}, retrying in {} ms", mirrorName, retryDelayMs, e);
                    scheduleTruncation(mirrorName, topicPartitions, retryDelayMs);
                }
            }, delayMs);
    }

    private Map<String, Map<Integer, Integer>> lastMirroredOffsetsToCoordinatorRecords(Map<MirrorUtils.PartitionKey, Integer> offsets) {
        Map<String, Map<Integer, Integer>> results = new HashMap<>();
        offsets.forEach((key, value) -> {
            Map<Integer, Integer> partitionOffsets = results.getOrDefault(key.topic(), new HashMap<>());
            partitionOffsets.put(key.partition(), value);
            results.put(key.topic(), partitionOffsets);
        });
        return results;
    }

    public int getLastMirroredOffset(String mirrorName, TopicPartition topicPartition) {
        return mirrorMetadataManager.getLastMirroredOffset(mirrorName, topicPartition);
    }

    /** Persists offsets to local or remote coordinators, optionally transitioning to STOPPED. */
    public void updateLastMirroredOffsets(String mirrorName, Map<String, Map<Integer, Integer>> partitionOffsets, boolean transitionToStopped) {
        // Separate partitions into local and remote coordinator groups
        Map<Integer, Set<TopicPartition>> localByCoordPartition = new HashMap<>();
        Map<String, Set<MirrorUtils.PartitionStateInfo>> remoteTopicMetadata = new HashMap<>();

        partitionOffsets.forEach((topic, offsetMap) -> {
            offsetMap.forEach((par, off) -> {
                if (isLocalCoordinator(mirrorName, topic, par)) {
                    int coordPartition = getCoordinatorPartitionByKey(
                        new MirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), par));
                    localByCoordPartition
                        .computeIfAbsent(coordPartition, k -> new HashSet<>())
                        .add(new TopicPartition(topic, par));
                } else {
                    remoteTopicMetadata
                        .computeIfAbsent(topic, k -> new HashSet<>())
                        .add(new MirrorUtils.PartitionStateInfo(par, null, off));
                }
            });
        });

        // Update in-memory cache once with all offsets
        var updatedOffsets = metadataManager.updateLastMirroredOffsets(mirrorName, partitionOffsets, Map.of());

        // Write to local coordinator partitions (one append per coordinator partition)
        localByCoordPartition.forEach((coordPartition, tps) -> {
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, coordPartition);
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
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

            // TODO: now we assume all partitions work without error, we should handle error cases
            if (transitionToStopped) {
                tps.forEach(tp ->
                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED));
            }
        });

        // Write to remote coordinator (batched by coordinator node internally)
        if (!remoteTopicMetadata.isEmpty()) {
            metadataManager.writeStatesToRemoteCoordinator(mirrorName, remoteTopicMetadata, Set.of(), res -> {
                if (transitionToStopped) {
                    res.data().topics().forEach(topic ->
                        topic.partitions().forEach(partition ->
                            transitionTo(mirrorName,
                                Set.of(new TopicPartition(topic.name(), partition.partitionIndex())),
                                MirrorPartitionState.STOPPED)));
                }
            });
        }
    }

    private static CoordinatorRecord generateMirrorPartitionState(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        var key = new MirrorPartitionStateKey().setMirrorName(mirrorName);
        var val = new MirrorPartitionStateValue().setTopicName(topicPartition.topic()).setPartition(topicPartition.partition()).setState(state.value());
        var apiVersion = new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private static CoordinatorRecord generateLastMirroredOffsets(String mirrorName, Map<String, Map<Integer, Integer>> offsets) {
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
        if (version == CoordinatorRecordType.LAST_MIRRORED_OFFSETS.id()) {
            return new LastMirroredOffsetsKey(new ByteBufferAccessor(buffer), version).mirrorName();
        } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
            return new MirrorPartitionStateKey(new ByteBufferAccessor(buffer), version).mirrorName();
        } else {
            throw new IllegalArgumentException("Unknown cluster mirror log key version " + version);
        }
    }

    private Map<String, Map<Integer, Integer>> readLastMirroredOffsetsValue(ByteBuffer buffer) {
        Map<String, Map<Integer, Integer>> offsets = new HashMap<>();
        short version = buffer.getShort();
        if (version <= LastMirroredOffsetsValue.HIGHEST_SUPPORTED_VERSION && version >= LastMirroredOffsetsValue.LOWEST_SUPPORTED_VERSION) {
            LastMirroredOffsetsValue value = new LastMirroredOffsetsValue(new ByteBufferAccessor(buffer), version);
            value.topics().forEach(t -> {
                Map<Integer, Integer> partitions = new HashMap<>();
                t.partitions().forEach(p -> partitions.put(p.partitionIndex(), p.lastMirroredOffset()));
                offsets.put(t.name(), partitions);
            });
        } else {
            throw new IllegalStateException("Unsupported last mirrored offsets value version: " + version);
        }
        return offsets;
    }

    private MirrorUtils.PartitionStateLogEntry readMirrorPartitionStateValue(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version <= MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION && version >= MirrorPartitionStateValue.LOWEST_SUPPORTED_VERSION) {
            MirrorPartitionStateValue value = new MirrorPartitionStateValue(new ByteBufferAccessor(buffer), version);
            return new MirrorUtils.PartitionStateLogEntry(value.topicName(), value.partition(), MirrorPartitionState.fromValue(value.state()));
        } else {
            throw new IllegalStateException("Unsupported partition state value version: " + version);
        }
    }

    private void loadMirrorMetadata(TopicPartition topicPartition) {
        log.info("Loading mirror metadata from {}.", topicPartition);
        long logEndOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(() -> -1L);

        replicaManager.getLog(topicPartition).foreach(partitionLog -> {
            ByteBuffer buffer = ByteBuffer.allocate(0);

            // loop breaks if leader changes at any time during the load, since logEndOffset is -1
            long currOffset = partitionLog.logStartOffset();

            // loop breaks if no records have been read, since the end of the log has been reached
            boolean readAtLeastOneRecord = true;
            int maxLength = 5 * 1024 * 1024;

            try {
                // might need a lock
                while (currOffset < logEndOffset && readAtLeastOneRecord && isActive.get()) {
                    log.debug("Reading mirror data from {} at offset {}.", topicPartition, currOffset);
                    FetchDataInfo fetchDataInfo = partitionLog.read(currOffset, maxLength, FetchIsolation.LOG_END, true);

                    readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes() > 0;

                    MemoryRecords memRecords;

                    if (fetchDataInfo.records instanceof MemoryRecords) {
                        memRecords = (MemoryRecords) fetchDataInfo.records;
                    } else if (fetchDataInfo.records instanceof FileRecords) {
                        FileRecords fileRecords = (FileRecords) fetchDataInfo.records;
                        int sizeInBytes = fileRecords.sizeInBytes();
                        int bytesNeeded = Math.max(maxLength, sizeInBytes);

                        // minOneMessage = true in the above read means that the buffer may need to be grown to ensure progress can be made
                        if (buffer.capacity() < bytesNeeded) {
                            if (maxLength < bytesNeeded)
                                log.warn("Loaded mirror data from {} with buffer larger ({} bytes) than " +
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
                            if (version == CoordinatorRecordType.LAST_MIRRORED_OFFSETS.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                Map<String, Map<Integer, Integer>> offsets = readLastMirroredOffsetsValue(record.value());
                                mirrorMetadataManager.updateLastMirroredOffsets(clusterName, offsets, Map.of());
                            } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                MirrorUtils.PartitionStateLogEntry value = readMirrorPartitionStateValue(record.value());
                                metadataManager.updatePartitionState(
                                        new MirrorUtils.PartitionKey(clusterName, value.topic(), value.partition()), value.state());
                            } else {
                                throw new IllegalArgumentException("Unknown cluster mirror log key version " + version);
                            }

                        });
                        currOffset = batch.nextOffset();
                    }
                }
            } catch (Throwable t) {
                log.error("Error loading mirrors from mirror state log {}", topicPartition, t);
            }

            // we've read all the mirrored records, transition the state for each topic partitions
            MirrorUtils.StateTransitionCallback callback = (mirrorName, topics, state)
                    -> handleStateTransition(mirrorName, topics, state);
            metadataManager.applyLoadedPartitionStates(callback);

            return null;
        });
    }

    /** Loads metadata from __mirror_state on leadership election. */
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {
        loadMirrorMetadata(new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionIndex));
    }

    /** Clears cached metadata on leadership resignation. */
    public void onResignation(
            int partitionIndex,
            OptionalInt partitionLeaderEpoch
    ) {
        sourceSenders.clear();
        metadataManager.clear();
    }

    public Set<String> getConfiguredMirrors() {
        return metadataManager.getConfiguredMirrors();
    }

    public Set<String> getConfiguredTopics(String mirrorName) {
        return metadataManager.getConfiguredTopics(mirrorName);
    }

    public String getSourceBootstrap(String mirrorName) {
        return metadataManager.getSourceBootstrap(mirrorName);
    }

    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        return metadataManager.getMirrorStates(mirrorName);
    }

    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Is already shutting down.");
            return;
        }

        log.info("Shutting down.");
        // stop the coordinator's periodic scheduler to prevent tasks running after shutdown
        try {
            scheduler.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down scheduler", e);
        }
        Utils.closeQuietly(metrics, "MirrorCoordinator metrics");
        log.info("Shutdown complete.");
    }

    public int getCoordinatorPartitionByKey(MirrorRecordKey key) {
        throwIfNotActive();
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    private int getCoordinatorPartitionByName(String mirrorName) {
        throwIfNotActive();
        return Utils.abs(mirrorName.hashCode()) % numPartitions;
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
