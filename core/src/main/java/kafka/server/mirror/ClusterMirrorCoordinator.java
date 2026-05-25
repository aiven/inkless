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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordKey;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.UnifiedLog;

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
import java.util.stream.Collectors;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.utils.Utils.require;

/**
 * Coordinates partition-level state transitions for Cluster Mirroring.
 * Persists state and last mirrored offsets in the {@code __cluster_mirror_state} topic,
 * distributed across brokers by hashing the mirror record key to a partition.
 */
public class ClusterMirrorCoordinator {
    private static final long RETRY_DELAY_MS = 5000L;

    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final KafkaConfig brokerConfig;
    private final ReplicaManager replicaManager;
    private final MirrorMetadataManager metadataManager;
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final MetadataCache metadataCache;
    private final Time time;
    private final ClusterMirrorRecordSerde serde;

    // Exponential backoff for FAILED state retries with 20% jitter.
    // With defaults (initial=1s, max=300s): attempt 0 ~1s, attempt 1 ~2s, attempt 2 ~4s, ..., attempt 8+ ~300s.
    private ExponentialBackoff failedRetryBackoff;

    private volatile int numPartitions = -1;

    public ClusterMirrorCoordinator(
            KafkaConfig brokerConfig,
            ReplicaManager replicaManager,
            Scheduler scheduler,
            Metrics metrics,
            MetadataCache metadataCache,
            Time time,
            MirrorMetadataManager mirrorMetadataManager
    ) {
        String name = "[" + ClusterMirrorCoordinator.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinator.class);
        this.brokerConfig = brokerConfig;
        this.replicaManager = replicaManager;
        this.metadataManager = mirrorMetadataManager;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;
        this.serde = new ClusterMirrorRecordSerde();
        this.metadataCache = metadataCache;
    }

    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME,
                getCoordinatorPartitionByKey(new ClusterMirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), partition)));
        var optLeaderAndIsr = metadataCache.getLeaderAndIsr(mirrorTopicPartition.topic(), mirrorTopicPartition.partition());
        return optLeaderAndIsr.isPresent() && optLeaderAndIsr.get().leader() == brokerConfig.nodeId();
    }

    // Executes the appropriate actions for a state transition.
    private void handleStateTransition(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        switch (newState) {
            case LOG_TRUNCATION:
                log.info("LOG_TRUNCATION for topics {}.", topicPartitions);
                scheduleTruncation(mirrorName, topicPartitions);
                break;
            case EPOCH_FENCING:
                log.info("EPOCH_FENCING for topics {}.", topicPartitions);
                metadataManager.scheduleBumpLeaderEpochs(mirrorName, topicPartitions)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            log.error("Failed to bump leader epoch for {}", topicPartitions, ex);
                            transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED);
                        } else {
                            transitionTo(mirrorName, topicPartitions, MirrorPartitionState.MIRRORING);
                        }
                    });
                break;
            case MIRRORING:
                log.info("MIRRORING topics {}.", topicPartitions);
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
                log.info("STOPPING for topics {}.", topicPartitions);
                // The order of STOPPING process:
                // 1. remove fetcher for mirror fetcher
                // 2. store LME because that's the last mirrored epoch before bumping leader epoch
                // 3. bump leader epoch to draw a line for future failback cluster recognize the new added records
                // 4. abort ongoing transactions using the updated leader epoch
                // 5. reset pid to expire all existing PIDs, including the new appended records in (4)
                // 6. move to STOPPED state
                replicaManager.mirrorFetcherManager().removeFetcherForPartitions(CollectionConverters.asScala(topicPartitions));
                collectAndUpdateLastMirrorEpochs(mirrorName, topicPartitions)
                    .thenCompose(v -> metadataManager.sendBumpLeaderEpochs(
                            metadataManager.buildLocalEpochBumpTargets(replicaManager.logManager(), topicPartitions)))
                    .thenCompose(v -> abortOngoingTransactions(topicPartitions))
                    .thenAccept(v -> writeMirrorPidResetAndStop(mirrorName, topicPartitions))
                    .exceptionally(ex -> {
                        log.error("Failed STOPPING transition for {}", topicPartitions, ex);
                        return null;
                    });
                break;
            case STOPPED:
                log.info("STOPPED for topics {}.", topicPartitions);
                // Topic is writable.
                break;
            case FAILED:
                log.info("FAILED for topics {}.", topicPartitions);
                scheduleFailedRetries(mirrorName, topicPartitions);
                break;
            default:
                log.error("Illegal state transition to {}", newState);
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    private void updateRetryAttemptsAndPrevState(String mirrorName, TopicPartition tp, MirrorPartitionState currentState, MirrorPartitionState newState) {
        ClusterMirrorUtils.PartitionKey key = new ClusterMirrorUtils.PartitionKey(mirrorName, tp.topic(), tp.partition());
        if (newState == MirrorPartitionState.FAILED) {
            metadataManager.failedRetryAttempts().merge(tp, 1, Integer::sum);
            metadataManager.partitionPreviousStates().putIfAbsent(key, currentState);
        } else if (newState == MirrorPartitionState.STOPPED
                || newState == MirrorPartitionState.PAUSED) {
            metadataManager.failedRetryAttempts().remove(tp);
            metadataManager.partitionPreviousStates().remove(key);
        }
    }

    private void scheduleFailedRetries(String mirrorName, Set<TopicPartition> topicPartitions) {
        int maxAttempts = brokerConfig.mirrorConfig().failedRetryMaxAttempts();
        topicPartitions.forEach(tp -> {
            int attempt = metadataManager.failedRetryAttempts().getOrDefault(tp, 0);
            if (maxAttempts > 0 && attempt >= maxAttempts) {
                log.error("Partition {} exceeded max retry attempts ({}), requires manual intervention.", tp, maxAttempts);
                return;
            }
            long delay = failedRetryBackoff.backoff(attempt);
            MirrorPartitionState targetState = metadataManager.partitionPreviousStates().getOrDefault(new ClusterMirrorUtils.PartitionKey(mirrorName, tp.topic(), tp.partition()), MirrorPartitionState.MIRRORING);
            log.info("Scheduling retry #{} for partition {} in {} ms with target state {}.", attempt, tp, delay, targetState);
            scheduler.scheduleOnce("MirrorFailedRetry-" + tp,
                () -> transitionTo(mirrorName, Set.of(tp), targetState), delay);
        });
    }

    /** Validates and persists partition state, then executes transition actions. */
    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        topicPartitions.forEach(tp -> {
            MirrorPartitionState currentState = metadataManager.getPartitionState(mirrorName, tp);
            if (MirrorPartitionState.isValidTransition(currentState, newState)) {
                log.debug("Transitioning partition {} from {} to {}.", tp, currentState, newState);
                updateRetryAttemptsAndPrevState(mirrorName, tp, currentState, newState);
                updateMirrorPartitionState(mirrorName, tp, newState)
                        .whenComplete((optTp, ex) -> {
                            if (ex != null) {
                                // TODO: a new component will handle state transitions from a shared queue, so retry means put back in the queue
                                log.error("Failed to update partition state for {}: {}, retrying", tp, ex.getMessage());
                                scheduler.scheduleOnce("MirrorStateUpdateRetry", () -> updateMirrorPartitionState(mirrorName, tp, newState), 100);
                            } else {
                                // successfully writes data into internal log
                                try {
                                    optTp.ifPresent(tp1 -> {
                                        // Atomically check-and-update to prevent duplicate transitions under concurrent calls.
                                        // MIRRORING is always re-run because the fetcher may have been removed on leadership
                                        // change and must be re-added (the operation is idempotent).
                                        boolean[] shouldTransition = {false};
                                        metadataManager.pendingPartitionStates().compute(tp1, (key, pendingState) -> {
                                            if (pendingState == MirrorPartitionState.MIRRORING || pendingState != newState) {
                                                shouldTransition[0] = true;
                                                return newState;
                                            }
                                            return pendingState;
                                        });
                                        if (shouldTransition[0]) {
                                            handleStateTransition(mirrorName, Set.of(tp1), newState);
                                        } else {
                                            log.debug("State transition for partition {} is already inflight, skipping.", tp1);
                                        }
                                    });
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

    /**
     * {@link kafka.server.ReplicaManager#appendRecords} allows only one record batch per partition per call,
     * otherwise, the LogValidator#getFirstBatchAndMaybeValidateNoMoreBatches will throw exception.
     * So we append in multiple rounds when needed.
     */
    private CompletableFuture<Void> abortOngoingTransactions(Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, List<MemoryRecords>> recordsByPartition = new HashMap<>();
        topicPartitions.forEach(tp -> {
            Option<List<MemoryRecords>> record = replicaManager.getLog(tp).map(UnifiedLog::buildEndTransactionRecords);
            if (record.isDefined() && !record.get().isEmpty()) {
                recordsByPartition.put(tp, record.get());
            }
        });

        if (recordsByPartition.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.debug("Appending abort markers for partitions: {}", recordsByPartition.keySet());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        while (!recordsByPartition.isEmpty()) {
            Map<TopicIdPartition, MemoryRecords> records = new HashMap<>();
            recordsByPartition.entrySet().removeIf(entry -> {
                TopicPartition tp = entry.getKey();
                List<MemoryRecords> recordsList = entry.getValue();
                Iterator<MemoryRecords> it = recordsList.iterator();
                if (it.hasNext()) {
                    MemoryRecords record = it.next();
                    records.put(replicaManager.topicIdPartition(tp), record);
                    it.remove();
                }
                return recordsList.isEmpty();
            });
            futures.add(appendAbortMarkers(records));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> appendAbortMarkers(Map<TopicIdPartition, MemoryRecords> records) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        replicaManager.appendRecords(
            // TODO: replace this with Cluster Mirror specific timeout
            Duration.ofSeconds(5).toMillis(),
            (short) -1,
            true,
            AppendOrigin.COORDINATOR,
            CollectionConverters.asScala(records),
            partitionResponses -> {
                partitionResponses.foreach(partitionRes -> {
                    ProduceResponse.PartitionResponse pr = partitionRes._2;
                    if (pr.error.code() != Errors.NONE.code()) {
                        // TODO: retry logic
                        log.error("Failed to append abort marker for {}: {}", partitionRes._1.topicPartition(), pr.error.message());
                    }
                    return null;
                });
                future.complete(null);

                return null;
            },
            ignored -> null,
            RequestLocal.noCaching(),
            CollectionConverters.asScala(Map.of())
        );
        return future;
    }

    /** Writes partition state and offset updates to the internal topic. */
    public void writePartitionStateInfo(String mirrorName,
                                        Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> topicMetadata,
                                        Consumer<WriteMirrorStatesResponse> callback) {
        String updatedMirrorName = ClusterMirrorUtils.originalMirrorName(mirrorName);
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

        updateLastMirrorEpochs(updatedMirrorName, offsets);

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

    private CompletableFuture<Void> collectAndUpdateLastMirrorEpochs(String mirrorName, Set<TopicPartition> topicPartitions) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Map<String, Map<Integer, Integer>> partitionOffsets = new HashMap<>();
        ClusterMirrorUtils.groupPartitionsByTopic(topicPartitions).forEach((topic, parts) -> {
            Map<Integer, Integer> offsets = new HashMap<>();
            parts.forEach(i -> replicaManager.getPartitionOrException(
                    new TopicPartition(topic, i)).log().foreach(log -> {
                        // don't need to store anything if latest epoch is empty
                        log.latestEpoch().ifPresent(epoch -> offsets.put(i, epoch));
                        return null;
                    }));
            partitionOffsets.put(topic, offsets);
        });

        futures.add(updateLastMirrorEpochs(mirrorName, partitionOffsets));

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Optional<TopicPartition>> updateMirrorPartitionState(String mirrorName,
                                                                                  TopicPartition topicPartition,
                                                                                  MirrorPartitionState newState) {
        CompletableFuture<Optional<TopicPartition>> future = new CompletableFuture<>();
        // no need to write a record for the same state again
        if (metadataManager.getPartitionState(mirrorName, topicPartition) == newState) {
            future.complete(Optional.of(topicPartition));
            return future;
        }
        if (isLocalCoordinator(mirrorName, topicPartition.topic(), topicPartition.partition())) {
            // this is the leader of the coordinator (async disk I/O operation)
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME,
                    getCoordinatorPartitionByKey(new ClusterMirrorRecordKey(
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
                                metadataManager.updatePartitionState(new ClusterMirrorUtils.PartitionKey(mirrorName, topicPartition.topic(), topicPartition.partition()), newState);
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
            Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> topicMetadata =
                    Map.of(topicPartition.topic(), Set.of(new ClusterMirrorUtils.PartitionStateInfo(topicPartition.partition(), newState, -1)));
            metadataManager.writeStatesToRemoteCoordinator(mirrorName, topicMetadata, Set.of(),
                    res -> res.data().topics().forEach(topic -> topic.partitions().forEach(par -> {
                        if (par.errorCode() == Errors.NONE.code()) {
                            metadataManager.updatePartitionState(new ClusterMirrorUtils.PartitionKey(mirrorName, topicPartition.topic(), topicPartition.partition()), newState);
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
        numPartitions = brokerConfig.mirrorConfig().stateTopicNumPartitions();
        failedRetryBackoff = new ExponentialBackoff(
                brokerConfig.mirrorConfig().failedRetryInitialBackoffMs(),
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                brokerConfig.mirrorConfig().failedRetryMaxBackoffMs(),
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
        metadataManager.initialize(
                (mirrorName, tp, state) -> transitionTo(mirrorName, Set.of(tp), state),
                this::tombstoneMirrorRecords,
                this::getCoordinatorPartitionByKey,
                this::getCoordinatorPartitionByName);

        scheduler.startup();

        // periodically query source cluster to get the metadata
        long metadataRefreshIntervalMs = brokerConfig.mirrorConfig().metadataRefreshIntervalMs();
        scheduler.schedule("MirrorMetadataRefresh", metadataManager::periodicSync, 0, metadataRefreshIntervalMs);

        log.info("Startup complete.");
    }

    /** Schedules truncation to align replicas with last mirrored epoch. */
    private void scheduleTruncation(String mirrorName, Set<TopicPartition> topicPartitions) {
        final Consumer<TopicPartition> truncateCallback =
            partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING);
        scheduler.scheduleOnce("LastMirrorEpochsTruncation",
            () -> {
                try {
                    metadataManager.truncateToLastMirrorEpochs(mirrorName, topicPartitions)
                        .whenComplete((descriptions, error) -> {
                            if (error != null) {
                                if (error instanceof UnsupportedVersionException) {
                                    log.warn("Source cluster doesn't support DESCRIBE_CLUSTER_MIRRORS. " +
                                        "Replication will be one-way without possible failback");
                                    Map<TopicPartition, Integer> epochs = topicPartitions.stream()
                                        .collect(Collectors.toMap(tp -> tp, tp -> -1));
                                    replicaManager.maybeTruncateForLeaderEpoch(epochs, truncateCallback);
                                } else {
                                    log.warn("Failed to truncate to last mirrored offsets for mirror {}", mirrorName, error);
                                    transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED);
                                }
                                return;
                            }

                            ClusterMirrorDescription description = descriptions.get(mirrorName);
                            if (description == null) {
                                log.warn("Mirror {} not found in source cluster. Replication will be one-way without possible failback",
                                    mirrorName);
                                Map<TopicPartition, Integer> epochs = topicPartitions.stream()
                                    .collect(Collectors.toMap(tp -> tp, tp -> -1));
                                replicaManager.maybeTruncateForLeaderEpoch(epochs, truncateCallback);
                                return;
                            }

                            Map<TopicPartition, Integer> epochs = new HashMap<>();
                            description.topics().forEach((topicName, leaderState) -> {
                                leaderState.forEach(leaderStateEntry -> {
                                    epochs.put(leaderStateEntry.topicPartition(), leaderStateEntry.lastMirrorEpoch());
                                });
                            });

                            // if the source cluster doesn't have mirror info, no result will be returned.
                            if (epochs.size() != topicPartitions.size()) {
                                topicPartitions.forEach(partition -> {
                                    if (!epochs.containsKey(partition)) {
                                        epochs.put(partition, -1);
                                    }
                                });
                            }

                            replicaManager.maybeTruncateForLeaderEpoch(epochs,
                                    partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING));
                        });
                } catch (Exception e) {
                    log.warn("Failed to truncate to last mirrored offsets for mirror {}", mirrorName, e);
                    transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED);
                }
            }, 0);
    }

    /**
     * After bump epoch and last-mirrored updates, writes the PID reset barrier per partition and transition to STOPPED state.
     * Retry the failed partitions until success.
     * TODO: handle failure, we can't retry forever
     */
    private void writeMirrorPidResetAndStop(String mirrorName, Set<TopicPartition> topicPartitions) {
        writeMirrorPidResetBarrier(mirrorName, topicPartitions).whenComplete((responses, ex) -> {
            if (ex != null) {
                log.error("Failed to write PID reset barrier for partitions {} in mirror {}", topicPartitions, mirrorName, ex);
                scheduler.scheduleOnce("MirrorPidResetBarrierRetry",
                    () -> writeMirrorPidResetAndStop(mirrorName, topicPartitions),
                        RETRY_DELAY_MS);
                return;
            }
            Set<TopicPartition> succeeded = new HashSet<>();
            Set<TopicPartition> failed = new HashSet<>();
            for (TopicPartition tp : topicPartitions) {
                ProduceResponse.PartitionResponse pr = responses.get(tp);
                if (pr == null) {
                    log.warn("Missing PID reset barrier response for partition {} in mirror {}, will retry", tp, mirrorName);
                    failed.add(tp);
                } else if (pr.error.code() == Errors.NONE.code()) {
                    succeeded.add(tp);
                } else {
                    log.warn("PID reset barrier error for partition {} in mirror {}: {}, will retry",
                        tp, mirrorName, pr.error.message());
                    failed.add(tp);
                }
            }
            if (!succeeded.isEmpty()) {
                transitionTo(mirrorName, succeeded, MirrorPartitionState.STOPPED);
            }
            if (!failed.isEmpty()) {
                scheduler.scheduleOnce("MirrorPidResetBarrierRetry",
                    () -> writeMirrorPidResetAndStop(mirrorName, failed),
                        RETRY_DELAY_MS);
            }
        });
    }

    private CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> writeMirrorPidResetBarrier(
            String mirrorName,
            Set<TopicPartition> topicPartitions) {
        CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> writePidResetBarrierFuture = new CompletableFuture<>();
        Uuid sourceClusterId = metadataManager.getSourceClusterId(mirrorName);
        if (sourceClusterId == null) {
            log.warn("Source cluster ID not available for mirror {}. Skipping PID reset barrier.", mirrorName);
            writePidResetBarrierFuture.complete(Map.of());
            return writePidResetBarrierFuture;
        }
        MirrorPidResetRecord record = new MirrorPidResetRecord()
            .setVersion(ControlRecordUtils.MIRROR_PID_RESET_CURRENT_VERSION)
            .setSourceClusterId(sourceClusterId.toString());
        long timestamp = time.milliseconds();
        for (TopicPartition tp : topicPartitions) {
            try {
                var topicIdPartition = replicaManager.topicIdPartition(tp);
                int bufferSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + 256;
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                MemoryRecords records = MemoryRecords.withMirrorPidResetRecord(0, timestamp, 0, buffer, record);
                replicaManager.appendRecords(
                    // TODO: replace this with Cluster Mirror specific timeout
                    Duration.ofSeconds(5).toMillis(),
                    (short) -1, // request ack from all ISR
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(topicIdPartition, records)),
                    partitionResponses -> {
                        Map<TopicPartition, ProduceResponse.PartitionResponse> result = new HashMap<>();
                        partitionResponses.foreach(partitionRes -> {
                            ProduceResponse.PartitionResponse pr = partitionRes._2;
                            if (pr.error.code() != Errors.NONE.code()) {
                                log.error("Failed to write PID reset barrier for partition {} in mirror {}: {}",
                                        tp, mirrorName, pr.error.message());
                            } else {
                                log.info("Wrote mirror PID reset barrier for partition {} in mirror {}", tp, mirrorName);
                            }
                            result.put(tp, pr);
                            return null;
                        });
                        writePidResetBarrierFuture.complete(result);
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of())
                );
            } catch (Exception e) {
                log.error("Failed to write PID reset barrier for partition {} in mirror {}", tp, mirrorName, e);
                writePidResetBarrierFuture.completeExceptionally(e);
            }
        }
        return writePidResetBarrierFuture;
    }

    private Map<String, Map<Integer, Integer>> lastMirrorEpochsToCoordinatorRecords(Map<ClusterMirrorUtils.PartitionKey, Integer> offsets) {
        Map<String, Map<Integer, Integer>> results = new HashMap<>();
        offsets.forEach((key, value) -> {
            Map<Integer, Integer> partitionOffsets = results.getOrDefault(key.topic(), new HashMap<>());
            partitionOffsets.put(key.partition(), value);
            results.put(key.topic(), partitionOffsets);
        });
        return results;
    }

    public Map<TopicPartition, Integer> getLastMirrorEpochs(String mirrorName) {
        return metadataManager.getLastMirrorEpochs(mirrorName);
    }

    /** Persists offsets to local or remote coordinators. */
    public CompletableFuture<Void> updateLastMirrorEpochs(String mirrorName, Map<String, Map<Integer, Integer>> partitionOffsets) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Separate partitions into local and remote coordinator groups
        Map<Integer, Set<TopicPartition>> localByCoordPartition = new HashMap<>();
        Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> remoteTopicMetadata = new HashMap<>();

        partitionOffsets.forEach((topic, offsetMap) -> {
            offsetMap.forEach((par, off) -> {
                if (isLocalCoordinator(mirrorName, topic, par)) {
                    int coordPartition = getCoordinatorPartitionByKey(
                        new ClusterMirrorRecordKey(mirrorName, metadataCache.getTopicId(topic), par));
                    localByCoordPartition
                        .computeIfAbsent(coordPartition, k -> new HashSet<>())
                        .add(new TopicPartition(topic, par));
                } else {
                    remoteTopicMetadata
                        .computeIfAbsent(topic, k -> new HashSet<>())
                        .add(new ClusterMirrorUtils.PartitionStateInfo(par, null, off));
                }
            });
        });

        // No partitions need updating (e.g. empty topic with no leader epoch)
        if (localByCoordPartition.isEmpty() && remoteTopicMetadata.isEmpty()) {
            future.complete(null);
            return future;
        }

        // Update in-memory cache once with all offsets
        var updatedOffsets = metadataManager.updateLastMirrorEpochs(mirrorName, partitionOffsets, Map.of());

        // Write to local coordinator partitions (one append per coordinator partition)
        localByCoordPartition.forEach((coordPartition, tps) -> {
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, coordPartition);
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
            var record = generateLastMirrorEpochs(mirrorName, lastMirrorEpochsToCoordinatorRecords(updatedOffsets));
            var keyBytes = serde.serializeKey(record);
            var valueBytes = serde.serializeValue(record);
            var timestamp = time.milliseconds();
            var memRecord = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, keyBytes, valueBytes));

            replicaManager.appendRecords(
                    // TODO: replace this with Cluster Mirror specific timeout
                    Duration.ofSeconds(5).toMillis(),
                    (short) -1, // request ack from all ISR
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(mirrorTopicIdPartition, memRecord)),
                    response -> {
                        response.foreach(res -> {
                            ProduceResponse.PartitionResponse partitionRes = res._2;
                            if (partitionRes.error.code() != Errors.NONE.code()) {
                                // TODO: retry logic
                                log.error("Failed to write last mirrored offsets to coordinator: {}", partitionRes.error.message());
                                future.completeExceptionally(partitionRes.error.exception());
                            } else {
                                future.complete(null);
                            }
                            return null;
                        });
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of())
            );
        });

        // Write to remote coordinator (batched by coordinator node internally)
        if (!remoteTopicMetadata.isEmpty()) {
            metadataManager.writeStatesToRemoteCoordinator(mirrorName, remoteTopicMetadata, Set.of(), res -> {
                res.data().topics().forEach(topicResult -> {
                    topicResult.partitions().forEach(partitionResult -> {
                        if (partitionResult.errorCode() != Errors.NONE.code()) {
                            log.error("Failed to write last mirrored offsets to remote coordinator: {}", partitionResult.errorCode());
                            future.completeExceptionally(Errors.forCode(partitionResult.errorCode()).exception());
                        } else {
                            future.complete(null);
                        }
                    });
                });
            });
        }

        return future;
    }

    private CoordinatorRecord generateMirrorPartitionState(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        short retryAttempt = (state == MirrorPartitionState.FAILED)
                ? metadataManager.failedRetryAttempts().getOrDefault(topicPartition, 0).shortValue()
                : 0;
        var key = new MirrorPartitionStateKey().setMirrorName(mirrorName);
        var val = new MirrorPartitionStateValue()
                .setTopicName(topicPartition.topic())
                .setPartition(topicPartition.partition())
                .setState(state.value())
                .setPreviousState(metadataManager.partitionPreviousStates().getOrDefault(
                        new ClusterMirrorUtils.PartitionKey(mirrorName, topicPartition.topic(), topicPartition.partition()), MirrorPartitionState.UNKNOWN).value())
                .setRetryAttempt(retryAttempt);
        var apiVersion = new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private static CoordinatorRecord generateLastMirrorEpochs(String mirrorName, Map<String, Map<Integer, Integer>> offsets) {
        var key = new LastMirrorEpochsKey().setMirrorName(mirrorName);
        var val = new LastMirrorEpochsValue();
        var topics = new ArrayList<LastMirrorEpochsValue.Topic>();
        offsets.forEach((topic, partitionOffsets) -> {
            var top = new LastMirrorEpochsValue.Topic().setName(topic);
            List<LastMirrorEpochsValue.Partition> partitions = new ArrayList<>();
            partitionOffsets.forEach((partition, offset) ->
                    partitions.add(new LastMirrorEpochsValue.Partition().setPartitionIndex(partition).setLastMirrorEpoch(offset)));
            top.setPartitions(partitions);
            topics.add(top);
        });
        val.setTopics(topics);
        var apiVersion = new ApiMessageAndVersion(val, LastMirrorEpochsValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private void tombstoneMirrorRecords(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> states = metadataManager.getMirrorStates(mirrorName);
        // Collect local coordinator partitions that need tombstones
        Set<Integer> localCoordPartitions = new HashSet<>();
        states.forEach((tp, state) -> {
            if (isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                localCoordPartitions.add(getCoordinatorPartitionByKey(
                    new ClusterMirrorRecordKey(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
            }
        });

        // Write one partition state tombstone and one offset tombstone per coordinator partition
        var timestamp = time.milliseconds();
        var stateTombstone = CoordinatorRecord.tombstone(new MirrorPartitionStateKey().setMirrorName(mirrorName));
        var offsetTombstone = CoordinatorRecord.tombstone(new LastMirrorEpochsKey().setMirrorName(mirrorName));

        for (int coordPartition : localCoordPartitions) {
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, coordPartition);
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
            var memRecord = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(timestamp, serde.serializeKey(stateTombstone), serde.serializeValue(stateTombstone)),
                new SimpleRecord(timestamp, serde.serializeKey(offsetTombstone), serde.serializeValue(offsetTombstone))
            );
            replicaManager.appendRecords(
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(Map.of(mirrorTopicIdPartition, memRecord)),
                partitionResponses -> null,
                ignored -> null,
                RequestLocal.noCaching(),
                CollectionConverters.asScala(Map.of())
            );
        }

        // Clean up in-memory caches
        states.keySet().forEach(tp ->
            metadataManager.removePartitionState(
                new ClusterMirrorUtils.PartitionKey(mirrorName, tp.topic(), tp.partition())));
        metadataManager.removeLastMirrorEpochs(mirrorName);
    }

    private String readMirrorNameFromKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version == CoordinatorRecordType.LAST_MIRROR_EPOCHS.id()) {
            return new LastMirrorEpochsKey(new ByteBufferAccessor(buffer), version).mirrorName();
        } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
            return new MirrorPartitionStateKey(new ByteBufferAccessor(buffer), version).mirrorName();
        } else {
            throw new IllegalArgumentException("Unknown cluster mirror log key version " + version);
        }
    }

    private Map<String, Map<Integer, Integer>> readLastMirrorEpochsValue(ByteBuffer buffer) {
        Map<String, Map<Integer, Integer>> offsets = new HashMap<>();
        short version = buffer.getShort();
        if (version <= LastMirrorEpochsValue.HIGHEST_SUPPORTED_VERSION && version >= LastMirrorEpochsValue.LOWEST_SUPPORTED_VERSION) {
            LastMirrorEpochsValue value = new LastMirrorEpochsValue(new ByteBufferAccessor(buffer), version);
            value.topics().forEach(t -> {
                Map<Integer, Integer> partitions = new HashMap<>();
                t.partitions().forEach(p -> partitions.put(p.partitionIndex(), p.lastMirrorEpoch()));
                offsets.put(t.name(), partitions);
            });
        } else {
            throw new IllegalStateException("Unsupported last mirrored offsets value version: " + version);
        }
        return offsets;
    }

    private ClusterMirrorUtils.PartitionStateLogEntry readMirrorPartitionStateValue(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version <= MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION && version >= MirrorPartitionStateValue.LOWEST_SUPPORTED_VERSION) {
            MirrorPartitionStateValue value = new MirrorPartitionStateValue(new ByteBufferAccessor(buffer), version);
            return new ClusterMirrorUtils.PartitionStateLogEntry(value.topicName(), value.partition(),
                    MirrorPartitionState.fromValue(value.state()),
                    MirrorPartitionState.fromValue(value.previousState()), value.retryAttempt());
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
                    } else if (fetchDataInfo.records instanceof FileRecords fileRecords) {
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
                    for (MutableRecordBatch batch : memRecords.batches()) {
                        batch.iterator().forEachRemaining(record -> {
                            require(record.hasKey(), "Mirror log's key should not be null");
                            short version = record.key().getShort();
                            if (version == CoordinatorRecordType.LAST_MIRROR_EPOCHS.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                Map<String, Map<Integer, Integer>> offsets = readLastMirrorEpochsValue(record.value());
                                metadataManager.updateLastMirrorEpochs(clusterName, offsets, Map.of());
                            } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
                                String clusterName = readMirrorNameFromKey(record.key());
                                ClusterMirrorUtils.PartitionStateLogEntry value = readMirrorPartitionStateValue(record.value());
                                ClusterMirrorUtils.PartitionKey pk = new ClusterMirrorUtils.PartitionKey(
                                        clusterName, value.topic(), value.partition());
                                metadataManager.updatePartitionState(pk, value.state());
                                metadataManager.partitionPreviousStates().put(pk, value.previousState());
                                TopicPartition tp = new TopicPartition(value.topic(), value.partition());
                                if (value.state() == MirrorPartitionState.FAILED && value.retryAttempt() > 0) {
                                    metadataManager.failedRetryAttempts().put(tp, value.retryAttempt());
                                } else {
                                    metadataManager.failedRetryAttempts().remove(tp);
                                }
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
            metadataManager.applyLoadedPartitionStates(this::handleStateTransition);

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
        metadataManager.clear();
    }

    public Set<String> getConfiguredMirrors() {
        return metadataManager.getConfiguredMirrors();
    }

    public int getActiveTopicCount(String mirrorName) {
        return metadataManager.getActiveTopicCount(mirrorName);
    }

    public String getSourceBootstrap(String mirrorName) {
        return metadataManager.getSourceBootstrap(mirrorName);
    }

    public Uuid getSourceClusterId(String mirrorName) {
        return metadataManager.getSourceClusterId(mirrorName);
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
        Utils.closeQuietly(metrics, "ClusterMirrorCoordinator metrics");
        log.info("Shutdown complete.");
    }

    public int getCoordinatorPartitionByKey(ClusterMirrorRecordKey key) {
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
