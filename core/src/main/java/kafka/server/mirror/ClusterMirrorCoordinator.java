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
import kafka.server.mirror.ClusterMirrorUtils.FailedPartitionInfo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
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
import org.apache.kafka.server.common.MirrorPartitionState;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static kafka.server.mirror.ClusterMirrorUtils.NON_RETRYABLE_ATTEMPT;
import static org.apache.kafka.common.utils.Utils.require;

/**
 * Coordinates partition-level state transitions for Cluster Mirroring, persisting partition
 * state and last mirror epochs to the {@code __mirror_state} topic.
 *
 * <p>Load is distributed by hashing each record key (mirror name + topic id + partition) to a
 * {@code __mirror_state} partition. Each broker coordinates the partitions it leads in that
 * topic; writes targeting a remote coordinator are forwarded via {@link MirrorMetadataManager}.
 */
@SuppressWarnings("ClassFanOutComplexity")
public class ClusterMirrorCoordinator {
    private final Logger log;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final KafkaConfig brokerConfig;
    private final ReplicaManager replicaManager;
    private final MirrorMetadataManager metadataManager;
    private final MetadataCache metadataCache;
    private final ClusterMirrorRecordSerde serde = new ClusterMirrorRecordSerde();
    private final Scheduler scheduler;
    private final Metrics metrics;
    private final Time time;

    public ClusterMirrorCoordinator(
            KafkaConfig brokerConfig,
            ReplicaManager replicaManager,
            MirrorMetadataManager metadataManager,
            MetadataCache metadataCache,
            Scheduler scheduler,
            Metrics metrics,
            Time time
    ) {
        String name = "[" + ClusterMirrorCoordinator.class.getSimpleName() + " id=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinator.class);
        this.brokerConfig = brokerConfig;
        this.replicaManager = replicaManager;
        this.metadataManager = metadataManager;
        this.metadataCache = metadataCache;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.time = time;
    }

    private boolean isLocalCoordinator(String mirrorName, String topic, int partition) {
        var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME,
                getCoordinatorPartitionByKey(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), partition)));
        var optLeaderAndIsr = metadataCache.getLeaderAndIsr(mirrorTopicPartition.topic(), mirrorTopicPartition.partition());
        return optLeaderAndIsr.isPresent() && optLeaderAndIsr.get().leader() == brokerConfig.nodeId();
    }

    /** Starts the coordinator. */
    public void start() {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("Is already running.");
            return;
        }

        log.info("Starting up.");

        metadataManager.initialize(
                brokerConfig.mirrorConfig().metadataRefreshIntervalMs(),
                (mirrorName, tp, state, errorMessage, nonRetryable) -> transitionTo(mirrorName, tp, state, errorMessage, nonRetryable),
                this::tombstoneMirrorRecords,
                this::getCoordinatorPartitionByKey,
                this::getCoordinatorPartitionByName);

        log.info("Startup complete.");
    }

    /** Shuts down the coordinator. */
    public void shutdown() {
        if (!isRunning.compareAndSet(true, false)) {
            log.warn("Is already shutting down.");
            return;
        }

        log.info("Shutting down.");

        // Close source admin clients before the scheduler shutdown so that any
        // in-flight periodicSync admin calls (describeCluster, describeTopics)
        // fail immediately instead of blocking for up to requestTimeoutMs each.
        metadataManager.closeSourceAdmins();
        try {
            scheduler.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down scheduler", e);
        }

        Utils.closeQuietly(metrics, "ClusterMirrorCoordinator metrics");

        log.info("Shutdown complete.");
    }

    /** Loads metadata from __mirror_state on leadership election. */
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {
        replayMirrorStateLog(new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, partitionIndex));
    }

    /** Clears cached metadata on leadership resignation. */
    public void onResignation(
            int partitionIndex,
            OptionalInt partitionLeaderEpoch
    ) {
        metadataManager.clearCache();
    }

    /**
     * Replays the {@code __mirror_state} log to rebuild in-memory partition states,
     * last mirror epochs, and retry metadata after a coordinator leadership election.
     */
    private void replayMirrorStateLog(TopicPartition topicPartition) {
        long logEndOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(() -> -1L);

        replicaManager.getLog(topicPartition).foreach(partitionLog -> {
            long startOffset = partitionLog.logStartOffset();
            long currOffset = startOffset;
            int maxLength = 5 * 1024 * 1024;
            int recordCount = 0;
            ByteBuffer buffer = ByteBuffer.allocate(0);

            log.info("Replaying mirror state log {} offsets [{}, {}).", topicPartition, startOffset, logEndOffset);

            try {
                while (currOffset < logEndOffset && isRunning.get()) {
                    FetchDataInfo fetchDataInfo = partitionLog.read(currOffset, maxLength, FetchIsolation.LOG_END, true);
                    if (fetchDataInfo.records.sizeInBytes() == 0) {
                        break;
                    }

                    MemoryRecords memRecords;
                    if (fetchDataInfo.records instanceof MemoryRecords mr) {
                        memRecords = mr;
                    } else if (fetchDataInfo.records instanceof FileRecords fileRecords) {
                        int bytesNeeded = Math.max(maxLength, fileRecords.sizeInBytes());
                        if (buffer.capacity() < bytesNeeded) {
                            buffer = ByteBuffer.allocate(bytesNeeded);
                        }
                        buffer.clear();
                        fileRecords.readInto(buffer, 0);
                        memRecords = MemoryRecords.readableRecords(buffer);
                    } else {
                        return null;
                    }

                    for (MutableRecordBatch batch : memRecords.batches()) {
                        for (var record : batch) {
                            require(record.hasKey(), "Mirror state log record key must not be null");
                            replayRecord(record);
                            recordCount++;
                        }
                        currOffset = batch.nextOffset();
                    }
                }
            } catch (Throwable t) {
                log.error("Failed to replay mirror state log {} at offset {}.", topicPartition, currOffset, t);
            }

            log.info("Replayed {} record(s) from mirror state log {}, now applying state transitions.",
                    recordCount, topicPartition);
            metadataManager.applyReplayedStates(this::handleStateTransition);

            return null;
        });
    }

    private void replayRecord(org.apache.kafka.common.record.Record record) {
        short version = record.key().getShort();
        String mirrorName = readMirrorNameFromKey(record.key());

        if (version == CoordinatorRecordType.LAST_MIRROR_EPOCHS.id()) {
            if (record.hasValue()) {
                metadataManager.updateLastMirrorEpochs(mirrorName, readLastMirrorEpochsValue(record.value()));
            } else {
                metadataManager.removeCachedMirror(mirrorName);
            }
        } else if (version == CoordinatorRecordType.MIRROR_PARTITION_STATE.id()) {
            if (record.hasValue()) {
                MirrorPartitionStateValue psv = readMirrorPartitionStateValue(record.value());
                metadataCache.getTopicName(psv.topicId()).ifPresent(topicName -> {
                    ClusterMirrorRecordKey pk = ClusterMirrorRecordKey.of(mirrorName, psv.topicId(), psv.partition());
                    MirrorPartitionState state = MirrorPartitionState.fromValue(psv.state());
                    MirrorPartitionState previousState = MirrorPartitionState.fromValue(psv.previousState());
                    metadataManager.updatePartitionState(pk, state);
                    restoreFailedState(mirrorName, pk, state, psv.retryAttempt(), psv.errorMessage(), previousState);
                });
            } else {
                metadataManager.removeCachedMirror(mirrorName);
            }
        } else {
            throw new IllegalArgumentException("Unknown mirror state log key version " + version);
        }
    }

    /** Executes the appropriate actions for a state transition. */
    private void handleStateTransition(String mirrorName, Set<TopicPartition> topicPartitions, MirrorPartitionState newState) {
        switch (newState) {
            case LOG_TRUNCATION:
                log.info("Mirror '{}' transitioning {} to LOG_TRUNCATION.", mirrorName, topicPartitions);
                scheduleTruncation(mirrorName, topicPartitions);
                break;
            case EPOCH_FENCING:
                log.info("Mirror '{}' transitioning {} to EPOCH_FENCING.", mirrorName, topicPartitions);
                metadataManager.scheduleBumpLeaderEpochs(mirrorName, topicPartitions)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            log.error("Mirror '{}' failed to bump leader epoch for {}.", mirrorName, topicPartitions, ex);
                            transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED, ex.getMessage());
                        } else {
                            transitionTo(mirrorName, topicPartitions, MirrorPartitionState.MIRRORING);
                        }
                    });
                break;
            case MIRRORING:
                log.info("Mirror '{}' transitioning {} to MIRRORING.", mirrorName, topicPartitions);
                replicaManager.maybeCreateMirrorFetchers(mirrorName, topicPartitions);
                break;
            case PAUSING:
                log.info("Mirror '{}' transitioning {} to PAUSING.", mirrorName, topicPartitions);
                replicaManager.mirrorFetcherManager().removeFetcherForPartitions(CollectionConverters.asScala(topicPartitions));
                topicPartitions.forEach(tp -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED));
                break;
            case PAUSED:
                log.info("Mirror '{}' transitioning {} to PAUSED.", mirrorName, topicPartitions);
                // topic is still read-only
                break;
            case STOPPING:
                log.info("Mirror '{}' transitioning {} to STOPPING.", mirrorName, topicPartitions);
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
                            metadataManager.getLatestLocalEpochs(replicaManager.logManager(), topicPartitions)))
                    .thenCompose(v -> abortOngoingTransactions(topicPartitions))
                    .thenAccept(v -> writeMirrorPidResetAndStop(mirrorName, topicPartitions))
                    .exceptionally(ex -> {
                        log.error("Mirror '{}' STOPPING transition failed for {}.", mirrorName, topicPartitions, ex);
                        return null;
                    });
                break;
            case STOPPED:
                log.info("Mirror '{}' transitioning {} to STOPPED.", mirrorName, topicPartitions);
                // Topic is writable.
                break;
            case FAILED:
                log.info("Mirror '{}' transitioning {} to FAILED.", mirrorName, topicPartitions);
                scheduleFailedRetries(mirrorName, topicPartitions);
                break;
            default:
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    private void restoreFailedState(String mirrorName, ClusterMirrorRecordKey pk, MirrorPartitionState state,
                                    int retryAttempt, String errorMessage, MirrorPartitionState previousState) {
        if (state == MirrorPartitionState.FAILED) {
            metadataManager.setFailedInfo(pk, new FailedPartitionInfo(retryAttempt, errorMessage, previousState));
        } else {
            metadataManager.clearFailedInfo(pk);
        }
    }

    private void updateFailedState(String mirrorName, TopicPartition tp, MirrorPartitionState currentState,
                                   MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
        ClusterMirrorRecordKey pk = ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
        if (newState == MirrorPartitionState.FAILED) {
            metadataManager.computeFailedInfo(pk, (key, existing) -> {
                int attempt;
                if (nonRetryable) {
                    attempt = NON_RETRYABLE_ATTEMPT;
                } else if (existing != null) {
                    attempt = existing.retryAttempt() + 1;
                } else {
                    attempt = 1;
                }
                // Use the actual current state unless this is a FAILED -> FAILED self-transition,
                // where we preserve the original pre-failure state for retry targeting
                MirrorPartitionState previousState;
                if (currentState == MirrorPartitionState.FAILED && existing != null) {
                    previousState = existing.previousState();
                } else {
                    previousState = currentState;
                }
                return new FailedPartitionInfo(attempt, errorMessage, previousState);
            });
        } else if (newState == MirrorPartitionState.LOG_TRUNCATION
                || newState == MirrorPartitionState.STOPPED
                || newState == MirrorPartitionState.PAUSED) {
            metadataManager.clearFailedInfo(pk);
        }
    }

    /** Schedules retries with exponential backoff for partitions in FAILED state, restoring their previous state. Skips non-retryable partitions. */
    private void scheduleFailedRetries(String mirrorName, Set<TopicPartition> topicPartitions) {
        int maxAttempts = brokerConfig.mirrorConfig().failedRetryMaxAttempts();
        topicPartitions.forEach(tp -> {
            FailedPartitionInfo fpi = metadataManager.getFailedInfo(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
            int attempt = fpi != null ? fpi.retryAttempt() : 1;
            if (attempt == NON_RETRYABLE_ATTEMPT) {
                log.debug("Skipping retry for partition {} because it is in non-retryable failed state, requires manual intervention.", tp);
                return;
            }
            if (attempt >= maxAttempts) {
                log.error("Partition {} exceeded max retry attempts ({}), requires manual intervention.", tp, maxAttempts);
                return;
            }

            // Exponential backoff for FAILED state retries with 20% jitter.
            // With defaults (initial=1s, max=300s): attempt 0 ~1s, attempt 1 ~2s, attempt 2 ~4s, ..., attempt 8+ ~300s.
            ExponentialBackoff failedRetryBackoff = new ExponentialBackoff(
                    brokerConfig.mirrorConfig().failedRetryInitialBackoffMs(),
                    CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                    brokerConfig.mirrorConfig().failedRetryMaxBackoffMs(),
                    CommonClientConfigs.RETRY_BACKOFF_JITTER);
            long delay = failedRetryBackoff.backoff(attempt);
            MirrorPartitionState targetState;
            if (fpi == null) {
                targetState = MirrorPartitionState.MIRRORING;
            } else if (fpi.previousState() == MirrorPartitionState.UNKNOWN) {
                // FAILED -> UNKNOWN is not a valid transition, so re-bootstrap via LOG_TRUNCATION instead
                targetState = MirrorPartitionState.LOG_TRUNCATION;
            } else {
                targetState = fpi.previousState();
            }
            log.info("Scheduling retry attempt #{} for partition {} in {} ms with target state {}.", attempt, tp, delay, targetState);
            scheduler.scheduleOnce("MirrorFailedRetry-" + tp, () -> transitionTo(mirrorName, Set.of(tp), targetState), delay);
        });
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                             MirrorPartitionState newState) {
        transitionTo(mirrorName, topicPartitions, newState, null, false);
    }

    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                             MirrorPartitionState newState, String errorMessage) {
        transitionTo(mirrorName, topicPartitions, newState, errorMessage, false);
    }

    /** Validates and persists a partition state transition, triggering follow-up actions on success. */
    public void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                             MirrorPartitionState newState, String errorMessage, boolean nonRetryable) {
        topicPartitions.forEach(tp -> {
            MirrorPartitionState currentState = metadataManager.getPartitionState(mirrorName, tp);
            if (MirrorPartitionState.isValidTransition(currentState, newState)) {
                log.debug("Transitioning partition {} from {} to {}.", tp, currentState, newState);
                updateFailedState(mirrorName, tp, currentState, newState, errorMessage, nonRetryable);
                updateMirrorPartitionState(mirrorName, tp, newState)
                        .whenComplete((optTp, ex) -> {
                            if (ex != null) {
                                // TODO: handle failure, we can't retry forever
                                log.error("Failed to update partition state for {}: {}, retrying", tp, ex.getMessage());
                                scheduler.scheduleOnce("MirrorStateUpdateRetry-" + tp,
                                        () -> transitionTo(mirrorName, Set.of(tp), newState, errorMessage, nonRetryable), 100);
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

    /** Appends abort markers for all ongoing transactions on the given partitions, one batch per appendRecords call. */
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
            Map<TopicIdPartition, MemoryRecords> batch = new HashMap<>();
            recordsByPartition.entrySet().removeIf(entry -> {
                TopicPartition tp = entry.getKey();
                List<MemoryRecords> recordsList = entry.getValue();
                Iterator<MemoryRecords> it = recordsList.iterator();
                if (it.hasNext()) {
                    MemoryRecords record = it.next();
                    batch.put(replicaManager.topicIdPartition(tp), record);
                    it.remove();
                }
                return recordsList.isEmpty();
            });
            CompletableFuture<Void> future = new CompletableFuture<>();
            replicaManager.appendRecords(
                // TODO: replace this with Cluster Mirror specific timeout
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(batch),
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
            futures.add(future);
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /** Write partition state and offset updates to the internal topic. */
    public void updateTopicMetadata(String mirrorName,
                                    Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> topicMetadata,
                                    Consumer<WriteMirrorStatesResponse> callback) {
        List<CompletableFuture<Optional<TopicPartition>>> updateMirrorPartitionStateFutures = new ArrayList<>();

        Map<String, Map<Integer, Integer>> offsets = new HashMap<>();
        Map<String, Set<Integer>> tps = new HashMap<>();
        topicMetadata.forEach((topic, partitions) -> {
            Set<Integer> partitionIndices = new HashSet<>();
            partitions.forEach(partition -> {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                partitionIndices.add(tp.partition());
                if (partition.state() != null && partition.state() != MirrorPartitionState.UNKNOWN) {
                    updateMirrorPartitionStateFutures.add(updateMirrorPartitionState(mirrorName, tp, partition.state()));
                }
                if (partition.leaderEpoch() != -1) {
                    offsets.putIfAbsent(topic, new HashMap<>());
                    offsets.get(topic).put(partition.partition(), partition.leaderEpoch());
                }
            });
            tps.put(topic, partitionIndices);
        });

        // Execute in parallel for efficiency, but wait for all partition state updates first, THEN wait for LME update
        CompletableFuture<Void> updateLastMirrorEpochsFuture = updateLastMirrorEpochs(mirrorName, offsets);
        CompletableFuture.allOf(updateMirrorPartitionStateFutures.toArray(CompletableFuture[]::new))
            .thenCompose(v -> updateLastMirrorEpochsFuture)
            .whenComplete((v, e) -> {
                WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to update last mirror partition state and LME for {}: {}", mirrorName, e);
                    data.setErrorCode(Errors.forException(e).code());
                    data.setErrorMessage(e.getMessage());
                } else {
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
                }
                callback.accept(new WriteMirrorStatesResponse(data));
            });
    }

    public void getTopicMetadata(String mirrorName,
                                 Map<String, Set<Integer>> partitions,
                                 Consumer<ReadMirrorStatesResponse> callback) {
        metadataManager.getTopicMetadata(mirrorName, partitions, callback);
    }

    private CompletableFuture<Void> collectAndUpdateLastMirrorEpochs(String mirrorName, Set<TopicPartition> topicPartitions) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Map<String, Map<Integer, Integer>> partitionOffsets = new HashMap<>();
        groupPartitionsByTopic(topicPartitions).forEach((topic, parts) -> {
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

    private static Map<String, Set<Integer>> groupPartitionsByTopic(Set<TopicPartition> topicPartitions) {
        Map<String, Set<Integer>> topicToPartitions = new HashMap<>();
        topicPartitions.forEach(tp -> {
            topicToPartitions.compute(tp.topic(), (k, preV) -> {
                if (preV == null) {
                    return Set.of(tp.partition());
                }
                Set<Integer> results = new HashSet<>(preV);
                results.add(tp.partition());
                return results;
            });
        });
        return topicToPartitions;
    }

    /** Persists last mirror epochs to local or remote coordinators. */
    public CompletableFuture<Void> updateLastMirrorEpochs(String mirrorName, Map<String, Map<Integer, Integer>> partitionOffsets) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Separate partitions into local and remote coordinator groups
        Map<Integer, Set<TopicPartition>> localByCoordPartition = new HashMap<>();
        Map<String, Set<ClusterMirrorUtils.PartitionStateInfo>> remoteTopicMetadata = new HashMap<>();

        partitionOffsets.forEach((topic, offsetMap) -> {
            offsetMap.forEach((par, off) -> {
                if (isLocalCoordinator(mirrorName, topic, par)) {
                    int coordPartition = getCoordinatorPartitionByKey(
                            ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topic), par));
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
        var updatedOffsets = metadataManager.updateLastMirrorEpochs(mirrorName, partitionOffsets);

        // Write to local coordinator partitions (one append per coordinator partition)
        localByCoordPartition.forEach((coordPartition, tps) -> {
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, coordPartition);
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
            var record = buildLastMirrorEpochsRecord(mirrorName, updatedOffsets);
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
                                // TODO: handle failure, we can't retry forever
                                log.error("Failed to write LMEs to coordinator: {}, retrying", partitionRes.error.message());
                                scheduler.scheduleOnce("LmeWriteRetry-" + mirrorName,
                                        () -> updateLastMirrorEpochs(mirrorName, partitionOffsets), 100);
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
                            // TODO: handle failure, we can't retry forever
                            log.error("Failed to write LMEs to remote coordinator: {}, retrying", partitionResult.errorCode());
                            scheduler.scheduleOnce("LmeWriteRetry-" + mirrorName,
                                    () -> updateLastMirrorEpochs(mirrorName, partitionOffsets), 100);
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

    private CompletableFuture<Optional<TopicPartition>> updateMirrorPartitionState(
            String mirrorName, TopicPartition topicPartition, MirrorPartitionState newState) {
        CompletableFuture<Optional<TopicPartition>> future = new CompletableFuture<>();
        // no need to write a record for the same state again
        if (metadataManager.getPartitionState(mirrorName, topicPartition) == newState) {
            future.complete(Optional.of(topicPartition));
            return future;
        }
        if (isLocalCoordinator(mirrorName, topicPartition.topic(), topicPartition.partition())) {
            // this is the leader of the coordinator (async disk I/O operation)
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME,
                    getCoordinatorPartitionByKey(ClusterMirrorRecordKey.of(
                            mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition())));
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
            var record = buildPartitionStateRecord(mirrorName, topicPartition, newState);
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
                                metadataManager.updatePartitionState(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition()), newState);
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
                            metadataManager.updatePartitionState(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition()), newState);
                            future.complete(Optional.of(topicPartition));
                        } else {
                            log.error("Failed to write partition state to remote coordinator: {}", par.errorCode());
                            future.completeExceptionally(Errors.forCode(par.errorCode()).exception());
                        }
                    })));

        }
        return future;
    }

    /** Schedules log truncation to align replicas with their last mirror epoch. */
    private void scheduleTruncation(String mirrorName, Set<TopicPartition> topicPartitions) {
        final Consumer<TopicPartition> truncateCallback =
            partition -> transitionTo(mirrorName, Set.of(partition), MirrorPartitionState.MIRRORING);
        scheduler.scheduleOnce("LastMirrorEpochsTruncation",
            () -> {
                Collection<ClusterMirrorListing> sourceMirrors;
                try {
                    sourceMirrors = metadataManager.listSourceClusterMirrors(mirrorName);
                    if (metadataManager.hasMirrorLoop(mirrorName, topicPartitions, sourceMirrors)) {
                        // should make it as a terminal error
                        transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED, "Detected mirror loop for mirror:" + mirrorName);
                        return;
                    }

                    metadataManager.sendLastMirrorEpochLookup(mirrorName, topicPartitions, sourceMirrors)
                        .whenComplete((epochs, rawError) -> {
                            if (rawError != null) {
                                Throwable error = rawError instanceof CompletionException && rawError.getCause() != null
                                        ? rawError.getCause() : rawError;
                                if (error instanceof UnsupportedVersionException) {
                                    log.warn("Source cluster doesn't support DescribeClusterMirror API. " +
                                        "Replication will be one-way without failback");
                                    Map<TopicPartition, Integer> fallback = topicPartitions.stream()
                                        .collect(Collectors.toMap(tp -> tp, tp -> -1));
                                    replicaManager.maybeTruncateForLeaderEpoch(fallback, truncateCallback);
                                } else {
                                    log.warn("Failed to truncate to last mirrored epoch for mirror {}", mirrorName, error);
                                    transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED, error.getMessage());
                                }
                                return;
                            }

                            if (epochs.size() != topicPartitions.size()) {
                                log.warn("The returned epoch size is not equal to the requested epoch size for mirror. " +
                                        "Requested topic partitions: {}, returned epochs: {}", topicPartitions, epochs);
                                topicPartitions.forEach(partition -> {
                                    if (!epochs.containsKey(partition)) {
                                        epochs.put(partition, -1);
                                    }
                                });
                            }

                            replicaManager.maybeTruncateForLeaderEpoch(epochs, truncateCallback);
                        });
                } catch (Exception e) {
                    log.warn("Failed to truncate to last mirror epochs for mirror {}", mirrorName, e);
                    transitionTo(mirrorName, topicPartitions, MirrorPartitionState.FAILED, e.getMessage());
                }
            }, 0);
    }

    /** Writes a PID reset control record per partition and transitions to STOPPED, retrying failed partitions. */
    private void writeMirrorPidResetAndStop(String mirrorName, Set<TopicPartition> topicPartitions) {
        writeMirrorPidReset(mirrorName, topicPartitions).whenComplete((responses, ex) -> {
            if (ex != null) {
                log.error("Failed to write PID reset record for partitions {} in mirror {}", topicPartitions, mirrorName, ex);
                // TODO: handle failure, we can't retry forever
                scheduler.scheduleOnce("MirrorPidResetRetry", () -> writeMirrorPidResetAndStop(mirrorName, topicPartitions), 5000);
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
                scheduler.scheduleOnce("MirrorPidResetRetry", () -> writeMirrorPidResetAndStop(mirrorName, failed), 5000);
            }
        });
    }

    private CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> writeMirrorPidReset(
            String mirrorName, Set<TopicPartition> topicPartitions) {
        CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> writePidResetFuture = new CompletableFuture<>();
        String sourceClusterId = metadataManager.getSourceClusterId(mirrorName);
        if (sourceClusterId == null) {
            log.warn("Source cluster ID not available for mirror {}. Skipping PID reset barrier.", mirrorName);
            writePidResetFuture.complete(Map.of());
            return writePidResetFuture;
        }
        MirrorPidResetRecord record = new MirrorPidResetRecord()
            .setVersion(ControlRecordUtils.MIRROR_PID_RESET_CURRENT_VERSION)
            .setSourceClusterId(sourceClusterId);
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
                        writePidResetFuture.complete(result);
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of())
                );
            } catch (Exception e) {
                log.error("Failed to write PID reset barrier for partition {} in mirror {}", tp, mirrorName, e);
                writePidResetFuture.completeExceptionally(e);
            }
        }
        return writePidResetFuture;
    }

    public Map<String, Map<TopicPartition, Integer>> processLastMirrorEpochLookup(
            Map<String, Map<String, Set<Integer>>> mirrorPartitions) {
        return metadataManager.processLastMirrorEpochLookup(mirrorPartitions);
    }

    private CoordinatorRecord buildPartitionStateRecord(String mirrorName, TopicPartition topicPartition, MirrorPartitionState state) {
        FailedPartitionInfo fpi = metadataManager.getFailedInfo(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(topicPartition.topic()), topicPartition.partition()));
        var key = new MirrorPartitionStateKey().setMirrorName(mirrorName);
        var val = new MirrorPartitionStateValue()
                .setTopicId(metadataCache.getTopicId(topicPartition.topic()))
                .setPartition(topicPartition.partition())
                .setState(state.value())
                .setPreviousState(fpi != null ? fpi.previousState().value() : MirrorPartitionState.UNKNOWN.value())
                .setRetryAttempt(fpi != null ? (short) fpi.retryAttempt() : (short) 0)
                .setErrorMessage(fpi != null ? fpi.errorMessage() : null);
        var apiVersion = new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION);
        return CoordinatorRecord.record(key, apiVersion);
    }

    private CoordinatorRecord buildLastMirrorEpochsRecord(String mirrorName, Map<ClusterMirrorRecordKey, Integer> offsets) {
        Map<Uuid, Map<Integer, Integer>> grouped = new HashMap<>();
        offsets.forEach((pk, value) -> grouped.computeIfAbsent(pk.topicId(), k -> new HashMap<>()).put(pk.partition(), value));

        var key = new LastMirrorEpochsKey().setMirrorName(mirrorName);
        var val = new LastMirrorEpochsValue();
        var topics = new ArrayList<LastMirrorEpochsValue.Topic>();
        grouped.forEach((topicId, partitionOffsets) -> {
            var top = new LastMirrorEpochsValue.Topic().setTopicId(topicId);
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
        Map<Integer, Set<TopicPartition>> coordPartitionToMirrorPartitions = new HashMap<>();
        states.forEach((tp, state) -> {
            if (isLocalCoordinator(mirrorName, tp.topic(), tp.partition())) {
                coordPartitionToMirrorPartitions.computeIfAbsent(getCoordinatorPartitionByKey(
                    ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())),
                        v -> new HashSet<>()).add(tp);
            }
        });

        // We can safely remove these caches here and only keep partitionStates for failed write below
        // so cleanupDeletedMirrorStates can pick up and retry.
        metadataManager.removeCachedMirror(mirrorName);
        metadataManager.removeStateForPartitions(states.keySet());

        if (coordPartitionToMirrorPartitions.isEmpty()) {
            // No local coordinator partitions — clear cached entries
            states.keySet().forEach(tp ->
                metadataManager.removePartitionState(
                    ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
            return;
        }

        var timestamp = time.milliseconds();
        var stateTombstone = CoordinatorRecord.tombstone(new MirrorPartitionStateKey().setMirrorName(mirrorName));
        var lmeTombstone = CoordinatorRecord.tombstone(new LastMirrorEpochsKey().setMirrorName(mirrorName));

        // Write one partition state tombstone and one offset tombstone per coordinator partition
        for (Map.Entry<Integer, Set<TopicPartition>> entry : coordPartitionToMirrorPartitions.entrySet()) {
            int coordPartition = entry.getKey();
            Set<TopicPartition> tps = entry.getValue();
            var mirrorTopicPartition = new TopicPartition(Topic.MIRROR_STATE_TOPIC_NAME, coordPartition);
            var mirrorTopicIdPartition = replicaManager.topicIdPartition(mirrorTopicPartition);
            var memRecord = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(timestamp, serde.serializeKey(stateTombstone), serde.serializeValue(stateTombstone)),
                new SimpleRecord(timestamp, serde.serializeKey(lmeTombstone), serde.serializeValue(lmeTombstone))
            );
            replicaManager.appendRecords(
                Duration.ofSeconds(5).toMillis(),
                (short) -1,
                true,
                AppendOrigin.COORDINATOR,
                CollectionConverters.asScala(Map.of(mirrorTopicIdPartition, memRecord)),
                response -> {
                    response.foreach(res -> {
                        ProduceResponse.PartitionResponse partitionRes = res._2;
                        if (partitionRes.error.code() != Errors.NONE.code()) {
                            log.warn("Failed to write tombstone to {}: {}. Will retry later.",
                                mirrorTopicPartition, partitionRes.error.message());
                        } else {
                            tps.forEach(tp -> metadataManager.removePartitionState(
                                ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition())));
                        }
                        return null;
                    });
                    return null;
                },
                ignored -> null,
                RequestLocal.noCaching(),
                CollectionConverters.asScala(Map.of())
            );
        }
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
                Optional<String> topicName = metadataCache.getTopicName(t.topicId());
                if (topicName.isEmpty()) return;
                Map<Integer, Integer> partitions = new HashMap<>();
                t.partitions().forEach(p -> partitions.put(p.partitionIndex(), p.lastMirrorEpoch()));
                offsets.put(topicName.get(), partitions);
            });
        } else {
            throw new IllegalStateException("Unsupported last mirrored epochs value version: " + version);
        }
        return offsets;
    }

    private MirrorPartitionStateValue readMirrorPartitionStateValue(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version <= MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION && version >= MirrorPartitionStateValue.LOWEST_SUPPORTED_VERSION) {
            return new MirrorPartitionStateValue(new ByteBufferAccessor(buffer), version);
        } else {
            throw new IllegalStateException("Unsupported partition state value version: " + version);
        }
    }

    public void scheduleMetadataRefresh(long intervalMs) {
        metadataManager.scheduleMetadataRefresh(intervalMs);
    }

    public String getSourceClusterId(String mirrorName) {
        return metadataManager.getSourceClusterId(mirrorName);
    }

    public String getSourceBootstrap(String mirrorName) {
        return metadataManager.getSourceBootstrap(mirrorName);
    }

    public Set<String> getConfiguredMirrors() {
        return metadataManager.getConfiguredMirrors();
    }

    public Map<TopicPartition, MirrorPartitionState> getMirrorStates(String mirrorName) {
        return metadataManager.getMirrorStates(mirrorName);
    }

    public void validateStartMirrorStates(StartMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateStartMirrorStates(data, callback);
    }

    public void validateStopMirrorStates(StopMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateStopMirrorStates(data, callback);
    }

    public void validatePauseMirrorStates(PauseMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validatePauseMirrorStates(data, callback);
    }

    public void validateResumeMirrorStates(ResumeMirrorTopicsRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateResumeMirrorStates(data, callback);
    }

    public Set<String> getConfiguredTopics(String mirrorName, boolean includePaused, boolean includeStopped) {
        return metadataManager.getConfiguredTopics(mirrorName, includePaused, includeStopped);
    }

    public int getActiveTopicCount(String mirrorName) {
        return metadataManager.getActiveTopicCount(mirrorName);
    }

    public FailedPartitionInfo getFailedInfo(String mirrorName, TopicPartition tp) {
        return metadataManager.getFailedInfo(ClusterMirrorRecordKey.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
    }

    public void validateDeleteMirrorStates(DeleteClusterMirrorRequestData data, Consumer<Optional<Errors>> callback) {
        metadataManager.validateDeleteMirrorStates(data, callback);
    }

    /**
     * Maps a mirror record key to a __mirror_state partition index.
     * Used by MirrorMetadataManager to route WriteMirrorStates and ReadMirrorStates RPCs to the correct coordinator broker.
     */
    public int getCoordinatorPartitionByKey(ClusterMirrorRecordKey key) {
        if (!isRunning.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
        return Utils.abs(key.asCoordinatorKey().hashCode()) % brokerConfig.mirrorConfig().stateTopicNumPartitions();
    }

    /**
     * Maps a mirror name to a __mirror_state partition index.
     * Used by MirrorMetadataManager to check whether the local broker is the coordinator for a given mirror
     * and needs to periodicaly refresh its metadata from the source cluster.
     */
    private int getCoordinatorPartitionByName(String mirrorName) {
        if (!isRunning.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
        return Utils.abs(mirrorName.hashCode()) % brokerConfig.mirrorConfig().stateTopicNumPartitions();
    }
}
