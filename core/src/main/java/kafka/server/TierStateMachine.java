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

package kafka.server;

import kafka.cluster.Partition;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.LeaderEndPoint;
import org.apache.kafka.server.PartitionFetchState;
import org.apache.kafka.server.ReplicaState;
import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageNotReadyException;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason.LeaderOffsetIncremented;

/**
 *  This class defines the APIs and implementation needed to handle any state transitions related to tiering
 *
 *  When started, the tier state machine will fetch the local log start offset of the
 *  leader and then build the follower's remote log aux state until the leader's
 *  local log start offset.
 */
public class TierStateMachine {
    private static final Logger log = LoggerFactory.getLogger(TierStateMachine.class);

    private final LeaderEndPoint leader;
    private final ReplicaManager replicaMgr;
    private final boolean useFutureLog;
    public TierStateMachine(LeaderEndPoint leader,
                            ReplicaManager replicaMgr,
                            boolean useFutureLog) {
        this.leader = leader;
        this.replicaMgr = replicaMgr;
        this.useFutureLog = useFutureLog;
    }

    /**
     * Start the tier state machine for the provided topic partition.
     *
     * @param topicPartition            the topic partition for which the tier state machine is to be started
     * @param topicId                   the optional unique identifier of the topic
     * @param currentLeaderEpoch        the current leader epoch of the partition
     * @param fetchStartOffsetAndEpoch  the offset on the leader's local log from which to start replicating logs
     * @param leaderLogStartOffset      the starting offset in the leader's log
     * @return the new PartitionFetchState after the successful start of the tier state machine
     * @throws Exception if an error occurs during the process, such as issues with remote storage
     */
    PartitionFetchState start(TopicPartition topicPartition,
                              Optional<Uuid> topicId,
                              int currentLeaderEpoch,
                              OffsetAndEpoch fetchStartOffsetAndEpoch,
                              long leaderLogStartOffset) throws Exception {
        long offsetToFetch;
        replicaMgr.brokerTopicStats().topicStats(topicPartition.topic()).buildRemoteLogAuxStateRequestRate().mark();
        replicaMgr.brokerTopicStats().allTopicsStats().buildRemoteLogAuxStateRequestRate().mark();

        UnifiedLog unifiedLog;
        if (useFutureLog) {
            unifiedLog = replicaMgr.futureLogOrException(topicPartition);
        } else {
            unifiedLog = replicaMgr.localLogOrException(topicPartition);
        }

        try {
            offsetToFetch = buildRemoteLogAuxState(topicPartition, currentLeaderEpoch, fetchStartOffsetAndEpoch.offset(), fetchStartOffsetAndEpoch.epoch(), leaderLogStartOffset, unifiedLog);
        } catch (RemoteStorageException e) {
            replicaMgr.brokerTopicStats().topicStats(topicPartition.topic()).failedBuildRemoteLogAuxStateRate().mark();
            replicaMgr.brokerTopicStats().allTopicsStats().failedBuildRemoteLogAuxStateRate().mark();
            throw e;
        }

        OffsetAndEpoch fetchLatestOffsetResult = leader.fetchLatestOffset(topicPartition, currentLeaderEpoch);
        long leaderEndOffset = fetchLatestOffsetResult.offset();

        long initialLag = leaderEndOffset - offsetToFetch;

        return new PartitionFetchState(topicId, offsetToFetch, Optional.of(initialLag), currentLeaderEpoch,
                ReplicaState.FETCHING, unifiedLog.latestEpoch());

    }

    private OffsetForLeaderEpochResponseData.EpochEndOffset fetchEarlierEpochEndOffset(Integer epoch,
                                                                                       TopicPartition partition,
                                                                                       Integer currentLeaderEpoch) {
        int previousEpoch = epoch - 1;

        // Find the end-offset for the epoch earlier to the given epoch from the leader
        Map<TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition> partitionsWithEpochs = new HashMap<>();
        partitionsWithEpochs.put(partition, new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(partition.partition()).setCurrentLeaderEpoch(currentLeaderEpoch).setLeaderEpoch(previousEpoch));
        var epochEndOffset = leader.fetchEpochEndOffsets(partitionsWithEpochs).get(partition);

        if (epochEndOffset == null) {
            throw new KafkaException("No response received for partition: " + partition);
        }

        if (epochEndOffset.errorCode() != Errors.NONE.code()) {
            throw Errors.forCode(epochEndOffset.errorCode()).exception();
        }

        return epochEndOffset;
    }

    private List<EpochEntry> readLeaderEpochCheckpoint(RemoteLogManager rlm,
                                                       RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws IOException, RemoteStorageException {
        try (InputStream inputStream = rlm.storageManager().fetchIndex(remoteLogSegmentMetadata, RemoteStorageManager.IndexType.LEADER_EPOCH);
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            CheckpointFile.CheckpointReadBuffer<EpochEntry> readBuffer = new CheckpointFile.CheckpointReadBuffer<>("", bufferedReader, 0, LeaderEpochCheckpointFile.FORMATTER);
            return readBuffer.read();
        }
    }

    private void buildProducerSnapshotFile(UnifiedLog unifiedLog,
                                           long nextOffset,
                                           RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                           RemoteLogManager rlm) throws IOException, RemoteStorageException {
        // Restore producer snapshot
        File snapshotFile = LogFileUtils.producerSnapshotFile(unifiedLog.dir(), nextOffset);
        Path tmpSnapshotFile = Paths.get(snapshotFile.getAbsolutePath() + ".tmp");
        // Copy it to snapshot file in atomic manner.
        try (InputStream inputStream = rlm.storageManager().fetchIndex(remoteLogSegmentMetadata, RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT)) {
            Files.copy(inputStream, tmpSnapshotFile, StandardCopyOption.REPLACE_EXISTING);
        }
        Utils.atomicMoveWithFallback(tmpSnapshotFile, snapshotFile.toPath(), false);

        // Reload producer snapshots.
        unifiedLog.producerStateManager().truncateFullyAndReloadSnapshots();
        unifiedLog.loadProducerState(nextOffset);
    }

    /**
     * It tries to build the required state for this partition from leader and remote storage so that it can start
     * fetching records from the leader. The return value is the next offset to fetch from the leader, which is the
     * next offset following the end offset of the remote log portion.
     */
    private Long buildRemoteLogAuxState(TopicPartition topicPartition,
                                        Integer currentLeaderEpoch,
                                        Long leaderLocalLogStartOffset,
                                        Integer epochForLeaderLocalLogStartOffset,
                                        Long leaderLogStartOffset,
                                        UnifiedLog unifiedLog) throws IOException, RemoteStorageException {

        if (!unifiedLog.remoteLogEnabled()) {
            // If the tiered storage is not enabled throw an exception back so that it will retry until the tiered storage
            // is set as expected.
            throw new RemoteStorageException("Couldn't build the state from remote store for partition " + topicPartition + ", as remote log storage is not yet enabled");
        }

        if (replicaMgr.remoteLogManager().isEmpty())
            throw new IllegalStateException("RemoteLogManager is not yet instantiated");

        RemoteLogManager rlm = replicaMgr.remoteLogManager().get();

        long seal = replicaMgr.classicToDisklessStartOffset(topicPartition);
        if (seal > 0 && leaderLocalLogStartOffset <= seal) {
            int lastClassicLeaderEpoch = replicaMgr.lastClassicLeaderEpoch(topicPartition);
            if (lastClassicLeaderEpoch != PartitionRegistration.NO_LAST_CLASSIC_LEADER_EPOCH) {
                return buildClassicPrefixAuxState(topicPartition, currentLeaderEpoch, leaderLocalLogStartOffset,
                        leaderLogStartOffset, seal, lastClassicLeaderEpoch, rlm, unifiedLog);
            }
        }

        // Find the respective leader epoch for (leaderLocalLogStartOffset - 1). We need to build the leader epoch cache
        // until that offset
        long previousOffsetToLeaderLocalLogStartOffset = leaderLocalLogStartOffset - 1;
        int targetEpoch;
        // If the existing epoch is 0, no need to fetch from earlier epoch as the desired offset(leaderLogStartOffset - 1)
        // will have the same epoch.
        if (epochForLeaderLocalLogStartOffset == 0) {
            targetEpoch = epochForLeaderLocalLogStartOffset;
        } else {
            // Fetch the earlier epoch/end-offset(exclusive) from the leader.
            OffsetForLeaderEpochResponseData.EpochEndOffset earlierEpochEndOffset = fetchEarlierEpochEndOffset(epochForLeaderLocalLogStartOffset, topicPartition, currentLeaderEpoch);
            // Check if the target offset lies within the range of earlier epoch. Here, epoch's end-offset is exclusive.
            if (earlierEpochEndOffset.endOffset() > previousOffsetToLeaderLocalLogStartOffset) {
                // Always use the leader epoch from returned earlierEpochEndOffset.
                // This gives the respective leader epoch, that will handle any gaps in epochs.
                // For ex, leader epoch cache contains:
                // leader-epoch   start-offset
                //  0               20
                //  1               85
                //  <2> - gap no messages were appended in this leader epoch.
                //  3               90
                //  4               98
                // There is a gap in leader epoch. For leaderLocalLogStartOffset as 90, leader-epoch is 3.
                // fetchEarlierEpochEndOffset(2) will return leader-epoch as 1, end-offset as 90.
                // So, for offset 89, we should return leader epoch as 1 like below.
                targetEpoch = earlierEpochEndOffset.leaderEpoch();
            } else {
                targetEpoch = epochForLeaderLocalLogStartOffset;
            }
        }

        if (!rlm.isPartitionReady(topicPartition)) {
            throw new RemoteStorageNotReadyException("RemoteLogManager is not ready for partition: " + topicPartition);
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = rlm.fetchRemoteLogSegmentMetadata(topicPartition, targetEpoch, previousOffsetToLeaderLocalLogStartOffset)
                .orElseThrow(() -> buildRemoteStorageException(topicPartition, targetEpoch, currentLeaderEpoch,
                        leaderLocalLogStartOffset, leaderLogStartOffset));


        // Build leader epoch cache, producer snapshots until remoteLogSegmentMetadata.endOffset() and start
        // segments from (remoteLogSegmentMetadata.endOffset() + 1)
        // Assign nextOffset with the offset from which next fetch should happen.
        long nextOffset = remoteLogSegmentMetadata.endOffset() + 1;

        // Truncate the existing local log before restoring the leader epoch cache and producer snapshots.
        Partition partition = replicaMgr.getPartitionOrException(topicPartition);
        partition.truncateFullyAndStartAt(nextOffset, useFutureLog, Optional.of(leaderLogStartOffset));
        // Increment start offsets
        unifiedLog.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented);
        unifiedLog.maybeIncrementLocalLogStartOffset(nextOffset, LeaderOffsetIncremented);

        // Build leader epoch cache.
        List<EpochEntry> epochs = readLeaderEpochCheckpoint(rlm, remoteLogSegmentMetadata);
        unifiedLog.leaderEpochCache().assign(epochs);

        log.info("Updated the epoch cache from remote tier till offset: {} with size: {} for {}", leaderLocalLogStartOffset, epochs.size(), partition);

        buildProducerSnapshotFile(unifiedLog, nextOffset, remoteLogSegmentMetadata, rlm);

        log.debug("Built the leader epoch cache and producer snapshots from remote tier for {}, " +
                        "with active producers size: {}, leaderLogStartOffset: {}, and logEndOffset: {}",
                partition, unifiedLog.producerStateManager().activeProducers().size(), leaderLogStartOffset, nextOffset);

        return nextOffset;
    }

    /**
     * Inkless: builds the state below the classic-to-diskless seal in phase 2 of the switch and returns the seal as the
     * next offset to fetch.
     * <p>
     * Remote storage can hold a segment from a leader epoch that an unclean election discarded. Such a segment can
     * hold the record before the local log start of the leader in the same epoch as the committed history and diverge
     * after it. Each leader epoch has one leader, so a segment that holds {@code seal - 1} in the last classic leader
     * epoch holds the committed history below the seal, even if it also holds discarded records past the seal. The
     * local log starts at the seal because the replica doesn't keep local classic records in phase 2.
     */
    private long buildClassicPrefixAuxState(TopicPartition topicPartition,
                                            int currentLeaderEpoch,
                                            long leaderLocalLogStartOffset,
                                            long leaderLogStartOffset,
                                            long seal,
                                            int lastClassicLeaderEpoch,
                                            RemoteLogManager rlm,
                                            UnifiedLog unifiedLog) throws IOException, RemoteStorageException {
        if (!rlm.isPartitionReady(topicPartition)) {
            throw new RemoteStorageNotReadyException("RemoteLogManager is not ready for partition: " + topicPartition);
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = rlm.fetchRemoteLogSegmentMetadata(topicPartition, lastClassicLeaderEpoch, seal - 1)
                .orElseThrow(() -> buildRemoteStorageException(topicPartition, lastClassicLeaderEpoch, currentLeaderEpoch,
                        leaderLocalLogStartOffset, leaderLogStartOffset));

        Partition partition = replicaMgr.getPartitionOrException(topicPartition);
        partition.truncateFullyAndStartAt(seal, useFutureLog, Optional.of(leaderLogStartOffset));
        unifiedLog.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented);
        unifiedLog.maybeIncrementLocalLogStartOffset(seal, LeaderOffsetIncremented);

        List<EpochEntry> epochs = readLeaderEpochCheckpoint(rlm, remoteLogSegmentMetadata).stream()
                .filter(entry -> entry.startOffset() < seal)
                .toList();
        unifiedLog.leaderEpochCache().assign(epochs);

        // The producer snapshot of a segment is at its end. Sealing aborts undecided transactions, so the empty
        // producer state that the truncation leaves is correct at the seal.
        if (remoteLogSegmentMetadata.endOffset() + 1 == seal) {
            buildProducerSnapshotFile(unifiedLog, seal, remoteLogSegmentMetadata, rlm);
            // An operator who lowers the seal can cut a transaction off from its marker. Diskless storage can't
            // complete the transaction, which would hold the last stable offset below the seal.
            OptionalLong firstUndecidedOffset = unifiedLog.producerStateManager().firstUndecidedOffset();
            if (firstUndecidedOffset.isPresent()) {
                log.warn("Remote segment {} for {} ends in a transaction from offset {} without a marker below the " +
                        "classic-to-diskless seal {}; starting the producer state empty at the seal",
                        remoteLogSegmentMetadata.remoteLogSegmentId(), topicPartition, firstUndecidedOffset.getAsLong(), seal);
                unifiedLog.producerStateManager().truncateFullyAndStartAt(seal);
                unifiedLog.loadProducerState(seal);
            }
        } else {
            log.warn("Remote segment {} for {} ends at {}, past the classic-to-diskless seal {}; rebuilding up to the seal",
                    remoteLogSegmentMetadata.remoteLogSegmentId(), topicPartition, remoteLogSegmentMetadata.endOffset(), seal);
        }

        log.info("Rebuilt the state of {} below the classic-to-diskless seal {} from remote segment {} with {} leader epochs",
                topicPartition, seal, remoteLogSegmentMetadata.remoteLogSegmentId(), epochs.size());
        return seal;
    }

    private RemoteStorageException buildRemoteStorageException(TopicPartition topicPartition,
                                                               int targetEpoch,
                                                               int currentLeaderEpoch,
                                                               long leaderLocalLogStartOffset,
                                                               long leaderLogStartOffset) {
        String message = String.format(
                "Couldn't build the state from remote store for partition: %s, currentLeaderEpoch: %d, " +
                        "leaderLocalLogStartOffset: %d, leaderLogStartOffset: %d, epoch: %d as the previous remote log segment metadata was not found",
                topicPartition, currentLeaderEpoch, leaderLocalLogStartOffset, leaderLogStartOffset, targetEpoch
        );
        return new RemoteStorageException(message);
    }
}
