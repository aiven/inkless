/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.storage.internals.log.{LogAppendInfo, LogStartOffsetIncrementReason}
import org.apache.kafka.server.LeaderEndPoint

import java.util.Optional
import scala.collection.mutable

class ReplicaFetcherThread(name: String,
                           leader: LeaderEndPoint,
                           brokerConfig: KafkaConfig,
                           failedPartitions: FailedPartitions,
                           replicaMgr: ReplicaManager,
                           quota: ReplicaQuota,
                           logPrefix: String)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false),
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  this.logIdent = logPrefix

  // Visible for testing
  private[server] val partitionsWithNewHighWatermark = mutable.Buffer[TopicPartition]()

  // Partitions that have caught up to a fully-switched diskless topic's classicToDisklessStartOffset
  // and should be evicted from this fetcher.
  private[server] val partitionsToEvictAfterDisklessSwitch = mutable.Buffer[TopicPartition]()

  // At the seal but not yet in metadata ISR, so we cannot evict. Visible for testing.
  private[server] val partitionsAwaitingIsrRecovery = mutable.Buffer[TopicPartition]()

  override protected def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = {
    replicaMgr.localLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = {
    replicaMgr.localLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      // This is thread-safe, so we don't expect any exceptions, but catch and log any errors
      // to avoid failing the caller, especially during shutdown. We will attempt to close
      // leaderEndpoint after the thread terminates.
      try {
        leader.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of $leader after initiating replica fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    // We don't expect any exceptions here, but catch and log any errors to avoid failing the caller,
    // especially during shutdown. It is safe to catch the exception here without causing correctness
    // issue because we are going to shutdown the thread and will not re-use the leaderEndpoint anyway.
    try {
      leader.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close $leader after shutting down replica fetcher thread", t)
    }
  }

  override def doWork(): Unit = {
    super.doWork()
    completeDelayedFetchRequests()
    evictFullySwitchedDisklessPartitions()
    backOffPartitionsAwaitingIsrRecovery()
  }

  /**
   * Whether the eviction check in processPartitionData should mark fully-switched partitions
   * for removal. The classic ReplicaFetcherThread enables this (so it self-evicts at the seal
   * and hands off to consolidation). The ConsolidationFetcherThread disables it because it
   * intentionally fetches for already-switched partitions.
   */

  protected def shouldEvictFullySwitchedDisklessPartitions: Boolean = true

  // Overridden to false by ConsolidationFetcherThread so consolidation throughput is reported via the
  // ConsolidationFetchBytesInPerSec meter (JMX: kafka.server:type=ReplicaManager,name=ConsolidationFetchBytesInPerSec)
  // instead of inflating the inter-broker ReplicationBytesInPerSec metric.
  protected def shouldRecordReplicationBytesIn: Boolean = true

  // process fetched data
  override def processPartitionData(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    partitionLeaderEpoch: Int,
    partitionData: FetchData
  ): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))

    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    if (logTrace)
      trace("Follower has replica log end offset %d for partition %s. Received %d bytes of messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, partitionLeaderEpoch)

    if (logTrace)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))
    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    var maybeUpdateHighWatermarkMessage = s"but did not update replica high watermark"
    log.maybeUpdateHighWatermark(partitionData.highWatermark).ifPresent { newHighWatermark =>
      maybeUpdateHighWatermarkMessage = s"and updated replica high watermark to $newHighWatermark"
      partitionsWithNewHighWatermark += topicPartition
    }

    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)
    if (logTrace)
      trace(s"Follower received high watermark ${partitionData.highWatermark} from the leader " +
        s"$maybeUpdateHighWatermarkMessage for partition $topicPartition")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    if (shouldRecordReplicationBytesIn)
      brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    // Stop fetching once the switch is complete: seal is committed, local LEO has reached it,
    // and this replica is in ISR. A consolidating partition evicts without waiting for ISR so
    // it can hand off to the consolidation fetcher.
    val inklessMetadataView = replicaMgr.inklessMetadataView()
    val classicToDisklessStartOffset = inklessMetadataView.getClassicToDisklessStartOffset(topicPartition)
    def isConsolidatingPartition: Boolean =
      brokerConfig.disklessRemoteStorageConsolidationEnabled &&
        inklessMetadataView.isConsolidatingDisklessTopic(topicPartition.topic)
    if (shouldEvictFullySwitchedDisklessPartitions &&
        classicToDisklessStartOffset >= 0 &&
        log.logEndOffset >= classicToDisklessStartOffset) {
      if (isConsolidatingPartition || inklessMetadataView.isReplicaInIsr(topicPartition, brokerConfig.brokerId)) {
        partitionsToEvictAfterDisklessSwitch += topicPartition
      } else {
        // The leader answers this fetch from immediateFetchResponses and does not park it
        // in the fetch purgatory, so maxWaitMs is ignored. Delay here or we re-fetch at
        // network rate until the ISR expansion lands.
        partitionsAwaitingIsrRecovery += topicPartition
      }
    }

    logAppendInfo
  }

  private def completeDelayedFetchRequests(): Unit = {
    if (partitionsWithNewHighWatermark.nonEmpty) {
      replicaMgr.completeDelayedFetchRequests(partitionsWithNewHighWatermark.toSeq)
      partitionsWithNewHighWatermark.clear()
    }
  }

  // Visible for testing. Must run from doWork, not processPartitionData: processFetchRequest
  // overwrites fetch state right after processPartitionData and would drop an inline delay.
  private[server] def backOffPartitionsAwaitingIsrRecovery(): Unit = {
    if (partitionsAwaitingIsrRecovery.nonEmpty) {
      val toDelay = partitionsAwaitingIsrRecovery.toSet
      partitionsAwaitingIsrRecovery.clear()
      delayPartitions(toDelay, brokerConfig.replicaFetchBackoffMs.toLong)
    }
  }

  private def evictFullySwitchedDisklessPartitions(): Unit = {
    if (partitionsToEvictAfterDisklessSwitch.nonEmpty) {
      val toEvict = partitionsToEvictAfterDisklessSwitch.toSet
      partitionsToEvictAfterDisklessSwitch.clear()
      info(s"Evicting partitions from this replica fetcher because they have completed the " +
        s"classic-to-diskless switch and the local log has caught up to the seal offset: $toEvict")
      replicaMgr.replicaFetcherManager.removeFetcherForPartitions(toEvict)
      replicaMgr.startConsolidationFetchersForCaughtUpClassicPartitions(toEvict)
    }
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   */
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(tp)

    partition.truncateTo(offsetTruncationState.offset, isFuture = false)

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp,
        offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }
}
