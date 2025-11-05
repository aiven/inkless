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

package kafka.server.mirror

import kafka.server._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState}
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.storage.internals.log.{LogAppendInfo, LogStartOffsetIncrementReason}

import java.util.Optional
import scala.collection.{Map, Set, mutable}

/**
 * Specialized fetcher thread for cross-cluster mirroring.
 *
 * This class extends AbstractFetcherThread to handle partitions that replicate from remote clusters.
 * Unlike ReplicaFetcherThread which handles intra-cluster replication, MirrorFetcherThread handles
 * cross-cluster replication where source and destination clusters have independent leader epochs.
 *
 * Key differences from ReplicaFetcherThread:
 * - Uses source cluster's leader epoch (not destination's) for append validation
 * - Routes fetcher operations to MirrorFetcherManager (not ReplicaFetcherManager)
 * - Syncs destination partition's epoch with source cluster's epoch
 *
 * @param name The name of the thread
 * @param leader The remote leader endpoint to fetch from (RemoteLeaderEndPoint)
 * @param brokerConfig Broker configuration
 * @param failedPartitions Tracker for partitions with errors
 * @param replicaMgr The local ReplicaManager
 * @param quota Replication quota for throttling
 * @param logPrefix Log prefix for this thread
 * @param mirrorName The name of the cluster mirror configuration
 */
class MirrorFetcherThread(name: String,
                          leader: LeaderEndPoint,
                          brokerConfig: KafkaConfig,
                          failedPartitions: FailedPartitions,
                          replicaMgr: ReplicaManager,
                          quota: ReplicaQuota,
                          logPrefix: String,
                          mirrorName: String)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false),
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats,
                                mirrorName) {
  this.logIdent = logPrefix

  // Visible for testing
  private[mirror] val partitionsWithNewHighWatermark = mutable.Buffer[TopicPartition]()

  override def doWork(): Unit = {
    super.doWork()
    // Complete delayed fetch requests for consumers waiting on mirrored partitions
    if (partitionsWithNewHighWatermark.nonEmpty) {
      replicaMgr.completeDelayedFetchRequests(partitionsWithNewHighWatermark.toSeq)
      partitionsWithNewHighWatermark.clear()
    }
  }

  override protected def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    replicaMgr.mirrorFetcherManager.removeFetcherForPartitions(partitions)
  }

  override protected def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    replicaMgr.mirrorFetcherManager.addFetcherForPartitions(partitionAndOffsets)
  }

  // process fetched data
  override def processPartitionData(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    partitionLeaderEpoch: Int,
    partitionData: FetchResponseData.PartitionData
  ): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))

    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    if (logTrace)
      trace("Mirror follower has replica log end offset %d for partition %s. Received %d bytes of messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append batches from the source cluster to the destination partition's log, preserving
    // the original leader epochs from the source cluster.
    //
    // Why we preserve source epochs:
    //   Consumers cache leader epochs from the batches they read. When a consumer reads from
    //   the source cluster and caches epoch=5 at offset=100, then fails over to the destination
    //   cluster, it will validate its cached epoch against the destination's log. If we rewrote
    //   epoch=5 to a different value (e.g., destination epoch=0), the consumer would receive
    //   FENCED_LEADER_EPOCH and reset its position, causing data loss or duplication.
    //
    // Epoch cache synchronization:
    //   The appendAsFollower operation automatically updates the destination partition's epoch
    //   cache to match the source cluster's epoch metadata. For each batch appended:
    //     - Extract the batch's leader epoch (e.g., epoch=5)
    //     - Record the mapping: epoch -> startOffset (e.g., 5 -> 40)
    //     - Add entry to epoch cache if this epoch is new
    //
    //   This ensures the destination's epoch cache becomes identical to the source's epoch cache,
    //   enabling correct truncation and divergence detection for all replicas.
    //
    // Two-level replication:
    //   After this append, the destination partition has batches with source epochs in its log.
    //   When destination followers (ReplicaFetcherThread) replicate from this partition, they
    //   receive these same batches with source epochs. The effectiveLeaderEpoch logic in
    //   ReplicaFetcherThread allows them to accept batches whose epochs are higher than the
    //   destination partition's local leader epoch, ensuring intra-cluster replication succeeds.
    //
    //   Result: All replicas (source leader, destination leader, destination followers) have
    //   identical log contents and epoch cache entries, maintaining consistency across clusters.
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, partitionLeaderEpoch)

    if (logTrace)
      trace("Mirror follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))

    val leaderLogStartOffset = partitionData.logStartOffset
    var maybeUpdateHighWatermarkMessage = s"but did not update replica high watermark"
    log.maybeUpdateHighWatermark(partitionData.highWatermark).ifPresent { newHighWatermark =>
      maybeUpdateHighWatermarkMessage = s"and updated replica high watermark to $newHighWatermark"
      partitionsWithNewHighWatermark += topicPartition
    }

    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)
    if (logTrace)
      trace(s"Mirror follower received high watermark ${partitionData.highWatermark} from the leader " +
        s"$maybeUpdateHighWatermarkMessage for partition $topicPartition")

    // Account for replication quota
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      try {
        leader.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of $leader after initiating mirror fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    try {
      leader.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close $leader after shutting down mirror fetcher thread", t)
    }
  }

  override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateTo(truncationState.offset, isFuture = false)
  }

  override def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  override def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.latestEpoch
  }

  override def latestEpochFromLog(topicPartition: TopicPartition): Optional[Integer] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.latestEpoch
  }

  override def logStartOffset(topicPartition: TopicPartition): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logStartOffset
  }

  override def logEndOffset(topicPartition: TopicPartition): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logEndOffset
  }

  override def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.endOffsetForEpoch(epoch)
  }
}
