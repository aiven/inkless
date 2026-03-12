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
import scala.collection.{Map, Set}
import scala.jdk.CollectionConverters._

/**
 * Fetcher thread for cross-cluster mirroring. Unlike ReplicaFetcherThread, this rewrites
 * leader epochs to the destination's local epoch and producer IDs to negative space,
 * and routes fetcher operations to MirrorFetcherManager.
 */
class MirrorFetcherThread(name: String,
                          leader: LeaderEndPoint,
                          brokerConfig: KafkaConfig,
                          failedPartitions: FailedPartitions,
                          replicaMgr: ReplicaManager,
                          quota: ReplicaQuota,
                          logPrefix: String,
                          mirrorName: String,
                          mirrorFetchBackoffMs: Int)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false),
                                fetchBackOffMs = mirrorFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats,
                                mirrorName) {
  this.logIdent = logPrefix

  override protected def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    replicaMgr.mirrorFetcherManager.removeFetcherForPartitions(partitions)
  }

  // invalidates stale source leaders for affected partitions before creating new fetchers
  override protected def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    replicaMgr.mirrorMetadataManager.foreach(_.invalidateSourceLeader(mirrorName, partitionAndOffsets.keySet.asJava))
    replicaMgr.maybeCreateMirrorFetchers(mirrorName, partitionAndOffsets.keySet.asJava)
  }

  // invalidates cached source leaders for affected partitions on IOException from the source cluster
  override protected def handleMirrorFetchConnectionFailure(mirrorPartitions: Set[TopicPartition]): Unit = {
    replicaMgr.mirrorMetadataManager.foreach(_.invalidateSourceLeader(mirrorName, mirrorPartitions.asJava))
    replicaMgr.maybeCreateMirrorFetchers(mirrorName, mirrorPartitions.asJava)
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

    // Append batches from the source cluster to the destination partition's log. Leader epochs
    // are rewritten to the destination's local epoch and producer IDs to negative space before append.
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, partitionLeaderEpoch)

    if (logTrace)
      trace("Mirror follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))

    val leaderLogStartOffset = partitionData.logStartOffset

    // This works as producer write with acks=1. The leader node will append data into log without HW incremented.
    // The leader's HW will be incremented only when all ISR (at least minISR) are caught up.
    if (!partition.maybeIncrementLeaderHWWithLock(log)) {
      trace(s"Mirror follower received high watermark ${partitionData.highWatermark} from the leader " +
        s"but did not update replica high watermark for partition $topicPartition")
    }

    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)

    // Update mirroring lag
    replicaMgr.updateMirrorLag(mirrorName, topicPartition, partitionData.highWatermark, log.highWatermark)

    // Account for replication quota
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  // return the mirror partition lag
  // TODO: Since we already record the lag in stats, maybe we don't cache the logInfo in mirrorFetcherManager anymore.
  override def getPartitionLag(topicPartition: TopicPartition, leaderHW: Long, nextOffset: Long, mirrorName: String): Long = {
    replicaMgr.mirrorFetcherManager.getMirrorLagInfo(mirrorName).get(topicPartition).map(_.lag).getOrElse(0L)
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
    partition.localLogOrException.latestEpochFromLog()
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
