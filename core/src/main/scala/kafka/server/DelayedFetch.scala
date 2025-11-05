/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import com.yammer.metrics.core.Meter
import io.aiven.inkless.control_plane.FindBatchResponse
import kafka.utils.Logging

import java.util.concurrent.TimeUnit
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.purgatory.DelayedOperation
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.LogOffsetMetadata

import scala.collection._
import scala.jdk.CollectionConverters._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString: String = {
    "[startOffsetMetadata: " + startOffsetMetadata +
      ", fetchInfo: " + fetchInfo +
      "]"
  }
}

/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory.
 * It includes classic and diskless fetch partition requests.
 * These cannot be completed separately because they need to serve the same callback.
 */
class DelayedFetch(
  params: FetchParams,
  classicFetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)],
  disklessFetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)] = Seq.empty,
  replicaManager: ReplicaManager,
  quota: ReplicaQuota,
  maxWaitMs: Option[Long] = None,
  minBytes: Option[Int] = None,
  responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit,
) extends DelayedOperation(maxWaitMs.getOrElse(params.maxWaitMs)) with Logging {
  var maybeBatchCoordinates: Option[Map[TopicIdPartition, FindBatchResponse]] = None

  override def toString: String = {
    s"DelayedFetch(params=$params" +
      s", numClassicPartitions=${classicFetchPartitionStatus.size}" +
      s", numDisklessPartitions=${disklessFetchPartitionStatus.size}" +
      ")"
  }

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: The replica is no longer available on this broker
   * Case C: This broker does not know of some partitions it tries to fetch
   * Case D: The partition is in an offline log directory on this broker
   * Case E: This broker is the leader, but the requested epoch is now fenced
   * Case F: The fetch offset locates not on the last segment of the log
   * Case G: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case H: A diverging epoch was found, return response to trigger truncation
   * Upon completion, should return whatever data is available for each valid partition
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0L
    classicFetchPartitionStatus.foreach {
      case (topicIdPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UNKNOWN_OFFSET_METADATA) {
            val partition = replicaManager.getPartitionOrException(topicIdPartition.topicPartition)
            val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, params.fetchOnlyLeader)

            val endOffset = params.isolation match {
              case FetchIsolation.LOG_END => offsetSnapshot.logEndOffset
              case FetchIsolation.HIGH_WATERMARK => offsetSnapshot.highWatermark
              case FetchIsolation.TXN_COMMITTED => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case G if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case F.
            if (fetchOffset.messageOffset > endOffset.messageOffset) {
              // Case F, this can happen when the new fetch operation is on a truncated leader
              debug(s"Satisfying fetch $this since it is fetching later segments of partition $topicIdPartition.")
              return forceComplete()
            } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
              if (fetchOffset.onOlderSegment(endOffset)) {
                // Case F, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                debug(s"Satisfying fetch $this immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!params.isFromFollower || !replicaManager.shouldLeaderThrottle(quota, partition, params.replicaId))
                  return forceComplete()
              } else if (fetchOffset.onSameSegment(endOffset)) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!params.isFromFollower || !replicaManager.shouldLeaderThrottle(quota, partition, params.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }

            // Case H: If truncation has caused diverging epoch while this request was in purgatory, return to trigger truncation
            fetchStatus.fetchInfo.lastFetchedEpoch.ifPresent { fetchEpoch =>
              val epochEndOffset = partition.lastOffsetForLeaderEpoch(fetchLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
              if (epochEndOffset.errorCode != Errors.NONE.code()
                || epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET
                || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
                debug(s"Could not obtain last offset for leader epoch for partition $topicIdPartition, epochEndOffset=$epochEndOffset.")
                return forceComplete()
              } else if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchStatus.fetchInfo.fetchOffset) {
                debug(s"Satisfying fetch $this since it has diverging epoch requiring truncation for partition " +
                  s"$topicIdPartition epochEndOffset=$epochEndOffset fetchEpoch=$fetchEpoch fetchOffset=${fetchStatus.fetchInfo.fetchOffset}.")
                return forceComplete()
              }
            }
          }
        } catch {
          case _: NotLeaderOrFollowerException =>  // Case A or Case B
            debug(s"Broker is no longer the leader or follower of $topicIdPartition, satisfy $this immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case C
            debug(s"Broker no longer knows of partition $topicIdPartition, satisfy $this immediately")
            return forceComplete()
          case _: KafkaStorageException => // Case D
            debug(s"Partition $topicIdPartition is in an offline log directory, satisfy $this immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case E
            debug(s"Broker is the leader of partition $topicIdPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $this immediately")
            return forceComplete()
        }
    }

    // adjust the max bytes for diskless fetches based on the percentage of diskless partitions
    // Complete the classic fetches first
    val classicRequestsSize = classicFetchPartitionStatus.size.toFloat
    val disklessRequestsSize = disklessFetchPartitionStatus.size.toFloat
    val totalRequestsSize = classicRequestsSize + disklessRequestsSize
    val disklessPercentage = disklessRequestsSize / totalRequestsSize
    val disklessParams = replicaManager.fetchParamsWithNewMaxBytes(params, disklessPercentage)

    tryCompleteDiskless(disklessFetchPartitionStatus, disklessParams.maxBytes) match {
      case Some(disklessAccumulatedSize) => accumulatedSize += disklessAccumulatedSize
      case None => forceComplete()
    }

    // Case G
    if (accumulatedSize >= minBytes.getOrElse(params.minBytes))
      forceComplete()
    else
      false
  }

  /**
   * The operation can be completed if:
   *
   * Case A: The fetch offset locates not on the last segment of the log
   * Case B: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case C: An error occurs while trying to find diskless batches
   * Case D: The fetch offset is equal to the end offset, meaning that we have reached the end of the log
   * Upon completion, should return whatever data is available for each valid partition
   */
  private def tryCompleteDiskless(
    fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)],
    disklessMaxBytes: Int
  ): Option[Long] = {
    var accumulatedSize = 0L
    val fetchPartitionStatusMap = fetchPartitionStatus.toMap

    maybeBatchCoordinates = try {
      Some(replicaManager.findDisklessBatches(fetchPartitionStatus, disklessMaxBytes))
    } catch {
      case e: Throwable =>
        error("Error while trying to find diskless batches on delayed fetch.", e)
        return None  // Case C
    }

    maybeBatchCoordinates match {
      case Some(exists) =>
        exists.values.foreach { r =>
          r.errors() match {
            case Errors.NONE =>
              if (r.batches().size() > 0) {
                // Gather topic id partition from first batch. Same for all batches in the response.
                val topicIdPartition = r.batches().get(0).metadata().topicIdPartition()
                val endOffset = r.highWatermark()

                val fetchPartitionStatus = fetchPartitionStatusMap.get(topicIdPartition)
                if (fetchPartitionStatus.isEmpty) {
                  warn(s"Fetch partition status for $topicIdPartition not found in delayed fetch $this.")
                  return None // Case C
                }

                val fetchOffset = fetchPartitionStatus.get.startOffsetMetadata
                // If the fetch offset is greater than the end offset, it means that the log has been truncated
                // If it is equal to the end offset, it means that we have reached the end of the log
                // If the fetch offset is less than the end offset, we can accumulate the size of the batches
                if (fetchOffset.messageOffset > endOffset) {
                  // Truncation happened
                  debug(s"Satisfying fetch $this since it is fetching later segments of partition $topicIdPartition.")
                  return None // Case A
                } else if (fetchOffset.messageOffset < endOffset) {
                  val bytesAvailable = r.estimatedByteSize(fetchOffset.messageOffset)
                  accumulatedSize += bytesAvailable // Case B: accumulate the size of the batches
                } // Case D: same as fetchOffset == endOffset, no new data available
              }
            case _ => return None  // Case C
          }
        }
      case None => // Case D
    }
    Some(accumulatedSize)
  }

  override def onExpiration(): Unit = {
    if (params.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    // Complete the classic fetches first
    val classicFetchInfos = classicFetchPartitionStatus.map { case (tp, status) =>
      tp -> status.fetchInfo
    }

    val classicRequestsSize = classicFetchPartitionStatus.size.toFloat
    val disklessRequestsSize = disklessFetchPartitionStatus.size.toFloat
    val totalRequestsSize = classicRequestsSize + disklessRequestsSize

    if (totalRequestsSize == 0) {
      // No partitions to fetch, just return an empty response
      responseCallback(Seq.empty)
      return
    }

    val fetchPartitionData = if (classicRequestsSize > 0) {
      // adjust the max bytes for classic fetches based on the percentage of classic partitions
      val classicPercentage = classicRequestsSize / totalRequestsSize
      val classicParams = replicaManager.fetchParamsWithNewMaxBytes(params, classicPercentage)

      val logReadResults = replicaManager.readFromLog(
        classicParams,
        classicFetchInfos,
        quota,
        readFromPurgatory = true
      )

      logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = params.isFromFollower &&
          replicaManager.isAddingReplica(tp.topicPartition, params.replicaId)

        tp -> result.toFetchPartitionData(isReassignmentFetch)
      }
    } else Seq.empty

    if (disklessRequestsSize > 0) {
      // Classic fetches are complete, now handle diskless fetches
      val disklessFetchInfos = disklessFetchPartitionStatus.map { case (tp, status) =>
        tp -> status.fetchInfo
      }
      val batchCoordinates = maybeBatchCoordinates match {
        case Some(batchCoordinates) => batchCoordinates
        case None =>
          responseCallback(Seq.empty)
          return
      }
      val disklessFetchResponseFuture = replicaManager.fetchDisklessMessages(batchCoordinates, disklessFetchInfos)

      // Combine the classic fetch results with the diskless fetch results
      disklessFetchResponseFuture.whenComplete { case (disklessFetchPartitionData, _) =>
        // Do a single response callback with both classic and diskless fetch results
        responseCallback(fetchPartitionData ++ disklessFetchPartitionData)
      }
    } else {
      // No diskless fetches, just return the classic fetch results
      responseCallback(fetchPartitionData)
    }
  }
}

object DelayedFetchMetrics {
  private val metricsGroup = new KafkaMetricsGroup(DelayedFetchMetrics.getClass)
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter: Meter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, Map(FetcherTypeKey -> "follower").asJava)
  val consumerExpiredRequestMeter: Meter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, Map(FetcherTypeKey -> "consumer").asJava)
}

