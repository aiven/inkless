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
import io.aiven.inkless.control_plane.FindBatchRequest
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
 * in the fetch operation purgatory
 */
class DelayedFetch(
  params: FetchParams,
  fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)],
  replicaManager: ReplicaManager,
  isInklessTopic: String => Boolean = _ => false,
  quota: ReplicaQuota,
  responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit
) extends DelayedOperation(params.maxWaitMs) with Logging {

  override def toString: String = {
    s"DelayedFetch(params=$params" +
      s", numPartitions=${fetchPartitionStatus.size}" +
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
    var accumulatedSize = 0
    val inklessFetchPartitionStatus = mutable.Buffer.empty[(TopicIdPartition, FetchPartitionStatus)]
    fetchPartitionStatus.foreach {
      case (topicIdPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UNKNOWN_OFFSET_METADATA && !isInklessTopic(topicIdPartition.topic())) {
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
          } else if (fetchOffset != LogOffsetMetadata.UNKNOWN_OFFSET_METADATA && isInklessTopic(topicIdPartition.topic())) {
            inklessFetchPartitionStatus += topicIdPartition -> fetchStatus
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

    tryCompleteInkless(inklessFetchPartitionStatus) match {
      case Some(inklessAccumulatedSize) => accumulatedSize += inklessAccumulatedSize
      case None => forceComplete()
    }

    // Case G
    if (accumulatedSize >= params.minBytes)
      forceComplete()
    else
      false
  }

  private def tryCompleteInkless(fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)]): Option[Int] = {
    var accumulatedSize = 0
    val fetchPartitionStatusMap = fetchPartitionStatus.toMap
    val requests = fetchPartitionStatus.map { case (topicIdPartition, fetchStatus) =>
      val fetchOffset = fetchStatus.startOffsetMetadata
      new FindBatchRequest(topicIdPartition, fetchOffset.messageOffset, fetchStatus.fetchInfo.maxBytes)
    }
    if (requests.isEmpty) { return Some(0) }

    val response = replicaManager.findInklessBatches(requests, Int.MaxValue)
    val r = response.get.asScala
    r.foreach { response =>
      response.errors() match {
        case Errors.NONE =>
          if (response.batches().size() > 0) {
            val topicIdPartition = response.batches().get(0).metadata().topicIdPartition()
            val endOffset = response.highWatermark()
            val fetchPartitionStatus = fetchPartitionStatusMap.get(topicIdPartition)
            if (fetchPartitionStatus.isDefined) {
              val fetchOffset = fetchPartitionStatus.get.startOffsetMetadata
              if (fetchOffset.messageOffset > endOffset) {
                // Truncation happened
                debug(s"Satisfying fetch $this since it is fetching later segments of partition $topicIdPartition.")
                return None
              } else if (fetchOffset.messageOffset < endOffset) {
                val bytesAvailable = safeLongToInt(response.estimatedByteSize(fetchOffset.messageOffset))
                accumulatedSize += bytesAvailable
              }
            }
          }
        case _ => return None
      }
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
    val fetchInfos = fetchPartitionStatus.map { case (tp, status) =>
      tp -> status.fetchInfo
    }

    val (inklessFetchInfos, classicFetchInfos) = fetchInfos.partition { case (k, _) => isInklessTopic(k.topic()) }
    val inklessParams = replicaManager.fetchParamsWithNewMaxBytes(params, inklessFetchInfos.size.toFloat / fetchInfos.size.toFloat)
    val classicParams = replicaManager.fetchParamsWithNewMaxBytes(params, classicFetchInfos.size.toFloat / fetchInfos.size.toFloat)

    val inklessFetchResponseFuture = replicaManager.fetchInklessMessages(inklessParams, inklessFetchInfos)

    val logReadResults = replicaManager.readFromLog(
      classicParams,
      classicFetchInfos,
      quota,
      readFromPurgatory = true
    )

    val classicFetchPartitionData = logReadResults.map { case (tp, result) =>
      val isReassignmentFetch = classicParams.isFromFollower &&
        replicaManager.isAddingReplica(tp.topicPartition, classicParams.replicaId)

      tp -> result.toFetchPartitionData(isReassignmentFetch)
    }

    inklessFetchResponseFuture.whenComplete{ case (inklessFetchPartitionData, _) =>
      responseCallback(classicFetchPartitionData ++ inklessFetchPartitionData)
    }

  }

  private def safeLongToInt(longValue: Long): Int = {
    if (longValue > Int.MaxValue) {
      Int.MaxValue
    } else if (longValue < Int.MinValue) {
      0
    } else {
      longValue.toInt
    }
  }

}

object DelayedFetchMetrics {
  private val metricsGroup = new KafkaMetricsGroup(DelayedFetchMetrics.getClass)
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter: Meter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, Map(FetcherTypeKey -> "follower").asJava)
  val consumerExpiredRequestMeter: Meter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, Map(FetcherTypeKey -> "consumer").asJava)
}

