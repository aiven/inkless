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

import io.aiven.inkless.common.SharedState
import io.aiven.inkless.control_plane.{ControlPlaneUnavailableException, ListOffsetsRequest => CpListOffsetsRequest}
import kafka.server.metadata.InklessMetadataView
import kafka.utils.Logging
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DeleteRecordsResponse
import org.apache.kafka.common.utils.Time

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

/**
 * Tracks the cross-tier earliest offset of the partitions on this broker that are in phase 1 of the
 * classic-to-diskless switch.
 *
 * In phase 1, `DeleteRecords` and retention move the log start of the leader's local log and the cross-tier
 * earliest offset in the control plane. The followers left the classic fetcher at the seal. Their local logs
 * still contain the deleted records, and the metadata routing sends consumers to them. A follower that becomes the
 * leader keeps its old log start until the remote log manager raises it. So every replica refuses a consumer read
 * of the classic prefix below the cross-tier earliest offset.
 *
 * The check on the fetch path reads only values in memory, and a scheduled refresh reads them from the control
 * plane in one call for every phase 1 partition on this broker. A slow control plane then never delays a request
 * handler thread. After a failed refresh the broker keeps the last values. They can only be too low, and the
 * check refuses only deleted offsets with them.
 */
final class DisklessPhaseOneCrossTierEarliest(metadataView: InklessMetadataView,
                                              consolidationEnabled: Boolean,
                                              sharedState: Option[SharedState],
                                              time: Time) extends Logging {
  import DisklessPhaseOneCrossTierEarliest._

  private val earliest = new ConcurrentHashMap[TopicPartition, java.lang.Long]()
  private val lastFailureLogMs = new AtomicLong(0L)

  private def isPhaseOne(tp: TopicPartition): Boolean =
    consolidationEnabled && metadataView.isConsolidatingDisklessTopic(tp.topic) &&
      metadataView.isClassicPrefixWaitingForUpload(tp)

  /**
   * Checks whether a consumer read below the seal of a phase 1 partition starts below the cross-tier earliest
   * offset of the last refresh.
   * True for such a read; false otherwise, and also when no refresh has succeeded yet.
   */
  def isPrefixReadBelowEarliest(isFromConsumer: Boolean, tp: TopicPartition, fetchOffset: Long, seal: Long): Boolean =
    isFromConsumer && seal >= 0 && fetchOffset < seal && isPhaseOne(tp) && {
      val value = earliest.get(tp)
      value != null && fetchOffset < value
    }

  private def advance(tp: TopicPartition, offset: Long): Unit =
    earliest.merge(tp, offset, (a, b) => if (a >= b) a else b)

  /**
   * Reads the cross-tier earliest offset of the hosted phase 1 partitions from the control plane in one call,
   * and forgets the partitions that left phase 1.
   */
  def refresh(hostedPartitions: Iterator[TopicPartition]): Unit = {
    val partitions = mutable.ArrayBuffer.empty[TopicIdPartition]
    val inPhaseOne = mutable.Set.empty[TopicPartition]
    hostedPartitions.foreach { tp =>
      val topicId = metadataView.getTopicId(tp.topic)
      if (isPhaseOne(tp) && topicId != null && !topicId.equals(Uuid.ZERO_UUID)) {
        partitions += new TopicIdPartition(topicId, tp.partition, tp.topic)
        inPhaseOne += tp
      }
    }
    earliest.keySet.retainAll(inPhaseOne.asJava)
    if (partitions.isEmpty || sharedState.isEmpty) return
    try {
      val requests = partitions.map(new CpListOffsetsRequest(_, CpListOffsetsRequest.EARLIEST_TIMESTAMP))
      sharedState.get.controlPlane().listOffsets(requests.asJava).forEach { response =>
        if (response.errors() == Errors.NONE && response.offset() >= 0)
          advance(response.topicIdPartition().topicPartition(), response.offset())
      }
    } catch {
      case e: ControlPlaneUnavailableException => throw e
      case e: Exception =>
        val now = time.milliseconds
        val lastMs = lastFailureLogMs.get
        if (now - lastMs >= FailureLogGapMs && lastFailureLogMs.compareAndSet(lastMs, now))
          warn(s"Failed to read the cross-tier earliest offset of ${partitions.size} phase 1 partitions from the " +
            "control plane. Consumer reads below the seal use the values of the last successful read.", e)
    }
  }

  /**
   * Returns the `DeleteRecords` response for a request whose cross-tier advance returned `advanced`, and how long
   * the leader waits before it sends that response.
   *
   * A phase 1 partition whose cross-tier earliest didn't advance gets `KAFKA_STORAGE_ERROR`, which clients retry:
   * in phase 1 only `DeleteRecords` advances it, and the followers would serve the deleted records until phase 2.
   * The leader's local delete is already done, and a retry is a no-op for it.
   *
   * The other brokers learn an advanced value at their next refresh. So the response waits two refresh intervals:
   * one for the next refresh to start, and one for its control-plane read. If the request timeout is shorter, the
   * leader answers `REQUEST_TIMED_OUT` for those partitions at the timeout, as `DelayedDeleteRecords` does when
   * the followers don't catch up in time. The leader keeps the records deleted.
   */
  def completeDeleteRecords(response: Map[TopicPartition, DeleteRecordsPartitionResult],
                            advanced: Map[TopicPartition, DeleteRecordsPartitionResult],
                            timeoutMs: Long): (Map[TopicPartition, DeleteRecordsPartitionResult], Long) = {
    val holdMs = sharedState.map(2 * _.config.crossTierLogStartReportInterval.toMillis).getOrElse(0L)
    var held = false
    val result = mutable.Map.empty[TopicPartition, DeleteRecordsPartitionResult]
    response.foreachEntry { (tp, original) =>
      val phaseOne = isPhaseOne(tp)
      result(tp) = advanced.get(tp) match {
        case Some(stored) if phaseOne =>
          advance(tp, stored.lowWatermark)
          held = holdMs > 0
          if (holdMs > timeoutMs) stored.setErrorCode(Errors.REQUEST_TIMED_OUT.code) else stored
        case Some(stored) => stored
        case None if phaseOne && original.errorCode == Errors.NONE.code =>
          new DeleteRecordsPartitionResult()
            .setPartitionIndex(tp.partition)
            .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case None => original
      }
    }
    (result, if (held) math.min(holdMs, timeoutMs) else 0L)
  }
}

object DisklessPhaseOneCrossTierEarliest {
  private val FailureLogGapMs = 60000L
}
