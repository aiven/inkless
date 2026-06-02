/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.inkless.consolidation

import io.aiven.inkless.consume.ConcatenatedRecords
import kafka.server.{FailedPartitions, InitialFetchState, KafkaConfig, ReplicaFetcherThread, ReplicaManager, ReplicaQuota}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{KafkaStorageException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.storage.internals.log.LogAppendInfo

import scala.util.Try

class ConsolidationFetcherThread(name: String,
                                 leader: LeaderEndPoint,
                                 brokerConfig: KafkaConfig,
                                 failedPartitions: FailedPartitions,
                                 replicaMgr: ReplicaManager,
                                 quota: ReplicaQuota,
                                 logPrefix: String,
                                 consolidationMetrics: Option[ConsolidationMetrics] = None) extends ReplicaFetcherThread(name, leader, brokerConfig, failedPartitions, replicaMgr, quota, logPrefix) {

  // A topic can be deleted between being scheduled for consolidation and reaching addPartitions.
  // Process each partition independently so one deleted topic doesn't block the rest of the batch.
  // This acquires partitionMapLock per partition (N times) — acceptable for small consolidation batches.
  override def addPartitions(initialFetchStates: scala.collection.Map[TopicPartition, InitialFetchState]): scala.collection.Set[TopicPartition] = {
    val added = scala.collection.mutable.Set[TopicPartition]()
    initialFetchStates.foreach { case (tp, state) =>
      try {
        added ++= super.addPartitions(Map(tp -> state))
      } catch {
        case e: KafkaStorageException =>
          info(s"Skipping partition $tp in addPartitions: ${e.getMessage}")
      }
    }
    added
  }

  // localLogOrException throws UnknownTopicOrPartitionException for deleted partitions,
  // but AbstractFetcherThread only handles KafkaStorageException in fetchOffsetAndTruncate.
  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    try {
      super.logEndOffset(topicPartition)
    } catch {
      case e: UnknownTopicOrPartitionException =>
        throw new KafkaStorageException(e.getMessage, e)
    }
  }

  override def toMemoryRecords(records: Records): MemoryRecords = {
    (records: @unchecked) match {
      case r: ConcatenatedRecords => r.toMemoryRecords
      case _ => super.toMemoryRecords(records)
    }
  }

  override protected def shouldEvictFullySwitchedDisklessPartitions: Boolean = false

  override def processPartitionData(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    partitionLeaderEpoch: Int,
    partitionData: FetchData
  ): Option[LogAppendInfo] = {
    val result = super.processPartitionData(topicPartition, fetchOffset, partitionLeaderEpoch, partitionData)

    consolidationMetrics.foreach { metrics =>
      val logOpt = Try(replicaMgr.getPartitionOrException(topicPartition).localLogOrException).toOption
      logOpt.foreach { log =>
        val disklessLogEndOffset = partitionData.highWatermark
        val localLogEndOffset = log.logEndOffset
        val remoteLogEndOffset = log.highestOffsetInRemoteStorage()

        metrics.updateLocalLag(topicPartition, Math.max(0L, disklessLogEndOffset - localLogEndOffset))
        if (remoteLogEndOffset >= 0) {
          metrics.updateTotalLag(topicPartition, Math.max(0L, disklessLogEndOffset - remoteLogEndOffset))
          metrics.updateDeletableMessages(topicPartition, Math.max(0L, remoteLogEndOffset - log.localLogStartOffset()))
        }
      }
    }

    result
  }
}
