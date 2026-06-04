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
import kafka.server.{FailedPartitions, KafkaConfig, ReplicaFetcherThread, ReplicaManager, ReplicaQuota}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.metadata.PartitionRegistration
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
    maybeStampDisklessLeaderEpoch(topicPartition, partitionData)
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

  /**
   * Stamp the frozen diskless leader epoch (E_d) onto materialized batches so the local log keeps a
   * monotonic epoch lineage (diskless records are produced with epoch 0, which would otherwise break
   * the LeaderEpochFileCache after a switched partition's higher classic epochs and disable divergence
   * truncation). Born-diskless / not-yet-switched partitions (E_d == NO_DISKLESS_LEADER_EPOCH) are left
   * at epoch 0. Done in place to reuse the append path's single flatten; partitionLeaderEpoch is outside
   * the batch CRC, so no checksum recompute is needed.
   */
  private def maybeStampDisklessLeaderEpoch(topicPartition: TopicPartition, partitionData: FetchData): Unit = {
    val disklessLeaderEpoch = replicaMgr.disklessLeaderEpoch(topicPartition)
    if (disklessLeaderEpoch == PartitionRegistration.NO_DISKLESS_LEADER_EPOCH) {
      return
    }
    FetchResponse.recordsOrFail(partitionData) match {
      case records: ConcatenatedRecords =>
        records.batches().forEach(batch => batch.setPartitionLeaderEpoch(disklessLeaderEpoch))
      case records: MemoryRecords =>
        records.batches().forEach(batch => batch.setPartitionLeaderEpoch(disklessLeaderEpoch))
      case _ =>
    }
  }
}
