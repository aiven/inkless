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

import io.aiven.inkless.control_plane.{ControlPlane, PruneDisklessLogsError, PruneDisklessLogsRequest}
import kafka.cluster.Partition
import kafka.server.ReplicaManager
import kafka.server.metadata.InklessMetadataView
import kafka.utils.Logging
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.metadata.PartitionRegistration

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

class ConsolidatedDisklessLogPruner(replicaManager: ReplicaManager,
                                    inklessMetadataView: InklessMetadataView,
                                    controlPlane: ControlPlane) extends Runnable with Logging {

  override def run(): Unit = {
    // Read the classic-to-diskless start offset once per partition and thread it through, so the
    // eligibility check and the per-partition prune decision always see the same value (no TOCTOU
    // between dropping SWITCH_PENDING and computing the safe prune offset).
    val eligiblePartitionsWithSeal = inklessMetadataView.getConsolidatingDisklessTopicPartitions.asScala
      .map(tip => (tip, inklessMetadataView.getClassicToDisklessStartOffset(tip.topicPartition)))
      .filter { case (_, seal) => seal != PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING }
      .map { case (tip, seal) => (replicaManager.getPartitionOrError(tip.topicPartition), seal) }
    eligiblePartitionsWithSeal
      .collect { case (Left(error), _) => error }
      .foreach(error => logger.warn("Got error during pruning consolidated diskless logs: {}", error.message))
    val requests = eligiblePartitionsWithSeal
      .collect { case (Right(partition), seal) => (partition, seal) }
      .flatMap { case (partition, seal) =>
        partition.topicId.flatMap { topicId =>
          partition.log.flatMap { log =>
            val highestRemoteOffset = log.highestOffsetInRemoteStorage
            if (highestRemoteOffset < 0) {
              None
            } else {
              safePruneOffset(partition, seal, highestRemoteOffset).map { safeHighestRemoteOffset =>
                val topicIdPartition = new TopicIdPartition(topicId, partition.topicPartition)
                new PruneDisklessLogsRequest(topicIdPartition, safeHighestRemoteOffset)
              }
            }
          }
        }
      }.toSeq.asJava
    if (!requests.isEmpty) {
      controlPlane.pruneDisklessLogs(requests).asScala.foreach { pruneDisklessLogsResponse =>
        if (pruneDisklessLogsResponse.error != PruneDisklessLogsError.NONE) {
          logger.warn("Prune diskless logs did not apply for {} (control plane reported {})",
            pruneDisklessLogsResponse.topicIdPartition,
            pruneDisklessLogsResponse.error)
        } else {
          replicaManager.getPartitionOrError(pruneDisklessLogsResponse.topicIdPartition.topicPartition) match {
            case Right(partition) =>
              val newDisklessLogStart = pruneDisklessLogsResponse.disklessLogStartOffset
              partition.maybeAdvanceConsolidationPruneFloor(newDisklessLogStart)
            case Left(error) => logger.warn("Couldn't update diskless start offset for {} due to: {}",
              pruneDisklessLogsResponse.topicIdPartition.topicPartition,
              error.message
            )
          }
        }
      }
    }
  }

  private def safePruneOffset(partition: Partition, seal: Long, highestRemoteOffset: Long): Option[Long] = {
    seal match {
      case PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET =>
        Some(highestRemoteOffset)
      case classicToDisklessStartOffset if classicToDisklessStartOffset >= 0 =>
        partition.getSafeConsolidatedDisklessPruneOffset(highestRemoteOffset)
      case unexpected =>
        logger.warn("Skipping pruning for {} due to unexpected classic-to-diskless start offset {}",
          partition.topicPartition,
          unexpected)
        None
    }
  }
}
