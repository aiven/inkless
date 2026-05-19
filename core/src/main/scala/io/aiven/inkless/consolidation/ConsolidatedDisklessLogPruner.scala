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
    val eligibleDisklessTopicIdPartitions = inklessMetadataView.getConsolidatingDisklessTopicPartitions.asScala
      .filter(tip => inklessMetadataView.getClassicToDisklessStartOffset(tip.topicPartition) != PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    val eitherErrorOrLog = eligibleDisklessTopicIdPartitions
      .map(tip => replicaManager.getPartitionOrError(tip.topicPartition))
      .partition(either => either.isLeft)
    eitherErrorOrLog._1
      .flatMap {
        case Left(error) => Some(error)
        case _ => None
      }
      .foreach(error => logger.warn("Got error during pruning consolidated diskless logs: {}", error.message))
    val requests = eitherErrorOrLog._2
      .flatMap {
        case Right(partition) =>
          partition.topicId.flatMap { topicId =>
            partition.log.flatMap { log =>
              val highestRemoteOffset = log.highestOffsetInRemoteStorage
              if (highestRemoteOffset < 0) {
                None
              } else {
                partition.getSafeConsolidatedDisklessPruneOffset(highestRemoteOffset)
                  .map { safeHighestRemoteOffset =>
                    val topicIdPartition = new TopicIdPartition(topicId, partition.topicPartition)
                    new PruneDisklessLogsRequest(topicIdPartition, safeHighestRemoteOffset)
                  }
              }
            }
          }
        case _ => None
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
              if (!partition.maybeUpdateDisklessLogStartOffset(newDisklessLogStart)) {
                logger.error("Diskless log start offset is non-monotonic. The old one ({}) is greater than the new ({}).")
              }
            case Left(error) => logger.warn("Couldn't update diskless start offset for {} due to: {}",
              pruneDisklessLogsResponse.topicIdPartition.topicPartition,
              error.message
            )
          }
        }
      }
    }
  }
}
