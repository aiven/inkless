package io.aiven.inkless.consolidation

import io.aiven.inkless.control_plane.{ControlPlane, PruneDisklessLogsRequest}
import kafka.server.ReplicaManager
import kafka.server.metadata.InklessMetadataView
import kafka.utils.Logging
import org.apache.kafka.common.TopicIdPartition

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}
import scala.jdk.OptionConverters.RichOptional

class ConsolidatedDisklessLogPruner(replicaManager: ReplicaManager,
                                    inklessMetadataView: InklessMetadataView,
                                    controlPlane: ControlPlane) extends Runnable with Logging {

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  override def run(): Unit = {
    val disklessTopicIdPartitions = inklessMetadataView.getConsolidatingDisklessTopicPartitions.asScala
    val eitherErrorOrLog = disklessTopicIdPartitions
      .map(tip => replicaManager.getPartitionOrError(tip.topicPartition))
      .partition(either => either.isLeft)
    eitherErrorOrLog._1
      .flatMap {
        case Left(error) => Some(error)
        case _ => None
      }
      .foreach(error => logger.warn("Got error during diskless consolidation reconciliation: {}", error.message))
    val requests = eitherErrorOrLog._2
      .flatMap {
        case Right(partition) =>
          partition.topicId.flatMap { topicId =>
            partition.log.flatMap { log =>
              val highestRemoteOffset = log.highestOffsetInRemoteStorage
              if (highestRemoteOffset < 0) {
                None
              } else {
                val topicIdPartition = new TopicIdPartition(topicId, partition.topicPartition)
                Some(new PruneDisklessLogsRequest(topicIdPartition, highestRemoteOffset))
              }
            }
          }
        case _ => None
      }.toSeq.asJava
    if (!requests.isEmpty) {
      controlPlane.pruneDisklessLogs(requests).asScala.foreach { pruneDisklessLogsResponse =>
        inklessMetadataView.getTopicName(pruneDisklessLogsResponse.topicIdPartition.topicId).toScala match {
          case Some(topicName) =>
            val responseTopicIdPartition = new TopicIdPartition(pruneDisklessLogsResponse.topicIdPartition.topicId,
              pruneDisklessLogsResponse.topicIdPartition.partition, topicName)
            replicaManager.getPartitionOrError(responseTopicIdPartition.topicPartition) match {
              case Right(partition) => partition.setDisklessStartOffset(pruneDisklessLogsResponse.disklessStartOffset)
              case Left(error) => logger.warn("Couldn't update diskless start offset for {} due to: {}",
                responseTopicIdPartition.topicPartition,
                error.message
              )
            }
          case None =>
            logger.warn("Couldn't update diskless start offset of topic with ID {} due to missing name", pruneDisklessLogsResponse.topicIdPartition.topicId)
        }
      }
    }
  }
}
