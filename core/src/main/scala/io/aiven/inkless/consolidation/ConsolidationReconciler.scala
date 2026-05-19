package io.aiven.inkless.consolidation

import io.aiven.inkless.control_plane.{ControlPlane, GetLogInfoRequest}
import kafka.cluster.Partition
import kafka.server.metadata.InklessMetadataView
import kafka.server.{InitialFetchState, KafkaConfig, ReplicaManager}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.image.TopicsDelta
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.util.Collections
import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava}
import scala.jdk.OptionConverters.RichOptional

class ConsolidationReconciler(config: KafkaConfig,
                              replicaManager: ReplicaManager,
                              inklessMetadataView: InklessMetadataView,
                              controlPlane: ControlPlane,
                              initialFetchOffset: UnifiedLog => Long,
                              consolidationFetcherManager: ConsolidationFetcherManager) extends Runnable with Logging {

  private val retriablePartitions = new mutable.HashMap[TopicPartition, Partition]()

  private val metricsPackage = "kafka.server"
  private val metricsClassName = this.getClass.getSimpleName
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)
  private val tags = Map("clientId" -> "Reconciler").asJava

  metricsGroup.newGauge("RetriablePartitionsCount", () => retriablePartitions.size, tags)

  override def run(): Unit = {
    val consolidatingPartitionAndOffsets = retriablePartitions synchronized {
      initConsolidatingPartitionFetching(retriablePartitions)
    }
    if (consolidatingPartitionAndOffsets.nonEmpty) {
      consolidationFetcherManager.addFetcherForPartitions(consolidatingPartitionAndOffsets)
    }
  }

  def initConsolidatingPartitionFetching(consolidatingDisklessPartitionsToStartFetching: mutable.HashMap[TopicPartition, Partition]
                                        ): mutable.HashMap[TopicPartition, InitialFetchState] = {
    val consolidatingPartitionAndOffsets = new mutable.HashMap[TopicPartition, InitialFetchState]

    consolidatingDisklessPartitionsToStartFetching.foreachEntry { (topicPartition, partition) =>
      val log = partition.localLogOrException
      val consolidationStartState =
        if (inklessMetadataView.isConsolidatingDisklessTopic(topicPartition.topic))
          reconcileSwitchedConsolidatingDisklessPartition(partition)
        else
          ConsolidationStartState.Ready(initialFetchOffset(log))
      consolidationStartState match {
        case ConsolidationStartState.Ready(offset) =>
          consolidatingPartitionAndOffsets.put(topicPartition, InitialFetchState(
            log.topicId.toScala,
            new BrokerEndPoint(-1, "diskless", -1),
            partition.getLeaderEpoch,
            offset
          ))
          removeRetriablePartition(topicPartition)
        case ConsolidationStartState.Retry(reason) =>
          logger.info(reason)
          addRetriablePartition(topicPartition, partition)
        case ConsolidationStartState.Unsafe(reason) =>
          logger.warn(reason)
          consolidationFetcherManager.addFailedPartition(topicPartition)
      }
    }
    consolidatingPartitionAndOffsets
  }

  private def addRetriablePartition(topicPartition: TopicPartition, partition: Partition): Unit = {
    retriablePartitions synchronized {
      retriablePartitions.put(topicPartition, partition)
    }
  }

  private def removeRetriablePartition(topicPartition: TopicPartition): Unit = {
    retriablePartitions synchronized {
      retriablePartitions.remove(topicPartition)
    }
  }

  def removeRetriablePartitions(topicPartitions: Set[TopicPartition]): Unit = {
    retriablePartitions synchronized {
      topicPartitions.foreach(tp => retriablePartitions.remove(tp))
    }
  }

  def onNewDelta(topicsDelta: TopicsDelta): Unit = {
    val localChanges = topicsDelta.localChanges(config.nodeId)
    // remove deleted topic-partitions
    retriablePartitions synchronized {
      localChanges.deletes.asScala.foreach(tp => {
        retriablePartitions.remove(tp)
        logger.debug("Removing {} from consolidation reconciliation as it has been deleted")
      })
      // only keep retrying partitions which are in the new follower or leader set
      retriablePartitions
        .filterInPlace { (tp, _) =>
          localChanges.leaders.containsKey(tp) || localChanges.followers.containsKey(tp)
        }
    }
  }

  private def reconcileSwitchedConsolidatingDisklessPartition(partition: Partition): ConsolidationStartState = {
    val tp = partition.topicPartition
    inklessMetadataView.getClassicToDisklessStartOffset(tp) match {
      case PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET =>
        // Born-diskless: no destructive reconciliation.
        ConsolidationStartState.Ready(initialFetchOffset(partition.localLogOrException))
      case PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING =>
        ConsolidationStartState.Retry(s"Skipping consolidation for $tp because classic-to-diskless migration is still pending")
      case classicToDisklessInitOffset if classicToDisklessInitOffset >= 0 =>
        val topicId = partition.topicId.getOrElse {
          inklessMetadataView.getTopicId(partition.topic)
        }
        val request = new GetLogInfoRequest(topicId, partition.topicPartition.partition)
        controlPlane.getLogInfo(Collections.singletonList(request)).asScala.headOption match {
          case Some(response) =>
            if (response.errors != Errors.NONE) {
              ConsolidationStartState.Retry(s"Cannot reconcile $tp, control plane returned: ${response.errors.message}")
            } else {
              val log = partition.localLogOrException
              val reconcileTarget = Math.max(Math.max(classicToDisklessInitOffset, response.logStartOffset), log.logStartOffset)
              if (log.logEndOffset < reconcileTarget) { // there is a gap in the log
                return ConsolidationStartState.Unsafe(s"Cannot safely reconcile $tp: local LEO ${log.logEndOffset} is below reconcile target $reconcileTarget")
              }
              if (log.logEndOffset > reconcileTarget) { // the local log end is greater than the start of the diskless log, prune it back
                logger.info(
                  s"Reconciling consolidating diskless partition $tp by truncating local log from LEO ${log.logEndOffset} to $reconcileTarget")
                partition.truncateTo(reconcileTarget, isFuture = false)
              }

              val preExistingRemote = log.highestOffsetInRemoteStorage
              if (preExistingRemote < reconcileTarget) {
                partition.setSafeConsolidatedDisklessPruneFloor(reconcileTarget)
                partition.maybeUpdateDisklessLogStartOffset(response.logStartOffset)
                ConsolidationStartState.Ready(reconcileTarget)
              } else {
                ConsolidationStartState.Unsafe(s"Consolidation pruning disabled for $tp: pre-existing highest remote offset $preExistingRemote is >= reconcile target $reconcileTarget")
              }
            }
          case None =>
            ConsolidationStartState.Retry(s"Skipping consolidation for $tp due to missing response from the control plane")
        }
      case unexpected =>
        ConsolidationStartState.Unsafe(s"Skipping consolidation for $tp due to unexpected classic-to-diskless start offset: $unexpected")
    }
  }
}
