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

import kafka.cluster.Partition
import kafka.server.{InitialFetchState, ReplicaManager}
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.TopicPartition
import kafka.controller.StateChangeLogger
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.storage.internals.log.UnifiedLog

import scala.collection.{Set, mutable}
import scala.jdk.OptionConverters.RichOptional

/**
 * Outcome of per-partition reconciliation before consolidation can start.
 *  - Ready: the partition is safe to hand to the consolidation fetcher at the given offset.
 *  - Retry: the partition cannot consolidate yet (e.g., pending seal, LEO below seal) — skip
 *    this round; a later metadata delta or classic-fetcher catch-up will re-trigger.
 *  - Failed: an unrecoverable error; mark the partition as failed in the fetcher manager.
 */
private sealed trait ConsolidationStartState
private object ConsolidationStartState {
  final case class Ready(offset: Long) extends ConsolidationStartState
  final case class Retry(reason: String) extends ConsolidationStartState
  final case class Failed(reason: Throwable) extends ConsolidationStartState
}

private class ReconciliationException(message: String) extends RuntimeException(message)

class ConsolidationReconciler(replicaManager: ReplicaManager,
                              stateChangeLogger: StateChangeLogger,
                              consolidationMetrics: ConsolidationMetrics,
                              inklessMetadataView: InklessMetadataView,
                              initialFetchOffset: UnifiedLog => Long,
                              consolidationFetcherManager: ConsolidationFetcherManager) {

  /**
   * Starts the consolidation fetchers for the given partitions in the parameter. These partitions
   * must be ready for consolidation in order to be started (meaning LEO == seal offset).
   * If a partition wasn't ready for consolidation because of some error or the LEO for the given
   * partition was behind the seal offset, then it will be logged and won't be started.
   *
   * @param consolidatingPartitions the consolidating partitions to start fetching.
   */
  def startConsolidationFetchers(consolidatingPartitions: mutable.HashMap[TopicPartition, Partition]): Unit = {
    if (consolidatingPartitions.nonEmpty) {
      val consolidatingPartitionAndOffsets: mutable.HashMap[TopicPartition, InitialFetchState] =
        initConsolidatingPartitionFetching(consolidatingPartitions)

      consolidationFetcherManager.addFetcherForPartitions(consolidatingPartitionAndOffsets)
      consolidatingPartitionAndOffsets.keys.foreach(tp => consolidationMetrics.registerPartition(tp))
    }
  }

  def startConsolidationFetchersForCaughtUpClassicPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    val consolidatingDisklessPartitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    topicPartitions.foreach { tp =>
      if (inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)) {
        replicaManager.onlinePartition(tp).foreach(partition => consolidatingDisklessPartitionsToStartFetching.put(tp, partition))
      }
    }
    startConsolidationFetchers(consolidatingDisklessPartitionsToStartFetching)
  }

  def initConsolidatingPartitionFetching(consolidatingDisklessPartitionsToStartFetching: mutable.HashMap[TopicPartition, Partition]
                                        ): mutable.HashMap[TopicPartition, InitialFetchState] = {
    val consolidatingPartitionAndOffsets = new mutable.HashMap[TopicPartition, InitialFetchState]

    consolidatingDisklessPartitionsToStartFetching.foreachEntry { (topicPartition, partition) =>
      val log = partition.localLogOrException
      reconcileSwitchedConsolidatingDisklessPartition(partition) match {
        case ConsolidationStartState.Ready(offset) =>
          consolidatingPartitionAndOffsets.put(topicPartition, InitialFetchState(
            log.topicId.toScala,
            new BrokerEndPoint(-1, "diskless", -1),
            partition.getLeaderEpoch,
            offset
          ))
        case ConsolidationStartState.Retry(reason) =>
          stateChangeLogger.info(reason)
        case ConsolidationStartState.Failed(reason) =>
          stateChangeLogger.error("Error happened during consolidating log reconciliation before initial fetch from diskless control plane", reason)
          consolidationFetcherManager.addFailedPartition(topicPartition)
      }
    }
    consolidatingPartitionAndOffsets
  }

  private def reconcileSwitchedConsolidatingDisklessPartition(partition: Partition): ConsolidationStartState = {
    val tp = partition.topicPartition
    inklessMetadataView.getClassicToDisklessStartOffset(tp) match {
      case PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET =>
        // Born-diskless/born-consolidated topics don't have a classic seal boundary to reconcile.
        ConsolidationStartState.Ready(initialFetchOffset(partition.localLogOrException))
      case PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING =>
        ConsolidationStartState.Retry(s"Skipping consolidation for $tp because classic-to-diskless migration is still pending")
      case seal if seal >= 0 =>
        val log = partition.localLogOrException
        if (log.logEndOffset < seal) {
          // The classic prefix hasn't been fully replicated locally yet; a classic catch-up
          // fetcher must finish bringing the local log up to the seal before consolidation
          // can take over.
          ConsolidationStartState.Retry(
            s"Skipping consolidation for $tp because local LEO ${log.logEndOffset} is below " +
              s"classic-to-diskless start offset $seal")
        } else {
          // LEO >= seal. This covers both the initial switch (LEO == seal, nothing consolidated
          // yet) and resuming an already-progressed partition after a restart, leadership
          // failover, or reassignment (the local log either kept its consolidated frontier or
          // was rehydrated from tiered storage). In every case we resume from the current local
          // LEO so we never re-consolidate or skip data the local log already holds.
          //
          // The prune floor is the higher of the seal and the current log start offset:
          //  - at first switch logStartOffset is still the classic prefix start, so the floor is
          //    the seal, which blocks pruning the diskless region until consolidation has tiered
          //    past the boundary;
          //  - on resume logStartOffset has advanced past the seal as consolidated segments were
          //    tiered and deleted, so it reflects real pruning progress.
          val pruneFloor = math.max(seal, log.logStartOffset)
          partition.ensureConsolidationPruneFloorAtLeast(pruneFloor)
          ConsolidationStartState.Ready(log.logEndOffset)
        }
      case unexpected =>
        ConsolidationStartState.Failed(new ReconciliationException(s"Skipping consolidation for $tp due to unexpected classic-to-diskless start offset: $unexpected"))
    }
  }
}
