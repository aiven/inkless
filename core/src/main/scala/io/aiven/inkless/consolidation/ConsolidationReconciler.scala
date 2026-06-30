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
import kafka.server.{InitialFetchState, ReplicaManager, ReplicationQuotaManager}
import kafka.server.metadata.InklessMetadataView
import kafka.controller.StateChangeLogger
import org.apache.kafka.common.TopicPartition
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
                              consolidationFetcherManager: ConsolidationFetcherManager,
                              consolidationQuotaManager: ReplicationQuotaManager) {

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

      // Mark topics throttled BEFORE starting fetchers: addFetcherForPartitions starts the threads
      // immediately and bytes only count toward the quota while the partition is already throttled,
      // so marking after would let the first fetch bypass the quota. All consolidating topics are
      // marked unconditionally. We never removeThrottle on stop (matching the classic ReplicaFetcher
      // pattern); the leftover topic -> List(-1) entries are tiny and bounded, so the residue is benign.
      consolidatingPartitionAndOffsets.keys.map(_.topic).toSet.foreach((topic: String) => consolidationQuotaManager.markThrottled(topic))
      consolidationFetcherManager.addFetcherForPartitions(consolidatingPartitionAndOffsets)
      consolidatingPartitionAndOffsets.keys.foreach(tp => consolidationMetrics.registerPartition(tp))
    }
  }

  def startConsolidationFetchersForCaughtUpClassicPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    val consolidatingDisklessPartitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    topicPartitions.foreach { tp =>
      replicaManager.onlinePartition(tp).foreach { partition =>
        // This hook is the only trigger for an untiered-diskless -> consolidated flip, since
        // remote.storage.enable=true is a config-only change the leader-delta path never re-enters.
        // The metadata cache behind isConsolidatingDisklessTopic can lag the config record that has
        // already enabled remote on the local log, so also trust the partition's own log: a diskless
        // topic whose local log has remote storage enabled is consolidating.
        val isConsolidating = inklessMetadataView.isConsolidatingDisklessTopic(tp.topic) ||
          (inklessMetadataView.isDisklessTopic(tp.topic) && partition.log.exists(_.remoteLogEnabled()))
        if (isConsolidating) {
          consolidatingDisklessPartitionsToStartFetching.put(tp, partition)
        }
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
          if (partition.isLeader && log.remoteLogEnabled()) {
            // A leader below the seal means its local log was lost (e.g. a wiped replica promoted
            // by the controller, whose metadata survived). Unlike a follower it has no peer to
            // replicate the classic prefix [0, seal) from -- that prefix lives only in remote.
            // Arm consolidation at the current LEO so the first fetch lands below the diskless WAL
            // start, DisklessLeaderEndPoint answers OFFSET_MOVED_TO_TIERED_STORAGE, and the
            // tier-state machine rebuilds the whole log from remote. Otherwise the partition would
            // wait forever for a classic catch-up that can never happen.
            stateChangeLogger.warn(s"Leader $tp is below the classic-to-diskless seal $seal at " +
              s"LEO ${log.logEndOffset}; assuming local-log loss and rebuilding from remote.")
            armConsolidationAtLeo(partition, log, seal)
          } else {
            // Follower (or remote not yet enabled) below the seal: a classic catch-up fetcher must
            // still bring the local log up to the seal before consolidation can take over.
            ConsolidationStartState.Retry(
              s"Skipping consolidation for $tp because local LEO ${log.logEndOffset} is below " +
                s"classic-to-diskless start offset $seal")
          }
        } else {
          // LEO >= seal: the initial switch (LEO == seal) or a resume after restart, failover, or
          // reassignment, where the local log kept or rehydrated its consolidated frontier. Resume
          // from the current LEO so we neither re-consolidate nor skip data already held locally.
          stateChangeLogger.info(s"Starting consolidation for $tp at LEO ${log.logEndOffset} " +
            s"(>= classic-to-diskless seal $seal)")
          armConsolidationAtLeo(partition, log, seal)
        }
      case unexpected =>
        ConsolidationStartState.Failed(new ReconciliationException(s"Skipping consolidation for $tp due to unexpected classic-to-diskless start offset: $unexpected"))
    }
  }

  private def armConsolidationAtLeo(partition: Partition, log: UnifiedLog, seal: Long): ConsolidationStartState = {
    partition.ensureConsolidationPruneFloorAtLeast(math.max(seal, log.logStartOffset))
    ConsolidationStartState.Ready(log.logEndOffset)
  }
}
