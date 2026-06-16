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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.logger.StateChangeLogger
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

      // Mark topics throttled BEFORE starting fetchers: addFetcherForPartitions starts the
      // fetcher threads immediately, and ReplicaFetcherThread only records bytes to the quota
      // when the partition is already throttled. Marking after would let the first fetch/append
      // bypass the quota. Unlike classic replication (where throttled replicas are set via topic
      // config during reassignment), consolidation marks all topics unconditionally — every
      // consolidating partition's bytes must count toward the dedicated bandwidth quota.
      //
      // We never removeThrottle on stop: this follows the classic ReplicaFetcher pattern, where
      // the topic-keyed throttle map is only cleared via config changes (ConfigHandler), not on
      // per-partition fetcher removal. Entries are tiny (topic -> List(-1)) and bounded by the
      // set of topics that have ever consolidated on this broker, so the residue is benign.
      consolidatingPartitionAndOffsets.keys.map(_.topic).toSet.foreach((topic: String) => consolidationQuotaManager.markThrottled(topic))
      consolidationFetcherManager.addFetcherForPartitions(consolidatingPartitionAndOffsets)
      consolidatingPartitionAndOffsets.keys.foreach(tp => consolidationMetrics.registerPartition(tp))
    }
  }

  def startConsolidationFetchersForCaughtUpClassicPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    val consolidatingDisklessPartitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    topicPartitions.foreach { tp =>
      replicaManager.onlinePartition(tp).foreach { partition =>
        // This hook is the only trigger for a classic/untiered-diskless -> consolidated config flip:
        // remote.storage.enable=true arrives as a config-only metadata change that the leader-delta
        // path never re-enters, so if we skip the partition here it never starts consolidating.
        // The broker metadata cache backing isConsolidatingDisklessTopic can lag the config record
        // that has *already* updated this partition's local log -- the cache image and the dynamic
        // config publisher are applied by separate publishers within the same metadata delta -- so
        // gating solely on the cache can spuriously drop a partition that has, in fact, just become
        // consolidating. The partition's own log config was updated synchronously before this hook
        // ran, so trust it too: a diskless topic whose local log now has remote storage enabled is
        // consolidating.
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
            // Leader with a below-seal local log. A healthy leader is never below the seal: the
            // seal is taken from the leader's own LEO when the classic log finishes draining, and
            // consolidation only ever appends above it. The only way to observe this is local-log
            // loss on the elected leader -- a full local-storage wipe / disaster-recovery restart
            // where the controller (whose metadata survived) elects a replica that came back with
            // an empty log. Unlike a follower (which can still replicate the classic prefix from a
            // live leader, hence the Retry below), a leader has no peer source: the classic prefix
            // [0, seal) lives only in the remote tier. Start consolidation anyway, armed at the
            // current (post-loss) LEO, so the fetcher's first request lands below the diskless WAL
            // start and DisklessLeaderEndPoint answers OFFSET_MOVED_TO_TIERED_STORAGE. That drives
            // the stock tier-state machine to rebuild the leader-epoch cache + producer snapshot
            // and the whole-log start from remote before resuming the WAL fetch; without this the
            // partition would wait forever for a classic catch-up that can never happen.
            val pruneFloor = math.max(seal, log.logStartOffset)
            partition.ensureConsolidationPruneFloorAtLeast(pruneFloor)
            ConsolidationStartState.Ready(log.logEndOffset)
          } else {
            // Follower (or remote storage not yet enabled) whose classic prefix hasn't been fully
            // replicated locally yet; a classic catch-up fetcher must finish bringing the local log
            // up to the seal before consolidation can take over.
            ConsolidationStartState.Retry(
              s"Skipping consolidation for $tp because local LEO ${log.logEndOffset} is below " +
                s"classic-to-diskless start offset $seal")
          }
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
