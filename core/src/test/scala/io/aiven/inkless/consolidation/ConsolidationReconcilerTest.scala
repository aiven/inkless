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
import kafka.controller.StateChangeLogger
import kafka.server.metadata.InklessMetadataView
import kafka.server.{InitialFetchState, ReplicaManager, ReplicationQuotaManager}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito
import org.mockito.Mockito._

import java.util.Optional
import scala.collection.mutable

class ConsolidationReconcilerTest {

  private val topicPartition = new TopicPartition("reconcile-topic", 0)
  private val topicId = Uuid.randomUuid()

  private def newReconciler(
    metadataView: InklessMetadataView,
    fetcherManager: ConsolidationFetcherManager = mock(classOf[ConsolidationFetcherManager]),
    initialFetchOffset: UnifiedLog => Long = _.highWatermark,
    quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  ): ConsolidationReconciler = {
    new ConsolidationReconciler(
      mock(classOf[ReplicaManager]),
      new StateChangeLogger(0, inControllerContext = false, None),
      mock(classOf[ConsolidationMetrics]),
      metadataView,
      initialFetchOffset,
      fetcherManager,
      quotaManager
    )
  }

  private def mockMetadataView(classicToDisklessStartOffset: Long): InklessMetadataView = {
    val view = mock(classOf[InklessMetadataView])
    when(view.isConsolidatingDisklessTopic(topicPartition.topic)).thenReturn(true)
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(classicToDisklessStartOffset)
    when(view.getTopicId(topicPartition.topic)).thenReturn(topicId)
    view
  }

  private def mockPartition(
    logStartOffset: Long,
    logEndOffset: Long,
    highWatermark: Long = 0L,
    highestRemoteOffset: Long = -1L
  ): (Partition, UnifiedLog) = {
    val log = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(logStartOffset)
    when(log.logEndOffset).thenReturn(logEndOffset)
    when(log.highWatermark).thenReturn(highWatermark)
    when(log.highestOffsetInRemoteStorage).thenReturn(highestRemoteOffset)
    when(log.topicId).thenReturn(Optional.of(topicId))

    val partition = mock(classOf[Partition])
    when(partition.topicPartition).thenReturn(topicPartition)
    when(partition.topic).thenReturn(topicPartition.topic)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.localLogOrException).thenReturn(log)
    when(partition.getLeaderEpoch).thenReturn(7)
    (partition, log)
  }

  private def initFetchState(
    reconciler: ConsolidationReconciler,
    partition: Partition
  ): mutable.HashMap[TopicPartition, InitialFetchState] = {
    reconciler.initConsolidatingPartitionFetching(mutable.HashMap(topicPartition -> partition))
  }

  @Test
  def testFirstSwitchStartsConsolidationAtSeal(): Unit = {
    // Just switched: local log is the frozen classic prefix [logStart, seal), LEO == seal, and
    // nothing has been consolidated yet. Start consolidating at the seal and gate pruning there.
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 100L)
    val reconciler = newReconciler(view)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(100L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition).ensureConsolidationPruneFloorAtLeast(100L)
  }

  @Test
  def testResumeAfterFailoverStartsFromLocalLeo(): Unit = {
    // Restart or leadership failover from a former follower: the local log already holds
    // consolidated data past the seal and earlier segments were tiered then deleted, so
    // logStartOffset has advanced past the seal. Resume from the current LEO and base the prune
    // floor on the current log start offset.
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val fetcherManager = mock(classOf[ConsolidationFetcherManager])
    val (partition, _) = mockPartition(logStartOffset = 150L, logEndOffset = 200L)
    val reconciler = newReconciler(view, fetcherManager)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(200L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition).ensureConsolidationPruneFloorAtLeast(150L)
    verify(fetcherManager, never()).addFailedPartition(topicPartition)
  }

  @Test
  def testResumeConsolidatedPastSealButNotYetTieredKeepsSealFloor(): Unit = {
    // Consolidated locally past the seal but nothing tiered yet, so logStartOffset is still the
    // classic prefix start. Resume from the current LEO but keep the prune floor at the seal so
    // the diskless region is not pruned before consolidation has tiered past the boundary.
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 200L)
    val reconciler = newReconciler(view)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(200L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition).ensureConsolidationPruneFloorAtLeast(100L)
  }

  @Test
  def testResumeAfterReassignmentStartsFromRehydratedLeo(): Unit = {
    // Reassignment to a fresh broker: the local log was rehydrated from tiered storage up to the
    // remote frontier (logStartOffset is the remote prefix start, LEO == highestRemote + 1) and
    // consolidation resumes from there to fill the still-diskless tail.
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val (partition, _) = mockPartition(logStartOffset = 150L, logEndOffset = 181L)
    val reconciler = newReconciler(view)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(181L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition).ensureConsolidationPruneFloorAtLeast(150L)
  }

  @Test
  def testBornDisklessPartitionUsesInitialFetchOffsetWithoutTruncation(): Unit = {
    val view = mockMetadataView(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 50L, highWatermark = 42L)
    val reconciler = newReconciler(view, initialFetchOffset = _ => 42L)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(42L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition, never()).ensureConsolidationPruneFloorAtLeast(anyLong())
  }

  @Test
  def testBornConsolidatedPartitionDoesNotValidateSwitchPruneFloor(): Unit = {
    val view = mockMetadataView(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val (partition, _) = mockPartition(logStartOffset = 100L, logEndOffset = 200L, highWatermark = 150L, highestRemoteOffset = 200L)
    val reconciler = newReconciler(view, initialFetchOffset = _ => 150L)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(150L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition, never()).ensureConsolidationPruneFloorAtLeast(anyLong())
  }

  @Test
  def testSwitchPendingPartitionIsRetriedWithoutMarkingFailed(): Unit = {
    val view = mockMetadataView(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    val fetcherManager = mock(classOf[ConsolidationFetcherManager])
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 50L)
    val reconciler = newReconciler(view, fetcherManager)

    val fetchStates = initFetchState(reconciler, partition)

    assertTrue(fetchStates.isEmpty)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(fetcherManager, never()).addFailedPartition(topicPartition)
  }

  @Test
  def testMigratedPartitionBelowClassicToDisklessStartOffsetIsRetriedWithoutMarkingFailed(): Unit = {
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val fetcherManager = mock(classOf[ConsolidationFetcherManager])
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 90L)
    val reconciler = newReconciler(view, fetcherManager)

    val fetchStates = initFetchState(reconciler, partition)

    assertTrue(fetchStates.isEmpty)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(fetcherManager, never()).addFailedPartition(topicPartition)
  }

  @Test
  def testStartConsolidationFetchersMarksThrottledBeforeStartingFetchers(): Unit = {
    // The fetcher only records bytes to the quota when the partition is already throttled, and
    // addFetcherForPartitions starts the threads immediately. Marking must therefore happen first,
    // otherwise the first fetch/append bypasses the dedicated bandwidth quota.
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val fetcherManager = mock(classOf[ConsolidationFetcherManager])
    val quotaManager = mock(classOf[ReplicationQuotaManager])
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 100L)
    val reconciler = newReconciler(view, fetcherManager, quotaManager = quotaManager)

    reconciler.startConsolidationFetchers(mutable.HashMap(topicPartition -> partition))

    val inOrder = Mockito.inOrder(quotaManager, fetcherManager)
    inOrder.verify(quotaManager).markThrottled(topicPartition.topic)
    inOrder.verify(fetcherManager).addFetcherForPartitions(any())
  }

}
