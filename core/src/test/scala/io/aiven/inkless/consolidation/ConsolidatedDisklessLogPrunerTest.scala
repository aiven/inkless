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

import io.aiven.inkless.control_plane.{ControlPlane, ControlPlaneUnavailableException, PruneDisklessLogsError, PruneDisklessLogsResponse}
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest
import io.aiven.inkless.delete.ConsolidatedDisklessLogPrunerMetrics
import kafka.cluster.Partition
import kafka.server.ReplicaManager
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito._

import java.util
import scala.util.{Left, Right}

class ConsolidatedDisklessLogPrunerTest {

  private val topicPartition = new TopicPartition("pruner-topic", 3)
  private val topicId = Uuid.randomUuid()
  private val tip = new TopicIdPartition(topicId, topicPartition)
  private val secondTopicPartition = new TopicPartition("second-pruner-topic", 4)
  private val secondTopicId = Uuid.randomUuid()
  private val secondTip = new TopicIdPartition(secondTopicId, secondTopicPartition)

  private val defaultMaxBatchesPerRequest = 2000

  private def newPruner(
    replicaManager: ReplicaManager,
    metadataView: InklessMetadataView,
    controlPlane: ControlPlane,
    maxBatchesPerRequest: Int = defaultMaxBatchesPerRequest,
    metrics: ConsolidatedDisklessLogPrunerMetrics = mock(classOf[ConsolidatedDisklessLogPrunerMetrics])
  ): ConsolidatedDisklessLogPruner =
    new ConsolidatedDisklessLogPruner(
      replicaManager,
      metadataView,
      controlPlane,
      maxBatchesPerRequest,
      metrics
    )

  private def readyPartition(
    highestRemote: Long,
    safePruneOffset: Option[Long] = None
  ): Partition = readyPartitionFor(topicPartition, topicId, highestRemote, safePruneOffset)

  private def readyPartitionFor(
    partitionTopic: TopicPartition,
    partitionTopicId: Uuid,
    highestRemote: Long,
    safePruneOffset: Option[Long]
  ): Partition = {
    val partition = mock(classOf[Partition])
    val unifiedLog = mock(classOf[UnifiedLog])
    when(partition.topicPartition).thenReturn(partitionTopic)
    when(partition.topicId).thenReturn(Some(partitionTopicId))
    when(partition.log).thenReturn(Some(unifiedLog))
    when(unifiedLog.highestOffsetInRemoteStorage()).thenReturn(highestRemote)
    if (highestRemote >= 0) {
      when(partition.getSafeConsolidatedDisklessPruneOffset(highestRemote)).thenReturn(safePruneOffset)
    }
    partition
  }

  @Test
  def testRunBuildsPruneRequestWhenTopicIdLogAndRemoteOffsetPresent(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    val partition = readyPartition(50L, Some(50L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    var captured: util.List[PruneDisklessLogsRequest] = null
    when(cp.pruneDisklessLogs(any(), anyInt())).thenAnswer(invocation => {
      captured = invocation.getArgument(0)
      util.Collections.emptyList()
    })

    newPruner(rm, view, cp).run()

    assertNotNull(captured)
    assertEquals(1, captured.size())
    assertEquals(50L, captured.get(0).highestRemoteOffset())
    assertEquals(topicId, captured.get(0).topicIdPartition.topicId)
    assertEquals(topicPartition.partition, captured.get(0).topicIdPartition.partition)
  }

  @Test
  def testRunPassesConstructedCapToControlPlane(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    val partition = readyPartition(50L, Some(50L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    var capturedCap = -1
    when(cp.pruneDisklessLogs(any(), anyInt())).thenAnswer(invocation => {
      capturedCap = invocation.getArgument[Int](1)
      util.Collections.emptyList()
    })

    newPruner(rm, view, cp, maxBatchesPerRequest = 37).run()

    assertEquals(37, capturedCap)
  }

  @Test
  def testRunAccumulatesMetricsAcrossPartitionsAndSuccessfulRuns(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])
    val metrics = mock(classOf[ConsolidatedDisklessLogPrunerMetrics])
    var recordedTotals = Seq.empty[(Long, Long)]

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip, secondTip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    when(view.getClassicToDisklessStartOffset(secondTopicPartition)).thenReturn(100L)
    val partition = readyPartition(50L, Some(50L))
    val secondPartition = readyPartitionFor(secondTopicPartition, secondTopicId, 50L, Some(50L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))
    when(rm.getPartitionOrError(secondTopicPartition)).thenReturn(Right(secondPartition))

    when(cp.pruneDisklessLogs(any(), anyInt())).thenReturn(
      util.List.of(
        new PruneDisklessLogsResponse(tip, 88L, 2, 30L, PruneDisklessLogsError.NONE),
        new PruneDisklessLogsResponse(secondTip, 89L, 3, 40L, PruneDisklessLogsError.NONE)
      )
    )
    doAnswer(invocation => {
      recordedTotals :+= (
        invocation.getArgument[Long](1),
        invocation.getArgument[Long](2)
      )
      null
    }).when(metrics).recordConsolidationPruneFinishedSuccessfully(anyLong(), anyLong(), anyLong())

    val pruner = newPruner(rm, view, cp, metrics = metrics)
    pruner.run()
    pruner.run()

    verify(metrics, times(2)).recordConsolidationPruneFinishedSuccessfully(anyLong(), anyLong(), anyLong())
    verify(metrics, never()).recordConsolidationPruneFinishedWithError()
    assertEquals(Seq((5L, 70L), (5L, 70L)), recordedTotals)
  }

  @Test
  def testRunRecordsZeroMetricsWhenNoPartitionsAreEligible(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])
    val metrics = mock(classOf[ConsolidatedDisklessLogPrunerMetrics])
    var recordedMetrics: Option[(Long, Long, Long)] = None

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Collections.emptySet())
    doAnswer(invocation => {
      recordedMetrics = Some((
        invocation.getArgument[Long](0),
        invocation.getArgument[Long](1),
        invocation.getArgument[Long](2)
      ))
      null
    }).when(metrics).recordConsolidationPruneFinishedSuccessfully(anyLong(), anyLong(), anyLong())

    newPruner(rm, view, cp, metrics = metrics).run()

    assertTrue(recordedMetrics.exists { case (duration, batches, bytes) =>
      duration >= 0 && batches == 0L && bytes == 0L
    })
    verify(metrics, never()).recordConsolidationPruneFinishedWithError()
  }

  @Test
  def testRunRecordsNoMetricsWhenACompletedPartitionIsFollowedByAnError(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])
    val metrics = mock(classOf[ConsolidatedDisklessLogPrunerMetrics])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip, secondTip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    when(view.getClassicToDisklessStartOffset(secondTopicPartition)).thenReturn(100L)
    val partition = readyPartition(50L, Some(50L))
    val secondPartition = readyPartitionFor(secondTopicPartition, secondTopicId, 50L, Some(50L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))
    when(rm.getPartitionOrError(secondTopicPartition)).thenReturn(Right(secondPartition))
    doThrow(new RuntimeException("partition update failed"))
      .when(secondPartition)
      .maybeAdvanceConsolidationPruneFloor(89L)
    when(cp.pruneDisklessLogs(any(), anyInt())).thenReturn(
      util.List.of(
        new PruneDisklessLogsResponse(tip, 88L, 2, 30L, PruneDisklessLogsError.NONE),
        new PruneDisklessLogsResponse(secondTip, 89L, 3, 40L, PruneDisklessLogsError.NONE)
      )
    )

    assertThrows(classOf[RuntimeException], () => newPruner(rm, view, cp, metrics = metrics).run())

    verify(partition).maybeAdvanceConsolidationPruneFloor(88L)
    verify(metrics, never()).recordConsolidationPruneFinishedSuccessfully(anyLong(), anyLong(), anyLong())
    verify(metrics).recordConsolidationPruneFinishedWithError()
  }

  @Test
  def testRunAdvancesNoFloorAndRecordsNoMetricsWhenControlPlaneThrows(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])
    val metrics = mock(classOf[ConsolidatedDisklessLogPrunerMetrics])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip, secondTip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    when(view.getClassicToDisklessStartOffset(secondTopicPartition)).thenReturn(100L)
    val partition = readyPartition(50L, Some(50L))
    val secondPartition = readyPartitionFor(secondTopicPartition, secondTopicId, 50L, Some(50L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))
    when(rm.getPartitionOrError(secondTopicPartition)).thenReturn(Right(secondPartition))
    when(cp.pruneDisklessLogs(any(), anyInt())).thenThrow(new RuntimeException("second partition failed"))

    assertThrows(classOf[RuntimeException], () => newPruner(rm, view, cp, metrics = metrics).run())

    verify(partition, never()).maybeAdvanceConsolidationPruneFloor(anyLong())
    verify(secondPartition, never()).maybeAdvanceConsolidationPruneFloor(anyLong())
    verify(metrics, never()).recordConsolidationPruneFinishedSuccessfully(anyLong(), anyLong(), anyLong())
    verify(metrics).recordConsolidationPruneFinishedWithError()
  }

  @Test
  def testRunBuildsPruneRequestForBornConsolidatedPartitionWithoutSafeFloor(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    when(view.getClassicToDisklessStartOffset(topicPartition))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val partition = readyPartition(50L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    var captured: util.List[PruneDisklessLogsRequest] = null
    when(cp.pruneDisklessLogs(any(), anyInt())).thenAnswer(invocation => {
      captured = invocation.getArgument(0)
      util.Collections.emptyList()
    })

    newPruner(rm, view, cp).run()

    assertNotNull(captured)
    assertEquals(1, captured.size())
    assertEquals(50L, captured.get(0).highestRemoteOffset())
    verify(partition, never()).getSafeConsolidatedDisklessPruneOffset(anyLong())
  }

  @Test
  def testRunOmitsSwitchedPartitionWhenSafeFloorNotReached(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    val partition = readyPartition(50L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    newPruner(rm, view, cp).run()

    verify(partition).getSafeConsolidatedDisklessPruneOffset(50L)
    verify(cp, never()).pruneDisklessLogs(any(), anyInt())
  }

  @Test
  def testRunBootstrapsSwitchedPartitionWhenRemotePrefixEndsBeforeSeal(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    val partition = readyPartition(99L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    var captured: util.List[PruneDisklessLogsRequest] = null
    when(cp.pruneDisklessLogs(any(), anyInt())).thenAnswer(invocation => {
      captured = invocation.getArgument(0)
      util.Collections.emptyList()
    })

    newPruner(rm, view, cp).run()

    assertNotNull(captured)
    assertEquals(1, captured.size())
    assertEquals(99L, captured.get(0).highestRemoteOffset())
  }

  @Test
  def testRunOmitsPruneWhenHighestOffsetInRemoteStorageNegative(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(-1L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    newPruner(rm, view, cp).run()

    verify(cp, never()).pruneDisklessLogs(any(), anyInt())
  }

  @Test
  def testRunOmitsPruneWhenClassicToDisklessSwitchIsPending(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    when(view.getClassicToDisklessStartOffset(topicPartition))
      .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

    newPruner(rm, view, cp).run()

    verify(rm, never()).getPartitionOrError(topicPartition)
    verify(cp, never()).pruneDisklessLogs(any(), anyInt())
  }

  @Test
  def testRunOmitsPruneWhenTopicIdMissing(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = mock(classOf[Partition])
    when(partition.topicId).thenReturn(None)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    newPruner(rm, view, cp).run()

    verify(cp, never()).pruneDisklessLogs(any(), anyInt())
  }

  @Test
  def testRunDoesNotSetDisklessStartWhenPruneReportsUnknownTopic(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L, Some(10L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any(), anyInt())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(
        responseTip, -1, -1, -1, PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION))
    )

    newPruner(rm, view, cp).run()

    verify(partition, never()).maybeAdvanceConsolidationPruneFloor(anyLong())
  }

  @Test
  def testRunUpdatesDisklessStartWhenTopicNameResolved(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L, Some(10L))
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any(), anyInt())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(responseTip, 88L, 0, 0, PruneDisklessLogsError.NONE))
    )

    newPruner(rm, view, cp).run()

    verify(partition).maybeAdvanceConsolidationPruneFloor(88L)
  }

  @Test
  def testRunDoesNotUpdatePartitionWhenGetPartitionFailsAfterPrune(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L, Some(10L))
    when(rm.getPartitionOrError(topicPartition))
      .thenReturn(Right(partition))
      .thenReturn(Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any(), anyInt())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(responseTip, 88L, 0, 0, PruneDisklessLogsError.NONE))
    )

    newPruner(rm, view, cp).run()

    verify(partition, never()).maybeAdvanceConsolidationPruneFloor(anyLong())
  }

  @Test
  def testRunPropagatesControlPlaneUnavailableException(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L, Some(10L))
    val metrics = mock(classOf[ConsolidatedDisklessLogPrunerMetrics])
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))
    when(cp.pruneDisklessLogs(any(), anyInt()))
      .thenThrow(new ControlPlaneUnavailableException("No diskless control plane is configured"))

    // The caller (ReplicaManager.runIfControlPlaneAvailable) is the one place that turns this into a quiet skip.
    assertThrows(classOf[ControlPlaneUnavailableException], () => newPruner(rm, view, cp, metrics = metrics).run())

    verify(metrics, never()).recordConsolidationPruneFinishedWithError()
    verify(metrics, never()).recordConsolidationPruneFinishedSuccessfully(anyLong(), anyLong(), anyLong())
  }
}
