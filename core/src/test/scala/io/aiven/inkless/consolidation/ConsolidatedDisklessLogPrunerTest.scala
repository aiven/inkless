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

import io.aiven.inkless.control_plane.{ControlPlane, PruneDisklessLogsError, PruneDisklessLogsResponse}
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest
import kafka.cluster.Partition
import kafka.server.ReplicaManager
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito._

import java.util
import java.util.Optional

import scala.util.{Left, Right}

class ConsolidatedDisklessLogPrunerTest {

  private val topicPartition = new TopicPartition("pruner-topic", 3)
  private val topicId = Uuid.randomUuid()
  private val tip = new TopicIdPartition(topicId, topicPartition)

  private def readyPartition(highestRemote: Long): Partition = {
    val partition = mock(classOf[Partition])
    val unifiedLog = mock(classOf[UnifiedLog])
    when(partition.topicPartition).thenReturn(topicPartition)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.log).thenReturn(Some(unifiedLog))
    when(unifiedLog.highestOffsetInRemoteStorage()).thenReturn(highestRemote)
    partition
  }

  @Test
  def testRunBuildsPruneRequestWhenTopicIdLogAndRemoteOffsetPresent(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(50L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    var captured: util.List[PruneDisklessLogsRequest] = null
    when(cp.pruneDisklessLogs(any())).thenAnswer(invocation => {
      captured = invocation.getArgument(0)
      util.Collections.emptyList()
    })

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    assertNotNull(captured)
    assertEquals(1, captured.size())
    assertEquals(50L, captured.get(0).highestRemoteOffset())
    assertEquals(topicId, captured.get(0).topicIdPartition.topicId)
    assertEquals(topicPartition.partition, captured.get(0).topicIdPartition.partition)
  }

  @Test
  def testRunOmitsPruneWhenHighestOffsetInRemoteStorageNegative(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(-1L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    verify(cp, never()).pruneDisklessLogs(any())
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

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    verify(cp, never()).pruneDisklessLogs(any())
  }

  @Test
  def testRunDoesNotSetDisklessStartWhenPruneReportsUnknownTopic(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(responseTip, null, PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION))
    )

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    verify(partition, never()).setDisklessStartOffset(anyLong())
    verify(view, never()).getTopicName(any(classOf[Uuid]))
  }

  @Test
  def testRunUpdatesDisklessStartWhenTopicNameResolved(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(responseTip, 88L, PruneDisklessLogsError.NONE))
    )
    when(view.getTopicName(topicId)).thenReturn(Optional.of(topicPartition.topic))

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    verify(partition).setDisklessStartOffset(88L)
  }

  @Test
  def testRunDoesNotSetDisklessStartWhenTopicNameMissing(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L)
    when(rm.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(responseTip, 88L, PruneDisklessLogsError.NONE))
    )
    when(view.getTopicName(topicId)).thenReturn(Optional.empty())

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    verify(partition, never()).setDisklessStartOffset(anyLong())
  }

  @Test
  def testRunDoesNotUpdatePartitionWhenGetPartitionFailsAfterPrune(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val view = mock(classOf[InklessMetadataView])
    val cp = mock(classOf[ControlPlane])

    when(view.getConsolidatingDisklessTopicPartitions).thenReturn(util.Set.of(tip))
    val partition = readyPartition(10L)
    when(rm.getPartitionOrError(topicPartition))
      .thenReturn(Right(partition))
      .thenReturn(Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

    val responseTip = new TopicIdPartition(topicId, topicPartition.partition, topicPartition.topic)
    when(cp.pruneDisklessLogs(any())).thenReturn(
      util.List.of(new PruneDisklessLogsResponse(responseTip, 88L, PruneDisklessLogsError.NONE))
    )
    when(view.getTopicName(topicId)).thenReturn(Optional.of(topicPartition.topic))

    new ConsolidatedDisklessLogPruner(rm, view, cp).run()

    verify(partition, never()).setDisklessStartOffset(anyLong())
  }
}
