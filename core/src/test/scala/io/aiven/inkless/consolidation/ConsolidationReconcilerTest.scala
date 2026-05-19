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

import io.aiven.inkless.control_plane.{ControlPlane, GetLogInfoResponse}
import kafka.cluster.Partition
import kafka.server.metadata.InklessMetadataView
import kafka.server.{InitialFetchState, KafkaConfig, ReplicaManager}
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito._

import java.util
import java.util.Optional
import scala.collection.mutable

class ConsolidationReconcilerTest {

  private val topicPartition = new TopicPartition("reconcile-topic", 0)
  private val topicId = Uuid.randomUuid()

  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  private def newConfig(): KafkaConfig =
    KafkaConfig.fromProps(TestUtils.createBrokerConfig(nodeId = 1))

  private def newReconciler(
    metadataView: InklessMetadataView,
    controlPlane: ControlPlane,
    fetcherManager: ConsolidationFetcherManager = mock(classOf[ConsolidationFetcherManager]),
    initialFetchOffset: UnifiedLog => Long = _.highWatermark
  ): ConsolidationReconciler = {
    new ConsolidationReconciler(
      newConfig(),
      mock(classOf[ReplicaManager]),
      metadataView,
      controlPlane,
      initialFetchOffset,
      fetcherManager
    )
  }

  private def mockMetadataView(classicToDisklessStartOffset: Long): InklessMetadataView = {
    val view = mock(classOf[InklessMetadataView])
    when(view.isConsolidatingDisklessTopic(topicPartition.topic)).thenReturn(true)
    when(view.getClassicToDisklessStartOffset(topicPartition)).thenReturn(classicToDisklessStartOffset)
    when(view.getTopicId(topicPartition.topic)).thenReturn(topicId)
    view
  }

  private def mockControlPlane(response: GetLogInfoResponse): ControlPlane = {
    val controlPlane = mock(classOf[ControlPlane])
    when(controlPlane.getLogInfo(any())).thenReturn(util.List.of(response))
    controlPlane
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
  def testMigratedPartitionTruncatesToMaxBoundaryBeforeFetching(): Unit = {
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val controlPlane = mockControlPlane(GetLogInfoResponse.success(120L, 300L, 100L, 1000L))
    val (partition, _) = mockPartition(logStartOffset = 130L, logEndOffset = 200L)
    val reconciler = newReconciler(view, controlPlane)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(130L, fetchStates(topicPartition).initOffset)
    verify(partition).truncateTo(130L, false)
    verify(partition).setSafeConsolidatedDisklessPruneFloor(130L)
    verify(partition).maybeUpdateDisklessLogStartOffset(120L)
  }

  @Test
  def testMigratedPartitionAtTargetStartsFetchingWithoutTruncating(): Unit = {
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val controlPlane = mockControlPlane(GetLogInfoResponse.success(100L, 300L, 100L, 1000L))
    val (partition, _) = mockPartition(logStartOffset = 100L, logEndOffset = 100L)
    val reconciler = newReconciler(view, controlPlane)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(100L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition).setSafeConsolidatedDisklessPruneFloor(100L)
    verify(partition).maybeUpdateDisklessLogStartOffset(100L)
  }

  @Test
  def testMigratedPartitionBelowTargetIsMarkedFailed(): Unit = {
    val view = mockMetadataView(classicToDisklessStartOffset = 100L)
    val controlPlane = mockControlPlane(GetLogInfoResponse.success(120L, 300L, 100L, 1000L))
    val fetcherManager = mock(classOf[ConsolidationFetcherManager])
    val (partition, _) = mockPartition(logStartOffset = 130L, logEndOffset = 125L)
    val reconciler = newReconciler(view, controlPlane, fetcherManager)

    val fetchStates = initFetchState(reconciler, partition)

    assertTrue(fetchStates.isEmpty)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(fetcherManager).addFailedPartition(topicPartition)
  }

  @Test
  def testBornDisklessPartitionUsesInitialFetchOffsetWithoutTruncation(): Unit = {
    val view = mockMetadataView(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val controlPlane = mockControlPlane(GetLogInfoResponse.success(0L, 300L, 0L, 1000L))
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 50L, highWatermark = 42L)
    val reconciler = newReconciler(view, controlPlane, initialFetchOffset = _ => 42L)

    val fetchStates = initFetchState(reconciler, partition)

    assertEquals(42L, fetchStates(topicPartition).initOffset)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(partition, never()).setSafeConsolidatedDisklessPruneFloor(anyLong())
    verify(controlPlane, never()).getLogInfo(any())
  }

  @Test
  def testSwitchPendingPartitionIsRetried(): Unit = {
    val view = mockMetadataView(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    val controlPlane = mockControlPlane(GetLogInfoResponse.success(0L, 300L, 0L, 1000L))
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 50L)
    val reconciler = newReconciler(view, controlPlane)

    val fetchStates = initFetchState(reconciler, partition)

    assertTrue(fetchStates.isEmpty)
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())
    verify(controlPlane, never()).getLogInfo(any())
  }

  @Test
  def testRunStartsFetcherWhenRetriablePartitionBecomesReady(): Unit = {
    val view = mockMetadataView(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    when(view.getClassicToDisklessStartOffset(topicPartition))
      .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val controlPlane = mockControlPlane(GetLogInfoResponse.success(0L, 300L, 0L, 1000L))
    val fetcherManager = mock(classOf[ConsolidationFetcherManager])
    val (partition, _) = mockPartition(logStartOffset = 0L, logEndOffset = 50L)
    val reconciler = newReconciler(view, controlPlane, fetcherManager, initialFetchOffset = _ => 42L)

    assertTrue(initFetchState(reconciler, partition).isEmpty)
    reconciler.run()

    verify(fetcherManager).addFetcherForPartitions(any())
  }
}
