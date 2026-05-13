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
import kafka.server._
import kafka.server.metadata.InklessMetadataView
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.storage.internals.log.{LogAppendInfo, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, when}

import java.nio.charset.StandardCharsets
import java.util.Optional
import scala.jdk.CollectionConverters._

class ConsolidationFetcherThreadTest {

  private val topicPartition = new TopicPartition("test-topic", 0)
  private val failedPartitions = new FailedPartitions
  private var metrics: ConsolidationMetrics = _

  @BeforeEach
  def setUp(): Unit = {
    metrics = new ConsolidationMetrics()
  }

  @AfterEach
  def tearDown(): Unit = {
    metrics.close()
    TestUtils.clearYammerMetrics()
  }

  private def createConsolidationFetcherThread(
    replicaManager: ReplicaManager,
    consolidationMetrics: Option[ConsolidationMetrics]
  ): ConsolidationFetcherThread = {
    val props = TestUtils.createBrokerConfig(nodeId=1)
    val config = KafkaConfig.fromProps(props)
    val leader = mock(classOf[LeaderEndPoint])
    when(leader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
    new ConsolidationFetcherThread(
      "consolidation-fetcher-test",
      leader,
      config,
      failedPartitions,
      replicaManager,
      QuotaFactory.UNBOUNDED_QUOTA,
      "[ConsolidationFetcherTest] ",
      consolidationMetrics
    )
  }

  private def mockReplicaManager(
    partition: Partition,
    inklessMetadataView: InklessMetadataView = null
  ): ReplicaManager = {
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getPartitionOrException(any[TopicPartition])).thenReturn(partition)
    when(replicaManager.brokerTopicStats).thenReturn(new BrokerTopicStats)
    val view = if (inklessMetadataView != null) inklessMetadataView else {
      val v = mock(classOf[InklessMetadataView])
      when(v.getClassicToDisklessStartOffset(any[TopicPartition]))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      v
    }
    when(replicaManager.inklessMetadataView()).thenReturn(view)
    when(replicaManager.replicaFetcherManager).thenReturn(mock(classOf[ReplicaFetcherManager]))
    replicaManager
  }

  private def mockPartitionWithLog(
    logEndOffset: Long,
    highestOffsetInRemoteStorage: Long,
    localLogStartOffset: Long = 0L
  ): Partition = {
    val log = mock(classOf[UnifiedLog])
    when(log.logEndOffset).thenReturn(logEndOffset)
    when(log.highestOffsetInRemoteStorage()).thenReturn(highestOffsetInRemoteStorage)
    when(log.localLogStartOffset()).thenReturn(localLogStartOffset)
    when(log.maybeUpdateHighWatermark(anyLong())).thenReturn(Optional.empty)

    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.appendRecordsToFollowerOrFutureReplica(any[MemoryRecords], any[Boolean], any[Int]))
      .thenReturn(Some(mock(classOf[LogAppendInfo])))
    partition
  }

  private def buildPartitionData(highWatermark: Long): FetchResponseData.PartitionData = {
    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    new FetchResponseData.PartitionData()
      .setPartitionIndex(topicPartition.partition)
      .setRecords(records)
      .setHighWatermark(highWatermark)
      .setLogStartOffset(0)
  }

  @Test
  def testMetricsUpdatedOnProcessPartitionData(): Unit = {
    val disklessLEO = 100L
    val localLEO = 80L
    val remoteOffset = 60L
    val localLogStart = 10L

    val partition = mockPartitionWithLog(localLEO, remoteOffset, localLogStart)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics))

    metrics.registerPartition(topicPartition)

    thread.processPartitionData(topicPartition, localLEO, Int.MaxValue, buildPartitionData(disklessLEO))

    assertEquals(40L, findGaugeValue("inkless.remote.consolidation.lag", topicPartition))
    assertEquals(20L, findGaugeValue("inkless.remote.consolidation.local.lag", topicPartition))
    assertEquals(50L, findGaugeValue("inkless.remote.consolidation.deletable.messages", topicPartition))
  }

  @Test
  def testRemoteLagSkippedWhenRemoteStorageNotActive(): Unit = {
    val disklessLEO = 100L
    val localLEO = 80L
    val remoteOffset = -1L

    val partition = mockPartitionWithLog(localLEO, remoteOffset)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics))

    metrics.registerPartition(topicPartition)

    thread.processPartitionData(topicPartition, localLEO, Int.MaxValue, buildPartitionData(disklessLEO))

    assertEquals(20L, findGaugeValue("inkless.remote.consolidation.local.lag", topicPartition))
    assertEquals(0L, findGaugeValue("inkless.remote.consolidation.lag", topicPartition))
    assertEquals(0L, findGaugeValue("inkless.remote.consolidation.deletable.messages", topicPartition))
  }

  @Test
  def testLagMetricsClampedToZeroWhenLocalAheadOfDiskless(): Unit = {
    val disklessLEO = 50L
    val localLEO = 60L
    val remoteOffset = 70L
    val localLogStart = 0L

    val partition = mockPartitionWithLog(localLEO, remoteOffset, localLogStart)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics))

    metrics.registerPartition(topicPartition)

    thread.processPartitionData(topicPartition, localLEO, Int.MaxValue, buildPartitionData(disklessLEO))

    assertEquals(0L, findGaugeValue("inkless.remote.consolidation.lag", topicPartition))
    assertEquals(0L, findGaugeValue("inkless.remote.consolidation.local.lag", topicPartition))
    assertEquals(70L, findGaugeValue("inkless.remote.consolidation.deletable.messages", topicPartition))
  }

  @Test
  def testNoMetricsRegisteredWhenConsolidationMetricsIsNone(): Unit = {
    val partition = mockPartitionWithLog(80L, 60L)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    thread.processPartitionData(topicPartition, 80L, Int.MaxValue, buildPartitionData(100L))

    assertNull(findGaugeOrNull("inkless.remote.consolidation.lag", topicPartition))
    assertNull(findGaugeOrNull("inkless.remote.consolidation.local.lag", topicPartition))
    assertNull(findGaugeOrNull("inkless.remote.consolidation.deletable.messages", topicPartition))
  }

  private def findGaugeOrNull(name: String, tp: TopicPartition): com.yammer.metrics.core.Gauge[_] = {
    val expectedScope = s"partition.${tp.partition}.topic.${tp.topic.replace(".", "_")}"
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (metricName, _) =>
        metricName.getName == name && metricName.getScope == expectedScope
      }
      .map(_._2.asInstanceOf[com.yammer.metrics.core.Gauge[_]])
      .orNull
  }

  private def findGaugeValue(name: String, tp: TopicPartition): Long = {
    val gauge = findGaugeOrNull(name, tp)
    assertNotNull(gauge, s"Gauge $name not found for $tp")
    gauge.asInstanceOf[com.yammer.metrics.core.Gauge[Long]].value()
  }
}
