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

import io.aiven.inkless.consume.{ConcatenatedRecords, FetchHandler, FetchOffsetHandler}
import kafka.cluster.Partition
import kafka.server._
import kafka.server.metadata.InklessMetadataView
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.RecordBatchTooLargeException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.{BaseRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.storage.internals.log.{LogAppendInfo, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{RETURNS_DEEP_STUBS, doNothing, mock, verify, when}

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

  // Partition whose follower append rejects the block as larger than segment.bytes, as UnifiedLog does
  // when a single batch exceeds segment.bytes. logEndOffset matches the fetch offset so the append is
  // reached (ReplicaFetcherThread.processPartitionData asserts offset == logEndOffset first).
  private def mockPartitionAppendTooLarge(logEndOffset: Long, segmentSize: Int): Partition = {
    val config = mock(classOf[org.apache.kafka.storage.internals.log.LogConfig])
    when(config.segmentSize()).thenReturn(segmentSize)
    val log = mock(classOf[UnifiedLog])
    when(log.logEndOffset).thenReturn(logEndOffset)
    when(log.config()).thenReturn(config)

    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.appendRecordsToFollowerOrFutureReplica(any[MemoryRecords], any[Boolean], any[Int]))
      .thenThrow(new RecordBatchTooLargeException("batch size 3000 exceeds segment size 2000"))
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

  private def recordsWithEpoch(baseOffset: Long, epoch: Int, value: String): MemoryRecords =
    MemoryRecords.withRecords(baseOffset, Compression.NONE, epoch,
      new SimpleRecord(1000, value.getBytes(StandardCharsets.UTF_8)))

  private def partitionDataWith(records: BaseRecords): FetchResponseData.PartitionData =
    new FetchResponseData.PartitionData()
      .setPartitionIndex(topicPartition.partition)
      .setRecords(records)
      .setHighWatermark(10L)
      .setLogStartOffset(0)

  // Capture the records actually appended to the local log and return each batch's leader epoch.
  private def appendedBatchEpochs(partition: Partition): List[Int] = {
    val captor = ArgumentCaptor.forClass(classOf[MemoryRecords])
    verify(partition).appendRecordsToFollowerOrFutureReplica(captor.capture(), any[Boolean], any[Int])
    captor.getValue.batches().asScala.map(_.partitionLeaderEpoch).toList
  }

  @Test
  def testStampsDisklessLeaderEpochOnConcatenatedRecords(): Unit = {
    val disklessLeaderEpoch = 7
    val partition = mockPartitionWithLog(logEndOffset = 0L, highestOffsetInRemoteStorage = -1L)
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.disklessLeaderEpoch(any[TopicPartition])).thenReturn(disklessLeaderEpoch)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    val records = new ConcatenatedRecords(java.util.List.of(
      recordsWithEpoch(0L, 0, "a"),
      recordsWithEpoch(1L, 0, "b")
    ))

    thread.processPartitionData(topicPartition, 0L, Int.MaxValue, partitionDataWith(records))

    assertEquals(List(disklessLeaderEpoch, disklessLeaderEpoch), appendedBatchEpochs(partition))
  }

  @Test
  def testStampsDisklessLeaderEpochOnMemoryRecords(): Unit = {
    val disklessLeaderEpoch = 7
    val partition = mockPartitionWithLog(logEndOffset = 0L, highestOffsetInRemoteStorage = -1L)
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.disklessLeaderEpoch(any[TopicPartition])).thenReturn(disklessLeaderEpoch)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    thread.processPartitionData(topicPartition, 0L, Int.MaxValue, partitionDataWith(recordsWithEpoch(0L, 0, "a")))

    assertEquals(List(disklessLeaderEpoch), appendedBatchEpochs(partition))
  }

  @Test
  def testBornDisklessLeavesEpochUntouched(): Unit = {
    val partition = mockPartitionWithLog(logEndOffset = 0L, highestOffsetInRemoteStorage = -1L)
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.disklessLeaderEpoch(any[TopicPartition]))
      .thenReturn(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    thread.processPartitionData(topicPartition, 0L, Int.MaxValue, partitionDataWith(recordsWithEpoch(0L, 0, "a")))

    // The guard short-circuits: had stamping run with E_d == -1 it would have written -1, not left 0.
    assertEquals(List(0), appendedBatchEpochs(partition))
  }

  /**
   * Scenario: a partition switched classic->diskless at seal offset 100 with a diskless leader epoch
   * E_d=5 captured at the switch. After a leader failover this follower still has a STALE classic tail past the seal:
   * its local log runs to LEO 130 and its latest local epoch is a classic epoch (2 < E_d). The
   * diskless region [100, ...) on the (new) diskless leader is authoritative, so the follower must
   * truncate its divergent tail back to the seal.
   */
  @Test
  def testFollowerTruncatesStaleClassicTailToSealAfterFailover(): Unit = {
    val seal = 100L
    val disklessLeaderEpoch = 5
    val staleClassicEpoch = 2
    val divergentLeo = 130L

    val log = mock(classOf[UnifiedLog])
    when(log.latestEpoch).thenReturn(Optional.of(Int.box(staleClassicEpoch)))
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(divergentLeo)
    when(log.endOffsetForEpoch(staleClassicEpoch))
      .thenReturn(Optional.of(new OffsetAndEpoch(divergentLeo, staleClassicEpoch)))

    val partition = mock(classOf[Partition])

    // Deep stubs so the truncation-complete callback (replicaAlterLogDirsManager.markPartitionsForTruncation,
    // a kafka.server-private member we cannot reference from here) is auto-stubbed to a no-op.
    val replicaManager = mock(classOf[ReplicaManager], RETURNS_DEEP_STUBS)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)
    when(replicaManager.getPartitionOrException(topicPartition)).thenReturn(partition)
    when(replicaManager.brokerTopicStats).thenReturn(new BrokerTopicStats)
    // Committed metadata for the switched partition: seal at 100, captured diskless epoch E_d=5.
    when(replicaManager.classicToDisklessStartOffset(topicPartition)).thenReturn(seal)
    when(replicaManager.disklessLeaderEpoch(topicPartition)).thenReturn(disklessLeaderEpoch)

    // Real diskless leader endpoint backing OffsetsForLeaderEpoch for the consolidating follower.
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val job = mock(classOf[FetchOffsetHandler.Job])
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    doNothing().when(job).start()

    val brokerEndPoint = new BrokerEndPoint(0, "localhost", 9092)
    // Quota "exceeded" so the post-truncation fetch round (buildFetch) is a no-op and the test stays
    // focused on truncation.
    val endpointQuota = mock(classOf[ReplicaQuota])
    when(endpointQuota.isQuotaExceeded).thenReturn(true)

    val props = TestUtils.createBrokerConfig(nodeId = 1)
    props.put("replica.fetch.backoff.ms", "1")
    val config = KafkaConfig.fromProps(props)

    val endpoint = new DisklessLeaderEndPoint(
      brokerEndPoint,
      fetchHandler,
      fetchOffsetHandler,
      replicaManager,
      config,
      endpointQuota,
      () => MetadataVersion.LATEST_PRODUCTION,
      () => 1L
    )

    val thread = new ConsolidationFetcherThread(
      "consolidation-fetcher-truncation-test",
      endpoint,
      config,
      failedPartitions,
      replicaManager,
      QuotaFactory.UNBOUNDED_QUOTA,
      "[ConsolidationFetcherTruncationTest] ",
      None
    )

    // Follower joins the consolidation fetcher in the Truncating phase (isTruncationOnFetchSupported=false).
    thread.addPartitions(Map(
      topicPartition -> InitialFetchState(None, brokerEndPoint, currentLeaderEpoch = 8, initOffset = divergentLeo)
    ))

    thread.doWork()

    // The stale tail [seal, LEO) is truncated away: the follower truncates back to the committed seal.
    verify(partition).truncateTo(seal, isFuture = false)
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

    assertEquals(40L, findGaugeValue("ConsolidationTotalLag", topicPartition))
    assertEquals(20L, findGaugeValue("ConsolidationLocalLag", topicPartition))
    assertEquals(50L, findGaugeValue("ConsolidationDeletableMessages", topicPartition))
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

    assertEquals(20L, findGaugeValue("ConsolidationLocalLag", topicPartition))
    assertEquals(0L, findGaugeValue("ConsolidationTotalLag", topicPartition))
    assertEquals(0L, findGaugeValue("ConsolidationDeletableMessages", topicPartition))
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

    assertEquals(0L, findGaugeValue("ConsolidationTotalLag", topicPartition))
    assertEquals(0L, findGaugeValue("ConsolidationLocalLag", topicPartition))
    assertEquals(70L, findGaugeValue("ConsolidationDeletableMessages", topicPartition))
  }

  @Test
  def testNoMetricsRegisteredWhenConsolidationMetricsIsNone(): Unit = {
    val partition = mockPartitionWithLog(80L, 60L)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    thread.processPartitionData(topicPartition, 80L, Int.MaxValue, buildPartitionData(100L))

    assertNull(findGaugeOrNull("ConsolidationTotalLag", topicPartition))
    assertNull(findGaugeOrNull("ConsolidationLocalLag", topicPartition))
    assertNull(findGaugeOrNull("ConsolidationDeletableMessages", topicPartition))
  }

  @Test
  def testOversizedBatchRethrownAsRetriableAndCounted(): Unit = {
    val fetchOffset = 50L
    val segmentSize = 2000
    val partition = mockPartitionAppendTooLarge(fetchOffset, segmentSize)
    val log = partition.localLogOrException
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics))

    metrics.registerPartition(topicPartition)

    // RecordBatchTooLargeException from the append is converted to ConsolidationSegmentOverflowException,
    // which extends InvalidRecordException so AbstractFetcherThread treats it as a soft, retriable
    // per-partition error (parked + backoff) rather than a hard failure.
    val ex = assertThrows(classOf[ConsolidationSegmentOverflowException],
      () => thread.processPartitionData(topicPartition, fetchOffset, Int.MaxValue, buildPartitionData(100L)))
    assertInstanceOf(classOf[InvalidRecordException], ex)
    assertInstanceOf(classOf[RecordBatchTooLargeException], ex.getCause)

    assertEquals(1L, findGaugeValue("ConsolidationOversizedBatch", topicPartition))
  }

  @Test
  def testOversizedBatchCounterIncrementsPerRetry(): Unit = {
    val fetchOffset = 50L
    val partition = mockPartitionAppendTooLarge(fetchOffset, segmentSize = 2000)
    val log = partition.localLogOrException
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics))

    metrics.registerPartition(topicPartition)

    // Each retry (while segment.bytes stays too small) increments the per-partition counter.
    (1 to 3).foreach { _ =>
      assertThrows(classOf[ConsolidationSegmentOverflowException],
        () => thread.processPartitionData(topicPartition, fetchOffset, Int.MaxValue, buildPartitionData(100L)))
    }

    assertEquals(3L, findGaugeValue("ConsolidationOversizedBatch", topicPartition))
  }

  @Test
  def testOversizedBatchWithoutMetricsStillRethrows(): Unit = {
    val fetchOffset = 50L
    val partition = mockPartitionAppendTooLarge(fetchOffset, segmentSize = 2000)
    val log = partition.localLogOrException
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    // No ConsolidationMetrics wired: still converts the exception, just no counter.
    assertThrows(classOf[ConsolidationSegmentOverflowException],
      () => thread.processPartitionData(topicPartition, fetchOffset, Int.MaxValue, buildPartitionData(100L)))
    assertNull(findGaugeOrNull("ConsolidationOversizedBatch", topicPartition))
  }

  @Test
  def testBrokerLevelAggregateGauges(): Unit = {
    val tp0 = new TopicPartition("topic-a", 0)
    val tp1 = new TopicPartition("topic-a", 1)

    metrics.registerPartition(tp0)
    metrics.registerPartition(tp1)

    metrics.updateTotalLag(tp0, 30L)
    metrics.updateTotalLag(tp1, 50L)
    metrics.updateLocalLag(tp0, 10L)
    metrics.updateLocalLag(tp1, 20L)
    metrics.updateDeletableMessages(tp0, 5L)
    metrics.updateDeletableMessages(tp1, 15L)

    assertEquals(80L, findBrokerGaugeValue("ConsolidationTotalLag"))
    assertEquals(30L, findBrokerGaugeValue("ConsolidationLocalLag"))
    assertEquals(20L, findBrokerGaugeValue("ConsolidationDeletableMessages"))

    metrics.unregisterPartition(tp0)

    assertEquals(50L, findBrokerGaugeValue("ConsolidationTotalLag"))
    assertEquals(20L, findBrokerGaugeValue("ConsolidationLocalLag"))
    assertEquals(15L, findBrokerGaugeValue("ConsolidationDeletableMessages"))
  }

  @Test
  def testOversizedBatchCounterAggregatesAndSurvivesReRegistration(): Unit = {
    val tp0 = new TopicPartition("topic-a", 0)
    val tp1 = new TopicPartition("topic-a", 1)

    metrics.registerPartition(tp0)
    metrics.registerPartition(tp1)

    metrics.recordOversizedBatch(tp0)
    metrics.recordOversizedBatch(tp0)
    metrics.recordOversizedBatch(tp1)

    assertEquals(2L, findGaugeValue("ConsolidationOversizedBatch", tp0))
    assertEquals(1L, findGaugeValue("ConsolidationOversizedBatch", tp1))
    assertEquals(3L, findBrokerGaugeValue("ConsolidationOversizedBatch"))

    // Re-registration must not reset the monotonic counter (unlike the lag gauges).
    metrics.registerPartition(tp0)
    assertEquals(2L, findGaugeValue("ConsolidationOversizedBatch", tp0))

    metrics.unregisterPartition(tp0)
    assertEquals(1L, findBrokerGaugeValue("ConsolidationOversizedBatch"))
    assertNull(findGaugeOrNull("ConsolidationOversizedBatch", tp0))
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

  private def findBrokerGaugeValue(name: String): Long = {
    val gauge = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (metricName, _) =>
        metricName.getName == name && metricName.getScope == null
      }
      .map(_._2.asInstanceOf[com.yammer.metrics.core.Gauge[Long]])
      .orNull
    assertNotNull(gauge, s"Broker-level gauge $name not found")
    gauge.value()
  }
}
