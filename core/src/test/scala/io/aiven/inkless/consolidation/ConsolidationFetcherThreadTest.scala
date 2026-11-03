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
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.RecordBatchTooLargeException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.record.internal.{BaseRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory
import org.apache.kafka.server.storage.log.FetchPartitionData
import org.apache.kafka.storage.internals.log.{LogAppendInfo, UnifiedLog}
import io.aiven.inkless.control_plane.{BatchInfo, BatchMetadata, FindBatchResponse}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{RETURNS_DEEP_STUBS, doNothing, mock, never, verify, when}

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.CompletableFuture
import java.util.{Optional, OptionalInt, OptionalLong}
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

  /**
   * An empty control-plane response must reach the follower as zero bytes with `Errors.NONE`.
   * A `validBytes` of 0 stops `AbstractFetcherThread` from demoting the partition, which leaves
   * the partition in place for the next iteration.
   *
   * The fragile hop is `selectRecordsWithinSegmentBudget`: its "at least one batch is always emitted for progress"
   * applies to trimming a non-empty block and must not become a floor for an empty one.
   *
   * Characterization test. The handler is stubbed, so it passes with or without the request-level grant;
   * verified non-vacuous by mutation instead. It does not assert the rotation gate, which is upstream code
   * covered against real SQL by `FindBatchesFairnessTest`.
   */
  @Test
  def testConsolidationFetchPropagatesEmptyControlPlaneResponseAsZeroBytes(): Unit = {
    val highWatermark = 100L
    val topicId = Uuid.randomUuid()
    val topicIdPartition = new TopicIdPartition(topicId, topicPartition)

    val log = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(0L)
    when(log.remoteLogEnabled()).thenReturn(false)
    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.topicPartition).thenReturn(topicPartition)

    val replicaManager = mock(classOf[ReplicaManager], RETURNS_DEEP_STUBS)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))
    when(replicaManager.crossTierEarliestOffset(topicPartition)).thenReturn(OptionalLong.empty())
    val purgatory = new DelayedOperationPurgatory[DelayedConsolidationFetch]("ConsolidationFetchTest", 1, 0)
    when(replicaManager.delayedConsolidationFetchPurgatory).thenReturn(purgatory)

    // The unbudgeted probe reports data, so the delayed op completes and runs the real fetch instead of
    // parking on minBytes. A real record, not a mock: FindBatchResponse has a success() factory, and a
    // partial mock would leave batches() returning null the moment tryComplete starts reading it.
    val probeBatch = new BatchInfo(1L, "probe-object",
      BatchMetadata.of(topicIdPartition, 0L, 64L * 1024 * 1024, 0L, 9L,
        System.currentTimeMillis(), System.currentTimeMillis(), TimestampType.CREATE_TIME))
    val probeResponse = FindBatchResponse.success(util.List.of(probeBatch), 0L, highWatermark)
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(util.List.of(probeResponse)))

    // The budgeted fetch served this partition nothing: the aggregate budget went to other partitions.
    val fetchHandler = mock(classOf[FetchHandler])
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(
      util.Map.of(topicIdPartition, new FetchPartitionData(
        Errors.NONE, highWatermark, 0L, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false))
    ))

    val props = TestUtils.createBrokerConfig(nodeId = 1)
    val config = KafkaConfig.fromProps(props)
    val endpoint = new DisklessLeaderEndPoint(
      new BrokerEndPoint(0, "localhost", 9092),
      fetchHandler,
      mock(classOf[FetchOffsetHandler]),
      replicaManager,
      config,
      mock(classOf[ReplicaQuota]),
      () => MetadataVersion.LATEST_PRODUCTION,
      () => 1L
    )

    try {
      val requestMap = new java.util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]()
      requestMap.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.empty()))
      val builder = FetchRequest.Builder
        .forReplica(MetadataVersion.LATEST_PRODUCTION.fetchRequestVersion, 1, 1L, 500, 1, requestMap)
        .setMaxBytes(64 * 1024 * 1024)

      val response = endpoint.fetch(builder)

      val partitionResponse = response.get(topicPartition)
      assertNotNull(partitionResponse)
      // Zero bytes, so AbstractFetcherThread computes validBytes == 0 and does not demote the partition.
      assertEquals(Errors.NONE.code, partitionResponse.errorCode)
      assertEquals(0, partitionResponse.records.sizeInBytes,
        "an unserved partition must return no records, so the fetcher's move-to-end rotation can advance it")
      // The high watermark still comes back, so consolidation lag stays visible while the partition waits.
      assertEquals(highWatermark, partitionResponse.highWatermark)
      assertEquals(0L, partitionResponse.logStartOffset)
    } finally {
      purgatory.shutdown()
    }
  }

  private def createConsolidationFetcherThread(
    replicaManager: ReplicaManager,
    consolidationMetrics: Option[ConsolidationMetrics],
    leaderOverride: Option[DisklessLeaderEndPoint] = None
  ): ConsolidationFetcherThread = {
    val props = TestUtils.createBrokerConfig(nodeId=1)
    val config = KafkaConfig.fromProps(props)
    val leader = leaderOverride.getOrElse {
      val mockedLeader = mock(classOf[DisklessLeaderEndPoint])
      when(mockedLeader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
      when(mockedLeader.takeTimeLagSample(any(), anyLong())).thenReturn(None)
      mockedLeader
    }
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
    localLogStartOffset: Long = 0L,
    appendedValidBytes: Int = 0
  ): Partition = {
    val log = mock(classOf[UnifiedLog])
    when(log.logEndOffset).thenReturn(logEndOffset)
    when(log.highestOffsetInRemoteStorage()).thenReturn(highestOffsetInRemoteStorage)
    when(log.localLogStartOffset()).thenReturn(localLogStartOffset)
    when(log.maybeUpdateHighWatermark(anyLong())).thenReturn(Optional.empty)

    val appendInfo = mock(classOf[LogAppendInfo])
    when(appendInfo.validBytes).thenReturn(appendedValidBytes)
    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.appendRecordsToFollowerOrFutureReplica(any[MemoryRecords], any[Boolean], any[Int]))
      .thenReturn(Some(appendInfo))
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

  private def recordsWithTimestamp(baseOffset: Long, timestamp: Long, value: String): MemoryRecords =
    MemoryRecords.withRecords(baseOffset, Compression.NONE,
      new SimpleRecord(timestamp, value.getBytes(StandardCharsets.UTF_8)))

  private def partitionDataWith(records: BaseRecords): FetchResponseData.PartitionData =
    partitionDataWith(records, 10L)

  private def partitionDataWith(records: BaseRecords, highWatermark: Long): FetchResponseData.PartitionData =
    new FetchResponseData.PartitionData()
      .setPartitionIndex(topicPartition.partition)
      .setRecords(records)
      .setHighWatermark(highWatermark)
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
  def testLocalTimeLagUsesFetchBoundarySpanIncludingEmptyResponse(): Unit = {
    val partition = mockPartitionWithLog(
      logEndOffset = 0L,
      highestOffsetInRemoteStorage = -1L,
      appendedValidBytes = 0)
    val replicaManager = mockReplicaManager(partition)
    val leader = mock(classOf[DisklessLeaderEndPoint])
    when(leader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
    when(leader.takeTimeLagSample(topicPartition, 0L))
      .thenReturn(Some(TimeLagSample(Some(100L), Some(300L))))
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics), Some(leader))
    metrics.registerPartition(topicPartition)

    thread.processPartitionData(topicPartition, 0L, Int.MaxValue, partitionDataWith(MemoryRecords.EMPTY))

    assertEquals(200L, findGaugeValue("ConsolidationLocalTimeLag", topicPartition))
  }

  @Test
  def testLocalTimeLagPublishesZeroForEmptyStartAfterFullDrain(): Unit = {
    val partition = mockPartitionWithLog(
      logEndOffset = 80L,
      highestOffsetInRemoteStorage = -1L,
      appendedValidBytes = 0)
    val replicaManager = mockReplicaManager(partition)
    val leader = mock(classOf[DisklessLeaderEndPoint])
    when(leader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
    when(leader.takeTimeLagSample(topicPartition, 80L))
      .thenReturn(Some(TimeLagSample(None, Some(300L), backlogConsumed = false)))
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics), Some(leader))
    metrics.registerPartition(topicPartition)
    metrics.reportLocalTimeLag(topicPartition, Some(100L), Some(300L), 80L, 100L)

    thread.processPartitionData(topicPartition, 80L, Int.MaxValue, partitionDataWith(MemoryRecords.EMPTY, 100L))

    assertEquals(200L, findGaugeValue("ConsolidationLocalTimeLag", topicPartition))

    when(leader.takeTimeLagSample(topicPartition, 80L))
      .thenReturn(Some(TimeLagSample(None, Some(300L), backlogConsumed = true)))
    thread.processPartitionData(topicPartition, 80L, Int.MaxValue, partitionDataWith(MemoryRecords.EMPTY, 100L))

    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", topicPartition))
  }

  @Test
  def testLocalTimeLagStartsAtFirstReturnedBatchThatDidNotAppend(): Unit = {
    val firstBatch = recordsWithTimestamp(0L, 100L, "first")
    val secondBatch = recordsWithTimestamp(1L, 200L, "second")
    val returnedRecords = new ConcatenatedRecords(util.List.of(firstBatch, secondBatch))
    val partition = mockPartitionWithLog(
      logEndOffset = 0L,
      highestOffsetInRemoteStorage = -1L,
      appendedValidBytes = firstBatch.sizeInBytes)
    val replicaManager = mockReplicaManager(partition)
    val leader = mock(classOf[DisklessLeaderEndPoint])
    when(leader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
    when(leader.takeTimeLagSample(topicPartition, 0L))
      .thenReturn(Some(TimeLagSample(Some(100L), Some(300L))))
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics), Some(leader))
    metrics.registerPartition(topicPartition)

    thread.processPartitionData(topicPartition, 0L, Int.MaxValue, partitionDataWith(returnedRecords))

    assertEquals(100L, findGaugeValue("ConsolidationLocalTimeLag", topicPartition))
  }

  @Test
  def testLocalTimeLagIsZeroForOneRemainingBatchAfterPartialDrain(): Unit = {
    val firstBatch = recordsWithTimestamp(0L, 100L, "first")
    val secondBatch = recordsWithTimestamp(1L, 103L, "second")
    val thirdBatch = recordsWithTimestamp(2L, 108L, "third")
    val returnedRecords = new ConcatenatedRecords(util.List.of(firstBatch, secondBatch))
    val partition = mockPartitionWithLog(
      logEndOffset = 0L,
      highestOffsetInRemoteStorage = -1L,
      appendedValidBytes = returnedRecords.sizeInBytes)
    val log = partition.localLogOrException
    when(log.logEndOffset).thenReturn(0L).thenReturn(2L)
    val replicaManager = mockReplicaManager(partition)
    val leader = mock(classOf[DisklessLeaderEndPoint])
    when(leader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
    val thirdBatchTimestamp = thirdBatch.batches().iterator().next().maxTimestamp()
    when(leader.takeTimeLagSample(topicPartition, 0L))
      .thenReturn(Some(TimeLagSample(Some(thirdBatchTimestamp), Some(thirdBatchTimestamp))))
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics), Some(leader))
    metrics.registerPartition(topicPartition)

    thread.processPartitionData(topicPartition, 0L, Int.MaxValue, partitionDataWith(returnedRecords, 3L))

    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", topicPartition))
  }

  @Test
  def testTracksFetcherLagWithoutRegisteringConsumerLag(): Unit = {
    val partition = mockPartitionWithLog(80L, 60L)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics))

    val lagMetrics = thread.fetcherLagStats.getAndMaybePut(topicPartition)
    lagMetrics.lag = 42L

    assertEquals(42L, thread.fetcherLagStats.stats.get(topicPartition).lag)
    assertFalse(
      KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.keys.exists { metricName =>
        metricName.getGroup == "kafka.server" &&
          metricName.getType == "FetcherLagMetrics" &&
          metricName.getName == FetcherMetrics.ConsumerLag
      },
      "ConsolidationFetcherThread must not register the generic FetcherLagMetrics.ConsumerLag JMX gauge")
  }

  @Test
  def testRecordsConsolidationFetchBytesInAndSkipsReplicationBytesIn(): Unit = {
    val validBytes = 512
    val partition = mockPartitionWithLog(logEndOffset = 80L, highestOffsetInRemoteStorage = 60L,
      appendedValidBytes = validBytes)
    val replicaManager = mockReplicaManager(partition)
    val brokerTopicStats = replicaManager.brokerTopicStats
    val thread = createConsolidationFetcherThread(replicaManager, None)

    thread.processPartitionData(topicPartition, 80L, Int.MaxValue, buildPartitionData(100L))

    verify(replicaManager).recordConsolidationFetchBytesIn(validBytes.toLong)
    // Consolidation throughput must not leak into the inter-broker ReplicationBytesInPerSec meter.
    assertEquals(0L, brokerTopicStats.allTopicsStats.replicationBytesInRate().get.count())
  }

  @Test
  def testDoesNotRecordConsolidationFetchBytesInWhenNoValidBytes(): Unit = {
    val partition = mockPartitionWithLog(logEndOffset = 80L, highestOffsetInRemoteStorage = 60L,
      appendedValidBytes = 0)
    val replicaManager = mockReplicaManager(partition)
    val thread = createConsolidationFetcherThread(replicaManager, None)

    thread.processPartitionData(topicPartition, 80L, Int.MaxValue, buildPartitionData(100L))

    verify(replicaManager, never()).recordConsolidationFetchBytesIn(anyLong())
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
  def testOversizedBatchPublishesTimeLagOfTheBatchThatDidNotAppend(): Unit = {
    val fetchOffset = 50L
    val partition = mockPartitionAppendTooLarge(fetchOffset, 2000)
    val log = partition.localLogOrException
    val replicaManager = mockReplicaManager(partition)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)
    val leader = mock(classOf[DisklessLeaderEndPoint])
    when(leader.brokerEndPoint()).thenReturn(new BrokerEndPoint(0, "localhost", 9092))
    when(leader.takeTimeLagSample(topicPartition, fetchOffset))
      .thenReturn(Some(TimeLagSample(Some(4000L), Some(5000L), backlogConsumed = true)))
    val thread = createConsolidationFetcherThread(replicaManager, Some(metrics), Some(leader))
    metrics.registerPartition(topicPartition)
    metrics.reportLocalTimeLag(topicPartition, Some(100L), Some(200L), fetchOffset, 100L)

    assertThrows(classOf[ConsolidationSegmentOverflowException],
      () => thread.processPartitionData(topicPartition, fetchOffset, Int.MaxValue, buildPartitionData(100L)))

    assertEquals(4000L, findGaugeValue("ConsolidationLocalTimeLag", topicPartition))
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
  def testLocalTimeLagHoldsUntilBothTimestampsArriveAndZerosWhenCaughtUp(): Unit = {
    val tp = new TopicPartition("topic-time", 0)
    metrics.registerPartition(tp)

    metrics.reportLocalTimeLag(tp, Some(100L), None, 80L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, Some(250L), 80L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, Some(100L), Some(250L), 80L, 100L)
    assertEquals(150L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, Some(350L), 80L, 100L, backlogConsumed = false)
    assertEquals(150L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, None, 80L, 100L)
    assertEquals(150L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, Some(RecordBatch.NO_TIMESTAMP), Some(300L), 80L, 100L)
    assertEquals(150L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, Some(100L), Some(RecordBatch.NO_TIMESTAMP), 80L, 100L)
    assertEquals(150L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, None, -1L, 100L)
    assertEquals(150L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, None, 100L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, None, 100L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))
  }

  @Test
  def testLocalTimeLagPublishesZeroForANegativeSpanAndBrokerAggregateUsesMaximum(): Unit = {
    val tp0 = new TopicPartition("topic-time", 0)
    val tp1 = new TopicPartition("topic-time", 1)
    metrics.registerPartition(tp0)
    metrics.registerPartition(tp1)

    metrics.reportLocalTimeLag(tp0, Some(200L), Some(100L), 0L, 100L)
    metrics.reportLocalTimeLag(tp1, Some(100L), Some(300L), 0L, 100L)

    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp0))
    assertEquals(200L, findGaugeValue("ConsolidationLocalTimeLag", tp1))
    assertEquals(200L, findBrokerGaugeValue("ConsolidationLocalTimeLag"))

    metrics.registerPartition(tp1)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp1))
    metrics.unregisterPartition(tp1)
    assertNull(findGaugeOrNull("ConsolidationLocalTimeLag", tp1))
    assertEquals(0L, findBrokerGaugeValue("ConsolidationLocalTimeLag"))
  }

  @Test
  def testLocalTimeLagReportsZeroForOnePendingBatchAfterCatchUp(): Unit = {
    val tp = new TopicPartition("topic-time", 2)
    metrics.registerPartition(tp)

    metrics.reportLocalTimeLag(tp, Some(500L), Some(500L), 80L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, None, 100L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))

    metrics.reportLocalTimeLag(tp, None, None, 100L, 100L)
    assertEquals(0L, findGaugeValue("ConsolidationLocalTimeLag", tp))
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

  @Test
  def testRemotePrefixUnknownGaugeTracksWaitingPartitions(): Unit = {
    val tp0 = new TopicPartition("topic-a", 0)
    val tp1 = new TopicPartition("topic-a", 1)

    metrics.registerPartition(tp0)
    metrics.registerPartition(tp1)
    assertEquals(0L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))

    metrics.setRemotePrefixUnknown(tp0, unknown = true)
    metrics.setRemotePrefixUnknown(tp1, unknown = true)
    assertEquals(1L, findGaugeValue("ConsolidationRemotePrefixUnknown", tp0))
    assertEquals(1L, findGaugeValue("ConsolidationRemotePrefixUnknown", tp1))
    assertEquals(2L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))

    metrics.setRemotePrefixUnknown(tp0, unknown = false)
    assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", tp0))
    assertEquals(1L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))

    metrics.unregisterPartition(tp1)
    assertEquals(0L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))
    assertNull(findGaugeOrNull("ConsolidationRemotePrefixUnknown", tp1))
  }

  @Test
  def testRemotePrefixUnknownGaugeResetsOnReRegistration(): Unit = {
    val tp0 = new TopicPartition("topic-a", 0)

    metrics.registerPartition(tp0)
    metrics.setRemotePrefixUnknown(tp0, unknown = true)
    assertEquals(1L, findGaugeValue("ConsolidationRemotePrefixUnknown", tp0))

    // Re-registration resets the latch to 0.
    metrics.registerPartition(tp0)
    assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", tp0))
    assertEquals(0L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))
  }

  @Test
  def testRemotePrefixUnknownClearDoesNotCreateGauge(): Unit = {
    val tp0 = new TopicPartition("topic-a", 0)

    // become-leader and become-follower pass every local replica through removeFetcherForPartitions.
    // Clearing an unregistered partition leaves the gauge uncreated.
    metrics.setRemotePrefixUnknown(tp0, unknown = false)
    assertNull(findGaugeOrNull("ConsolidationRemotePrefixUnknown", tp0))
    assertEquals(0L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))
  }

  @Test
  def testRemoveFetcherForPartitionsClearsRemotePrefixUnknownWithoutUnregistering(): Unit = {
    metrics.registerPartition(topicPartition)
    metrics.updateTotalLag(topicPartition, 30L)
    metrics.setRemotePrefixUnknown(topicPartition, unknown = true)
    assertEquals(1L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))
    assertEquals(30L, findGaugeValue("ConsolidationTotalLag", topicPartition))

    val props = TestUtils.createBrokerConfig(nodeId = 1)
    val config = KafkaConfig.fromProps(props)
    val manager = new ConsolidationFetcherManager(
      config,
      mock(classOf[ReplicaManager]),
      mock(classOf[ReplicationQuotaManager]),
      mock(classOf[FetchHandler]),
      mock(classOf[FetchOffsetHandler]),
      Some(metrics)
    )
    try {
      manager.removeFetcherForPartitions(Set(topicPartition))
      assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))
      // Lag gauges stay registered.
      assertEquals(30L, findGaugeValue("ConsolidationTotalLag", topicPartition))
      assertNotNull(findGaugeOrNull("ConsolidationRemotePrefixUnknown", topicPartition))
    } finally {
      manager.shutdown()
    }
  }

  @Test
  def testRemotePrefixUnknownIgnoresStaleGenerationAfterRemoval(): Unit = {
    metrics.registerPartition(topicPartition)
    val generation = metrics.remotePrefixGeneration(topicPartition)
    metrics.setRemotePrefixUnknown(topicPartition, unknown = true, generation)
    assertEquals(1L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))

    metrics.bumpRemotePrefixGeneration(topicPartition)
    assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))

    metrics.setRemotePrefixUnknown(topicPartition, unknown = true, generation)
    assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))
    assertEquals(0L, findBrokerGaugeValue("ConsolidationRemotePrefixUnknown"))
  }

  @Test
  def testUnregisterKeepsRemotePrefixGenerationMonotonic(): Unit = {
    metrics.registerPartition(topicPartition)
    val first = metrics.remotePrefixGeneration(topicPartition)
    assertEquals(1L, first)
    metrics.setRemotePrefixUnknown(topicPartition, unknown = true, first)
    assertEquals(1L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))

    metrics.unregisterPartition(topicPartition)
    assertNull(findGaugeOrNull("ConsolidationRemotePrefixUnknown", topicPartition))
    assertEquals(2L, metrics.remotePrefixGeneration(topicPartition))
    metrics.setRemotePrefixUnknown(topicPartition, unknown = true, first)
    assertNull(findGaugeOrNull("ConsolidationRemotePrefixUnknown", topicPartition))

    metrics.registerPartition(topicPartition)
    val second = metrics.remotePrefixGeneration(topicPartition)
    assertEquals(3L, second)
    assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))
    metrics.setRemotePrefixUnknown(topicPartition, unknown = true, first)
    assertEquals(0L, findGaugeValue("ConsolidationRemotePrefixUnknown", topicPartition))
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
