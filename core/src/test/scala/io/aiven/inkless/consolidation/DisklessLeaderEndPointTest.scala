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
import kafka.server.{KafkaConfig, QuotaFactory, ReplicaManager, ReplicaQuota}
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.{KafkaStorageException, NotLeaderOrFollowerException, UnknownTopicOrPartitionException}
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, Records, SimpleRecord}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, OffsetsForLeaderEpochResponse}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory
import org.apache.kafka.server.storage.log.FetchPartitionData
import org.apache.kafka.server.{PartitionFetchState, ReplicaState}
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError
import org.apache.kafka.storage.internals.log.{LogConfig, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{doNothing, mock, verify, when}

import java.util
import java.nio.ByteBuffer
import java.util.Optional
import java.util.OptionalInt
import java.util.OptionalLong
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._
import scala.util.{Left, Right}

class DisklessLeaderEndPointTest {

  private val brokerEndPoint = new BrokerEndPoint(1, "localhost", 9092)
  private val topicPartition = new TopicPartition("diskless-topic", 0)
  private val topicId = Uuid.randomUuid()
  private val topicIdPartition = new TopicIdPartition(topicId, topicPartition)

  private def kafkaConfig: KafkaConfig = {
    val props = TestUtils.createBrokerConfig(brokerEndPoint.id, port = brokerEndPoint.port)
    KafkaConfig.fromProps(props)
  }

  /**
   * A [[ReplicaManager]] mock whose consolidation purgatory completes the delayed fetch inline, so
   * [[DisklessLeaderEndPoint.fetch]] runs the mocked [[FetchHandler]] synchronously.
   * forceComplete() runs onComplete() directly (bypassing tryComplete/findDisklessBatches), which
   * keeps these tests focused on fetch response mapping. The delayed-op mechanics are covered by
   * DelayedConsolidationFetchTest.
   */
  private def replicaManagerMock(): ReplicaManager = {
    val replicaManager = mock(classOf[ReplicaManager])
    val purgatory = mock(classOf[DelayedOperationPurgatory[DelayedConsolidationFetch]])
    when(purgatory.tryCompleteElseWatch(any(), any())).thenAnswer { invocation =>
      invocation.getArgument(0, classOf[DelayedConsolidationFetch]).forceComplete()
    }
    when(replicaManager.delayedConsolidationFetchPurgatory).thenReturn(purgatory)
    replicaManager
  }

  private def unifiedLogMock(logStartOffset: Long, segmentSize: Int, maxMessageSize: Int): UnifiedLog = {
    val log = mock(classOf[UnifiedLog])
    val config = mock(classOf[LogConfig])
    when(log.logStartOffset).thenReturn(logStartOffset)
    when(config.segmentSize()).thenReturn(segmentSize)
    when(config.maxMessageSize()).thenReturn(maxMessageSize)
    when(log.config()).thenReturn(config)
    log
  }

  private def newEndPoint(
    fetchHandler: FetchHandler,
    fetchOffsetHandler: FetchOffsetHandler,
    replicaManager: ReplicaManager,
    quota: ReplicaQuota = QuotaFactory.UNBOUNDED_QUOTA,
    metadataVersion: MetadataVersion = MetadataVersion.LATEST_PRODUCTION,
    brokerEpoch: Long = 7L
  ): DisklessLeaderEndPoint =
    new DisklessLeaderEndPoint(
      brokerEndPoint,
      fetchHandler,
      fetchOffsetHandler,
      replicaManager,
      kafkaConfig,
      quota,
      () => metadataVersion,
      () => brokerEpoch
    )

  /**
   * Builds a replica fetch request that asks for `requestedOffset` on [[topicPartition]], carrying
   * the real [[topicId]] so that [[DisklessLeaderEndPoint.fetch]] resolves the request's
   * `fetchOffset` for that partition (it keys requested offsets by `TopicIdPartition`).
   */
  private def fetchBuilderForOffset(requestedOffset: Long, leaderEpoch: Int = 0): FetchRequest.Builder = {
    val partitionData = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(
      topicPartition,
      new FetchRequest.PartitionData(topicId, requestedOffset, 0L, 1024 * 1024, Optional.of(Int.box(leaderEpoch)))
    )
    FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)
  }

  /**
   * Wires an endpoint where the diskless fetch handler reports a WAL that starts at `disklessStart`
   * (the control-plane `log_start_offset`, advanced as the WAL is pruned), while the local
   * [[UnifiedLog]] reports the whole-log start `localLogStartOffset` and remote storage state
   * `remoteLogEnabled`. This models a consolidating partition whose prefix `[localLogStartOffset,
   * disklessStart)` was tiered to remote and pruned from the WAL.
   */
  private def consolidatedPrefixEndPoint(
    localLogStartOffset: Long,
    disklessStart: Long,
    highWatermark: Long,
    remoteLogEnabled: Boolean
  ): DisklessLeaderEndPoint = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(localLogStartOffset)
    when(localLog.remoteLogEnabled()).thenReturn(remoteLogEnabled)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      highWatermark,
      disklessStart,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))
    newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
  }

  @Test
  def testBuildFetchProducesReplicaFetch(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val log = unifiedLogMock(logStartOffset = 11L, segmentSize = Int.MaxValue, maxMessageSize = 1024 * 1024)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      99L,
      Optional.empty(),
      4,
      ReplicaState.FETCHING,
      Optional.empty()
    )
    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))

    assertTrue(result.partitionsWithError.isEmpty)
    assertTrue(result.result.isPresent)
    val replicaFetch = result.result.get
    assertTrue(replicaFetch.partitionData.containsKey(topicPartition))
    assertEquals(topicId, replicaFetch.partitionData.get(topicPartition).topicId)
    assertEquals(99L, replicaFetch.partitionData.get(topicPartition).fetchOffset)
  }

  @Test
  def testFetchMapsFetchHandlerResponseToPartitionData(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(55L)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val aborted = util.List.of(new FetchResponseData.AbortedTransaction())
    val fetchData = new FetchPartitionData(
      Errors.NONE,
      99L,
      1L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.of(88L),
      Optional.of(aborted),
      OptionalInt.empty(),
      false
    )
    val responseMap = Map(topicIdPartition -> fetchData).asJava
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(responseMap))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val result = endPoint.fetch(fetchBuilder).asScala

    assertEquals(1, result.size)
    val pd = result(topicPartition)
    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(99L, pd.highWatermark)
    assertEquals(88L, pd.lastStableOffset)
    assertEquals(55L, pd.logStartOffset)
    assertEquals(aborted, pd.abortedTransactions)
    assertEquals(MemoryRecords.EMPTY, pd.records)
  }

  @Test
  def testFetchUsesInvalidLastStableOffsetWhenOptionalEmpty(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(0L)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      10L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(FetchResponse.INVALID_LAST_STABLE_OFFSET, pd.lastStableOffset)
  }

  @Test
  def testFetchEarliestOffsetUsesEarliestTimestamp(): Unit = {
    verifyListOffsetTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, _.fetchEarliestOffset(topicPartition, 3))
  }

  @Test
  def testFetchLatestOffsetUsesLatestTimestamp(): Unit = {
    verifyListOffsetTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, _.fetchLatestOffset(topicPartition, 3))
  }

  @Test
  def testFetchEarliestLocalOffsetUsesEarliestLocalTimestamp(): Unit = {
    verifyListOffsetTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, _.fetchEarliestLocalOffset(topicPartition, 3))
  }

  private def verifyListOffsetTimestamp(expectedTimestamp: Long, invoke: DisklessLeaderEndPoint => OffsetAndEpoch): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new TimestampAndOffset(0L, 10L, Optional.of(5)))
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))
    doNothing().when(job).start()
    // Not a switched topic: no seal / captured diskless epoch, so the holder's epoch is propagated as-is.
    when(replicaManager.classicToDisklessStartOffset(topicPartition))
      .thenReturn(org.apache.kafka.metadata.PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(replicaManager.disklessLeaderEpoch(topicPartition))
      .thenReturn(org.apache.kafka.metadata.PartitionRegistration.NO_DISKLESS_LEADER_EPOCH)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val result = invoke(endPoint)

    assertEquals(new OffsetAndEpoch(10L, 5), result)

    val captor = ArgumentCaptor.forClass(classOf[org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition])
    verify(job).add(eqTo(topicPartition), captor.capture())
    assertEquals(expectedTimestamp, captor.getValue.timestamp)
    assertEquals(topicPartition.partition, captor.getValue.partitionIndex)
    assertEquals(3, captor.getValue.currentLeaderEpoch)
  }

  /**
   * Wires an endpoint whose diskless list-offsets returns `offset` carrying the placeholder epoch
   * [[LeaderAndIsr.INITIAL_LEADER_EPOCH]] (0) that [[FetchOffsetHandler]] always stamps -- the diskless
   * path does not know real per-offset epochs -- against a partition with the given seal and diskless
   * leader epoch `E_d`.
   */
  private def listOffsetEndPointWithPlaceholderEpoch(offset: Long, seal: Long, disklessLeaderEpoch: Int): DisklessLeaderEndPoint = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new TimestampAndOffset(0L, offset, Optional.of(Int.box(LeaderAndIsr.INITIAL_LEADER_EPOCH))))
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))
    doNothing().when(job).start()
    when(replicaManager.classicToDisklessStartOffset(topicPartition)).thenReturn(seal)
    when(replicaManager.disklessLeaderEpoch(topicPartition)).thenReturn(disklessLeaderEpoch)

    newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
  }

  @Test
  def testFetchEarliestLocalOffsetResolvesDisklessRegionToDisklessEpoch(): Unit = {
    // Switched topic recovery: the diskless WAL start (231261) sits in the diskless region [seal, LEO),
    // whose segments were tiered under E_d=5. FetchOffsetHandler returns only the placeholder epoch 0,
    // so the endpoint must override it with E_d -- otherwise the tier-state rebuild looks up the remote
    // segment at epoch 0 and loops forever ("previous remote log segment metadata was not found").
    val endPoint = listOffsetEndPointWithPlaceholderEpoch(offset = 231261L, seal = 150000L, disklessLeaderEpoch = 5)
    assertEquals(new OffsetAndEpoch(231261L, 5), endPoint.fetchEarliestLocalOffset(topicPartition, 3))
  }

  @Test
  def testFetchEarliestOffsetInClassicPrefixKeepsPlaceholderEpoch(): Unit = {
    // An offset below the seal belongs to the classic prefix; its epochs are restored from the remote
    // leader-epoch checkpoint during the rebuild, so the endpoint keeps the placeholder epoch (0) here.
    val endPoint = listOffsetEndPointWithPlaceholderEpoch(offset = 0L, seal = 150000L, disklessLeaderEpoch = 5)
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, 3))
  }

  @Test
  def testFetchEarliestLocalOffsetWithoutDisklessEpochKeepsPlaceholderEpoch(): Unit = {
    // Born-diskless / pre-E_d-switch partition: no seal and no captured diskless leader epoch, so the
    // placeholder epoch (0) is kept (born-diskless segments are tiered under epoch 0; pre-E_d keeps
    // legacy behavior).
    val endPoint = listOffsetEndPointWithPlaceholderEpoch(
      offset = 231261L,
      seal = org.apache.kafka.metadata.PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET,
      disklessLeaderEpoch = org.apache.kafka.metadata.PartitionRegistration.NO_DISKLESS_LEADER_EPOCH
    )
    assertEquals(new OffsetAndEpoch(231261L, 0), endPoint.fetchEarliestLocalOffset(topicPartition, 3))
  }

  @Test
  def testListDisklessOffsetThrowsWhenTopicNotDiskless(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(false)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    assertThrows(
      classOf[UnknownTopicOrPartitionException],
      () => endPoint.fetchLatestOffset(topicPartition, 0)
    )
  }

  @Test
  def testListDisklessOffsetPropagatesHolderException(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.of(new UnknownTopicOrPartitionException("missing")),
      Optional.empty()
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    assertThrows(
      classOf[UnknownTopicOrPartitionException],
      () => endPoint.fetchEarliestOffset(topicPartition, 0)
    )
  }

  @Test
  def testFetchEpochEndOffsetsReturnsEmptyForEmptyInput(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)

    assertTrue(endPoint.fetchEpochEndOffsets(util.Map.of()).isEmpty)
  }

  @Test
  def testFetchEpochEndOffsetsUndefinedEpoch(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    doNothing().when(job).start()

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)

    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH)
      )
    ).asScala

    val expected = new EpochEndOffset()
      .setPartition(topicPartition.partition)
      .setErrorCode(Errors.NONE.code)
    assertEquals(Map(topicPartition -> expected), result)
  }

  @Test
  def testFetchEpochEndOffsetsUnknownTopicPartition(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(false)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(1)
      )
    ).asScala

    val expected = new EpochEndOffset()
      .setPartition(topicPartition.partition)
      .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    assertEquals(Map(topicPartition -> expected), result)
  }

  @Test
  def testFetchEpochEndOffsetsSuccess(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new TimestampAndOffset(0L, 100L, Optional.of(2)))
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val leaderEpoch = 9
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(leaderEpoch)
      )
    ).asScala

    val expected = new EpochEndOffset()
      .setPartition(topicPartition.partition)
      .setErrorCode(Errors.NONE.code)
      .setLeaderEpoch(leaderEpoch)
      .setEndOffset(100L)
    assertEquals(Map(topicPartition -> expected), result)
  }

  @Test
  def testFetchEpochEndOffsetsBelowDisklessEpochReturnsSeal(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    doNothing().when(job).start()
    // Switched partition: classic prefix ends at the seal (100), diskless region carries epoch 5.
    when(replicaManager.classicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    when(replicaManager.disklessLeaderEpoch(topicPartition)).thenReturn(5)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    // A classic-prefix epoch (3 < 5) must resolve to the seal, without a diskless list-offsets call.
    val queriedEpoch = 3
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(queriedEpoch)
      )
    ).asScala

    val expected = new EpochEndOffset()
      .setPartition(topicPartition.partition)
      .setErrorCode(Errors.NONE.code)
      .setLeaderEpoch(queriedEpoch)
      .setEndOffset(100L)
    assertEquals(Map(topicPartition -> expected), result)
    verify(job, org.mockito.Mockito.never()).add(eqTo(topicPartition), any())
  }

  @Test
  def testFetchEpochEndOffsetsAtOrAboveDisklessEpochReturnsDisklessLeo(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new TimestampAndOffset(0L, 250L, Optional.of(5)))
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))
    when(replicaManager.classicToDisklessStartOffset(topicPartition)).thenReturn(100L)
    when(replicaManager.disklessLeaderEpoch(topicPartition)).thenReturn(5)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    // The diskless epoch itself (5 >= 5) resolves to the current diskless LEO.
    val queriedEpoch = 5
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(queriedEpoch)
      )
    ).asScala

    val expected = new EpochEndOffset()
      .setPartition(topicPartition.partition)
      .setErrorCode(Errors.NONE.code)
      .setLeaderEpoch(queriedEpoch)
      .setEndOffset(250L)
    assertEquals(Map(topicPartition -> expected), result)
  }

  @Test
  def testFetchEpochEndOffsetsBornDisklessReturnsDisklessLeo(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new TimestampAndOffset(0L, 42L, Optional.of(0)))
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))
    // Born-diskless / never switched: no classic seal and no captured diskless epoch.
    when(replicaManager.classicToDisklessStartOffset(topicPartition))
      .thenReturn(org.apache.kafka.metadata.PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(replicaManager.disklessLeaderEpoch(topicPartition))
      .thenReturn(org.apache.kafka.metadata.PartitionRegistration.NO_DISKLESS_LEADER_EPOCH)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(0)
      )
    ).asScala

    assertEquals(42L, result(topicPartition).endOffset)
    assertEquals(Errors.NONE.code, result(topicPartition).errorCode)
  }

  @Test
  def testFetchEpochEndOffsetsHolderException(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val holder = new FileRecordsOrError(
      Optional.of(new KafkaStorageException("offline")),
      Optional.empty()
    )
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(CompletableFuture.completedFuture(holder))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(3)
      )
    ).asScala

    assertEquals(Errors.KAFKA_STORAGE_ERROR.code, result(topicPartition).errorCode)
  }

  @Test
  def testFetchEpochEndOffsetsFutureGetThrows(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val job = mock(classOf[FetchOffsetHandler.Job])

    val failed = new CompletableFuture[FileRecordsOrError]()
    failed.completeExceptionally(new RuntimeException("boom"))
    when(fetchOffsetHandler.createJob()).thenReturn(job)
    when(job.mustHandle(topicPartition.topic())).thenReturn(true)
    when(job.add(eqTo(topicPartition), any())).thenReturn(failed)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val result = endPoint.fetchEpochEndOffsets(
      util.Map.of(
        topicPartition,
        new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(2)
      )
    ).asScala

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, result(topicPartition).errorCode)
  }

  @Test
  def testBuildFetchReturnsEmptyWhenQuotaExceeded(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val quota = mock(classOf[ReplicaQuota])
    when(quota.isQuotaExceeded).thenReturn(true)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager, quota = quota)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )
    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))

    assertTrue(result.result.isEmpty)
    assertTrue(result.partitionsWithError.isEmpty)
  }

  @Test
  def testBuildFetchMarksPartitionWithKafkaStorageException(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    when(replicaManager.localLogOrException(topicPartition)).thenThrow(new KafkaStorageException("bad log"))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )
    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))

    assertTrue(result.result.isEmpty)
    assertEquals(Set(topicPartition), result.partitionsWithError.asScala.toSet)
  }

  @Test
  def testBuildFetchMarksPartitionWithUnknownTopicOrPartitionException(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    when(replicaManager.localLogOrException(topicPartition)).thenThrow(new UnknownTopicOrPartitionException("deleted"))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )
    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))

    assertTrue(result.result.isEmpty)
    assertEquals(Set(topicPartition), result.partitionsWithError.asScala.toSet)
  }

  @Test
  def testBuildFetchSkipsPartitionWhenFollowerShouldThrottle(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val log = unifiedLogMock(logStartOffset = 0L, segmentSize = Int.MaxValue, maxMessageSize = 1024 * 1024)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)

    val quota = mock(classOf[ReplicaQuota])
    when(quota.isQuotaExceeded).thenReturn(false, true)
    when(quota.isThrottled(topicPartition)).thenReturn(true)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager, quota = quota)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.of(100L),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )
    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))

    assertTrue(result.result.isEmpty)
    assertTrue(result.partitionsWithError.isEmpty)
  }

  @Test
  def testFetchReturnsLocalLogStartOffsetWhenOffsetOutOfRangeAndPartitionAvailable(): Unit = {
    // OFFSET_OUT_OF_RANGE from the control plane enters the same logStartOffset resolution path as
    // NONE. When the requested offset is outside the consolidated prefix (empty request ->
    // requestedOffset=-1 < logStartOffset=0), no redirect fires and logStartOffset is taken from
    // the local log (0L), not UNKNOWN_OFFSET.
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(0L)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.OFFSET_OUT_OF_RANGE,
      50L,
      7L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchSignalsOffsetMovedToTieredStorageWhenControlPlaneReturnsOffsetOutOfRange(): Unit = {
    // The control plane returns OFFSET_OUT_OF_RANGE when the requested offset is below
    // log_start_offset (the current WAL start, advanced as batches are pruned to remote storage).
    // For offsets in [localLogStart, disklessStart) this means the data was already tiered --
    // the endpoint must redirect just as it does for the NONE+empty-batch case.
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(0L)
    when(localLog.remoteLogEnabled()).thenReturn(true)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    // Control plane returns OFFSET_OUT_OF_RANGE with logStartOffset=100 (WAL start after pruning).
    val fetchData = new FetchPartitionData(
      Errors.OFFSET_OUT_OF_RANGE,
      200L,
      100L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)

    // Offset 50 is in [0, 100) -- within the consolidated prefix pruned to remote.
    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 50L)).get(topicPartition)

    assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchOverlaysPartitionErrorAndUnknownLogStartWhenLookupFailsAndDisklessWasOk(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Left(Errors.NOT_LEADER_OR_FOLLOWER))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      50L,
      3L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchKeepsDisklessErrorWhenLookupFailsButDisklessAlreadyFailed(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Left(Errors.KAFKA_STORAGE_ERROR))

    val fetchData = new FetchPartitionData(
      Errors.OFFSET_OUT_OF_RANGE,
      50L,
      12L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchSetsUnknownServerErrorAndUnknownLogStartWhenLocalLogStartAheadOfHighWatermark(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(200L)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      99L,
      1L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchSetsUnknownLogStartWhenLocalLogUnavailable(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenThrow(new NotLeaderOrFollowerException("no local log"))
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      99L,
      1L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchSignalsOffsetMovedToTieredStorageForConsolidatedRemotePrefix(): Unit = {
    // Whole log starts at 0; diskless WAL was pruned up to 100; remote tier holds [0, 100).
    // A fetch for offset 0 (e.g. a rehydrating fetcher armed at highWatermark=0) must be told the
    // data moved to tiered storage so the stock tier-state machine rebuilds the remote prefix.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 0L)).get(topicPartition)

    assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code, pd.errorCode)
    // The whole-log start (0) is preserved so the rebuild keeps logStartOffset at 0, not the WAL start.
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchSignalsOffsetMovedToTieredStorageForOffsetJustBelowDisklessStart(): Unit = {
    // Boundary: the last offset of the consolidated prefix (disklessStart - 1) must still be redirected.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 99L)).get(topicPartition)

    assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchDoesNotSignalWhenRequestedOffsetAtDisklessStart(): Unit = {
    // Steady state: the fetcher requests at/after the WAL start, which is served straight from the WAL.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 100L)).get(topicPartition)

    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchDoesNotSignalWhenRequestedOffsetAboveDisklessStart(): Unit = {
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 150L)).get(topicPartition)

    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchDoesNotSignalWhenRemoteLogDisabled(): Unit = {
    // Without tiered storage there is nothing to rebuild from; do not hijack the response.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = false)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 0L)).get(topicPartition)

    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchDoesNotSignalWhenRequestedOffsetBelowWholeLogStart(): Unit = {
    // Below the whole-log start is a genuine out-of-range; leave it to the normal fetch path.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 50L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 10L)).get(topicPartition)

    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(50L, pd.logStartOffset)
  }

  @Test
  def testFetchDoesNotSignalWhenNoConsolidatedPrefixExists(): Unit = {
    // WAL start == whole-log start: there is no consolidated remote prefix, so the range is empty.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 100L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 100L)).get(topicPartition)

    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(100L, pd.logStartOffset)
  }

  @Test
  def testFetchSignalsOffsetMovedToTieredStorageForSwitchedTopicWithNonZeroEpoch(): Unit = {
    // Switched (classic -> diskless) topic: the diskless interval rides at a non-zero leader epoch
    // (diskless_epoch = last_classic_epoch + 1), so the fetcher's request carries a non-zero current
    // leader epoch. The redirect must remain epoch-agnostic: an offset in the consolidated prefix
    // still has to be served from the remote tier regardless of the request's leader epoch.
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = 0L, leaderEpoch = 5)).get(topicPartition)

    assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchSignalsOffsetMovedToTieredStorageForSwitchedTopicClassicRegionOffset(): Unit = {
    // Switched topic with a classic prefix tiered to remote: offsets [0, seal) are classic and
    // [seal, disklessStart) are consolidated diskless. A fetch landing in the classic region (below
    // the diskless WAL start) must also redirect to the remote tier, not skip to the WAL start.
    val seal = 40L
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val pd = endPoint.fetch(fetchBuilderForOffset(requestedOffset = seal - 1, leaderEpoch = 5)).get(topicPartition)

    assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testFetchDoesNotSignalWhenRequestOmitsOffset(): Unit = {
    // A request that does not include this partition leaves requestedOffset unresolved (-1): no signal.
    // The request carries a different partition (so the fetch is not empty), while the mocked handler
    // still returns topicPartition, exercising the getOrElse(-1) fallback in fetch().
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val otherPartition = new TopicPartition("other-topic", 0)
    val request = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    request.put(otherPartition, new FetchRequest.PartitionData(Uuid.randomUuid(), 0L, 0L, 1024 * 1024, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, request).setMaxBytes(100_000)
    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)

    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testConsolidationFetchLoopRedirectsRemotePrefixThroughRealFetchRequest(): Unit = {
    // Integration of the two leader-side methods the consolidation fetcher uses: the request that
    // buildFetch produces from a PartitionFetchState (armed at offset 0 after a local-log loss) is
    // fed straight into fetch, exercising the real FetchRequest version/topic-id plumbing. The diskless
    // WAL starts at 100 and remote storage holds the [0, 100) prefix, so the round trip must surface
    // OFFSET_MOVED_TO_TIERED_STORAGE for offset 0.
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = unifiedLogMock(logStartOffset = 0L, segmentSize = Int.MaxValue, maxMessageSize = 1024 * 1024)
    when(localLog.remoteLogEnabled()).thenReturn(true)
    when(partition.localLogOrException).thenReturn(localLog)
    // buildFetch resolves the request log start offset via localLogOrException(tp)...
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(localLog)
    // ...while fetch resolves the partition via getPartitionOrError(tp).
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      200L,
      100L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      4,
      ReplicaState.FETCHING,
      Optional.empty()
    )

    val replicaFetch = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))
    assertTrue(replicaFetch.result.isPresent, "buildFetch should produce a fetch request")

    val pd = endPoint.fetch(replicaFetch.result.get.fetchRequest).get(topicPartition)
    assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code, pd.errorCode)
    assertEquals(0L, pd.logStartOffset)
  }

  @Test
  def testConsolidationFetchConfigsAreUsed(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val log = unifiedLogMock(logStartOffset = 0L, segmentSize = 2 * 1024 * 1024, maxMessageSize = 1024 * 1024)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)

    val props = TestUtils.createBrokerConfig(brokerEndPoint.id, port = brokerEndPoint.port)
    props.setProperty(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_MAX_BYTES_CONFIG, "20971520") // 20MB per-partition
    props.setProperty(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_RESPONSE_MAX_BYTES_CONFIG, "41943040") // 40MB response-level
    props.setProperty(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_MIN_BYTES_CONFIG, "4096")
    props.setProperty(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_MAX_WAIT_MS_CONFIG, "1000")
    val config = KafkaConfig.fromProps(props)

    val endPoint = new DisklessLeaderEndPoint(
      brokerEndPoint,
      fetchHandler,
      fetchOffsetHandler,
      replicaManager,
      config,
      QuotaFactory.UNBOUNDED_QUOTA,
      () => MetadataVersion.LATEST_PRODUCTION,
      () => 7L
    )

    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )
    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))
    assertTrue(result.result.isPresent)

    val fetchRequest = result.result.get.fetchRequest.build()
    assertEquals(41943040, fetchRequest.maxBytes)
    assertEquals(1000, fetchRequest.maxWait)
    assertEquals(4096, fetchRequest.minBytes)
    val partitionData = fetchRequest.fetchData(util.Map.of(topicId, topicPartition.topic))
    assertFalse(partitionData.isEmpty, "fetchData should contain the partition")
    assertEquals(1024 * 1024, partitionData.values().iterator().next().maxBytes)
  }

  @Test
  def testConsolidationFetchLeavesHeadroomForWholeBatchOvershoot(): Unit = {
    val segmentBytes = 4096
    val maxMessageBytes = 512
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    // findBatches returns whole batches, so it may exceed the request maxBytes by one batch.
    // Follower append still requires the full returned records block to fit in one segment.
    val log = unifiedLogMock(logStartOffset = 0L, segmentSize = segmentBytes, maxMessageSize = maxMessageBytes)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)

    val props = TestUtils.createBrokerConfig(brokerEndPoint.id, port = brokerEndPoint.port)
    props.setProperty(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_MAX_BYTES_CONFIG, segmentBytes.toString)
    val config = KafkaConfig.fromProps(props)
    val endPoint = new DisklessLeaderEndPoint(
      brokerEndPoint,
      fetchHandler,
      fetchOffsetHandler,
      replicaManager,
      config,
      QuotaFactory.UNBOUNDED_QUOTA,
      () => MetadataVersion.LATEST_PRODUCTION,
      () => 7L
    )

    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )

    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))
    assertTrue(result.result.isPresent)

    val fetchRequest = result.result.get.fetchRequest.build()
    val partitionData = fetchRequest.fetchData(util.Map.of(topicId, topicPartition.topic))
    val maxBytes = partitionData.values().iterator().next().maxBytes
    // Leave one max-message-sized batch of headroom below segment.bytes.
    assertEquals(segmentBytes - maxMessageBytes, maxBytes)
    assertTrue(maxBytes + maxMessageBytes <= segmentBytes)
  }

  @Test
  def testConsolidationFetchClampsUnsafeSegmentConfigToOneByte(): Unit = {
    val segmentBytes = 1024 * 1024
    val maxMessageBytes = segmentBytes + 12
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val log = unifiedLogMock(logStartOffset = 0L, segmentSize = segmentBytes, maxMessageSize = maxMessageBytes)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )

    val result = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))
    assertTrue(result.result.isPresent)

    val fetchRequest = result.result.get.fetchRequest.build()
    val partitionData = fetchRequest.fetchData(util.Map.of(topicId, topicPartition.topic))
    assertEquals(1, partitionData.values().iterator().next().maxBytes)
  }

  @Test
  def testConsolidationFetchConfigDefaults(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerEndPoint.id, port = brokerEndPoint.port)
    val config = KafkaConfig.fromProps(props)

    assertEquals(10 * 1024 * 1024, config.disklessConsolidationFetchMaxBytes)
    assertEquals(64 * 1024 * 1024, config.disklessConsolidationFetchResponseMaxBytes)
    assertEquals(8 * 1024 * 1024, config.disklessConsolidationFetchMinBytes)
    assertEquals(1000, config.disklessConsolidationFetchMaxWaitMs)
    assertEquals(1, config.disklessConsolidationNumFetchers)
    assertEquals(0, config.disklessConsolidationFindBatchesMaxPerPartition)
    assertEquals(4, config.disklessConsolidationFetchMetadataThreadPoolSize)
    assertEquals(8, config.disklessConsolidationFetchDataThreadPoolSize)
  }

  /** Single-batch MemoryRecords starting at `baseOffset` with `numRecords` records of `valueSize` bytes each. */
  private def singleBatch(baseOffset: Long, numRecords: Int, valueSize: Int): MemoryRecords = {
    val value = new Array[Byte](valueSize)
    val records = (0 until numRecords).map(_ => new SimpleRecord(value)).toArray
    MemoryRecords.withRecords(baseOffset, Compression.NONE, records: _*)
  }

  /**
   * One coordinate holding `batchCount` byte-contiguous single-record physical batches (offsets
   * `baseOffset .. baseOffset + batchCount - 1`), modelling a `commit_file_v2` coalesced run: multiple
   * physical batches materialized into a single MemoryRecords.
   */
  private def coalescedCoordinate(baseOffset: Long, batchCount: Int, valueSize: Int): MemoryRecords = {
    val batches = (0 until batchCount).map(i => singleBatch(baseOffset + i, 1, valueSize))
    val buffer = ByteBuffer.allocate(batches.map(_.sizeInBytes()).sum)
    batches.foreach(b => buffer.put(b.buffer().duplicate()))
    buffer.flip()
    MemoryRecords.readableRecords(buffer)
  }

  private def baseOffsets(records: Records): Seq[Long] =
    records.batches().iterator().asScala.map(_.baseOffset()).toSeq

  /**
   * Runs [[DisklessLeaderEndPoint.fetch]] with the fetch handler returning `records`, a per-partition
   * request carrying `fetchOffset`/`maxBytes`, and a healthy local log (so no tiered redirect fires and
   * the records survive to the response).
   */
  private def fetchClampedRecords(records: Records, fetchOffset: Long, maxBytes: Int): Records = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(partition.localLogOrException).thenReturn(localLog)
    when(localLog.logStartOffset).thenReturn(0L)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    val fetchData = new FetchPartitionData(
      Errors.NONE,
      1000L,
      0L,
      records,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    partitionData.put(topicPartition, new FetchRequest.PartitionData(topicId, fetchOffset, 0L, maxBytes, Optional.of(Int.box(0))))
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000_000)
    endPoint.fetch(fetchBuilder).get(topicPartition).records.asInstanceOf[Records]
  }

  @Test
  def testFetchClampStopsAtBatchBoundaryOnceOverBudget(): Unit = {
    // Three equal-sized physical batches at offsets 0,1,2. With maxBytes just under the size of the
    // first two batches, the first is taken (progress), the second is the one that crosses the budget
    // and is still included (bounded overshoot), and the third is dropped.
    val batch0 = singleBatch(baseOffset = 0L, numRecords = 1, valueSize = 256)
    val size = batch0.sizeInBytes()
    val records = new ConcatenatedRecords(util.List.of(
      batch0,
      singleBatch(baseOffset = 1L, numRecords = 1, valueSize = 256),
      singleBatch(baseOffset = 2L, numRecords = 1, valueSize = 256)
    ))

    val clamped = fetchClampedRecords(records, fetchOffset = 0L, maxBytes = 2 * size - 1)

    assertEquals(Seq(0L, 1L), baseOffsets(clamped))
    assertEquals(2 * size, clamped.sizeInBytes())
  }

  @Test
  def testFetchClampSkipsBatchesBelowFetchOffset(): Unit = {
    // A prior fetch truncated a coalesced coordinate, so this re-fetch starts mid-run at offset 1.
    // Batches ending before the fetch offset must be dropped so the follower does not re-append them.
    val records = new ConcatenatedRecords(util.List.of(
      singleBatch(baseOffset = 0L, numRecords = 1, valueSize = 64),
      singleBatch(baseOffset = 1L, numRecords = 1, valueSize = 64),
      singleBatch(baseOffset = 2L, numRecords = 1, valueSize = 64)
    ))

    val clamped = fetchClampedRecords(records, fetchOffset = 1L, maxBytes = 10 * 1024 * 1024)

    assertEquals(Seq(1L, 2L), baseOffsets(clamped))
  }

  @Test
  def testFetchClampAlwaysEmitsOneBatchForProgress(): Unit = {
    // maxBytes smaller than a single batch: the first batch is still emitted (progress) and the next
    // batch is dropped, so a small budget cannot stall the fetcher.
    val batch0 = singleBatch(baseOffset = 0L, numRecords = 1, valueSize = 512)
    val records = new ConcatenatedRecords(util.List.of(
      batch0,
      singleBatch(baseOffset = 1L, numRecords = 1, valueSize = 512)
    ))

    val clamped = fetchClampedRecords(records, fetchOffset = 0L, maxBytes = 1)

    assertEquals(Seq(0L), baseOffsets(clamped))
    assertEquals(batch0.sizeInBytes(), clamped.sizeInBytes())
  }

  @Test
  def testFetchClampIsNoOpWhenBlockFitsBudget(): Unit = {
    // Everything fits under maxBytes and starts at the fetch offset: the original records instance is
    // returned unchanged to preserve the ConcatenatedRecords zero-copy send path.
    val records = new ConcatenatedRecords(util.List.of(
      singleBatch(baseOffset = 0L, numRecords = 1, valueSize = 64),
      singleBatch(baseOffset = 1L, numRecords = 1, valueSize = 64)
    ))

    val clamped = fetchClampedRecords(records, fetchOffset = 0L, maxBytes = 10 * 1024 * 1024)

    assertSame(records, clamped)
  }

  @Test
  def testFetchClampKeepsCoalescedBlockWithinSegmentEndToEnd(): Unit = {
    // With segment.bytes tuned small (supported, e.g. for eager tiering), the control plane can admit two
    // coalesced coordinates (each a multi-batch commit_file_v2 run) because its per-partition byte limit
    // is applied at coordinate granularity and always includes the coordinate that crosses the limit.
    // Their combined size then exceeds segment.bytes, which would make the follower's single append fail
    // with RecordBatchTooLargeException. buildFetch reserves maxBytes = segment.bytes - max.message.bytes;
    // the per-batch clamp in fetch() must trim the block back within the segment, splitting a coalesced
    // coordinate mid-run if needed.
    val segmentBytes = 4096
    val maxMessageBytes = 1024

    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = replicaManagerMock()
    val log = unifiedLogMock(logStartOffset = 0L, segmentSize = segmentBytes, maxMessageSize = maxMessageBytes)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)
    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(replicaManager.getPartitionOrError(topicPartition)).thenReturn(Right(partition))

    // Coordinate A (offsets 0..4) and B (offsets 5..8), each physical batch < max.message.bytes.
    val coordA = coalescedCoordinate(baseOffset = 0L, batchCount = 5, valueSize = 500)
    val coordB = coalescedCoordinate(baseOffset = 5L, batchCount = 4, valueSize = 500)
    val combined = new ConcatenatedRecords(util.List.of(coordA, coordB))
    assertTrue(combined.sizeInBytes() > segmentBytes,
      s"precondition: the unclamped block (${combined.sizeInBytes()}) must overflow the segment ($segmentBytes)")

    val fetchData = new FetchPartitionData(
      Errors.NONE, 1000L, 0L, combined,
      Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val fetchState = new PartitionFetchState(
      Optional.of(topicId), 0L, Optional.empty(), 4, ReplicaState.FETCHING, Optional.empty()
    )

    // buildFetch reserves the segment-safe budget, then fetch() enforces it per batch on the same request.
    val replicaFetch = endPoint.buildFetch(util.Map.of(topicPartition, fetchState))
    assertTrue(replicaFetch.result.isPresent)
    val requestedMaxBytes = replicaFetch.result.get.partitionData.get(topicPartition).maxBytes
    assertEquals(segmentBytes - maxMessageBytes, requestedMaxBytes)

    val clamped = endPoint.fetch(replicaFetch.result.get.fetchRequest).get(topicPartition).records.asInstanceOf[Records]

    // Solved: the block the follower appends now fits the segment (no RecordBatchTooLargeException)...
    assertTrue(clamped.sizeInBytes() <= segmentBytes,
      s"clamped block (${clamped.sizeInBytes()}) must fit the segment ($segmentBytes)")
    // ...within the classic one-batch overshoot bound...
    assertTrue(clamped.sizeInBytes() <= requestedMaxBytes + maxMessageBytes)
    // ...and it was actually trimmed (a coalesced coordinate split mid-run), starting at the fetch offset.
    assertTrue(clamped.sizeInBytes() < combined.sizeInBytes())
    assertEquals(0L, baseOffsets(clamped).head)
  }
}
