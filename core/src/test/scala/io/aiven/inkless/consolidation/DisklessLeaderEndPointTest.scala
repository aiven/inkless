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

import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
import kafka.cluster.Partition
import kafka.server.{KafkaConfig, QuotaFactory, ReplicaManager, ReplicaQuota}
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.{KafkaStorageException, NotLeaderOrFollowerException, UnknownTopicOrPartitionException}
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, OffsetsForLeaderEpochResponse}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.storage.log.FetchPartitionData
import org.apache.kafka.server.{PartitionFetchState, ReplicaState}
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{doNothing, mock, verify, when}

import java.util
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
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))
    newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
  }

  @Test
  def testBuildFetchProducesReplicaFetch(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(11L)
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
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    val responseMap = Map(topicIdPartition -> fetchData).asJava
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(responseMap))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
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
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)

    assertTrue(endPoint.fetchEpochEndOffsets(util.Map.of()).isEmpty)
  }

  @Test
  def testFetchEpochEndOffsetsUndefinedEpoch(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
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
    val replicaManager = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(0L)
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
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
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
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
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
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchKeepsDisklessErrorWhenLookupFailsButDisklessAlreadyFailed(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchSetsUnknownServerErrorAndUnknownLogStartWhenLocalLogStartAheadOfHighWatermark(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }

  @Test
  def testFetchSetsUnknownLogStartWhenLocalLogUnavailable(): Unit = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = mock(classOf[ReplicaManager])
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
      false,
      Optional.empty()
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
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
    val endPoint = consolidatedPrefixEndPoint(localLogStartOffset = 0L, disklessStart = 100L, highWatermark = 200L, remoteLogEnabled = true)

    val emptyRequest = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, emptyRequest).setMaxBytes(100_000)
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
    val replicaManager = mock(classOf[ReplicaManager])
    val partition = mock(classOf[Partition])
    val localLog = mock(classOf[UnifiedLog])
    when(localLog.logStartOffset).thenReturn(0L)
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
      false,
      Optional.empty()
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
    val replicaManager = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(0L)
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
    assertEquals(20971520, partitionData.values().iterator().next().maxBytes)
  }

  @Test
  def testConsolidationFetchConfigDefaults(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerEndPoint.id, port = brokerEndPoint.port)
    val config = KafkaConfig.fromProps(props)

    assertEquals(1 * 1024 * 1024, config.disklessConsolidationFetchMaxBytes)
    assertEquals(10 * 1024 * 1024, config.disklessConsolidationFetchResponseMaxBytes)
    assertEquals(1, config.disklessConsolidationFetchMinBytes)
    assertEquals(500, config.disklessConsolidationFetchMaxWaitMs)
    assertEquals(1, config.disklessConsolidationNumFetchers)
    assertEquals(0, config.disklessConsolidationFindBatchesMaxPerPartition)
    assertEquals(4, config.disklessConsolidationFetchMetadataThreadPoolSize)
    assertEquals(8, config.disklessConsolidationFetchDataThreadPoolSize)
  }
}
