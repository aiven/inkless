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
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, OffsetsForLeaderEpochResponse}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
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
      false
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
      false
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

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val result = invoke(endPoint)

    assertEquals(new OffsetAndEpoch(10L, 5), result)

    val captor = ArgumentCaptor.forClass(classOf[org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition])
    verify(job).add(eqTo(topicPartition), captor.capture())
    assertEquals(expectedTimestamp, captor.getValue.timestamp)
    assertEquals(topicPartition.partition, captor.getValue.partitionIndex)
    assertEquals(3, captor.getValue.currentLeaderEpoch)
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
  def testFetchReturnsUnknownLogStartOffsetWhenDisklessErrorEvenIfPartitionAvailable(): Unit = {
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
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code, pd.errorCode)
    assertEquals(-1L, pd.logStartOffset)
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
      false
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
      false
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
      false
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
      false
    )
    when(fetchHandler.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(Map(topicIdPartition -> fetchData).asJava))

    val endPoint = newEndPoint(fetchHandler, fetchOffsetHandler, replicaManager)
    val partitionData = new util.HashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchBuilder = FetchRequest.Builder.forReplica(12, 1, 1L, 100, 1, partitionData).setMaxBytes(100_000)

    val pd = endPoint.fetch(fetchBuilder).get(topicPartition)
    assertEquals(Errors.NONE.code, pd.errorCode)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, pd.logStartOffset)
  }
}
