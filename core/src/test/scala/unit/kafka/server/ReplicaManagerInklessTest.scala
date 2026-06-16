/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import io.aiven.inkless.common.SharedState
import io.aiven.inkless.config.InklessConfig
import io.aiven.inkless.consolidation.{ConsolidatedDisklessLogPruner, ConsolidationFetcherManager}
import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
import io.aiven.inkless.control_plane.{BatchInfo, BatchMetadata, ControlPlane, ControlPlaneException, FindBatchResponse, DeleteRecordsResponse => CpDeleteRecordsResponse}
import io.aiven.inkless.produce.AppendHandler
import kafka.cluster.Partition
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{InklessMetadataView, KRaftMetadataCache}
import kafka.server.share.DelayedShareFetch
import kafka.utils.TestUtils
import kafka.utils.TestUtils.waitUntilTrue
import kafka.log.LogManager
import org.apache.kafka.common.{DirectoryId, IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderPartition, OffsetForLeaderTopic}
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image._
import org.apache.kafka.metadata.{InitDisklessLogFields, LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.purgatory.{DelayedDeleteRecords, DelayedOperationPurgatory, DelayedRemoteFetch, DelayedRemoteListOffsets, TopicPartitionOperationKey}
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.server.util.timer.MockTimer
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.apache.kafka.storage.internals.checkpoint.LazyOffsetCheckpoints
import org.apache.kafka.storage.internals.log.{AppendOrigin, AsyncOffsetReadFutureHolder, FetchDataInfo, LogConfig, LogDirFailureChannel, LogOffsetMetadata, LogOffsetSnapshot, LogReadResult, OffsetResultHolder, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{Answers, ArgumentMatchers, MockedConstruction, Mockito}

import java.io.File
import java.util
import java.util.{Collections, Optional, OptionalInt, OptionalLong, Properties}
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}
import java.util.function.Consumer
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class ReplicaManagerInklessTest {

  private val metrics = new Metrics
  private val time = new MockTime
  private var quotaManager: QuotaManagers = _
  private var alterPartitionManager: AlterPartitionManager = _

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)
    alterPartitionManager = mock(classOf[AlterPartitionManager])
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "", "")
  }

  @AfterEach
  def tearDown(): Unit = {
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  val RECORDS: MemoryRecords = MemoryRecords.withRecords(
    2.toByte, 0L, Compression.NONE, TimestampType.CREATE_TIME, 123L, 0.toShort, 0, 0, false, new SimpleRecord(0, "hello".getBytes())
  )
  val disklessTopicPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless")
  val classicTopicPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "classic")

  @Test
  def testAppendDisklessEntries(): Unit = {
    val entriesPerPartition = Map(disklessTopicPartition -> RECORDS)
    val disklessResponse = Map(disklessTopicPartition -> new PartitionResponse(Errors.NONE))
    val disklessFutureResult = CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
      disklessResponse.asJava
    )
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(disklessFutureResult)
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesPerPartition,
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1)).apply(disklessResponse)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDisklessLeaderEpochDelegatesToMetadataView(): Unit = {
    val replicaManager = createReplicaManager(List(disklessTopicPartition.topic()))
    try {
      when(replicaManager.inklessMetadataView().getDisklessLeaderEpoch(disklessTopicPartition.topicPartition()))
        .thenReturn(7)

      assertEquals(7, replicaManager.disklessLeaderEpoch(disklessTopicPartition.topicPartition()))
      verify(replicaManager.inklessMetadataView()).getDisklessLeaderEpoch(disklessTopicPartition.topicPartition())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testClassicToDisklessStartOffsetDelegatesToMetadataView(): Unit = {
    val replicaManager = createReplicaManager(List(disklessTopicPartition.topic()))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(42L)

      assertEquals(42L, replicaManager.classicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendValidDisklessAndInvalidClassic(): Unit = {
    val entriesPerPartition = Map(
      disklessTopicPartition -> RECORDS,
      classicTopicPartition -> RECORDS,
    )
    val disklessResponse = Map(disklessTopicPartition -> new PartitionResponse(Errors.NONE))
    val disklessFutureResult = CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
      disklessResponse.asJava
    )
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(disklessFutureResult)
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesPerPartition,
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))
        .apply(
          disklessResponse ++
            // ReplicaManager will always reply with UNKNOWN_TOPIC_OR_PARTITION because topic does not exist.
            Map(classicTopicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendDisklessAndClassicEntries(): Unit = {
    val entriesPerPartition = Map(
      disklessTopicPartition -> RECORDS,
      classicTopicPartition -> RECORDS,
    )
    val disklessResponse = Map(disklessTopicPartition -> new PartitionResponse(Errors.NONE))
    val disklessFutureResult = CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
      disklessResponse.asJava
    )
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(disklessFutureResult)
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      val topicDelta = new TopicsDelta(TopicsImage.EMPTY)
      topicDelta.replay(new TopicRecord()
        .setName(classicTopicPartition.topic)
        .setTopicId(classicTopicPartition.topicId)
      )
      topicDelta.replay(new PartitionRecord()
        .setTopicId(classicTopicPartition.topicId)
        .setPartitionId(classicTopicPartition.partition)
        .setLeader(1)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
        .setReplicas(List[Integer](1).asJava)
        .setIsr(List[Integer](1).asJava)
      )

      val metadataImage = imageFromTopics(topicDelta.apply())
      replicaManager.applyDelta(topicDelta, metadataImage)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesPerPartition,
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))
        .apply(
          disklessResponse ++
            Map(classicTopicPartition -> new PartitionResponse(Errors.NONE, 0, -1, 0))
        )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendWithInvalidDisklessAndValidCLassic(): Unit = {
    val entriesPerPartition = Map(
      disklessTopicPartition -> RECORDS,
      classicTopicPartition -> RECORDS,
    )
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
          util.Map.of(disklessTopicPartition, new PartitionResponse(Errors.INVALID_REQUEST))
      ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      val topicDelta = new TopicsDelta(TopicsImage.EMPTY)
      topicDelta.replay(new TopicRecord()
        .setName(classicTopicPartition.topic)
        .setTopicId(classicTopicPartition.topicId)
      )
      topicDelta.replay(new PartitionRecord()
        .setTopicId(classicTopicPartition.topicId)
        .setPartitionId(classicTopicPartition.partition)
        .setLeader(1)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
        .setReplicas(List[Integer](1).asJava)
        .setIsr(List[Integer](1).asJava)
      )

      val metadataImage = imageFromTopics(topicDelta.apply())
      replicaManager.applyDelta(topicDelta, metadataImage)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesPerPartition,
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))
        .apply(Map(
          // diskless entries get an INVALID_REQUEST
          disklessTopicPartition -> new PartitionResponse(Errors.INVALID_REQUEST),
          // classic entries get a regular response, in this case the topic does not exist
          classicTopicPartition -> new PartitionResponse(Errors.NONE, 0, -1, 0)
        ))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendWithInvalidDisklessAndClassic(): Unit = {
    val entriesPerPartition = Map(
      disklessTopicPartition -> RECORDS,
      classicTopicPartition -> RECORDS,
    )
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.failedFuture[util.Map[TopicIdPartition, PartitionResponse]](
           new Exception()
        ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesPerPartition,
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))
        .apply(Map(
          // diskless entries get an UNKNOWN_SERVER_ERROR for the write failure
          disklessTopicPartition -> new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR),
          // classic entries get a regular response, in this case the topic does not exist
          classicTopicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION),
        ))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendDisklessSwitchPendingReturnsReplicaNotAvailable(): Unit = {
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
          util.Map.of[TopicIdPartition, PartitionResponse]()
        ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(disklessTopicPartition -> RECORDS),
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))(
        Map(disklessTopicPartition -> new PartitionResponse(Errors.REPLICA_NOT_AVAILABLE))
      )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendDisklessSwitchPendingWithReadyPartitions(): Unit = {
    val switchedPartition = new TopicIdPartition(Uuid.randomUuid(), 1, "diskless")
    val switchedPartitionResponse = Map(switchedPartition -> new PartitionResponse(Errors.NONE))
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
          switchedPartitionResponse.asJava
        ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(switchedPartition.topicPartition()))
        .thenReturn(100L)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(disklessTopicPartition -> RECORDS, switchedPartition -> RECORDS),
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))(
        Map(
          disklessTopicPartition -> new PartitionResponse(Errors.REPLICA_NOT_AVAILABLE),
          switchedPartition -> new PartitionResponse(Errors.NONE),
        )
      )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendDisklessSwitchPendingWithHandlerFailure(): Unit = {
    val switchedPartition = new TopicIdPartition(Uuid.randomUuid(), 1, "diskless")
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.failedFuture[util.Map[TopicIdPartition, PartitionResponse]](
          new Exception("control plane unavailable")
        ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(switchedPartition.topicPartition()))
        .thenReturn(100L)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(disklessTopicPartition -> RECORDS, switchedPartition -> RECORDS),
        responseCallback = responseCallback,
      )

      // Pending partition is unaffected by the handler failure — only ready entries get the error fallback
      verify(responseCallback, times(1))(
        Map(
          disklessTopicPartition -> new PartitionResponse(Errors.REPLICA_NOT_AVAILABLE),
          switchedPartition -> new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR),
        )
      )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendDisklessSwitchPendingWithClassicEntries(): Unit = {
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
          util.Map.of[TopicIdPartition, PartitionResponse]()
        ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      appendHandlerCtor.close()
    }

    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(disklessTopicPartition -> RECORDS, classicTopicPartition -> RECORDS),
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))(
        Map(
          disklessTopicPartition -> new PartitionResponse(Errors.REPLICA_NOT_AVAILABLE),
          classicTopicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION),
        )
      )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testAppendAlwaysDisklessPartitionIsUnaffected(): Unit = {
    val expectedResponse = Map(disklessTopicPartition -> new PartitionResponse(Errors.NONE))
    val appendHandlerCtorMockInitializer: MockedConstruction.MockInitializer[AppendHandler] = {
      case (mock, _) =>
        when(mock.handle(any(), any())).thenReturn(CompletableFuture.completedFuture[util.Map[TopicIdPartition, PartitionResponse]](
          expectedResponse.asJava
        ))
    }
    val appendHandlerCtor = mockConstruction(classOf[AppendHandler], appendHandlerCtorMockInitializer)
    val replicaManager = createReplicaManager(List(disklessTopicPartition.topic()))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      val responseCallback = mock(classOf[Function[Map[TopicIdPartition, PartitionResponse], Unit]])
      replicaManager.appendRecords(
        timeout = 0,
        requiredAcks = -1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(disklessTopicPartition -> RECORDS),
        responseCallback = responseCallback,
      )

      verify(responseCallback, times(1))(expectedResponse)
      verify(appendHandlerCtor.constructed().get(0), times(1)).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      appendHandlerCtor.close()
    }
  }

  @Test
  def testDeleteRecordsHybridBelowStartOffsetDeletesOnlyLocalLog(): Unit = {
    val controlPlane = mock(classOf[ControlPlane])
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(controlPlane),
      disklessManagedReplicasEnabled = true,
    )
    try {
      val partition = setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)

      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(disklessTopicPartition.topicPartition() -> 50L),
        responseCallback = response => responseData = response,
      )

      assertEquals(Errors.NONE.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(50L, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
      assertEquals(50L, partition.localLogOrException.logStartOffset)
      verify(controlPlane, never()).deleteRecords(anyList())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsHybridPastStartOffsetDeletesLocalThenDiskless(): Unit = {
    val controlPlane = mock(classOf[ControlPlane])
    when(controlPlane.deleteRecords(anyList())).thenReturn(util.List.of(CpDeleteRecordsResponse.success(150L)))
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(controlPlane),
      topicIdMapping = Map(disklessTopicPartition.topic() -> disklessTopicPartition.topicId()),
      disklessManagedReplicasEnabled = true,
    )
    try {
      val partition = setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)

      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(disklessTopicPartition.topicPartition() -> 150L),
        responseCallback = response => responseData = response,
      )

      waitUntilTrue(() => responseData.nonEmpty, "Hybrid delete records response was not completed")
      assertEquals(Errors.NONE.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(150L, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
      assertEquals(101L, partition.localLogOrException.logStartOffset)
      verify(controlPlane, timeout(5000)).deleteRecords(anyList())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsMixedClassicAndPureDisklessSplitsRequest(): Unit = {
    val controlPlane = mock(classOf[ControlPlane])
    when(controlPlane.deleteRecords(anyList())).thenReturn(util.List.of(CpDeleteRecordsResponse.success(80L)))
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(controlPlane),
      topicIdMapping = Map(disklessTopicPartition.topic() -> disklessTopicPartition.topicId()),
    )
    try {
      val classicPartition = setupHybridLeaderPartition(replicaManager, classicTopicPartition, localEndOffset = 50L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(
          classicTopicPartition.topicPartition() -> 20L,
          disklessTopicPartition.topicPartition() -> 80L,
        ),
        responseCallback = response => responseData = response,
      )

      waitUntilTrue(() => responseData.size == 2, "Mixed delete records response was not completed")
      assertEquals(Errors.NONE.code, responseData(classicTopicPartition.topicPartition()).errorCode)
      assertEquals(20L, responseData(classicTopicPartition.topicPartition()).lowWatermark)
      assertEquals(20L, classicPartition.localLogOrException.logStartOffset)
      assertEquals(Errors.NONE.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(80L, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
      verify(controlPlane, timeout(5000)).deleteRecords(anyList())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsMixedClassicAndDisklessWithoutInterceptorOnlyFailsDiskless(): Unit = {
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      inklessSharedStateEnabled = false,
    )
    try {
      val classicPartition = setupHybridLeaderPartition(replicaManager, classicTopicPartition, localEndOffset = 50L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(
          classicTopicPartition.topicPartition() -> 20L,
          disklessTopicPartition.topicPartition() -> 80L,
        ),
        responseCallback = response => responseData = response,
      )

      assertEquals(Errors.NONE.code, responseData(classicTopicPartition.topicPartition()).errorCode)
      assertEquals(20L, responseData(classicTopicPartition.topicPartition()).lowWatermark)
      assertEquals(20L, classicPartition.localLogOrException.logStartOffset)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsConsolidatingDisklessWithLocalLogDeletesLocalThenDiskless(): Unit = {
    val controlPlane = mock(classOf[ControlPlane])
    when(controlPlane.deleteRecords(anyList())).thenReturn(util.List.of(CpDeleteRecordsResponse.success(150L)))
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(controlPlane),
      topicIdMapping = Map(disklessTopicPartition.topic() -> disklessTopicPartition.topicId()),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    )
    try {
      val partition = setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(disklessTopicPartition.topicPartition() -> 150L),
        responseCallback = response => responseData = response,
      )

      waitUntilTrue(() => responseData.nonEmpty, "Consolidating delete records response was not completed")
      assertEquals(Errors.NONE.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(150L, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
      assertEquals(101L, partition.localLogOrException.logStartOffset)
      verify(controlPlane, timeout(5000)).deleteRecords(anyList())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsHybridDoesNotDeleteDisklessWhenLocalDeleteFails(): Unit = {
    val controlPlane = mock(classOf[ControlPlane])
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(controlPlane),
      topicIdMapping = Map(disklessTopicPartition.topic() -> disklessTopicPartition.topicId()),
      disklessManagedReplicasEnabled = true,
    )
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)

      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(disklessTopicPartition.topicPartition() -> 150L),
        responseCallback = response => responseData = response,
      )

      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
      verify(controlPlane, never()).deleteRecords(anyList())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsHybridHighWatermarkDeletesLocalThenDiskless(): Unit = {
    val controlPlane = mock(classOf[ControlPlane])
    when(controlPlane.deleteRecords(anyList())).thenReturn(util.List.of(CpDeleteRecordsResponse.success(200L)))
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(controlPlane),
      topicIdMapping = Map(disklessTopicPartition.topic() -> disklessTopicPartition.topicId()),
      disklessManagedReplicasEnabled = true,
    )
    try {
      val partition = setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)
      @volatile var responseData: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] = Map.empty
      replicaManager.deleteRecords(
        timeout = 0L,
        offsetPerPartition = Map(disklessTopicPartition.topicPartition() -> DeleteRecordsRequest.HIGH_WATERMARK),
        responseCallback = response => responseData = response,
      )
      waitUntilTrue(() => responseData.nonEmpty, "HIGH_WATERMARK hybrid delete records response was not completed")
      assertEquals(Errors.NONE.code, responseData(disklessTopicPartition.topicPartition()).errorCode)
      assertEquals(200L, responseData(disklessTopicPartition.topicPartition()).lowWatermark)
      // Local log should be deleted up to classicToDisklessStartOffset (not HIGH_WATERMARK)
      assertEquals(101L, partition.localLogOrException.logStartOffset)
      verify(controlPlane, timeout(5000)).deleteRecords(anyList())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchDisklessBelowStartOffsetReadsFromClassicLogWhenManagedReplicasEnabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    // Given managed replicas enabled
    val replicaManager = spy(createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp), disklessManagedReplicasEnabled = true))
    try {
      // Given a diskless topic with classicToDisklessStartOffset = 100
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition())).thenReturn(100L)

      // Given a classic log read result for offsets below diskless start offset
      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      // When fetching messages below the diskless start offset
      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the request is served from the unified log and not from diskless fetch handler
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchDisklessBelowStartOffsetFailsWhenManagedReplicasDisabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    // Given managed replicas are disabled
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = false,
    ))
      
    try {
      // Given a diskless topic with classicToDisklessStartOffset = 100
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition())).thenReturn(100L)

      // When fetching messages below the diskless start offset
      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the request fails
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.INVALID_REQUEST, responseData(disklessTopicPartition).error)
      assertEquals(MemoryRecords.EMPTY, responseData(disklessTopicPartition).records)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchDisklessSwitchPendingReadsFromClassicLogWhenManagedReplicasEnabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp), disklessManagedReplicasEnabled = true))

    try {
      // Given a diskless topic with classicToDisklessStartOffset = -2 (switch pending)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the request is served from the unified log (classic path) because switch is still pending
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchDisklessSwitchPendingFailsWhenManagedReplicasDisabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = false,
    ))

    try {
      // Given a diskless topic with classicToDisklessStartOffset = -2 (switch pending)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the request fails because managed replicas are disabled and data is still in UnifiedLog
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.INVALID_REQUEST, responseData(disklessTopicPartition).error)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchFullDisklessTopicRoutesDiskless(): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(110L)
    when(findBatchResponse.estimatedByteSize(50L)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
    ))
    try {
      // Given a full diskless topic with classicToDisklessStartOffset = -1 (never switched)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the request is served from diskless path (not from UnifiedLog)
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.NONE, responseData(disklessTopicPartition).error)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), times(1)).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchFailDisklessWhenFromReplicaAndUnmanagedReplicas(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    // Given a topic partition that is fully diskless (never switched) and managed replicas are disabled
    val replicaManager = spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = false))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      val fetchParams = new FetchParams(
        1, 1L, // follower fetch
        10L, 100, 200, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 10L, 0L, 123, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }

      // When we try to fetch messages from the diskless topic partition with a follower fetch,
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then we should get an immediate empty response.
      assertNotNull(responseData)
      assertEquals(0, responseData.size)
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @ParameterizedTest(name = "testFetchDisklessAtOrAboveStartOffsetUsesDiskless with managedReplicasEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testFetchDisklessAtOrAboveClassicToDisklessStartOffset(managedReplicasEnabled: Boolean): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(110L)
    when(findBatchResponse.estimatedByteSize(100L)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = managedReplicasEnabled,
    ))
    try {
      // Given a diskless topic with classicToDisklessStartOffset = 100
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition())).thenReturn(100L)

      // When fetching messages at the diskless start offset
      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 100L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the request is served from diskless fetch handler and not from the unified log
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.NONE, responseData(disklessTopicPartition).error)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), times(1)).handle(any(), any())
      verify(cp, times(1)).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  private def stubConsolidatingPartitionWithLocalLeo(replicaManager: ReplicaManager, localLeo: Long): Unit = {
    stubConsolidatingPartitionWithLocalLeoFor(replicaManager, disklessTopicPartition, localLeo)
  }

  private def stubConsolidatingPartitionWithLocalLeoFor(replicaManager: ReplicaManager, tp: TopicIdPartition, localLeo: Long): Unit = {
    val mockPartition = mock(classOf[Partition])
    val mockLog = mock(classOf[UnifiedLog])
    when(mockLog.logEndOffset).thenReturn(localLeo)
    when(mockPartition.log).thenReturn(Some(mockLog))
    doReturn(Right(mockPartition)).when(replicaManager).getPartitionOrError(
      ArgumentMatchers.eq(tp.topicPartition()))
  }

  private def stubConsolidatingPartitionWithoutLocalLog(replicaManager: ReplicaManager): Unit = {
    val mockPartition = mock(classOf[Partition])
    when(mockPartition.log).thenReturn(None)
    doReturn(Right(mockPartition)).when(replicaManager).getPartitionOrError(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()))
  }

  private def stubConsolidatingPartitionAsError(replicaManager: ReplicaManager, error: Errors): Unit = {
    doReturn(Left(error)).when(replicaManager).getPartitionOrError(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()))
  }

  private def waitForFetchResponse(responseData: => Map[TopicIdPartition, FetchPartitionData]): Unit = {
    TestUtils.waitUntilTrue(() => responseData != null, "Expected fetch response", 10_000)
  }

  @Test
  def testFetchConsolidatingDisklessBelowLocalLeoReadsFromUnifiedLogWhenManagedReplicasEnabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)
      stubConsolidatingPartitionWithLocalLeo(replicaManager, localLeo = 200L)

      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessPartitionOfflineReturnsKafkaStorageError(): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(250L)
    when(findBatchResponse.estimatedByteSize(120L)).thenReturn(0)
    when(findBatchResponse.errors()).thenReturn(Errors.OFFSET_OUT_OF_RANGE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))

    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      stubConsolidatingPartitionAsError(replicaManager, Errors.KAFKA_STORAGE_ERROR)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => responseData = response.toMap
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.KAFKA_STORAGE_ERROR, responseData(disklessTopicPartition).error)
      assertEquals(MemoryRecords.EMPTY, responseData(disklessTopicPartition).records)
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessPartitionMissingReplicaReturnsNotLeaderOrFollower(): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      stubConsolidatingPartitionAsError(replicaManager, Errors.NOT_LEADER_OR_FOLLOWER)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => responseData = response.toMap
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responseData(disklessTopicPartition).error)
      assertEquals(MemoryRecords.EMPTY, responseData(disklessTopicPartition).records)
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessUnknownPartitionReturnsUnknownTopicOrPartition(): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      stubConsolidatingPartitionAsError(replicaManager, Errors.UNKNOWN_TOPIC_OR_PARTITION)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => responseData = response.toMap
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, responseData(disklessTopicPartition).error)
      assertEquals(MemoryRecords.EMPTY, responseData(disklessTopicPartition).records)
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessErrorAppendedWhenOtherPartitionReadsFromUnifiedLog(): Unit = {
    val disklessTopicPartition2 = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless2")
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic(), disklessTopicPartition2.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic(), disklessTopicPartition2.topic()),
      topicIdMapping = Map(disklessTopicPartition2.topic() -> disklessTopicPartition2.topicId()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition2.topicPartition()))
        .thenReturn(100L)

      doReturn(Left(Errors.KAFKA_STORAGE_ERROR)).when(replicaManager).getPartitionOrError(
        ArgumentMatchers.eq(disklessTopicPartition.topicPartition()))
      stubConsolidatingPartitionWithLocalLeoFor(replicaManager, disklessTopicPartition2, localLeo = 200L)

      doReturn(Seq(disklessTopicPartition2 ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty()),
        disklessTopicPartition2 -> new PartitionData(disklessTopicPartition2.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(2, responseData.size)
      assertEquals(Errors.KAFKA_STORAGE_ERROR, responseData(disklessTopicPartition).error)
      assertEquals(MemoryRecords.EMPTY, responseData(disklessTopicPartition).records)
      assertEquals(Errors.NONE, responseData(disklessTopicPartition2).error)
      assertEquals(RECORDS, responseData(disklessTopicPartition2).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessAboveClassicStartButBelowLocalLeoReadsFromUnifiedLog(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(50L)
      stubConsolidatingPartitionWithLocalLeo(replicaManager, localLeo = 200L)

      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 75L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessAtOrAboveLocalLeoUsesDisklessPath(): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(110L)
    when(findBatchResponse.estimatedByteSize(100L)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)
      stubConsolidatingPartitionWithLocalLeo(replicaManager, localLeo = 100L)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 100L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.NONE, responseData(disklessTopicPartition).error)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), times(1)).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessWithNoLocalLogUsesDisklessDespiteFetchBelowClassicStart(): Unit = {
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        110L, 100L,
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor = mockFetchHandler(disklessResponse)
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(110L)
    when(findBatchResponse.estimatedByteSize(50L)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)
      stubConsolidatingPartitionWithoutLocalLog(replicaManager)

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(Errors.NONE, responseData(disklessTopicPartition).error)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), times(1)).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingMetadataButConsolidationDisabledUsesClassicStartOffsetRule(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)

      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFetchConsolidatingDisklessSwitchPendingReadsFromUnifiedLogWhenConsolidationEnabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
      disklessRemoteStorageConsolidationEnabled = true,
      consolidatingDisklessTopics = Set(disklessTopicPartition.topic()),
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
      // Ensure we don't generate an additional invalid response from the consolidating-partition check.
      stubConsolidatingPartitionWithoutLocalLog(replicaManager)

      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
          Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        -1, -1L,
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      waitForFetchResponse(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFindBatches(): Unit = {
    // Given
    val disklessTopicPartition2 = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless2")
    val fetchOffset = 15L  // fetch offset below the high watermark
    val hwm = fetchOffset + 10L

    // Prepare the FindBatchResponse to be returned by the ControlPlane for the requests that will be executed
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(hwm)
    when(findBatchResponse.estimatedByteSize(fetchOffset)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse, findBatchResponse))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(
      disklessTopicPartition -> new FetchPartitionData(
        Errors.NONE, hwm, 0L, RECORDS, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false
      ),
      disklessTopicPartition2 -> new FetchPartitionData(
        Errors.NONE, hwm, 0L, RECORDS, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false
      )
    )
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)
    val replicaManager = try {
      createReplicaManager(
        List(disklessTopicPartition.topic(), disklessTopicPartition2.topic()),
        controlPlane = Some(cp),
        topicIdMapping = Map(disklessTopicPartition2.topic() -> disklessTopicPartition2.topicId())
      )
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    // When we try to fetch messages from the diskless topic partitions with a consumer fetch, with a request that
    // does not specify the topic id
    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      100, 100, 100, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val fetchInfos = Seq(
      disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty()),
      // fetch from diskless2 without specifying the topic id
      new TopicIdPartition(Uuid.ZERO_UUID, disklessTopicPartition2.topicPartition()) -> new PartitionData(Uuid.ZERO_UUID, fetchOffset, 0L, 123, Optional.empty()),
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
    }
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

    // Response also includes records from the topic without topic id
    assertNotNull(responseData)
    assertEquals(2, responseData.size)
    assertEquals(disklessResponse, responseData)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchDisklessSatisfiesMinBytes(): Unit = {
    // Given
    val minBytes = RECORDS.sizeInBytes()  // Same as the size of the records to ensure it satisfies minBytes
    val fetchOffset = 15L  // fetch offset below the high watermark
    val hwm = fetchOffset + 10L
    val maxWaitMs = 10L
    val maxBytes = 10

    // Prepare the FindBatchResponse to be returned by the ControlPlane when it tries to complete the delayed fetch.
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(hwm)
    when(findBatchResponse.estimatedByteSize(fetchOffset)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        hwm, 0L,  // log offset range
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp))
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    // When we try to fetch messages from the diskless topic partition with a consumer fetch
    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      maxWaitMs, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val fetchInfos = Seq(
      disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty())
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
    }
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)
    // Then we should get a response with the diskless topic partition data,
    // as tryComplete can fulfill the minBytes requirement and force complete the delayed fetch.
    assertNotNull(responseData)
    assertEquals(1, responseData.size)
    assertEquals(disklessResponse(disklessTopicPartition), responseData(disklessTopicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchDisklessAndClassicSatisfiesMinBytes(): Unit = {
    // Given
    val minBytes = RECORDS.sizeInBytes() * 2  // Same as the size of the records from both topic types to ensure it satisfies minBytes
    val fetchOffset = 15L  // fetch offset below the high watermark
    val hwm = fetchOffset + 10L
    val maxWaitMs = 10L
    val maxBytes = 10

    // Prepare the FindBatchResponse to be returned by the ControlPlane when it tries to complete the delayed fetch.
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(hwm)
    when(findBatchResponse.estimatedByteSize(fetchOffset)).thenReturn(RECORDS.sizeInBytes()) // first half of the bytes available from diskless
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        hwm, 0L,  // log offset range
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)

    val replicaManager = try {
      // spy to inject readFromLog mock
      spy(createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp)))
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    // Prepare the classic topic partition response
    doReturn(Seq(classicTopicPartition ->
      new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
        Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
      ))
    ).when(replicaManager).readFromLog(any(), any(), any(), any())
    val partition = mock(classOf[Partition])
    val endOffset = new LogOffsetMetadata(11L, 0L, RECORDS.sizeInBytes())  // next half of bytes available from classic
    val offsetSnapshot = new LogOffsetSnapshot(0L, endOffset, endOffset, endOffset)
    when(partition.fetchOffsetSnapshot(any(), any())).thenReturn(offsetSnapshot)
    doReturn(partition)
      .when(replicaManager).getPartitionOrException(classicTopicPartition.topicPartition())

    // When we try to fetch messages from both topic types partitions with a consumer fetch
    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      maxWaitMs, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val fetchInfos = Seq(
      disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty()),
      classicTopicPartition -> new PartitionData(classicTopicPartition.topicId(), 1L, 0L, 123, Optional.empty()),
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
    }
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)
    // Then we should get a response with the both topic types data
    assertNotNull(responseData)
    assertEquals(2, responseData.size)
    // diskless topic partition data
    assertEquals(disklessResponse(disklessTopicPartition), responseData(disklessTopicPartition))
    // classic topic partition data
    val classicPartitionResponse = responseData(classicTopicPartition)
    assertEquals(Errors.NONE, classicPartitionResponse.error)
    assertEquals(0L, classicPartitionResponse.logStartOffset)
    assertEquals(10L, classicPartitionResponse.highWatermark)
    assertEquals(RECORDS, classicPartitionResponse.records)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchDisklessAndClassicSatisfiesMinBytesJustWithClassicRecords(): Unit = {
    // Given
    val minBytes = RECORDS.sizeInBytes()  // Satisfied just with the records from the classic topic
    val fetchOffset = 15L  // fetch offset below the high watermark
    val hwm = fetchOffset + 10L
    val maxWaitMs = 10L
    val maxBytes = 10

    // Prepare the FindBatchResponse to be returned by the ControlPlane when it tries to complete the delayed fetch.
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(hwm)
    when(findBatchResponse.estimatedByteSize(fetchOffset)).thenReturn(RECORDS.sizeInBytes()) // first half of the bytes available from diskless
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(
        Errors.NONE,
        hwm, 0L,  // log offset range
        RECORDS,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    )
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)

    val replicaManager = try {
      // spy to inject readFromLog mock
      spy(createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp)))
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    // Prepare the classic topic partition response
    doReturn(Seq(classicTopicPartition ->
      new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
        Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
      ))
    ).when(replicaManager).readFromLog(any(), any(), any(), any())
    val partition = mock(classOf[Partition])
    val endOffset = new LogOffsetMetadata(11L, 0L, RECORDS.sizeInBytes())  // next half of bytes available from classic
    val offsetSnapshot = new LogOffsetSnapshot(0L, endOffset, endOffset, endOffset)
    when(partition.fetchOffsetSnapshot(any(), any())).thenReturn(offsetSnapshot)
    doReturn(partition)
      .when(replicaManager).getPartitionOrException(classicTopicPartition.topicPartition())

    // When we try to fetch messages from both topic types partitions with a consumer fetch
    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      maxWaitMs, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val fetchInfos = Seq(
      disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty()),
      classicTopicPartition -> new PartitionData(classicTopicPartition.topicId(), 1L, 0L, 123, Optional.empty()),
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
    }
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)
    // Then we should get a response with the both topic types data
    assertNotNull(responseData)
    assertEquals(2, responseData.size)
    // diskless topic partition data
    assertEquals(disklessResponse(disklessTopicPartition), responseData(disklessTopicPartition))
    // classic topic partition data
    val classicPartitionResponse = responseData(classicTopicPartition)
    assertEquals(Errors.NONE, classicPartitionResponse.error)
    assertEquals(0L, classicPartitionResponse.logStartOffset)
    assertEquals(10L, classicPartitionResponse.highWatermark)
    assertEquals(RECORDS, classicPartitionResponse.records)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchDisklessLessThanMinBytes(): Unit = {
    // Given
    val minBytes = Int.MaxValue  // Set minBytes to a value larger than the size of the records to ensure it does not satisfy minBytes
    val fetchOffset = 15L
    val hwm = fetchOffset  // high watermark is the same as fetch offset, so no new data available
    val maxWaitMs = 1000L  // wait enough for delayed fetch to expire
    val maxBytes = 10

    // Prepare the FindBatchResponse to be returned by the ControlPlane when it tries to complete the delayed fetch.
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(hwm)
    when(findBatchResponse.estimatedByteSize(fetchOffset)).thenReturn(RECORDS.sizeInBytes())
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(Errors.NONE, fetchOffset - 10L, 0L, RECORDS, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false))
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp))
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      maxWaitMs, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val fetchInfos = Seq(
      disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty())
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val latch = new CountDownLatch(1)
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
      latch.countDown()
    }
    // Ensure no delayed fetch is in the purgatory before we start
    assertEquals(0, replicaManager.delayedFetchPurgatory.numDelayed())
    // When we try to fetch messages from the diskless topic partition with a consumer fetch
    // and the response does not satisfy minBytes, it should be delayed in the purgatory
    // until the delayed fetch expires.
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)
    assertNull(responseData)
    assertEquals(1, replicaManager.delayedFetchPurgatory.numDelayed())

    latch.await(10, TimeUnit.SECONDS) // Wait for the delayed fetch to expire
    replicaManager.delayedFetchPurgatory.checkAndComplete(new TopicPartitionOperationKey(disklessTopicPartition.topicPartition()))
    assertEquals(0, replicaManager.delayedFetchPurgatory.numDelayed())
    assertNotNull(responseData)
    assertEquals(1, responseData.size)
    assertEquals(disklessResponse(disklessTopicPartition), responseData(disklessTopicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchDisklessAndClassicLessThanMinBytes(): Unit = {
    // Given
    val minBytes = Int.MaxValue  // Set minBytes to a value larger than the size of the records to ensure it does not satisfy minBytes
    val fetchOffset = 15L
    val hwm = fetchOffset  // high watermark is the same as fetch offset, so no new data available
    val maxWaitMs = 1000L  // wait enough for delayed fetch to expire
    val maxBytes = 10

    // Prepare the FindBatchResponse to be returned by the ControlPlane when it tries to complete the delayed fetch.
    val batchMetadata = mock(classOf[BatchMetadata])
    when(batchMetadata.topicIdPartition()).thenReturn(disklessTopicPartition)
    val batch = mock(classOf[BatchInfo])
    when(batch.metadata()).thenReturn(batchMetadata)
    val findBatchResponse = mock(classOf[FindBatchResponse])
    when(findBatchResponse.batches()).thenReturn(util.List.of(batch))
    when(findBatchResponse.highWatermark()).thenReturn(hwm)
    when(findBatchResponse.estimatedByteSize(fetchOffset)).thenReturn(minBytes - 10L)
    when(findBatchResponse.errors()).thenReturn(Errors.NONE)
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenReturn(util.List.of(findBatchResponse))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(Errors.NONE, 10L, 0L, RECORDS, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false))
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp)))
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    // Prepare the classic topic partition response
    doReturn(Seq(classicTopicPartition ->
      new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
        Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
      ))
    ).when(replicaManager).readFromLog(any(), any(), any(), any())
    val partition = mock(classOf[Partition])
    val endOffset = new LogOffsetMetadata(11L, 0L, RECORDS.sizeInBytes())  // next half of bytes available from classic
    val offsetSnapshot = new LogOffsetSnapshot(0L, endOffset, endOffset, endOffset)
    when(partition.fetchOffsetSnapshot(any(), any())).thenReturn(offsetSnapshot)
    doReturn(partition)
      .when(replicaManager).getPartitionOrException(classicTopicPartition.topicPartition())

    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      maxWaitMs, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val disklessPartitionData = new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty())
    val classicPartitionData = new PartitionData(classicTopicPartition.topicId(), 1L, 0L, 123, Optional.empty())
    val fetchInfos = Seq(
      disklessTopicPartition -> disklessPartitionData,
      classicTopicPartition -> classicPartitionData,
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val latch = new CountDownLatch(1)
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
      latch.countDown()
    }
    // Ensure no delayed fetch is in the purgatory before we start
    assertEquals(0, replicaManager.delayedFetchPurgatory.numDelayed())
    // When we try to fetch messages from both types of topic partitions with a consumer fetch
    // and the response does not satisfy minBytes, it should be delayed in the purgatory
    // until the delayed fetch expires.
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)
    assertNull(responseData)
    assertEquals(1, replicaManager.delayedFetchPurgatory.numDelayed())

    latch.await(10, TimeUnit.SECONDS) // Wait for the delayed fetch to expire
    replicaManager.delayedFetchPurgatory.checkAndComplete(new TopicPartitionOperationKey(disklessTopicPartition.topicPartition()))
    assertEquals(0, replicaManager.delayedFetchPurgatory.numDelayed())
    assertNotNull(responseData)
    assertEquals(2, responseData.size)
    assertEquals(disklessResponse(disklessTopicPartition), responseData(disklessTopicPartition))
    val classicPartitionResponse = responseData(classicTopicPartition)
    assertEquals(Errors.NONE, classicPartitionResponse.error)
    assertEquals(0L, classicPartitionResponse.logStartOffset)
    assertEquals(10L, classicPartitionResponse.highWatermark)
    assertEquals(RECORDS, classicPartitionResponse.records)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchFailingDisklessAndValidClassicLessThanMinBytes(): Unit = {
    // Given
    val minBytes = Int.MaxValue  // Set minBytes to a value larger than the size of the records to ensure it does not satisfy minBytes
    val fetchOffset = 15L
    val maxWaitMs = 1000L  // wait enough for delayed fetch to expire
    val maxBytes = 10

    // Prepare the FindBatchResponse to be returned by the ControlPlane when it tries to complete the delayed fetch.
    val cp = mock(classOf[ControlPlane])
    when(cp.findBatches(any(), any(), any())).thenThrow(new ControlPlaneException("Error in control plane"))

    // Prepare the FetchHandler response to be called when the forceComplete method is invoked.
    val disklessResponse = Map(disklessTopicPartition ->
      new FetchPartitionData(Errors.KAFKA_STORAGE_ERROR, -1, -1, MemoryRecords.EMPTY, Optional.empty, OptionalLong.empty, Optional.empty, OptionalInt.empty, false))
    val fetchHandlerCtor: MockedConstruction[FetchHandler] = mockFetchHandler(disklessResponse)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), controlPlane = Some(cp)))
    } finally {
      fetchHandlerCtor.close()
    }

    try {
    // Prepare the classic topic partition response
    doReturn(Seq(classicTopicPartition ->
      new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(1L, 0L, 0), RECORDS),
        Optional.empty(), 10L, 0L, 10L, 0L, 0L, OptionalLong.empty(), Errors.NONE
      ))
    ).when(replicaManager).readFromLog(any(), any(), any(), any())
    val partition = mock(classOf[Partition])
    val endOffset = new LogOffsetMetadata(11L, 0L, RECORDS.sizeInBytes())  // next half of bytes available from classic
    val offsetSnapshot = new LogOffsetSnapshot(0L, endOffset, endOffset, endOffset)
    when(partition.fetchOffsetSnapshot(any(), any())).thenReturn(offsetSnapshot)
    doReturn(partition)
      .when(replicaManager).getPartitionOrException(classicTopicPartition.topicPartition())

    val fetchParams = new FetchParams(
      -1, -1L, // not follower fetch
      maxWaitMs, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty()
    )
    val disklessPartitionData = new PartitionData(disklessTopicPartition.topicId(), fetchOffset, 0L, 123, Optional.empty())
    val classicPartitionData = new PartitionData(classicTopicPartition.topicId(), 1L, 0L, 123, Optional.empty())
    val fetchInfos = Seq(
      disklessTopicPartition -> disklessPartitionData,
      classicTopicPartition -> classicPartitionData,
    )
    @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
    val latch = new CountDownLatch(1)
    val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
      responseData = response.toMap
      latch.countDown()
    }
    // Ensure no delayed fetch is in the purgatory before we start
    assertEquals(0, replicaManager.delayedFetchPurgatory.numDelayed())
    // When we try to fetch messages from both types of topic partitions with a consumer fetch
    // and the response does not satisfy minBytes, it should be delayed in the purgatory
    // until the delayed fetch expires.
    replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)
    assertEquals(0, replicaManager.delayedFetchPurgatory.numDelayed())

    latch.await(10, TimeUnit.SECONDS) // Wait for the delayed fetch to expire
    assertNotNull(responseData)
    assertEquals(2, responseData.size)
    assertEquals(disklessResponse(disklessTopicPartition), responseData(disklessTopicPartition))
    val classicPartitionResponse = responseData(classicTopicPartition)
    assertEquals(Errors.NONE, classicPartitionResponse.error)
    assertEquals(0L, classicPartitionResponse.logStartOffset)
    assertEquals(10L, classicPartitionResponse.highWatermark)
    assertEquals(RECORDS, classicPartitionResponse.records)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochDisklessWithControlPlaneError(): Unit = {
    val errorResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.of(new UnknownTopicOrPartitionException("not found")),
      Optional.empty()
    )

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(errorResult))
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    val requestedEpochInfo = Seq(
      new OffsetForLeaderTopic()
        .setTopic(disklessTopicPartition.topic())
        .setPartitions(util.List.of(
          new OffsetForLeaderPartition()
            .setPartition(disklessTopicPartition.partition())
            .setLeaderEpoch(0)
        ))
    )

    val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

    assertEquals(1, result.size)
    val topicResult = result.head
    assertEquals(disklessTopicPartition.topic(), topicResult.topic())
    assertEquals(1, topicResult.partitions().size())
    val partitionResult = topicResult.partitions().get(0)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionResult.errorCode())
    assertEquals(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET, partitionResult.endOffset())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochSwitchPendingUsesLocalLog(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NONE.code, partitionResult.errorCode())
      assertEquals(101L, partitionResult.endOffset())
      verify(jobMock, never()).add(any(), any())
      verify(jobMock, never()).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochHybridAtSwitchBoundaryUsesLocalLog(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NONE.code, partitionResult.errorCode())
      assertEquals(101L, partitionResult.endOffset())
      verify(jobMock, never()).add(any(), any())
      verify(jobMock, never()).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochHybridAtSwitchBoundaryUsesFollowerLocalLog(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      val partition = setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      val remoteLeaderId = replicaManager.config.brokerId + 1
      partition.makeFollower(
        partitionRegistration(
          remoteLeaderId,
          leaderEpoch = 1,
          isr = Array(remoteLeaderId, replicaManager.config.brokerId),
          partitionEpoch = 1,
          replicas = Array(replicaManager.config.brokerId, remoteLeaderId)),
        isNew = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava),
        Some(disklessTopicPartition.topicId()))
      assertFalse(partition.isLeader)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NONE.code, partitionResult.errorCode())
      assertEquals(101L, partitionResult.endOffset())
      verify(jobMock, never()).add(any(), any())
      verify(jobMock, never()).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochHybridAtSwitchBoundaryRejectsLaggingFollowerLocalLog(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      val partition = setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 50L)
      val remoteLeaderId = replicaManager.config.brokerId + 1
      partition.makeFollower(
        partitionRegistration(
          remoteLeaderId,
          leaderEpoch = 1,
          isr = Array(remoteLeaderId),
          partitionEpoch = 1,
          replicas = Array(replicaManager.config.brokerId, remoteLeaderId)),
        isNew = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava),
        Some(disklessTopicPartition.topicId()))
      assertFalse(partition.isLeader)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(101L)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResult.errorCode())
      verify(jobMock, never()).add(any(), any())
      verify(jobMock, never()).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochHybridFallsBackToDisklessWhenClassicLogCannotAnswer(): Unit = {
    val disklessResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 200L, Optional.of[Integer](0))))
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessResult))
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NONE.code, partitionResult.errorCode())
      assertEquals(200L, partitionResult.endOffset())
      verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
      verify(jobMock).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochHybridFallsBackToDisklessWhenClassicOffsetIsUndefined(): Unit = {
    val disklessResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 200L, Optional.of[Integer](1))))
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessResult))
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(1)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NONE.code, partitionResult.errorCode())
      assertEquals(200L, partitionResult.endOffset())
      verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
      verify(jobMock).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochHybridInvalidSwitchOffsetReturnsError(): Unit = {
    val replicaManager = createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    try {
      setupHybridLeaderPartition(replicaManager, disklessTopicPartition, localEndOffset = 101L)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(-3L)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, partitionResult.errorCode())
      assertEquals(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET, partitionResult.endOffset())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochDisklessWithoutFetchOffsetHandlerReturnsError(): Unit = {
    val replicaManager = createReplicaManager(
      List(disklessTopicPartition.topic()),
      disklessManagedReplicasEnabled = true,
      inklessSharedStateEnabled = false)
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(0)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, partitionResult.errorCode())
      assertEquals(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET, partitionResult.endOffset())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testLastOffsetForLeaderEpochDisklessUndefinedEpochDoesNotFetchOffset(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    var fetchOffsetHandlerMock: FetchOffsetHandler = null
    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        fetchOffsetHandlerMock = handlerMock
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true)
    } finally {
      fetchOffsetHandlerCtor.close()
    }
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      val requestedEpochInfo = Seq(
        new OffsetForLeaderTopic()
          .setTopic(disklessTopicPartition.topic())
          .setPartitions(util.List.of(
            new OffsetForLeaderPartition()
              .setPartition(disklessTopicPartition.partition())
              .setLeaderEpoch(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH)
          ))
      )

      val result = replicaManager.lastOffsetForLeaderEpoch(requestedEpochInfo)

      val partitionResult = result.head.partitions().get(0)
      assertEquals(Errors.NONE.code, partitionResult.errorCode())
      assertEquals(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET, partitionResult.endOffset())
      assertEquals(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH, partitionResult.leaderEpoch())
      verify(fetchOffsetHandlerMock, never()).createJob()
      verify(jobMock, never()).add(any(), any())
      verify(jobMock, never()).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetPureDisklessRoutesToDisklessPathWhenNoSwitch(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val taskFuture = new CompletableFuture[OffsetResultHolder.FileRecordsOrError]()
    when(jobMock.add(any[TopicPartition], any[ListOffsetsPartition])).thenReturn(taskFuture)
    when(jobMock.cancelHandler()).thenReturn(CompletableFuture.completedFuture(null))

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = false))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    // No switch — pure diskless: classicToDisklessStartOffset == -1. Combined with managed
    // replicas disabled, the router falls into case 1 and routes the lookup to the diskless
    // control plane.
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

    // Complete the diskless task with a successful offset before invoking fetchOffset so the
    // callback can resolve synchronously.
    taskFuture.complete(new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 200L, Optional.of[Integer](0)))))

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(200L, partitionResponse.offset())

    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(replicaManager, never()).fetchOffsetForTimestamp(any(), anyLong(), any(), any(), anyBoolean())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetLatestUsesClassicPathWhenSwitchPending(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 50L, Optional.of[Integer](0))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(50L, partitionResponse.offset())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetLatestUsesDisklessPathWhenSwitchComplete(): Unit = {
    val successResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 200L, Optional.of[Integer](0))))

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(successResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(200L, partitionResponse.offset())

    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(replicaManager, never()).fetchOffsetForTimestamp(any(), anyLong(), any(), any(), anyBoolean())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetEarliestUsesClassicPathWhenSwitchPending(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 0L, Optional.of[Integer](0))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(0L, partitionResponse.offset())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetEarliestUsesDisklessPathWhenSwitchCompleteAndManagedReplicasEnabled(): Unit = {
    val successResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 0L, Optional.of[Integer](0))))

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(successResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(0L, partitionResponse.offset())

    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(replicaManager, never()).fetchOffsetForTimestamp(any(), anyLong(), any(), any(), anyBoolean())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetEarliestUsesDisklessPathWhenSwitchCompleteAndManagedReplicasDisabled(): Unit = {
    val successResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 0L, Optional.of[Integer](0))))

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(successResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = false))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(0L, partitionResponse.offset())

    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(replicaManager, never()).fetchOffsetForTimestamp(any(), anyLong(), any(), any(), anyBoolean())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetEarliestUsesClassicPathWhenSwitchCompleteAndClassicHasData(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Stub the local log so classicHasData is true (logStartOffset < classicToDisklessStartOffset).
    val unifiedLogMock = mock(classOf[UnifiedLog])
    when(unifiedLogMock.logStartOffset).thenReturn(0L)
    val logManagerMock = mock(classOf[LogManager])
    when(logManagerMock.getLog(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyBoolean()))
      .thenReturn(Some(unifiedLogMock))
    doReturn(logManagerMock).when(replicaManager).logManager

    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 0L, Optional.of[Integer](0))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(0L, partitionResponse.offset())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetEarliestLocalAlwaysUsesClassicWhenSwitchComplete(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 0L, Optional.of[Integer](0))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 8.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(0L, partitionResponse.offset())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetLatestTieredAlwaysUsesClassicWhenSwitchComplete(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, 0L, Optional.of[Integer](0))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 9.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(0L, partitionResponse.offset())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampUsesClassicPathOnMatch(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Classic returns a real match at offset 42 for the requested timestamp.
    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(123L, 42L, Optional.of[Integer](0))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(42L, partitionResponse.offset())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampFallsBackToDisklessOnNoMatch(): Unit = {
    val disklessSuccess = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(456L, 200L, Optional.of[Integer](1))))

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessSuccess))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Classic returns no match (empty result, no error).
    val emptyResultHolder = new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]())
    doReturn(emptyResultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(200L, partitionResponse.offset())
    assertEquals(456L, partitionResponse.timestamp())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampPropagatesClassicError(): Unit = {
    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Classic raises NotLeaderOrFollower (a real error). classicFetchOffset converts it to a
    // response with errorCode = NOT_LEADER_OR_FOLLOWER which must be propagated to the client.
    doThrow(new org.apache.kafka.common.errors.NotLeaderOrFollowerException("not leader"))
      .when(replicaManager).fetchOffsetForTimestamp(
        ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 1.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResponse.errorCode())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampBatchesDisklessFallbackWhenClassicRemoteSyncEmpty(): Unit = {
    // Classic delegates to a remote/tiered lookup whose taskFuture is *already complete* by the
    // time route() inspects it (e.g. RLM short-circuits or hits a metadata cache). withFallback's
    // thenCompose then runs the fallback factory synchronously on the request thread, before
    // ReplicaManager.fetchOffset has called job.start(), so the diskless fallback can be added
    // to the existing per-request job and batched with the rest of the request — no fresh job
    // and no extra control-plane RPC are needed.
    val emptyClassicRemoteResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.empty())
    val classicRemoteTaskFuture =
      CompletableFuture.completedFuture[OffsetResultHolder.FileRecordsOrError](emptyClassicRemoteResult)
    val classicRemoteJobFuture = new CompletableFuture[Void]()
    val classicRemoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      classicRemoteJobFuture, classicRemoteTaskFuture)

    val disklessSuccess = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(456L, 200L, Optional.of[Integer](1))))

    val batchJobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(batchJobMock.mustHandle(any())).thenReturn(true)
    when(batchJobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessSuccess))
    when(batchJobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(batchJobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(batchJobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Classic returns a holder pointing at a pre-completed async remote-storage lookup.
    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(classicRemoteHolder))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(200L, partitionResponse.offset())
    assertEquals(456L, partitionResponse.timestamp())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    // The diskless fallback was batched into the per-request job rather than into a fresh one;
    // exactly one job (the outer batch) was created and started exactly once.
    verify(batchJobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(batchJobMock).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampFallsBackToDisklessOnNewJobWhenClassicRemoteAsync(): Unit = {
    // Classic delegates to an async remote/tiered lookup whose taskFuture is *not yet complete*
    // when route() inspects it. withFallback's thenCompose lambda will therefore fire later, on
    // the thread that completes the classic future, by which point the per-request job has
    // already been started by ReplicaManager.fetchOffset. The diskless fallback must use a
    // fresh, independently-started job.
    val classicRemoteTaskFuture = new CompletableFuture[OffsetResultHolder.FileRecordsOrError]()
    val classicRemoteJobFuture = new CompletableFuture[Void]()
    val classicRemoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      classicRemoteJobFuture, classicRemoteTaskFuture)

    val disklessSuccess = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(456L, 200L, Optional.of[Integer](1))))

    val batchJobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(batchJobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(batchJobMock).start()

    // Fallback job created on demand once the async classic future completes empty.
    val fallbackJobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(fallbackJobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessSuccess))
    when(fallbackJobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(fallbackJobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        // First createJob() is the outer batch; second is the diskless fallback created when
        // the async classic remote future eventually completes empty.
        when(handlerMock.createJob()).thenReturn(batchJobMock, fallbackJobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(classicRemoteHolder))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    // Classic future hasn't completed yet → response is parked in the purgatory and the
    // fallback hasn't fired. The outer batch job was started; the fallback job has not been
    // created yet.
    assertNull(responseTopics)
    verify(batchJobMock, never()).add(any(), any())
    verify(batchJobMock).start()
    verify(fallbackJobMock, never()).add(any(), any())
    verify(fallbackJobMock, never()).start()

    // Now complete the classic remote future as empty. This drives the fallback chain
    // synchronously on this thread, which creates and starts the fresh fallback job and
    // ultimately fires the response callback via the purgatory's checkAndComplete hook.
    val emptyClassicRemoteResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.empty())
    classicRemoteTaskFuture.complete(emptyClassicRemoteResult)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(200L, partitionResponse.offset())
    assertEquals(456L, partitionResponse.timestamp())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    // The outer batch job was never used to add a partition because classic returned async.
    verify(batchJobMock, never()).add(any(), any())
    // The diskless fallback was issued on a fresh job created on demand.
    verify(fallbackJobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(fallbackJobMock).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampForwardsClassicRemoteHit(): Unit = {
    // Classic delegates to an async (remote/tiered) lookup that resolves with a real
    // timestamp/offset hit. The wrapper must forward it as-is and never touch diskless.
    val classicRemoteHit = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(789L, 42L, Optional.of[Integer](3))))
    val classicRemoteTaskFuture =
      CompletableFuture.completedFuture[OffsetResultHolder.FileRecordsOrError](classicRemoteHit)
    val classicRemoteJobFuture = new CompletableFuture[Void]()
    val classicRemoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      classicRemoteJobFuture, classicRemoteTaskFuture)

    val batchJobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(batchJobMock.mustHandle(any())).thenReturn(true)
    doNothing().when(batchJobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(batchJobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(classicRemoteHolder))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(42L, partitionResponse.offset())
    assertEquals(789L, partitionResponse.timestamp())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    // No diskless fallback should fire when classic remote answered authoritatively.
    verify(batchJobMock, never()).add(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetMaxTimestampFallsBackToClassicWhenDisklessEmpty(): Unit = {
    // Diskless reports a genuine empty result (no exception, no offset) → fall back to classic.
    val emptyDisklessResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.empty())

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(emptyDisklessResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Classic returns a successful timestamp/offset that the helper substitutes into the
    // wrapped diskless future before completing the purgatory.
    val resultHolder = new OffsetResultHolder(
      Optional.of(new FileRecords.TimestampAndOffset(123L, 7L, Optional.of[Integer](2))))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(7L, partitionResponse.offset())
    assertEquals(123L, partitionResponse.timestamp())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetFallsBackToClassicRemoteStorageWhenDisklessEmpty(): Unit = {
    // Diskless reports a genuine empty result. Classic delegates to an async remote-storage lookup
    // (e.g. the matching offset has been tiered). The wrapper must chain the remote future into
    // the wrapped future so the offset is surfaced to the client.
    val emptyDisklessResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.empty())

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(emptyDisklessResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    // Classic returns a holder that points at an async remote-storage task already resolved with a
    // successful timestamp/offset; the wrapper must chain it through.
    val remoteResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(789L, 42L, Optional.of[Integer](3))))
    val remoteTaskFuture = CompletableFuture.completedFuture[OffsetResultHolder.FileRecordsOrError](remoteResult)
    val remoteJobFuture = new CompletableFuture[Void]()
    val remoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      remoteJobFuture, remoteTaskFuture)
    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(remoteHolder))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    assertEquals(42L, partitionResponse.offset())
    assertEquals(789L, partitionResponse.timestamp())

    verify(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())
    verify(jobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetMaxTimestampClassicRemoteEmptyWithMaybeOffsetsErrorSurfacesError(): Unit = {
    // Diskless empty → fall back to classic. Classic delegates to an async remote-storage lookup
    // that completes empty AND carries a pending maybeOffsetsError. The wrapper must apply the
    // same semantics as DelayedRemoteListOffsets and surface the version-mapped error in the
    // response, even though the classic metadata is no longer attached to the wrapped status.
    val emptyDisklessResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(), Optional.empty())
    val emptyClassicRemoteResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(), Optional.empty())

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(emptyDisklessResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val classicRemoteTaskFuture =
      CompletableFuture.completedFuture[OffsetResultHolder.FileRecordsOrError](emptyClassicRemoteResult)
    val classicRemoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      new CompletableFuture[Void](), classicRemoteTaskFuture)

    val pendingError = new org.apache.kafka.common.errors.OffsetNotAvailableException("retry later")
    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(classicRemoteHolder))
    resultHolder.maybeOffsetsError(Optional.of(pendingError))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.OFFSET_NOT_AVAILABLE.code, partitionResponse.errorCode())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetMaxTimestampClassicRemoteHitBeyondLastFetchableOffsetSurfacesError(): Unit = {
    // Diskless empty → fall back to classic. Classic returns a remote async lookup with a
    // lastFetchableOffset clamp AND a pending maybeOffsetsError. The remote completes with a hit
    // beyond the clamp, which must be surfaced as the pending error rather than as the offset.
    val emptyDisklessResult = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(), Optional.empty())
    // Classic remote returns a hit at offset 100, but lastFetchableOffset is 50 → clamped.
    val classicRemoteHit = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(123L, 100L, Optional.of[Integer](2))))

    val jobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(jobMock.mustHandle(any())).thenReturn(true)
    when(jobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(emptyDisklessResult))
    when(jobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(jobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(jobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val classicRemoteTaskFuture =
      CompletableFuture.completedFuture[OffsetResultHolder.FileRecordsOrError](classicRemoteHit)
    val classicRemoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      new CompletableFuture[Void](), classicRemoteTaskFuture)

    val pendingError = new org.apache.kafka.common.errors.OffsetNotAvailableException("not yet")
    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(classicRemoteHolder))
    resultHolder.lastFetchableOffset(Optional.of(50L))
    resultHolder.maybeOffsetsError(Optional.of(pendingError))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.OFFSET_NOT_AVAILABLE.code, partitionResponse.errorCode())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchOffsetSpecificTimestampClassicRemoteHitBeyondLastFetchableOffsetFallsBackToDiskless(): Unit = {
    // Specific timestamp: classic delegates to an async remote-storage lookup with a
    // lastFetchableOffset clamp but no pending maybeOffsetsError. The remote returns a hit
    // beyond the clamp, which translates to "no match" — and the wrapper must then fall back to
    // diskless instead of returning the clamped offset. The classic remote future is already
    // complete at routing time, so the diskless fallback batches into the per-request job
    // rather than creating a fresh one.
    val disklessSuccess = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(456L, 200L, Optional.of[Integer](1))))
    // Classic remote returns a hit at offset 100, but lastFetchableOffset is 50 → clamped.
    val classicRemoteHit = new OffsetResultHolder.FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(123L, 100L, Optional.of[Integer](2))))

    val batchJobMock = Mockito.mock(classOf[FetchOffsetHandler.Job])
    when(batchJobMock.mustHandle(any())).thenReturn(true)
    when(batchJobMock.add(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessSuccess))
    when(batchJobMock.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    doNothing().when(batchJobMock).start()

    val fetchOffsetHandlerCtorInit: MockedConstruction.MockInitializer[FetchOffsetHandler] = {
      case (handlerMock, _) =>
        when(handlerMock.createJob()).thenReturn(batchJobMock)
    }
    val fetchOffsetHandlerCtor = mockConstruction(classOf[FetchOffsetHandler], fetchOffsetHandlerCtorInit)

    val replicaManager = try {
      spy(createReplicaManager(List(disklessTopicPartition.topic()), disklessManagedReplicasEnabled = true))
    } finally {
      fetchOffsetHandlerCtor.close()
    }

    try {
    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
      .thenReturn(100L)

    val classicRemoteTaskFuture =
      CompletableFuture.completedFuture[OffsetResultHolder.FileRecordsOrError](classicRemoteHit)
    val classicRemoteHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      new CompletableFuture[Void](), classicRemoteTaskFuture)

    val resultHolder = new OffsetResultHolder(
      Optional.empty[FileRecords.TimestampAndOffset](),
      Optional.of(classicRemoteHolder))
    resultHolder.lastFetchableOffset(Optional.of(50L))
    doReturn(resultHolder).when(replicaManager).fetchOffsetForTimestamp(
      ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), anyLong(), any(), any(), anyBoolean())

    val topics = Seq(new ListOffsetsTopic()
      .setName(disklessTopicPartition.topic())
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(disklessTopicPartition.partition())
        .setTimestamp(123L)
        .setCurrentLeaderEpoch(0))))

    @volatile var responseTopics: util.Collection[ListOffsetsTopicResponse] = null
    val callback: Consumer[util.Collection[ListOffsetsTopicResponse]] = (r: util.Collection[ListOffsetsTopicResponse]) => responseTopics = r

    replicaManager.fetchOffset(topics, Set.empty, IsolationLevel.READ_UNCOMMITTED,
      ListOffsetsRequest.CONSUMER_REPLICA_ID, "client", 0, 7.toShort,
      (e, p) => new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex).setErrorCode(e.code),
      callback)

    assertNotNull(responseTopics)
    val partitionResponse = responseTopics.asScala.head.partitions().get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode())
    // The diskless fallback's hit must be surfaced (offset 200 / timestamp 456), not the
    // clamped classic remote hit (offset 100 / timestamp 123).
    assertEquals(200L, partitionResponse.offset())
    assertEquals(456L, partitionResponse.timestamp())
    verify(batchJobMock).add(ArgumentMatchers.eq(disklessTopicPartition.topicPartition()), any())
    verify(batchJobMock).start()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testConsolidationFetcherManagerConstructedWhenRemoteStorageConsolidationEnabled(): Unit = {
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager])
    try {
      val replicaManager = createReplicaManager(
        List(disklessTopicPartition.topic()),
        disklessRemoteStorageConsolidationEnabled = true,
      )
      try {
        assertEquals(1, consolidationCtor.constructed().size())
      } finally {
        replicaManager.shutdown(checkpointHW = false)
      }
    } finally {
      consolidationCtor.close()
    }
  }

  @Test
  def testConsolidatedDisklessLogPrunerNotConstructedWhenRemoteStorageConsolidationDisabled(): Unit = {
    val prunerCtor = mockConstruction(classOf[ConsolidatedDisklessLogPruner])
    try {
      val replicaManager = createReplicaManager(List(disklessTopicPartition.topic()))
      try {
        assertEquals(0, prunerCtor.constructed().size())
      } finally {
        replicaManager.shutdown(checkpointHW = false)
      }
    } finally {
      prunerCtor.close()
    }
  }

  @Test
  def testConsolidatedDisklessLogPrunerConstructedWhenRemoteStorageConsolidationEnabled(): Unit = {
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager])
    val prunerCtor = mockConstruction(classOf[ConsolidatedDisklessLogPruner])
    try {
      val replicaManager = createReplicaManager(
        List(disklessTopicPartition.topic()),
        disklessRemoteStorageConsolidationEnabled = true,
      )
      try {
        assertEquals(1, prunerCtor.constructed().size())
      } finally {
        replicaManager.shutdown(checkpointHW = false)
      }
    } finally {
      prunerCtor.close()
      consolidationCtor.close()
    }
  }

  @Test
  def testConsolidationFetcherManagerWiredOnConsolidatingDisklessBecomeLeader(): Unit = {
    val consolidatingTopic = disklessTopicPartition.topic()
    val tp = disklessTopicPartition.topicPartition()

    val ctorInit: MockedConstruction.MockInitializer[ConsolidationFetcherManager] = {
      case (mock, _) =>
        when(mock.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    }
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager], ctorInit)
    try {
      val replicaManager = createReplicaManager(
        List(consolidatingTopic),
        disklessRemoteStorageConsolidationEnabled = true,
        consolidatingDisklessTopics = Set(consolidatingTopic),
      )
      try {
        val mockCfm = consolidationCtor.constructed().get(0)
        when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
          .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
        val oneReplica = Seq[Integer](1).asJava
        val delta = createLeaderDelta(
          disklessTopicPartition.topicId,
          tp,
          1,
          oneReplica,
          oneReplica,
        )
        replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

        verify(mockCfm, atLeastOnce()).shutdownIdleFetcherThreads()
        verify(mockCfm).removeFetcherForPartitions(Set(tp))
        verify(mockCfm).addFetcherForPartitions(
          Map(tp -> InitialFetchState(
            topicId = Some(disklessTopicPartition.topicId),
            leader = new BrokerEndPoint(-1, "diskless", -1),
            currentLeaderEpoch = 0,
            initOffset = 0
          ))
        )
      } finally {
        replicaManager.shutdown(checkpointHW = false)
        verify(consolidationCtor.constructed().get(0)).shutdown()
      }
    } finally {
      consolidationCtor.close()
    }
  }

  @Test
  def testConsolidationFetcherManagerWiredOnConsolidatingDisklessBecomeFollower(): Unit = {
    val consolidatingTopic = disklessTopicPartition.topic()
    val tp = disklessTopicPartition.topicPartition()

    val ctorInit: MockedConstruction.MockInitializer[ConsolidationFetcherManager] = {
      case (mock, _) =>
        when(mock.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    }
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager], ctorInit)
    try {
      val replicaManager = createReplicaManager(
        List(consolidatingTopic),
        disklessRemoteStorageConsolidationEnabled = true,
        consolidatingDisklessTopics = Set(consolidatingTopic),
      )
      try {
        val mockCfm = consolidationCtor.constructed().get(0)
        when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
          .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
        val delta = createFollowerDelta(
          disklessTopicPartition.topicId,
          tp,
          followerId = 1,
          leaderId = 2,
        )
        replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

        verify(mockCfm, atLeastOnce()).shutdownIdleFetcherThreads()
        verify(mockCfm).removeFetcherForPartitions(Set(tp))
        verify(mockCfm).addFetcherForPartitions(
          Map(tp -> InitialFetchState(
            topicId = Some(disklessTopicPartition.topicId),
            leader = new BrokerEndPoint(-1, "diskless", -1),
            currentLeaderEpoch = 0,
            initOffset = 0
          ))
        )
      } finally {
        replicaManager.shutdown(checkpointHW = false)
        verify(consolidationCtor.constructed().get(0)).shutdown()
      }
    } finally {
      consolidationCtor.close()
    }
  }

  @Test
  def testApplyDeltaCreatesPartitionForDisklessTopicWithLocalLogOnLeader(): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1

    val replicaManager = createReplicaManager(List(topicName))
    try {
      // Pre-create a local log to simulate a post-restart scenario where the log
      // was loaded from disk but no Partition object exists yet.
      replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      assertTrue(replicaManager.logManager.getLog(tp).isDefined)

      // No partition should exist before applying the delta.
      assertEquals(HostedPartition.None, replicaManager.getPartition(tp))

      // Apply a delta that makes this broker the leader.
      val delta = new TopicsDelta(TopicsImage.EMPTY)
      delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
        .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
        .setLeader(brokerId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      // Partition should now exist, be online, sealed, and be the leader.
      val partition = replicaManager.getPartition(tp)
      assertTrue(partition.isInstanceOf[HostedPartition.Online], "Partition should be online")
      val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
      assertTrue(onlinePartition.isLeader, "Partition should be leader")
      assertTrue(onlinePartition.isSealed, "Partition should be sealed")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaAdvancesHwmToSealOffsetForSealedLeaderWithStaleCheckpoint(): Unit = {
    // After restart, makeLeader reloads HW from the on-disk checkpoint file.
    // If the checkpoint is stale (e.g. unclean shutdown, or checkpoint interval
    // hadn't fired since HW advanced), HW will be below the seal offset.
    // Since the partition is sealed, no produces or follower fetches can ever
    // advance HW naturally — consumers cannot read classic data below the seal.
    // applyDelta must detect this and restore HW to the seal offset.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1

    val replicaManager = spy(createReplicaManager(List(topicName)))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 5L)

      // Mark the partition as fully switched with classicToDisklessStartOffset = 10.
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(10L)

      // Apply a delta that makes this broker the leader (post-restart path).
      val delta = new TopicsDelta(TopicsImage.EMPTY)
      delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
        .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
        .setLeader(brokerId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      val partition = replicaManager.getPartition(tp)
      assertTrue(partition.isInstanceOf[HostedPartition.Online], "Partition should be online")
      val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
      assertTrue(onlinePartition.isLeader, "Partition should be leader")
      assertTrue(onlinePartition.isSealed, "Partition should be sealed")
      // HW must have been advanced to the seal offset.
      assertEquals(10L, onlinePartition.localLogOrException.highWatermark,
        "High watermark should be advanced to the seal offset")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaDoesNotAdvanceHwmForSealedLeaderWithCurrentCheckpoint(): Unit = {
    // Post-restart with a fresh checkpoint (HW == seal): no advancement needed.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1

    val replicaManager = spy(createReplicaManager(List(topicName)))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 10L)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(10L)

      val delta = new TopicsDelta(TopicsImage.EMPTY)
      delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
        .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
        .setLeader(brokerId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      val partition = replicaManager.getPartition(tp)
      val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
      // HW should remain at 10 (unchanged, since it's already at the seal).
      assertEquals(10L, onlinePartition.localLogOrException.highWatermark,
        "High watermark should remain at seal offset when already caught up")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaSkipsPartitionForConsolidatingDisklessTopicWithLocalLogOnLeader(): Unit = {
    val topicName = "consolidating-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1

    val ctorInit: MockedConstruction.MockInitializer[ConsolidationFetcherManager] = {
      case (mock, _) =>
        when(mock.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    }
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager], ctorInit)
    try {
      val replicaManager = createReplicaManager(
        List(topicName),
        disklessRemoteStorageConsolidationEnabled = true,
        consolidatingDisklessTopics = Set(topicName),
      )
      try {
        replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
        assertTrue(replicaManager.logManager.getLog(tp).isDefined)

        assertEquals(HostedPartition.None, replicaManager.getPartition(tp))

        val delta = new TopicsDelta(TopicsImage.EMPTY)
        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
        delta.replay(new PartitionRecord()
          .setPartitionId(0)
          .setTopicId(topicId)
          .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
          .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
          .setLeader(brokerId)
          .setLeaderEpoch(0)
          .setPartitionEpoch(0)
        )
        replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

        // No sealed partition should be created for a consolidating diskless topic,
        // even though a local log exists.
        val partition = replicaManager.getPartition(tp)
        assertTrue(partition.isInstanceOf[HostedPartition.Online], "Partition should be online")
        val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
        assertTrue(onlinePartition.isLeader, "Partition should be leader")
        assertFalse(onlinePartition.isSealed, "Partition should NOT be sealed")
      } finally {
        replicaManager.shutdown(checkpointHW = false)
      }
    } finally {
      consolidationCtor.close()
    }
  }

  @Test
  def testApplyDeltaSkipsPartitionForConsolidatingDisklessTopicWithLocalLogOnFollower(): Unit = {
    val topicName = "consolidating-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val ctorInit: MockedConstruction.MockInitializer[ConsolidationFetcherManager] = {
      case (mock, _) =>
        when(mock.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    }
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager], ctorInit)
    try {
      val replicaManager = createReplicaManager(
        List(topicName),
        disklessRemoteStorageConsolidationEnabled = true,
        consolidatingDisklessTopics = Set(topicName),
      )
      try {
        replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
        assertTrue(replicaManager.logManager.getLog(tp).isDefined)

        assertEquals(HostedPartition.None, replicaManager.getPartition(tp))

        val delta = new TopicsDelta(TopicsImage.EMPTY)
        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
        delta.replay(new PartitionRecord()
          .setPartitionId(0)
          .setTopicId(topicId)
          .setReplicas(util.Arrays.asList(brokerId, leaderId))
          .setIsr(util.Arrays.asList(brokerId, leaderId))
          .setLeader(leaderId)
          .setLeaderEpoch(0)
          .setPartitionEpoch(0)
        )
        replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

        // No sealed partition should be created for a consolidating diskless topic on a follower,
        // even though a local log exists.
        val partition = replicaManager.getPartition(tp)
        assertTrue(partition.isInstanceOf[HostedPartition.Online], "Partition should be online")
        val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
        assertFalse(onlinePartition.isLeader, "Partition should be follower")
        assertFalse(onlinePartition.isSealed, "Partition should NOT be sealed")
      } finally {
        replicaManager.shutdown(checkpointHW = false)
      }
    } finally {
      consolidationCtor.close()
    }
  }

  @Test
  def testApplyDeltaCreatesPartitionForDisklessTopicWithLocalLogOnFollower(): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val replicaManager = createReplicaManager(List(topicName))
    try {
      replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      assertTrue(replicaManager.logManager.getLog(tp).isDefined)

      assertEquals(HostedPartition.None, replicaManager.getPartition(tp))

      // Apply a delta that makes this broker a follower.
      val delta = new TopicsDelta(TopicsImage.EMPTY)
      delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, leaderId))
        .setIsr(util.Arrays.asList(brokerId, leaderId))
        .setLeader(leaderId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      val partition = replicaManager.getPartition(tp)
      assertTrue(partition.isInstanceOf[HostedPartition.Online], "Partition should be online")
      val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
      assertFalse(onlinePartition.isLeader, "Partition should be follower")
      assertTrue(onlinePartition.isSealed, "Partition should be sealed")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  // Build a follower delta where `brokerId` is a follower of `leaderId` for the given topic.
  private def disklessFollowerDelta(topicName: String, topicId: Uuid, brokerId: Int, leaderId: Int): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
    delta.replay(new PartitionRecord()
      .setPartitionId(0)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(brokerId, leaderId))
      .setIsr(util.Arrays.asList(brokerId, leaderId))
      .setLeader(leaderId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
    )
    delta
  }

  // Stage a follower partition with a chosen LEO and HW so the next `applyDelta` sees
  // exactly that state — used to simulate a post-restart broker whose local log lags
  // the diskless seal offset.
  private def populateLocalLogAtLeoAndCheckpointedHwm(
      replicaManager: ReplicaManager, tp: TopicPartition,
      log: UnifiedLog, leo: Long, hw: Long): Unit = {
    if (leo > 0) {
      // Append records via the follower path with `partitionLeaderEpoch == 0`. This sets
      // `log.logEndOffset == leo` and, because the batch carries an explicit leader epoch,
      // populates the leader-epoch cache so `log.latestEpoch.isPresent` — which is what
      // makes the catch-up fetcher's `initialFetchOffset` resolve to LEO.
      val simpleRecords = (0L until leo).map(i => new SimpleRecord(s"msg-$i".getBytes)).toArray
      val records = MemoryRecords.withRecords(Compression.NONE, 0, simpleRecords: _*)
      log.appendAsFollower(records, 0)
    }
    // Write the HW into the on-disk checkpoint file. The next `applyDelta` will call
    // `Partition.createLog`, which overwrites the in-memory HW with the checkpointed
    // value (defaulting to 0 when no entry exists), so any HW we want the test to start
    // from has to live on disk before applyDelta runs.
    val checkpoint = replicaManager.highWatermarkCheckpoints(log.parentDir)
    checkpoint.write(util.Collections.singletonMap(tp, java.lang.Long.valueOf(hw)))
    // Keep the in-memory HW in sync for any reads that happen before `applyDelta`
    // re-loads it from the checkpoint.
    log.maybeUpdateHighWatermark(hw)
  }

  @Test
  def testApplyDeltaStartsCatchUpFetcherForDisklessFollowerWithLaggingHwm(): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      // Pre-create the local log with LEO=10, HW=5 — the broker's classic HW lags the seal point.
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 5L)

      // Mark the partition as fully switched with classicToDisklessStartOffset = 10.
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(10L)

      // Apply the follower delta.
      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      // A fetcher must be scheduled against the leader so that HW can advance to the seal offset.
      val leaderEndpoint = ClusterImageTest.IMAGE1.broker(leaderId).listeners().get("PLAINTEXT")
      verify(mockFetcherManager).addFetcherForPartitions(Map(tp -> InitialFetchState(
        topicId = Some(topicId),
        leader = new BrokerEndPoint(leaderId, leaderEndpoint.host(), leaderEndpoint.port()),
        currentLeaderEpoch = 0,
        initOffset = 10L
      )))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaDoesNotStartCatchUpFetcherWhenDisklessFollowerHwmAtSealOffset(): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      // Local log is already caught up to the seal point: LEO=HW=10, seal=10.
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 10L)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(10L)

      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      // Nothing to catch up — no fetcher should be added for this partition.
      verify(mockFetcherManager, never()).addFetcherForPartitions(any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaDoesNotStartCatchUpFetcherForNeverSwitchedDisklessFollower(): Unit = {
    // Never-switched diskless topic (seal == -1) on a broker that has no on-disk
    // classic data: there is nothing here to expose to consumers below a seal, and
    // there is no upper bound to fetch up to. Nothing should be done on this path.
    val topicName = "fully-diskless-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      assertTrue(replicaManager.logManager.getLog(tp).isEmpty)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      assertEquals(HostedPartition.None, replicaManager.getPartition(tp))
      verify(mockFetcherManager, never()).addFetcherForPartitions(any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaSchedulesFetcherForDisklessFollowerDuringSwitchPending(): Unit = {
    // PENDING window: the topic is already flagged diskless but the controller has
    // not yet committed the seal offset (-2). The leader has already sealed its log
    // and frozen its LEO; followers must keep replicating up to that frozen LEO
    // with a normal ReplicaFetcher. Verify that the first follower-side applyDelta
    // during PENDING arms a fetcher pointing at the leader from the follower's LEO.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 5L)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      val leaderEndpoint = ClusterImageTest.IMAGE1.broker(leaderId).listeners().get("PLAINTEXT")
      verify(mockFetcherManager).addFetcherForPartitions(Map(tp -> InitialFetchState(
        topicId = Some(topicId),
        leader = new BrokerEndPoint(leaderId, leaderEndpoint.host(), leaderEndpoint.port()),
        currentLeaderEpoch = 0,
        initOffset = 10L
      )))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaReschedulesFetcherForDisklessFollowerOnLeaderChangeDuringSwitchPending(): Unit = {
    // PENDING + leader change: if the original leader crashes mid-switch and a
    // new leader is elected, the follower's existing fetcher would still target the
    // dead broker and replication would stall (the leader can never commit the seal
    // because it sees no follower fetch traffic). This test pins down that a new
    // leader epoch during PENDING reschedules the fetcher onto the new leader.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val firstLeaderId = 2
    val secondLeaderId = 0

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 5L)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      // First apply: leader == firstLeaderId, leaderEpoch == 0.
      val delta1 = disklessFollowerDelta(topicName, topicId, brokerId, firstLeaderId)
      replicaManager.applyDelta(delta1, imageFromTopics(delta1.apply()))

      val firstEndpoint = ClusterImageTest.IMAGE1.broker(firstLeaderId).listeners().get("PLAINTEXT")
      verify(mockFetcherManager).addFetcherForPartitions(Map(tp -> InitialFetchState(
        topicId = Some(topicId),
        leader = new BrokerEndPoint(firstLeaderId, firstEndpoint.host(), firstEndpoint.port()),
        currentLeaderEpoch = 0,
        initOffset = 10L
      )))

      // Second apply: leader moves to secondLeaderId, leaderEpoch bumps to 1.
      val delta2 = new TopicsDelta(TopicsImage.EMPTY)
      delta2.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta2.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, firstLeaderId, secondLeaderId))
        .setIsr(util.Arrays.asList(brokerId, firstLeaderId, secondLeaderId))
        .setLeader(secondLeaderId)
        .setLeaderEpoch(1)
        .setPartitionEpoch(1)
      )
      replicaManager.applyDelta(delta2, imageFromTopics(delta2.apply()))

      val secondEndpoint = ClusterImageTest.IMAGE1.broker(secondLeaderId).listeners().get("PLAINTEXT")
      verify(mockFetcherManager).addFetcherForPartitions(Map(tp -> InitialFetchState(
        topicId = Some(topicId),
        leader = new BrokerEndPoint(secondLeaderId, secondEndpoint.host(), secondEndpoint.port()),
        currentLeaderEpoch = 1,
        initOffset = 10L
      )))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaDoesNotRescheduleFetcherForDisklessFollowerDuringSwitchPendingOnSameEpoch(): Unit = {
    // Same leader, same leader epoch: the existing fetcher is still valid, so a
    // second applyDelta during PENDING must not redundantly reschedule it.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 10L, hw = 5L)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      // Only the first apply should have armed the fetcher.
      verify(mockFetcherManager, times(1)).addFetcherForPartitions(any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFollowerFetchAtClassicToDisklessStartOffsetReturnsEmptyAndIdle(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
    ))
    try {
      // Given a fully-switched diskless topic with classicToDisklessStartOffset = 100
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)

      // When a follower fetches at offset >= classicToDisklessStartOffset
      val fetchParams = new FetchParams(
        1, 1L, // follower fetch
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 100L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Then the response is empty with HW clamped to the seal offset and we never touched
      // diskless storage or the local log on behalf of the follower.
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      val data = responseData(disklessTopicPartition)
      assertEquals(Errors.NONE, data.error)
      assertEquals(MemoryRecords.EMPTY, data.records)
      assertEquals(100L, data.highWatermark)
      // logStartOffset must NOT advance the follower's local log start offset (would delete classic data).
      assertEquals(0L, data.logStartOffset)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFollowerFetchAtClassicToDisklessStartOffsetEmptyEvenWhenManagedReplicasDisabled(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = false,
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)

      val fetchParams = new FetchParams(
        1, 1L, // follower fetch
        0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 150L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      // Same outcome regardless of managedReplicasEnabled: follower never sees diskless data.
      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      val data = responseData(disklessTopicPartition)
      assertEquals(Errors.NONE, data.error)
      assertEquals(MemoryRecords.EMPTY, data.records)
      assertEquals(100L, data.highWatermark)
      verify(replicaManager, never()).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testFollowerFetchBelowClassicToDisklessStartOffsetReadsFromClassicLog(): Unit = {
    val fetchHandlerCtor = mockFetchHandler(Map.empty)
    val cp = mock(classOf[ControlPlane])
    val replicaManager = spy(createReplicaManager(
      List(disklessTopicPartition.topic()),
      controlPlane = Some(cp),
      disklessManagedReplicasEnabled = true,
    ))
    try {
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(disklessTopicPartition.topicPartition()))
        .thenReturn(100L)

      // Below the seal offset the follower should still be able to catch up via classic.
      doReturn(Seq(disklessTopicPartition ->
        new LogReadResult(
          new FetchDataInfo(new LogOffsetMetadata(50L, 0L, 0), RECORDS),
          Optional.empty(), 100L, 0L, 100L, 0L, 0L, OptionalLong.empty(), Errors.NONE
        ))
      ).when(replicaManager).readFromLog(any(), any(), any(), any())

      val fetchParams = new FetchParams(
        1, 1L, // follower fetch
        0L, 1, 1024, FetchIsolation.LOG_END, Optional.empty()
      )
      val fetchInfos = Seq(
        disklessTopicPartition -> new PartitionData(disklessTopicPartition.topicId(), 50L, 0L, 1024, Optional.empty())
      )

      @volatile var responseData: Map[TopicIdPartition, FetchPartitionData] = null
      val responseCallback = (response: Seq[(TopicIdPartition, FetchPartitionData)]) => {
        responseData = response.toMap
      }
      replicaManager.fetchMessages(fetchParams, fetchInfos, QuotaFactory.UNBOUNDED_QUOTA, responseCallback)

      assertNotNull(responseData)
      assertEquals(1, responseData.size)
      assertEquals(RECORDS, responseData(disklessTopicPartition).records)
      verify(replicaManager, times(1)).readFromLog(any(), any(), any(), any())
      verify(fetchHandlerCtor.constructed().get(0), never()).handle(any(), any())
      verify(cp, never()).findBatches(any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      fetchHandlerCtor.close()
    }
  }

  @Test
  def testIsPartitionSwitchedFromClassicToDiskless(): Unit = {
    val replicaManager = spy(createReplicaManager(List(disklessTopicPartition.topic())))
    try {
      val tp = disklessTopicPartition.topicPartition()

      val classicTp = classicTopicPartition.topicPartition()
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(classicTp))
        .thenReturn(100L)
      assertFalse(replicaManager.isPartitionSwitchedFromClassicToDiskless(classicTopicPartition))

      // Diskless but never-switched partition: classicToDisklessStartOffset == -1.
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      assertFalse(replicaManager.isPartitionSwitchedFromClassicToDiskless(disklessTopicPartition))

      // Switch pending: classicToDisklessStartOffset == -2 (sealed but offset not committed).
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
      assertFalse(replicaManager.isPartitionSwitchedFromClassicToDiskless(disklessTopicPartition))

      // Switched (just sealed, seal at offset 0).
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(0L)
      assertTrue(replicaManager.isPartitionSwitchedFromClassicToDiskless(disklessTopicPartition))

      // Switched (seal at a later offset).
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(100L)
      assertTrue(replicaManager.isPartitionSwitchedFromClassicToDiskless(disklessTopicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaSkipsPartitionForDisklessTopicWithoutLocalLog(): Unit = {
    val topicName = "fully-diskless-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1

    val replicaManager = spy(createReplicaManager(List(topicName)))
    try {
      // Never-switched diskless topic: no local log on this broker AND no committed
      // classicToDisklessStartOffset. Without an explicit stub Mockito would return
      // 0L which means "switched, seal at offset 0" -- not what this test models.
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
      assertTrue(replicaManager.logManager.getLog(tp).isEmpty)

      val delta = new TopicsDelta(TopicsImage.EMPTY)
      delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
        .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
        .setLeader(brokerId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      // No partition should be created for a diskless topic without local data.
      assertEquals(HostedPartition.None, replicaManager.getPartition(tp))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaCreatesPartitionAndStartsCatchUpFetcherForNewlyAddedDisklessReplica(): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      // Newly added replica on a *switched* diskless topic: no local log yet, but
      // the topic has a committed classicToDisklessStartOffset (seal). The broker
      // needs to create a local log on the fly and arm a catch-up fetcher to
      // backfill the classic-era prefix from another replica, so it can later
      // serve reads below the seal if leadership moves to it.
      assertTrue(replicaManager.logManager.getLog(tp).isEmpty)
      assertEquals(HostedPartition.None, replicaManager.getPartition(tp))

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(10L)

      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      // makeFollower must have created the local log + Partition object.
      val partition = replicaManager.getPartition(tp)
      assertTrue(partition.isInstanceOf[HostedPartition.Online], "Partition should be online")
      assertTrue(replicaManager.logManager.getLog(tp).isDefined, "Local log should be created")
      val onlinePartition = partition.asInstanceOf[HostedPartition.Online].partition
      assertFalse(onlinePartition.isLeader, "Partition should be follower")
      assertTrue(onlinePartition.isSealed, "Partition should be sealed")

      // Fresh log starts empty (LEO == HW == 0 < seal == 10), so a catch-up fetcher
      // must be armed against the leader with initialFetchOffset == 0.
      val leaderEndpoint = ClusterImageTest.IMAGE1.broker(leaderId).listeners().get("PLAINTEXT")
      verify(mockFetcherManager).addFetcherForPartitions(Map(tp -> InitialFetchState(
        topicId = Some(topicId),
        leader = new BrokerEndPoint(leaderId, leaderEndpoint.host(), leaderEndpoint.port()),
        currentLeaderEpoch = 0,
        initOffset = 0L
      )))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaDoesNotStartCatchUpFetcherForNewlyAddedReplicaWhenSwitchPending(): Unit = {
    // A newly added replica must NOT spin up a local log + catch-up fetcher while
    // the switch is still pending (seal == -2). There is no upper bound to
    // fetch up to yet, and the controller will re-trigger applyLocalFollowersDelta
    // once it commits the seal (bumping partitionEpoch), at which point the
    // standard path will create the log and arm the fetcher.
    val topicName = "pending-switch-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      assertTrue(replicaManager.logManager.getLog(tp).isEmpty)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val delta = disklessFollowerDelta(topicName, topicId, brokerId, leaderId)
      replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

      assertEquals(HostedPartition.None, replicaManager.getPartition(tp))
      assertTrue(replicaManager.logManager.getLog(tp).isEmpty)
      verify(mockFetcherManager, never()).addFetcherForPartitions(any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def setupHybridLeaderPartition(replicaManager: ReplicaManager,
                                         topicIdPartition: TopicIdPartition,
                                         localEndOffset: Long): Partition = {
    val topicDelta = new TopicsDelta(TopicsImage.EMPTY)
    topicDelta.replay(new TopicRecord()
      .setName(topicIdPartition.topic())
      .setTopicId(topicIdPartition.topicId()))
    topicDelta.replay(new PartitionRecord()
      .setTopicId(topicIdPartition.topicId())
      .setPartitionId(topicIdPartition.partition())
      .setLeader(replicaManager.config.brokerId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
      .setReplicas(List[Integer](replicaManager.config.brokerId).asJava)
      .setIsr(List[Integer](replicaManager.config.brokerId).asJava))

    val (partition, _) = replicaManager.getOrCreatePartition(
      topicIdPartition.topicPartition(),
      topicDelta,
      topicIdPartition.topicId()).get
    partition.makeLeader(
      partitionRegistration(
        replicaManager.config.brokerId,
        leaderEpoch = 0,
        isr = Array(replicaManager.config.brokerId),
        partitionEpoch = 0,
        replicas = Array(replicaManager.config.brokerId)),
      isNew = false,
      new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava),
      None)

    if (localEndOffset > 0) {
      val records = (0L until localEndOffset).map { i =>
        new SimpleRecord(s"key-$i".getBytes, s"value-$i".getBytes)
      }.toArray
      val log = partition.localLogOrException
      log.appendAsLeader(MemoryRecords.withRecords(0L, Compression.NONE, 0, records: _*), 0)
      log.updateHighWatermark(localEndOffset)
    }
    partition
  }

  private def mockFetchHandler(disklessResponse: Map[TopicIdPartition, FetchPartitionData]) = {
    // We use constructor mocking here to inject a FetchHandler mock into ReplicaManager,
    // because ReplicaManager internally constructs its own FetchHandler instance and does not
    // provide a way to inject a mock or test double. This approach is not ideal, as it makes
    // the test more brittle and tightly coupled to the implementation details of ReplicaManager.
    // A cleaner alternative would be to refactor ReplicaManager to accept a FetchHandler via
    // constructor or setter injection, which would improve testability and reduce reliance on
    // mocking internals. Consider refactoring in the future if feasible.
    val fetchHandlerCtorMockInitializer: MockedConstruction.MockInitializer[FetchHandler] = {
      case (mock, _) => when(mock.handle(any(), any())).thenReturn(CompletableFuture.completedFuture(disklessResponse.asJava))
    }
    val fetchHandlerCtor = mockConstruction(classOf[FetchHandler], fetchHandlerCtorMockInitializer)
    fetchHandlerCtor
  }

  private def createReplicaManager(
    disklessTopics: Seq[String],
    controlPlane: Option[ControlPlane] = None,
    topicIdMapping: Map[String, Uuid] = Map.empty,
    disklessManagedReplicasEnabled: Boolean = false,
    disklessRemoteStorageConsolidationEnabled: Boolean = false,
    consolidatingDisklessTopics: Set[String] = Set.empty,
    mockReplicaFetcherManager: Option[ReplicaFetcherManager] = None,
    inklessSharedStateEnabled: Boolean = true,
    initDisklessLogManager: Option[InitDisklessLogManager] = None
  ): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(1, logDirCount = 2)
    if (disklessManagedReplicasEnabled || disklessRemoteStorageConsolidationEnabled) {
      props.put(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
    }
    if (disklessRemoteStorageConsolidationEnabled) {
      props.put(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true")
      props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    }
    props.put(
      ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG,
      (disklessManagedReplicasEnabled || disklessRemoteStorageConsolidationEnabled).toString
    )
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(new Properties()))
    val sharedState = mock(classOf[SharedState], Answers.RETURNS_DEEP_STUBS)
    when(sharedState.time()).thenReturn(Time.SYSTEM)
    val inklessConfigMap = new util.HashMap[String, Object]()
    // Disable lagging consumer feature — not relevant for these tests
    inklessConfigMap.put("fetch.lagging.consumer.thread.pool.size", Integer.valueOf(0))
    when(sharedState.config()).thenReturn(new InklessConfig(inklessConfigMap))
    when(sharedState.controlPlane()).thenReturn(controlPlane.getOrElse(mock(classOf[ControlPlane])))
    when(sharedState.maybeLaggingFetchStorage()).thenReturn(Optional.empty())
    val inklessMetadata = mock(classOf[InklessMetadataView])
    when(inklessMetadata.isDisklessTopic(any())).thenReturn(false)
    when(inklessMetadata.getClassicToDisklessStartOffset(any()))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadata.getTopicId(anyString())).thenAnswer{ invocation =>
      val topicName = invocation.getArgument(0, classOf[String])
      topicIdMapping.getOrElse(topicName, Uuid.ZERO_UUID)
    }
    disklessTopics.foreach(t => when(inklessMetadata.isDisklessTopic(t)).thenReturn(true))
    consolidatingDisklessTopics.foreach(t => when(inklessMetadata.isConsolidatingDisklessTopic(t)).thenReturn(true))
    when(sharedState.metadata()).thenReturn(inklessMetadata)

    val logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = time.scheduler,
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = logDirFailureChannel,
      alterPartitionManager = alterPartitionManager,
      inklessSharedState = if (inklessSharedStateEnabled) Some(sharedState) else None,
      inklessMetadataView = Some(inklessMetadata),
      initDisklessLogManager = initDisklessLogManager,
    ) {
      override protected def createReplicaFetcherManager(
        metrics: Metrics,
        time: Time,
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = {
        mockReplicaFetcherManager.getOrElse(super.createReplicaFetcherManager(metrics, time, quotaManager))
      }
    }
  }

  private def partitionRegistration(leader: Int,
                                    leaderEpoch: Int,
                                    isr: Array[Int],
                                    partitionEpoch: Int,
                                    replicas: Array[Int]): PartitionRegistration = {
    new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(partitionEpoch)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
  }

  private def createLeaderDelta(
    topicId: Uuid,
    partition: TopicPartition,
    leaderId: Integer,
    replicas: util.List[Integer],
    isr: util.List[Integer],
    leaderEpoch: Int = 0): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    val effectiveReplicas = Option(replicas).getOrElse(java.util.List.of(leaderId))
    val effectiveIsr = Option(isr).getOrElse(java.util.List.of(leaderId))

    delta.replay(new TopicRecord()
      .setName(partition.topic)
      .setTopicId(topicId)
    )

    delta.replay(new PartitionRecord()
      .setPartitionId(partition.partition)
      .setTopicId(topicId)
      .setReplicas(effectiveReplicas)
      .setIsr(effectiveIsr)
      .setLeader(leaderId)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(0)
    )

    delta
  }

  private def createFollowerDelta(
    topicId: Uuid,
    partition: TopicPartition,
    followerId: Int,
    leaderId: Int,
    leaderEpoch: Int = 0): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)

    delta.replay(new TopicRecord()
      .setName(partition.topic)
      .setTopicId(topicId)
    )

    delta.replay(new PartitionRecord()
      .setPartitionId(partition.partition)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(followerId, leaderId))
      .setIsr(util.Arrays.asList(followerId, leaderId))
      .setLeader(leaderId)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(0)
    )

    delta
  }

  private def imageFromTopics(topicsImage: TopicsImage): MetadataImage = {
    val featuresImageLatest = new FeaturesImage(
      Collections.emptyMap(),
      MetadataVersion.latestProduction())
    new MetadataImage(
      new MetadataProvenance(100L, 10, 1000L, true),
      featuresImageLatest,
      ClusterImageTest.IMAGE1,
      topicsImage,
      ConfigurationsImage.EMPTY,
      ClientQuotasImage.EMPTY,
      ProducerIdsImage.EMPTY,
      AclsImage.EMPTY,
      ScramImage.EMPTY,
      DelegationTokenImage.EMPTY
    )
  }

  // --- Helpers for hybrid/switched partition fetch tests ---

  private val switchedTopic = "test-topic"
  private val switchedTopicId = Uuid.fromString("YK2ed2GaTH2JpgzUaJ8tgg")

  private def setupReplicaManagerWithMockedPurgatories(
    aliveBrokerIds: Seq[Int]
  ): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(0)
    val path1 = TestUtils.tempRelativeDir("data").getAbsolutePath
    val path2 = TestUtils.tempRelativeDir("data2").getAbsolutePath
    props.put("log.dirs", path1 + "," + path2)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(new Properties()))
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))

    val metadataCache: KRaftMetadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.topicConfig(anyString())).thenReturn(new Properties())
    when(metadataCache.topicIdsToNames()).thenReturn(Map(switchedTopicId -> switchedTopic).asJava)
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    when(metadataCache.hasAliveBroker(anyInt)).thenAnswer { invocation =>
      aliveBrokers.map(_.id()).contains(invocation.getArgument(0).asInstanceOf[Int])
    }
    when(metadataCache.getAliveBrokerNode(anyInt, any[ListenerName])).thenAnswer { invocation =>
      Optional.of(aliveBrokers.find(_.id == invocation.getArgument(0).asInstanceOf[Integer]).get)
    }
    when(metadataCache.getAliveBrokerNodes(any[ListenerName])).thenReturn(aliveBrokers.asJava)
    when(metadataCache.getAliveBrokerEpoch(1)).thenReturn(util.Optional.of(0L))
    when(metadataCache.contains(new TopicPartition(switchedTopic, 0))).thenReturn(true)

    val timer = new MockTimer(time)
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce]("Produce", timer, 0, false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch]("Fetch", timer, 0, false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords]("DeleteRecords", timer, 0, false)
    val mockDelayedRemoteFetchPurgatory = new DelayedOperationPurgatory[DelayedRemoteFetch]("DelayedRemoteFetch", timer, 0, false)
    val mockDelayedRemoteListOffsetsPurgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets]("RemoteListOffsets", timer, 0, false)
    val mockDelayedShareFetchPurgatory = new DelayedOperationPurgatory[DelayedShareFetch]("ShareFetch", timer, 0, false)

    KafkaRequestHandler.setBypassThreadCheck(true)

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      brokerTopicStats = new BrokerTopicStats(false),
      delayedProducePurgatoryParam = Some(mockProducePurgatory),
      delayedFetchPurgatoryParam = Some(mockFetchPurgatory),
      delayedDeleteRecordsPurgatoryParam = Some(mockDeleteRecordsPurgatory),
      delayedRemoteFetchPurgatoryParam = Some(mockDelayedRemoteFetchPurgatory),
      delayedRemoteListOffsetsPurgatoryParam = Some(mockDelayedRemoteListOffsetsPurgatory),
      delayedShareFetchPurgatoryParam = Some(mockDelayedShareFetchPurgatory),
    )
  }

  private class CallbackResult[T] {
    private var value: Option[T] = None

    def assertFired: T = {
      assertTrue(hasFired, "Callback has not been fired")
      value.get
    }

    def hasFired: Boolean = value.isDefined

    def fire(value: T): Unit = {
      this.value = Some(value)
    }
  }

  private def fetchPartitionAsConsumer(
    replicaManager: ReplicaManager,
    partition: TopicIdPartition,
    partitionData: PartitionData,
  ): CallbackResult[FetchPartitionData] = {
    val result = new CallbackResult[FetchPartitionData]()
    def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responseStatus.size)
      result.fire(responseStatus.head._2)
    }
    val params = new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, 1, 0L, 1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty())
    replicaManager.fetchMessages(params, Seq(partition -> partitionData), QuotaFactory.UNBOUNDED_QUOTA, fetchCallback)
    result
  }

  private def fetchPartitionAsFollower(
    replicaManager: ReplicaManager,
    partition: TopicIdPartition,
    partitionData: PartitionData,
    replicaId: Int,
  ): CallbackResult[FetchPartitionData] = {
    val result = new CallbackResult[FetchPartitionData]()
    def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responseStatus.size)
      result.fire(responseStatus.head._2)
    }
    val params = new FetchParams(replicaId, 1, 0L, 1, 1024 * 1024, FetchIsolation.LOG_END, Optional.empty())
    replicaManager.fetchMessages(params, Seq(partition -> partitionData), QuotaFactory.UNBOUNDED_QUOTA, fetchCallback)
    result
  }

  @Test
  def testFetchFollowerAllowedForOlderClientsOnHybridDisklessPartition(): Unit = {
    val replicaManager = spy(setupReplicaManagerWithMockedPurgatories(aliveBrokerIds = Seq(0, 1)))

    try {
      val tp0 = new TopicPartition(switchedTopic, 0)
      val tidp0 = new TopicIdPartition(switchedTopicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

      val followerDelta = createFollowerDelta(switchedTopicId, tp0, 0, 1)
      val followerImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerImage)

      // Mark this partition as switched from classic to diskless (classicToDisklessStartOffset >= 0).
      doReturn(true).when(replicaManager).isPartitionSwitchedFromClassicToDiskless(tidp0)

      // Consumer fetch with empty ClientMetadata (older FETCH versions, no rackId) targeting
      // a non-leader broker. Without the override this would fail with NOT_LEADER_OR_FOLLOWER;
      // switched partitions allow any in-sync replica to serve the classic portion of the log.
      val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100, Optional.of(0))
      val fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData)
      assertEquals(Errors.NONE, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchFromFollowerStillRequiresLeaderOnSwitchedPartition(): Unit = {
    // Regression test: the switched-partition override must NOT relax leader-only for
    // broker-to-broker follower replication.
    val replicaManager = spy(setupReplicaManagerWithMockedPurgatories(aliveBrokerIds = Seq(0, 1)))

    try {
      val tp0 = new TopicPartition(switchedTopic, 0)
      val tidp0 = new TopicIdPartition(switchedTopicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

      val followerDelta = createFollowerDelta(switchedTopicId, tp0, 0, 1)
      val followerImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerImage)

      // Even if the partition is marked as switched, follower fetches must keep failing.
      doReturn(true).when(replicaManager).isPartitionSwitchedFromClassicToDiskless(tidp0)

      val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100, Optional.of(0))
      val fetchResult = fetchPartitionAsFollower(replicaManager, tidp0, partitionData, replicaId = 1)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaReconcilesCommittedSealOffsetWithOnlySealOffsetChange(): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2
    val sealOffset = 10L

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    // Truncation of a switched partition must run for non-consolidating diskless topics too,
    // so consolidation is intentionally left disabled here.
    val replicaManager = spy(createReplicaManager(
      Seq(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 13L, hw = sealOffset)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      val pendingDelta = new TopicsDelta(TopicsImage.EMPTY)
      pendingDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      val pendingRecord = new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, leaderId))
        .setIsr(util.Arrays.asList(brokerId, leaderId))
        .setLeader(leaderId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      pendingRecord.unknownTaggedFields().add(
        InitDisklessLogFields.encodeClassicToDisklessStartOffset(
          PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING))
      pendingDelta.replay(pendingRecord)

      val pendingImage = imageFromTopics(pendingDelta.apply())
      replicaManager.applyDelta(pendingDelta, pendingImage)

      clearInvocations(mockFetcherManager)
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(sealOffset)

      val sealDelta = new TopicsDelta(pendingImage.topics())
      val sealRecord = new PartitionChangeRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
      sealRecord.unknownTaggedFields().add(
        InitDisklessLogFields.encodeClassicToDisklessStartOffset(sealOffset))
      sealDelta.replay(sealRecord)

      val localChanges = sealDelta.localChanges(brokerId)
      assertTrue(localChanges.leaders.isEmpty)
      assertTrue(localChanges.followers.containsKey(tp))

      replicaManager.applyDelta(sealDelta, imageFromTopics(sealDelta.apply()))

      // The committed seal truncates the stale local tail (LEO 13 -> seal 10). Stopping the
      // now-caught-up classic fetcher is the ReplicaFetcherThread's self-eviction job, so the
      // metadata-only seal commit itself must not touch the fetcher manager.
      assertEquals(sealOffset, replicaManager.localLogOrException(tp).logEndOffset)
      verify(mockFetcherManager, never()).removeFetcherForPartitions(any())
      verify(mockFetcherManager, never()).addFetcherForPartitions(any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaDoesNotTruncateWhenSealAlreadyCommittedInBaseImage(): Unit = {
    // Counterpart to testApplyDeltaReconcilesCommittedSealOffsetWithOnlySealOffsetChange:
    // there the seal flips PENDING -> committed in this delta and the stale tail is truncated.
    // Here the seal is ALREADY committed in the base image, so a later (metadata-only) delta
    // must NOT re-truncate an already-progressed partition. This pins down the
    // `sealJustCommitted` guard, which relies entirely on the delta.image() previous
    // registration check.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val leaderId = 2
    val sealOffset = 10L

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val replicaManager = spy(createReplicaManager(
      Seq(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      // Local log runs past the seal (LEO 13 > seal 10), as it would for a partition that has
      // already progressed beyond the seal on this broker.
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = 13L, hw = sealOffset)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(sealOffset)

      // First delta already carries a committed seal (>= 0). Because the base image (EMPTY) has
      // no prior registration, this is treated as "not just committed" and must not truncate;
      // catching such a partition up is the catch-up/consolidation path's job.
      val committedDelta = new TopicsDelta(TopicsImage.EMPTY)
      committedDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      val committedRecord = new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, leaderId))
        .setIsr(util.Arrays.asList(brokerId, leaderId))
        .setLeader(leaderId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      committedRecord.unknownTaggedFields().add(
        InitDisklessLogFields.encodeClassicToDisklessStartOffset(sealOffset))
      committedDelta.replay(committedRecord)

      val committedImage = imageFromTopics(committedDelta.apply())
      replicaManager.applyDelta(committedDelta, committedImage)

      assertEquals(13L, replicaManager.localLogOrException(tp).logEndOffset,
        "A seal that predates this broker's view must not be truncated by the just-committed guard")

      clearInvocations(mockFetcherManager)

      // A later metadata-only change that leaves the seal unchanged at 10. An empty
      // PartitionChangeRecord still bumps the partition epoch (PartitionRegistration.merge), so
      // the partition is re-processed as a follower, but the seal now already exists in the base
      // image (sealJustCommitted == false).
      val laterDelta = new TopicsDelta(committedImage.topics())
      laterDelta.replay(new PartitionChangeRecord()
        .setTopicId(topicId)
        .setPartitionId(0))

      val localChanges = laterDelta.localChanges(brokerId)
      assertTrue(localChanges.followers.containsKey(tp))

      replicaManager.applyDelta(laterDelta, imageFromTopics(laterDelta.apply()))

      // The stale tail beyond the seal is NOT re-truncated, and no fetcher work is triggered by
      // the metadata-only delta.
      assertEquals(13L, replicaManager.localLogOrException(tp).logEndOffset)
      verify(mockFetcherManager, never()).removeFetcherForPartitions(any())
      verify(mockFetcherManager, never()).addFetcherForPartitions(any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testApplyDeltaFencesLeaderWithLocalLogBelowCommittedSeal(): Unit = {
    // Corruption-detection branch of maybeTruncateNewlySwitchedPartition: the switch is committed
    // (PENDING -> seal) while this broker is the leader, but the local log ends below the seal.
    // This is unreachable in normal operation (unclean leader election is not supported by the
    // diskless switch). Reaching here therefore means the leader's classic prefix has a hole in
    // (LEO, seal), i.e. local log corruption, so the partition must be fenced offline.
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val brokerId = 1
    val otherReplica = 2
    val sealOffset = 10L
    val localLeo = 5L

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    // disklessManagedReplicasEnabled and a present InitDisklessLogManager are both required so the
    // active classic-to-diskless switch branch runs for the leader (it bails out early otherwise),
    // letting the seal-commit delta reach maybeTruncateNewlySwitchedPartition. The seal is committed
    // (>= 0, not PENDING), so the manager is never actually invoked here and a mock suffices.
    val replicaManager = spy(createReplicaManager(
      Seq(topicName),
      disklessManagedReplicasEnabled = true,
      mockReplicaFetcherManager = Some(mockFetcherManager),
      initDisklessLogManager = Some(mock(classOf[InitDisklessLogManager]))
    ))
    try {
      val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
      populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = localLeo, hw = localLeo)

      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
        .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

      // Bring the partition online as a sealed leader with the switch still PENDING.
      val pendingDelta = new TopicsDelta(TopicsImage.EMPTY)
      pendingDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      val pendingRecord = new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(topicId)
        .setReplicas(util.Arrays.asList(brokerId, otherReplica))
        .setIsr(util.Arrays.asList(brokerId, otherReplica))
        .setLeader(brokerId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      pendingRecord.unknownTaggedFields().add(
        InitDisklessLogFields.encodeClassicToDisklessStartOffset(
          PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING))
      pendingDelta.replay(pendingRecord)

      val pendingImage = imageFromTopics(pendingDelta.apply())
      replicaManager.applyDelta(pendingDelta, pendingImage)

      val leaderBefore = replicaManager.getPartition(tp).asInstanceOf[HostedPartition.Online].partition
      assertTrue(leaderBefore.isLeader, "Partition should be the leader before the seal commit")
      assertEquals(localLeo, replicaManager.localLogOrException(tp).logEndOffset)

      // Commit the seal at an offset above the leader's local LEO.
      when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(sealOffset)
      val sealDelta = new TopicsDelta(pendingImage.topics())
      val sealRecord = new PartitionChangeRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
      sealRecord.unknownTaggedFields().add(
        InitDisklessLogFields.encodeClassicToDisklessStartOffset(sealOffset))
      sealDelta.replay(sealRecord)

      val localChanges = sealDelta.localChanges(brokerId)
      assertTrue(localChanges.leaders.containsKey(tp))

      replicaManager.applyDelta(sealDelta, imageFromTopics(sealDelta.apply()))

      // The leader's local log is below the committed seal (a hole in the classic prefix), which
      // can only be corruption, so the partition is fenced offline rather than left to serve it.
      val partition = replicaManager.getPartition(tp)
      assertTrue(partition.isInstanceOf[HostedPartition.Offline],
        s"Leader below the committed seal must be fenced offline, but was $partition")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testSwitchedConsolidatingFollowerBelowSealStaysOnClassicFetcher(): Unit = {
    // A switched consolidating follower whose local log is still below the committed seal must
    // keep replicating the classic prefix via the classic ReplicaFetcher (which self-evicts and
    // hands off to consolidation once it reaches the seal). It must NOT be routed straight to the
    // consolidation fetcher, otherwise the reconciler would Retry it and, since a follower's only
    // catch-up path is the classic fetcher, it would be stranded without ever reaching the seal.
    val consolidatingTopic = disklessTopicPartition.topic()
    val tp = disklessTopicPartition.topicPartition()

    val ctorInit: MockedConstruction.MockInitializer[ConsolidationFetcherManager] = {
      case (mock, _) =>
        when(mock.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    }
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager], ctorInit)
    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockReplicaFetcherManager.removeFetcherForPartitions(any()))
      .thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    try {
      val replicaManager = createReplicaManager(
        List(consolidatingTopic),
        disklessRemoteStorageConsolidationEnabled = true,
        consolidatingDisklessTopics = Set(consolidatingTopic),
        mockReplicaFetcherManager = Some(mockReplicaFetcherManager),
      )
      try {
        val mockCfm = consolidationCtor.constructed().get(0)
        // Seal committed at 100, but the freshly created follower log is empty (LEO 0 < seal).
        when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
          .thenReturn(100L)
        val delta = createFollowerDelta(
          disklessTopicPartition.topicId,
          tp,
          followerId = 1,
          leaderId = 2,
        )
        replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

        // Routed to the classic fetcher path (removed/re-added there), not to consolidation.
        verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(tp))
        verify(mockCfm, never()).addFetcherForPartitions(any())
      } finally {
        replicaManager.shutdown(checkpointHW = false)
        verify(consolidationCtor.constructed().get(0)).shutdown()
      }
    } finally {
      consolidationCtor.close()
    }
  }

  @Test
  def testConsolidatingSwitchSealsRegistersThenStartsConsolidationOnSealCommit(): Unit = {
    // Drives the consolidating classic-to-diskless switch on the *leader* across the two metadata
    // deltas it actually produces:
    //
    //   (delta 2) Pending switch, epoch bumped: the partition is already online and the topic is
    //     already consolidating (diskless.enable + remote.storage.enable were flipped together).
    //     It must take the seal+register branch -- seal the classic log and register with the
    //     InitDisklessLogManager -- NOT the consolidating become-leader branch (which would skip
    //     the seal and strand the switch in CLASSIC_TO_DISKLESS_SWITCH_PENDING). No consolidation
    //     fetcher may start yet: the reconciler Retries while the seal is pending.
    //
    //   (delta 3) Seal commit, PENDING -> committed seal: this PartitionChangeRecord carries no
    //     leader field, so the leader epoch is unchanged, but it still bumps the *partition* epoch.
    //     A led partition with a bumped partition epoch re-enters applyLocalLeadersDelta via
    //     localChanges.leaders, and -- now that the seal is committed (not pending) -- takes the
    //     consolidating become-leader branch, which starts the consolidation fetcher from the
    //     local LEO (== seal).
    //
    // Delta 1 only exists to bring the partition online (as a still-pending leader) so that delta 2
    // sees an existing online partition, mirroring a real classic leader that was already serving.
    val topicName = disklessTopicPartition.topic()
    val topicId = disklessTopicPartition.topicId
    val tp = disklessTopicPartition.topicPartition()
    val brokerId = 1
    val otherReplica = 2
    val sealOffset = 10L

    val mockIdlm = mock(classOf[InitDisklessLogManager])
    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockReplicaFetcherManager.removeFetcherForPartitions(any()))
      .thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    val ctorInit: MockedConstruction.MockInitializer[ConsolidationFetcherManager] = {
      case (mock, _) =>
        when(mock.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])
    }
    val consolidationCtor = mockConstruction(classOf[ConsolidationFetcherManager], ctorInit)
    try {
      val replicaManager = spy(createReplicaManager(
        List(topicName),
        disklessRemoteStorageConsolidationEnabled = true,
        consolidatingDisklessTopics = Set(topicName),
        mockReplicaFetcherManager = Some(mockReplicaFetcherManager),
        initDisklessLogManager = Some(mockIdlm)
      ))
      try {
        val mockCfm = consolidationCtor.constructed().get(0)

        // The classic prefix is already fully on local disk: LEO == HW == seal.
        val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
        populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo = sealOffset, hw = sealOffset)

        when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
          .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

        // --- delta 1: bring the partition online as a (still pending) leader on this broker ---
        val onlineDelta = new TopicsDelta(TopicsImage.EMPTY)
        onlineDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
        val onlineRecord = new PartitionRecord()
          .setPartitionId(0)
          .setTopicId(topicId)
          .setReplicas(util.Arrays.asList(brokerId, otherReplica))
          .setIsr(util.Arrays.asList(brokerId, otherReplica))
          .setLeader(brokerId)
          .setLeaderEpoch(0)
          .setPartitionEpoch(0)
        onlineRecord.unknownTaggedFields().add(
          InitDisklessLogFields.encodeClassicToDisklessStartOffset(
            PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING))
        onlineDelta.replay(onlineRecord)
        val onlineImage = imageFromTopics(onlineDelta.apply())
        replicaManager.applyDelta(onlineDelta, onlineImage)
        assertTrue(replicaManager.getPartition(tp).isInstanceOf[HostedPartition.Online],
          "Partition should be online after the first delta")

        // --- delta 2: pending switch with a leader-epoch bump -> seal + register ---
        clearInvocations(mockCfm)
        val pendingDelta = new TopicsDelta(onlineImage.topics())
        val pendingRecord = new PartitionChangeRecord()
          .setTopicId(topicId)
          .setPartitionId(0)
          .setLeader(brokerId) // forces a leader-epoch bump so it lands in localChanges.leaders
        pendingRecord.unknownTaggedFields().add(
          InitDisklessLogFields.encodeClassicToDisklessStartOffset(
            PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING))
        pendingDelta.replay(pendingRecord)
        val pendingImage = imageFromTopics(pendingDelta.apply())
        assertTrue(pendingDelta.localChanges(brokerId).leaders.containsKey(tp),
          "Epoch bump should put the pending switch in localChanges.leaders")
        replicaManager.applyDelta(pendingDelta, pendingImage)

        // Seal+register ran (not the consolidating become-leader branch), and no consolidation
        // fetcher was started for the partition while the seal is still pending.
        verify(mockIdlm).registerPartition(any(classOf[Partition]), ArgumentMatchers.eq(topicId))
        val leaderEpochAfterSeal = replicaManager.getPartition(tp)
          .asInstanceOf[HostedPartition.Online].partition.getLeaderEpoch
        verify(mockCfm, never()).addFetcherForPartitions(any())

        // --- delta 3: seal commit (PENDING -> committed seal), no leader-epoch bump ---
        clearInvocations(mockCfm)
        when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp)).thenReturn(sealOffset)
        val sealDelta = new TopicsDelta(pendingImage.topics())
        val sealRecord = new PartitionChangeRecord()
          .setTopicId(topicId)
          .setPartitionId(0)
        sealRecord.unknownTaggedFields().add(
          InitDisklessLogFields.encodeClassicToDisklessStartOffset(sealOffset))
        sealDelta.replay(sealRecord)
        // The seal commit leaves the leader epoch unchanged but bumps the partition epoch, so the
        // led partition still re-enters applyLocalLeadersDelta through localChanges.leaders.
        assertTrue(sealDelta.localChanges(brokerId).leaders.containsKey(tp),
          "Seal commit should re-enter applyLocalLeadersDelta via the partition-epoch bump")
        replicaManager.applyDelta(sealDelta, imageFromTopics(sealDelta.apply()))

        // Consolidation now starts from the local LEO (== seal) via the consolidating become-leader
        // branch, because the partition is no longer pending.
        verify(mockCfm).addFetcherForPartitions(
          Map(tp -> InitialFetchState(
            topicId = Some(topicId),
            leader = new BrokerEndPoint(-1, "diskless", -1),
            currentLeaderEpoch = leaderEpochAfterSeal,
            initOffset = sealOffset
          ))
        )
      } finally {
        replicaManager.shutdown(checkpointHW = false)
        verify(consolidationCtor.constructed().get(0)).shutdown()
      }
    } finally {
      consolidationCtor.close()
    }
  }
}
