/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import io.aiven.inkless.control_plane.{ControlPlane, InitDisklessLogProducerState => CpProducerState, InitDisklessLogResponse => CpInitResponse}
import kafka.cluster.Partition
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{InitDisklessLogRequestData, InitDisklessLogResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, InitDisklessLogRequest, InitDisklessLogResponse, RequestHeader}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.util.MockScheduler
import org.apache.kafka.common.utils.MockTime
import kafka.log.UnifiedLog
import org.apache.kafka.storage.internals.log.{ProducerStateEntry, ProducerStateManager}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.util
import java.util.Optional
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

class InitDisklessLogManagerTest {

  private val brokerId = 0
  private val brokerEpoch = 42L
  private val topicId = Uuid.randomUuid()
  private val tp0 = new TopicPartition("test-topic", 0)

  private var channelManager: MockInitDisklessLogChannelManager = _
  private var controlPlane: ControlPlane = _
  private var mockTime: MockTime = _
  private var scheduler: MockScheduler = _
  private var manager: InitDisklessLogManager = _

  @BeforeEach
  def setUp(): Unit = {
    channelManager = new MockInitDisklessLogChannelManager()
    controlPlane = mock(classOf[ControlPlane])
    mockTime = new MockTime()
    scheduler = new MockScheduler(mockTime)
    manager = new InitDisklessLogManager(
      controllerChannelManager = channelManager,
      controlPlane = controlPlane,
      scheduler = scheduler,
      brokerId = brokerId,
      brokerEpochSupplier = () => brokerEpoch
    )
  }

  private def fireLinger(): Unit = {
    mockTime.sleep(manager.lingerMs)
    scheduler.tick()
  }

  private def fireRetry(): Unit = {
    mockTime.sleep(manager.initialRetryBackoffMs + 1)
    scheduler.tick()
  }

  private def pollAndComplete(response: InitDisklessLogResponseData): Unit = {
    assertEquals(1, channelManager.requests.size())
    channelManager.requests.poll().complete(response)
  }

  private def mockPartition(
    tp: TopicPartition = tp0,
    hw: Long,
    leo: Long,
    leaderEpoch: Int = 1,
    isLeader: Boolean = true
  ): Partition = {
    val partition = mock(classOf[Partition])
    val log = mock(classOf[UnifiedLog])
    val producerStateManager = mock(classOf[ProducerStateManager])

    when(partition.topicPartition).thenReturn(tp)
    when(partition.isSealed).thenReturn(true)
    when(partition.isLeader).thenReturn(isLeader)
    when(partition.getLeaderEpoch).thenReturn(leaderEpoch)
    when(partition.log).thenReturn(Some(log))
    when(partition.maybeAddListener(any())).thenReturn(true)
    when(log.highWatermark).thenReturn(hw)
    when(log.logEndOffset).thenReturn(leo)
    when(log.producerStateManager).thenReturn(producerStateManager)
    when(producerStateManager.activeProducers()).thenReturn(new util.HashMap())

    partition
  }

  @Test
  def testRegisterPartitionHWEqualsLEO(): Unit = {
    // Given a partition where HW equals LEO
    val partition = mockPartition(hw = 100, leo = 100)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then the state transitions to SendingToController and a batch send is scheduled (not fired yet)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the scheduled batch fires automatically
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
  }

  @Test
  def testRegisterPartitionHWBelowLEO(): Unit = {
    // Given a partition where HW is below LEO
    val partition = mockPartition(hw = 50, leo = 100)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then the controller is not called and state is WaitingForHW
    assertTrue(channelManager.requests.isEmpty)
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp0))
  }

  @Test
  def testHighWatermarkAdvancedTriggersControllerCall(): Unit = {
    // Given a partition registered with HW < LEO
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp0))

    // When HW advances but does not reach LEO
    val log = partition.log.get
    when(log.highWatermark).thenReturn(80L)
    manager.onHighWatermarkUpdated(tp0, 80)

    // Then no batch is scheduled and state stays WaitingForHW
    assertTrue(channelManager.requests.isEmpty)
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp0))

    // When HW catches up to LEO
    when(log.highWatermark).thenReturn(100L)
    manager.onHighWatermarkUpdated(tp0, 100)

    // Then the state transitions to SendingToController and a batch is scheduled
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the controller is called and state becomes AwaitingMetadata
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
  }

  @Test
  def testRequestContainsCorrectData(): Unit = {
    // Given a partition with a specific leader epoch and active producer state
    val partition = mockPartition(hw = 100, leo = 100, leaderEpoch = 5)

    val producerStateManager = partition.log.get.producerStateManager
    val producerEntry = new ProducerStateEntry(
      42L, 1.toShort, 0, 5000L,
      java.util.OptionalLong.empty(),
      Optional.of(new org.apache.kafka.storage.internals.log.BatchMetadata(14, 99, 4, 5000L))
    )
    val producers = new util.HashMap[java.lang.Long, ProducerStateEntry]()
    producers.put(42L, producerEntry)
    when(producerStateManager.activeProducers()).thenReturn(producers)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then the batch is scheduled but not yet sent
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the request contains correct broker metadata, topic, partition, and producer state data
    fireLinger()
    assertEquals(1, channelManager.requests.size())
    val captured = channelManager.requests.poll()
    val request = captured.requestData

    assertEquals(brokerId, request.brokerId())
    assertEquals(brokerEpoch, request.brokerEpoch())
    assertEquals(1, request.topics().size())

    val topicData = request.topics().get(0)
    assertEquals(topicId, topicData.topicId())
    assertEquals(1, topicData.partitions().size())

    val partitionData = topicData.partitions().get(0)
    assertEquals(0, partitionData.partitionId())
    assertEquals(100, partitionData.disklessStartOffset())
    assertEquals(5, partitionData.leaderEpoch())
    assertEquals(1, partitionData.producerStates().size())

    val ps = partitionData.producerStates().get(0)
    assertEquals(42L, ps.producerId())
    assertEquals(1.toShort, ps.producerEpoch())
    assertEquals(10, ps.baseSequence())
    assertEquals(14, ps.lastSequence())
    assertEquals(99, ps.assignedOffset())
    assertEquals(5000L, ps.batchMaxTimestamp())

    captured.complete(makeSuccessResponse(topicId, 0))
  }

  @Test
  def testDuplicateRegisterPartitionInWaitingForHWStaysWaiting(): Unit = {
    // Given a partition registered with HW < LEO
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)

    // When the same partition is registered again
    manager.registerPartition(partition, topicId)

    // Then it is tracked once and state remains WaitingForHW
    assertEquals(Set(tp0), manager.getTrackedPartitions)
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp0))
  }

  @Test
  def testReEvaluateWaitingForHWTransitionsWhenHWCaughtUp(): Unit = {
    // Given a partition registered with HW < LEO (WaitingForHW)
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp0))

    // When HW catches up and the partition is re-registered
    val log = partition.log.get
    when(log.highWatermark).thenReturn(100L)
    manager.registerPartition(partition, topicId)

    // Then it transitions to SendingToController
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the controller is called and state becomes AwaitingMetadata
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
  }

  @Test
  def testReEvaluateSendingToControllerEnsuresBatchScheduled(): Unit = {
    // Given a partition in SendingToController after a retriable error
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the partition is re-registered (simulating leadership bounce-back),
    // lingerMs preempts the retry backoff
    manager.registerPartition(partition, topicId)
    fireLinger()

    // Then the preempting linger batch fires and the controller is called again
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))

    // And the stale retry task fires harmlessly (partition already in AwaitingMetadata)
    mockTime.sleep(manager.initialRetryBackoffMs)
    scheduler.tick()
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testReEvaluateAwaitingMetadataIsNoop(): Unit = {
    // Given a partition that has successfully transitioned to AwaitingMetadata
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))

    // When the partition is re-registered
    manager.registerPartition(partition, topicId)

    // Then state remains AwaitingMetadata with no additional controller call
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testRemovePartition(): Unit = {
    // Given a registered partition
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Set(tp0), manager.getTrackedPartitions)

    // When the partition is removed
    manager.removePartition(tp0)

    // Then it is no longer tracked
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(None, manager.getInitState(tp0))
  }

  @Test
  def testPermanentErrorRemovesFromTracking(): Unit = {
    // Given a partition registered with HW = LEO and a batch scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the scheduled batch fires and the controller returns a permanent error (FENCED_LEADER_EPOCH)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.FENCED_LEADER_EPOCH))

    // Then the partition is removed from tracking
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testInvalidRequestErrorRemovesFromTracking(): Unit = {
    // Given a partition registered with HW = LEO and a batch scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the scheduled batch fires and the controller returns INVALID_REQUEST
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.INVALID_REQUEST))

    // Then the partition is removed from tracking
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testRetriableErrorSchedulesRetry(): Unit = {
    // Given a partition registered with HW = LEO and a batch scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the scheduled batch fires and the controller returns a retriable error (NOT_CONTROLLER)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))

    // Then the partition stays in SendingToController
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the retry fires and succeeds
    fireRetry()
    pollAndComplete(makeSuccessResponse(topicId, 0))

    // Then state transitions to AwaitingMetadata
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
  }

  @Test
  def testExceptionSchedulesRetry(): Unit = {
    // Given a partition registered with HW = LEO and a batch scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the scheduled batch fires and the controller call times out
    fireLinger()
    channelManager.requests.poll().timeout()

    // Then the partition stays in SendingToController
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the retry fires and succeeds
    fireRetry()
    pollAndComplete(makeSuccessResponse(topicId, 0))

    // Then state transitions to AwaitingMetadata
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
  }

  @Test
  def testRetryCancelledWhenNotLeader(): Unit = {
    // Given a partition that received a retriable error with a retry scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the partition loses leadership before the retry fires
    when(partition.isLeader).thenReturn(false)
    fireRetry()

    // Then the retry is skipped and the partition is removed from tracking
    assertTrue(channelManager.requests.isEmpty)
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testNotLeaderSkipsSendingToController(): Unit = {
    // Given a partition that is not the leader
    val partition = mockPartition(hw = 100, leo = 100, isLeader = false)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then a batch is scheduled (leadership is checked at send time, not registration time)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // And after lingerMs elapses and the batch fires
    fireLinger()

    // Then the controller is not called because the partition is not the leader
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testOnFailedRemovesPartition(): Unit = {
    // Given a registered partition
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Set(tp0), manager.getTrackedPartitions)

    // When onFailed is called for the partition
    manager.onFailed(tp0)

    // Then the partition is removed from tracking
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testOnDeletedRemovesPartition(): Unit = {
    // Given a registered partition
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertEquals(Set(tp0), manager.getTrackedPartitions)

    // When onDeleted is called for the partition
    manager.onDeleted(tp0)

    // Then the partition is removed from tracking
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testUnsealedPartitionIsSkipped(): Unit = {
    // Given a partition that is not sealed
    val partition = mock(classOf[Partition])
    when(partition.topicPartition).thenReturn(tp0)
    when(partition.isSealed).thenReturn(false)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then it is not tracked and the controller is not called
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testPartitionWithNoLogIsSkipped(): Unit = {
    // Given a sealed partition with no log
    val partition = mock(classOf[Partition])
    when(partition.topicPartition).thenReturn(tp0)
    when(partition.isSealed).thenReturn(true)
    when(partition.log).thenReturn(None)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then it is not tracked and the controller is not called
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testExponentialBackoffCapsAtMax(): Unit = {
    // Given a partition that always receives retriable errors
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))

    // When many retries occur
    for (_ <- 1 to 20) {
      mockTime.sleep(manager.maxRetryBackoffMs + 1)
      scheduler.tick()
      pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    }

    // Then the partition remains in SendingToController (backoff doesn't overflow)
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
  }

  // --- Batching tests ---

  @Test
  def testOnlyReadyPartitionsAreBatched(): Unit = {
    // Given three partitions where only two have HW = LEO
    val tp1 = new TopicPartition("test-topic", 1)
    val tp2 = new TopicPartition("test-topic", 2)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 50, leo = 200)
    val partition2 = mockPartition(tp = tp2, hw = 300, leo = 300)

    // When all three are registered
    manager.registerPartition(partition0, topicId)
    manager.registerPartition(partition1, topicId)
    manager.registerPartition(partition2, topicId)

    // Then only the two ready partitions are in SendingToController, the third waits for HW
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp1))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp2))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, only the two ready partitions are sent to the controller
    fireLinger()
    val captured = channelManager.requests.poll()
    val request = captured.requestData
    assertEquals(1, request.topics().size())
    assertEquals(2, request.topics().get(0).partitions().size())
    val partitionIds = request.topics().get(0).partitions().asScala.map(_.partitionId()).toSet
    assertEquals(Set(0, 2), partitionIds)

    captured.complete(makeBatchSuccessResponse(topicId, Seq(0, 2)))

    // And the ready partitions transition to AwaitingMetadata while the waiting one stays
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(Some(InitState.WaitingForHW), manager.getInitState(tp1))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp2))

    // When the third partition's HW catches up
    val log1 = partition1.log.get
    when(log1.highWatermark).thenReturn(200L)
    manager.onHighWatermarkUpdated(tp1, 200)

    // Then it transitions to SendingToController
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp1))

    // And after lingerMs elapses, it is sent to the controller on its own
    fireLinger()
    val lateRequest = channelManager.requests.peek().requestData
    assertEquals(1, lateRequest.topics().get(0).partitions().size())
    assertEquals(1, lateRequest.topics().get(0).partitions().get(0).partitionId())
    pollAndComplete(makeSuccessResponse(topicId, 1))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp1))
  }

  @Test
  def testMultiplePartitionsSameTopicBatchedInSingleRequest(): Unit = {
    // Given three partitions of the same topic, all with HW = LEO
    val tp1 = new TopicPartition("test-topic", 1)
    val tp2 = new TopicPartition("test-topic", 2)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)
    val partition2 = mockPartition(tp = tp2, hw = 300, leo = 300)

    // When all three are registered before the linger fires
    manager.registerPartition(partition0, topicId)
    manager.registerPartition(partition1, topicId)
    manager.registerPartition(partition2, topicId)

    // Then all are in SendingToController and the controller hasn't been called yet
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp1))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp2))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, a single controller call is made containing all three partitions
    fireLinger()
    val request = channelManager.requests.peek().requestData
    assertEquals(1, request.topics().size())
    assertEquals(topicId, request.topics().get(0).topicId())
    assertEquals(3, request.topics().get(0).partitions().size())
    val partitionIds = request.topics().get(0).partitions().asScala.map(_.partitionId()).toSet
    assertEquals(Set(0, 1, 2), partitionIds)
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1, 2)))

    // And all partitions transition to AwaitingMetadata
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp1))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp2))
  }

  @Test
  def testMultipleTopicsBatchedInSingleRequest(): Unit = {
    // Given partitions from two different topics, both with HW = LEO
    val topicId2 = Uuid.randomUuid()
    val tp1 = new TopicPartition("other-topic", 0)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)

    val successResponse = new InitDisklessLogResponseData().setTopics(util.List.of(
      new InitDisklessLogResponseData.TopicResponse()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(0).setErrorCode(Errors.NONE.code()))),
      new InitDisklessLogResponseData.TopicResponse()
        .setTopicId(topicId2)
        .setPartitions(util.List.of(
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(0).setErrorCode(Errors.NONE.code())))
    ))

    // When both are registered before the linger fires
    manager.registerPartition(partition0, topicId)
    manager.registerPartition(partition1, topicId2)

    // Then both are in SendingToController and the controller hasn't been called yet
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp1))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, a single controller call is made with both topics
    fireLinger()
    val request = channelManager.requests.peek().requestData
    assertEquals(2, request.topics().size())
    val requestTopicIds = request.topics().asScala.map(_.topicId()).toSet
    assertEquals(Set(topicId, topicId2), requestTopicIds)
    pollAndComplete(successResponse)

    // And both partitions transition to AwaitingMetadata
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp1))
  }

  @Test
  def testBatchMixedResults(): Unit = {
    // Given three partitions registered and sent in a batch
    val tp1 = new TopicPartition("test-topic", 1)
    val tp2 = new TopicPartition("test-topic", 2)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)
    val partition2 = mockPartition(tp = tp2, hw = 300, leo = 300)

    val mixedResponse = new InitDisklessLogResponseData().setTopics(util.List.of(
      new InitDisklessLogResponseData.TopicResponse()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(0).setErrorCode(Errors.NONE.code()),
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(1).setErrorCode(Errors.FENCED_LEADER_EPOCH.code()),
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(2).setErrorCode(Errors.NOT_CONTROLLER.code())
        ))
    ))

    // When all partitions are registered
    manager.registerPartition(partition0, topicId)
    manager.registerPartition(partition1, topicId)
    manager.registerPartition(partition2, topicId)
    assertTrue(channelManager.requests.isEmpty)

    // And the scheduled batch fires with mixed results (success, permanent error, retriable error)
    fireLinger()
    pollAndComplete(mixedResponse)

    // Then each partition transitions to the appropriate state
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(None, manager.getInitState(tp1))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp2))

    // When the retry fires for the retriable-error partition
    fireRetry()
    val retryRequest = channelManager.requests.peek().requestData
    assertEquals(1, retryRequest.topics().get(0).partitions().size())
    assertEquals(2, retryRequest.topics().get(0).partitions().get(0).partitionId())
    pollAndComplete(makeSuccessResponse(topicId, 2))

    // Then only that partition is retried and transitions to AwaitingMetadata
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp2))
  }

  @Test
  def testBatchExceptionRetriesAllPartitions(): Unit = {
    // Given two partitions registered and sent in a batch
    val tp1 = new TopicPartition("test-topic", 1)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)

    // When both are registered and the scheduled batch fires with a timeout
    manager.registerPartition(partition0, topicId)
    manager.registerPartition(partition1, topicId)
    fireLinger()
    channelManager.requests.poll().timeout()

    // Then both partitions stay in SendingToController
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp1))

    // When the retry fires and succeeds
    fireRetry()
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))

    // Then both partitions transition to AwaitingMetadata
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp1))
  }

  @Test
  def testPartitionsArrivingDuringHWWaitAreBatchedLater(): Unit = {
    // Given two partitions registered with HW < LEO
    val tp1 = new TopicPartition("test-topic", 1)
    val partition0 = mockPartition(tp = tp0, hw = 50, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 50, leo = 100)
    manager.registerPartition(partition0, topicId)
    manager.registerPartition(partition1, topicId)
    assertTrue(channelManager.requests.isEmpty)

    // When both HWs catch up
    val log0 = partition0.log.get
    val log1 = partition1.log.get
    when(log0.highWatermark).thenReturn(100L)
    when(log1.highWatermark).thenReturn(100L)
    manager.onHighWatermarkUpdated(tp0, 100)
    manager.onHighWatermarkUpdated(tp1, 100)

    // Then both transition to SendingToController and a batch is scheduled
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp1))
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, a single controller call is made with both partitions
    fireLinger()
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp1))
  }

  @Test
  def testNewPartitionPreemptsRetryBackoff(): Unit = {
    // Given a partition in SendingToController with a retry backoff pending
    val tp1 = new TopicPartition("test-topic", 1)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    manager.registerPartition(partition0, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When a new ready partition is registered within the lingerMs window
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)
    manager.registerPartition(partition1, topicId)

    // Then both partitions are in SendingToController
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp1))

    // And after lingerMs elapses, the backoff is preempted and both partitions are sent
    fireLinger()
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp1))
  }

  @Test
  def testComputeBackoffSafeForZeroAttempt(): Unit = {
    // Given a partition that receives a retriable error on first attempt
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertEquals(Some(InitState.SendingToController), manager.getInitState(tp0))

    // When the first retry fires
    fireRetry()
    pollAndComplete(makeSuccessResponse(topicId, 0))

    // Then the backoff is positive (no overflow) and the retry succeeds
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
  }

  private def makeSuccessResponse(topicId: Uuid, partitionId: Int): InitDisklessLogResponseData = {
    new InitDisklessLogResponseData().setTopics(util.List.of(
      new InitDisklessLogResponseData.TopicResponse()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(partitionId)
            .setErrorCode(Errors.NONE.code())
        ))
    ))
  }

  private def makeBatchSuccessResponse(topicId: Uuid, partitionIds: Seq[Int]): InitDisklessLogResponseData = {
    val partitions = new util.ArrayList[InitDisklessLogResponseData.PartitionResponse]()
    partitionIds.foreach { id =>
      partitions.add(new InitDisklessLogResponseData.PartitionResponse()
        .setPartitionId(id)
        .setErrorCode(Errors.NONE.code()))
    }
    new InitDisklessLogResponseData().setTopics(util.List.of(
      new InitDisklessLogResponseData.TopicResponse()
        .setTopicId(topicId)
        .setPartitions(partitions)
    ))
  }

  private def makeErrorResponse(topicId: Uuid, partitionId: Int, error: Errors): InitDisklessLogResponseData = {
    new InitDisklessLogResponseData().setTopics(util.List.of(
      new InitDisklessLogResponseData.TopicResponse()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new InitDisklessLogResponseData.PartitionResponse()
            .setPartitionId(partitionId)
            .setErrorCode(error.code())
        ))
    ))
  }

  @Test
  def testMetadataAppliedCallsControlPlaneAndRemovesTracking(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any())).thenReturn(util.List.of(CpInitResponse.success()))

    manager.onDisklessInitMetadataApplied(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      disklessStartOffset = 100L,
      producerStates = util.List.of(new CpProducerState(1L, 0.toShort, 0, 1, 100L, 1000L))
    )

    scheduler.tick()

    verify(controlPlane).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testMetadataAppliedAlreadyInitializedIsTerminalSuccess(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any())).thenReturn(util.List.of(CpInitResponse.alreadyInitialized()))

    manager.onDisklessInitMetadataApplied(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      disklessStartOffset = 100L,
      producerStates = util.List.of()
    )

    scheduler.tick()

    verify(controlPlane).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testMetadataAppliedRetriableErrorSchedulesRetry(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any()))
      .thenReturn(util.List.of(new CpInitResponse(Errors.NOT_CONTROLLER)))
      .thenReturn(util.List.of(CpInitResponse.success()))

    manager.onDisklessInitMetadataApplied(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      disklessStartOffset = 100L,
      producerStates = util.List.of()
    )

    scheduler.tick()
    assertEquals(Some(InitState.AwaitingMetadata), manager.getInitState(tp0))
    verify(controlPlane, times(1)).initDisklessLog(any())

    fireRetry()
    verify(controlPlane, times(2)).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testMetadataAppliedRepeatedCallbackIsIdempotent(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any())).thenReturn(util.List.of(CpInitResponse.success()))

    manager.onDisklessInitMetadataApplied(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      disklessStartOffset = 100L,
      producerStates = util.List.of()
    )
    manager.onDisklessInitMetadataApplied(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      disklessStartOffset = 100L,
      producerStates = util.List.of()
    )

    scheduler.tick()

    verify(controlPlane, atLeastOnce()).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }
}

/**
 * Lightweight mock that captures requests and allows tests to complete them with a response.
 */
private[server] class MockInitDisklessLogChannelManager extends NodeToControllerChannelManager {

  case class CapturedRequest(
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ) {
    def requestData: InitDisklessLogRequestData = {
      requestBuilder.build(ApiKeys.INIT_DISKLESS_LOG.latestVersion()).asInstanceOf[InitDisklessLogRequest].data()
    }

    def complete(responseData: InitDisklessLogResponseData): Unit = {
      val response = new InitDisklessLogResponse(responseData)
      val header = new RequestHeader(ApiKeys.INIT_DISKLESS_LOG, ApiKeys.INIT_DISKLESS_LOG.latestVersion(), "", 0)
      val clientResponse = new ClientResponse(header, callback, "-1", 0L, 0L, false, null, null, response)
      callback.onComplete(clientResponse)
    }

    def timeout(): Unit = {
      callback.onTimeout()
    }
  }

  val requests = new ConcurrentLinkedQueue[CapturedRequest]()

  override def start(): Unit = {}
  override def shutdown(): Unit = {}
  override def controllerApiVersions(): Optional[org.apache.kafka.clients.NodeApiVersions] = Optional.empty()
  override def getTimeoutMs: Long = 60000L

  override def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    requests.add(CapturedRequest(request, callback))
  }
}
