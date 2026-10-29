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
import org.apache.kafka.common.message.{InitDisklessLogRequestData, InitDisklessLogResponseData, SetClassicPrefixInRemoteStorageRequestData, SetClassicPrefixInRemoteStorageResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, InitDisklessLogRequest, InitDisklessLogResponse, RequestHeader, SetClassicPrefixInRemoteStorageRequest, SetClassicPrefixInRemoteStorageResponse}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.internal.RecordBatch
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.partition.PartitionListener
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.util.MockScheduler
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.storage.internals.log.{BatchMetadata, ProducerStateEntry, ProducerStateManager, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import java.util
import java.util.Optional
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class InitDisklessLogManagerTest {

  private val brokerId = 0
  private val brokerEpoch = 42L
  private val topicId = Uuid.randomUuid()
  private val tp0 = new TopicPartition("test-topic", 0)

  private var channelManager: MockInitDisklessLogChannelManager = _
  private var controlPlane: ControlPlane = _
  private var mockTime: HookedMockTime = _
  private var scheduler: MockScheduler = _
  private var manager: InitDisklessLogManager = _
  private var listenersByTp: mutable.Map[TopicPartition, PartitionListener] = _

  @BeforeEach
  def setUp(): Unit = {
    channelManager = new MockInitDisklessLogChannelManager()
    controlPlane = mock(classOf[ControlPlane])
    mockTime = new HookedMockTime()
    scheduler = new MockScheduler(mockTime)
    listenersByTp = mutable.Map.empty
    manager = new InitDisklessLogManager(
      controllerChannelManager = channelManager,
      controlPlane = controlPlane,
      scheduler = scheduler,
      brokerId = brokerId,
      brokerEpochSupplier = () => brokerEpoch,
      time = mockTime
    )
  }

  @AfterEach
  def tearDown(): Unit = {
    if (manager != null) manager.removeMetrics()
  }

  private def fireLinger(): Unit = {
    mockTime.sleep(manager.lingerMs)
    scheduler.tick()
  }

  private def fireRetry(): Unit = {
    mockTime.sleep(manager.retryPeriodMs + 1)
    scheduler.tick()
  }

  private def fireUploadCheck(): Unit = {
    mockTime.sleep(manager.uploadCheckPeriodMs)
    scheduler.tick()
  }

  private def expectedBackoffMs(attemptNumber: Int): Long = {
    var delay = manager.retryPeriodMs
    var exponent = attemptNumber - 1
    while (exponent > 0 && delay < manager.maxRetryTimeMs) {
      if (delay >= (manager.maxRetryTimeMs + 1L) / 2L) {
        delay = manager.maxRetryTimeMs
      } else {
        delay = delay * 2L
      }
      exponent -= 1
    }
    Math.min(delay, manager.maxRetryTimeMs)
  }

  private def pollAndComplete(response: InitDisklessLogResponseData): Unit = {
    assertEquals(1, channelManager.requests.size())
    channelManager.requests.poll().complete(response)
  }

  private def assertState[T <: InitDisklessLogState](tp: TopicPartition)(implicit m: Manifest[T]): Unit = {
    val state = manager.getInitState(tp)
    assertTrue(
      state.exists(s => m.runtimeClass.isInstance(s)),
      s"Expected state ${m.runtimeClass.getSimpleName} for $tp but was $state"
    )
  }

  private def listenerFor(tp: TopicPartition): PartitionListener = {
    listenersByTp.getOrElse(tp, throw new AssertionError(s"Missing listener for $tp"))
  }

  private def triggerHighWatermarkUpdate(tp: TopicPartition, offset: Long): Unit = {
    listenerFor(tp).onHighWatermarkUpdated(tp, offset)
  }

  private def triggerFailed(tp: TopicPartition): Unit = {
    listenerFor(tp).onFailed(tp)
  }

  private def triggerDeleted(tp: TopicPartition): Unit = {
    listenerFor(tp).onDeleted(tp)
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
    doAnswer(invocation => {
      val listener = invocation.getArgument[PartitionListener](0)
      listenersByTp.put(tp, listener)
      true
    }).when(partition).maybeAddListener(any(classOf[PartitionListener]))
    when(log.highWatermark).thenReturn(hw)
    when(log.logEndOffset).thenReturn(leo)
    when(log.logStartOffset).thenReturn(0L)
    // Matches the value of a log whose remote log manager hasn't copied anything yet.
    when(log.highestOffsetInRemoteStorage).thenReturn(-1L)
    when(log.producerStateManager()).thenReturn(producerStateManager)
    when(producerStateManager.activeProducers()).thenReturn(new util.HashMap())

    partition
  }

  // ProducerStateEntry's constructor and addBatch method are package-private to
  // org.apache.kafka.storage.internals.log, so cross-package tests mock the entry instead
  // of building one via addBatch.
  private def mockProducerEntry(producerId: Long, producerEpoch: Short, batches: BatchMetadata*): ProducerStateEntry = {
    val entry = mock(classOf[ProducerStateEntry])
    when(entry.producerId()).thenReturn(producerId)
    when(entry.producerEpoch()).thenReturn(producerEpoch)
    when(entry.batchMetadata()).thenReturn(util.List.of(batches: _*))
    entry
  }

  @Test
  def testRegisterPartitionHWEqualsLEO(): Unit = {
    // Given a partition where HW equals LEO
    val partition = mockPartition(hw = 100, leo = 100)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then the state transitions to SendingToController and a batch send is scheduled (not fired yet)
    assertState[SendingToController](tp0)
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the scheduled batch fires automatically
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testRegisterPartitionHWBelowLEO(): Unit = {
    // Given a partition where HW is below LEO
    val partition = mockPartition(hw = 50, leo = 100)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then the controller is not called and state is WaitingForHW
    assertTrue(channelManager.requests.isEmpty)
    assertState[WaitingForReplication](tp0)
  }

  @Test
  def testHighWatermarkAdvancedTriggersControllerCall(): Unit = {
    // Given a partition registered with HW < LEO
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertState[WaitingForReplication](tp0)

    // When HW advances but does not reach LEO
    val log = partition.log.get
    when(log.highWatermark).thenReturn(80L)
    triggerHighWatermarkUpdate(tp0, 80)

    // Then no batch is scheduled and state stays WaitingForHW
    assertTrue(channelManager.requests.isEmpty)
    assertState[WaitingForReplication](tp0)

    // When HW catches up to LEO
    when(log.highWatermark).thenReturn(100L)
    triggerHighWatermarkUpdate(tp0, 100)

    // Then the state transitions to SendingToController and a batch is scheduled
    assertState[SendingToController](tp0)
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the controller is called and state becomes AwaitingMetadata
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testRequestContainsCorrectData(): Unit = {
    // Given a partition with a specific leader epoch and active producer state
    val partition = mockPartition(hw = 100, leo = 100, leaderEpoch = 5)

    val producerStateManager = partition.log.get.producerStateManager()
    val producerEntry = mockProducerEntry(42L, 1.toShort,
      new BatchMetadata(14, 99, 4, 5000L),
      new BatchMetadata(19, 104, 4, 6000L))
    val producers = new util.HashMap[java.lang.Long, ProducerStateEntry]()
    producers.put(42L, producerEntry)
    when(producerStateManager.activeProducers()).thenReturn(producers)

    // When the partition is registered
    manager.registerPartition(partition, topicId)

    // Then the batch is scheduled but not yet sent
    assertState[SendingToController](tp0)
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
    assertEquals(2, partitionData.producerStates().size())

    val ps = partitionData.producerStates().get(0)
    assertEquals(42L, ps.producerId())
    assertEquals(1.toShort, ps.producerEpoch())
    assertEquals(10, ps.baseSequence())
    assertEquals(14, ps.lastSequence())
    assertEquals(95, ps.assignedOffset())
    assertEquals(5000L, ps.batchMaxTimestamp())

    val ps2 = partitionData.producerStates().get(1)
    assertEquals(42L, ps2.producerId())
    assertEquals(1.toShort, ps2.producerEpoch())
    assertEquals(15, ps2.baseSequence())
    assertEquals(19, ps2.lastSequence())
    assertEquals(100, ps2.assignedOffset())
    assertEquals(6000L, ps2.batchMaxTimestamp())

    captured.complete(makeSuccessResponse(topicId, 0))
  }

  @Test
  def testRequestContainsAllRetainedBatchesForAllActiveProducers(): Unit = {
    // Given a partition with multiple active producers and several retained batches per producer
    val partition = mockPartition(hw = 250, leo = 250, leaderEpoch = 7)

    // Producer id 42, epoch 1, with three retained batches.
    val producerEntry = mockProducerEntry(42L, 1.toShort,
      new BatchMetadata(12, 112, 2, 1000L),
      new BatchMetadata(15, 120, 2, 2000L),
      new BatchMetadata(17, 130, 1, 3000L))

    // Producer id 77, epoch 3, with two retained batches.
    val otherProducerEntry = mockProducerEntry(77L, 3.toShort,
      new BatchMetadata(4, 204, 4, 4000L),
      new BatchMetadata(7, 208, 2, 5000L))

    val producers = new util.HashMap[java.lang.Long, ProducerStateEntry]()
    producers.put(producerEntry.producerId(), producerEntry)
    producers.put(otherProducerEntry.producerId(), otherProducerEntry)
    // Producer id 88 is empty and should not be included in the request.
    producers.put(88L, mockProducerEntry(88L, RecordBatch.NO_PRODUCER_EPOCH))

    val producerStateManager = partition.log.get.producerStateManager()
    when(producerStateManager.activeProducers()).thenReturn(producers)

    // When the partition is registered and the init request is sent
    manager.registerPartition(partition, topicId)
    fireLinger()

    // Then every non-empty retained batch is represented as its own producer state
    assertEquals(1, channelManager.requests.size())
    val captured = channelManager.requests.poll()
    val request = captured.requestData
    val partitionData = request.topics().get(0).partitions().get(0)
    val producerStates = partitionData.producerStates().asScala.toSeq

    assertEquals(250, partitionData.disklessStartOffset())
    assertEquals(7, partitionData.leaderEpoch())
    assertEquals(5, producerStates.size)
    assertFalse(producerStates.exists(_.producerId() == 88L))

    // Tuple format: (producerEpoch, baseSequence, lastSequence, assignedOffset, batchMaxTimestamp).
    def statesFor(producerId: Long): Seq[(Short, Int, Int, Long, Long)] =
      producerStates
        .filter(_.producerId() == producerId)
        .map(ps => (ps.producerEpoch(), ps.baseSequence(), ps.lastSequence(), ps.assignedOffset(), ps.batchMaxTimestamp()))

    assertEquals(Seq(
      (1.toShort, 10, 12, 110L, 1000L),
      (1.toShort, 13, 15, 118L, 2000L),
      (1.toShort, 16, 17, 129L, 3000L)
    ), statesFor(producerEntry.producerId()))
    assertEquals(Seq(
      (3.toShort, 0, 4, 200L, 4000L),
      (3.toShort, 5, 7, 206L, 5000L)
    ), statesFor(otherProducerEntry.producerId()))

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
    assertState[WaitingForReplication](tp0)
  }

  @Test
  def testReEvaluateWaitingForHWTransitionsWhenHWCaughtUp(): Unit = {
    // Given a partition registered with HW < LEO (WaitingForHW)
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertState[WaitingForReplication](tp0)

    // When HW catches up and the partition is re-registered
    val log = partition.log.get
    when(log.highWatermark).thenReturn(100L)
    manager.registerPartition(partition, topicId)

    // Then it transitions to SendingToController
    assertState[SendingToController](tp0)
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, the controller is called and state becomes AwaitingMetadata
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testReEvaluateSendingToControllerEnsuresBatchScheduled(): Unit = {
    // Given a partition in SendingToController after a retriable error
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertState[SendingToController](tp0)

    // When the partition is re-registered (simulating leadership bounce-back),
    // lingerMs allows adding new work before the next periodic retry tick
    manager.registerPartition(partition, topicId)
    fireLinger()

    // Then the linger batch fires and the controller is called again
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)

    // And the periodic retry tick fires harmlessly (partition already in AwaitingMetadata)
    mockTime.sleep(manager.retryPeriodMs)
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
    assertState[AwaitingMetadata](tp0)

    // When the partition is re-registered
    manager.registerPartition(partition, topicId)

    // Then state remains AwaitingMetadata with no additional controller call
    assertState[AwaitingMetadata](tp0)
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
  def testShutdownDrainsTrackedAndUnregistersMetrics(): Unit = {
    // Given several partitions across different states
    val tp1 = new TopicPartition("test-topic", 1)
    val tp2 = new TopicPartition("test-topic", 2)
    val p0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val p1 = mockPartition(tp = tp1, hw = 50, leo = 100)
    val p2 = mockPartition(tp = tp2, hw = 200, leo = 200)
    manager.registerPartition(p0, topicId)
    manager.registerPartition(p1, topicId)
    manager.registerPartition(p2, topicId)
    fireLinger()
    // p0 and p2 are in-flight to the controller; p1 is parked waiting for HW.
    assertEquals(1, channelManager.requests.size())
    assertEquals(Set(tp0, tp1, tp2), manager.getTrackedPartitions)
    assertTrue(initDisklessMetricsRegistered, "metrics should be registered while manager is alive")

    val failedBefore = meterCount(InitDisklessLogManager.InitFailedPerSecMetricName)
    val completedBefore = meterCount(InitDisklessLogManager.InitCompletedPerSecMetricName)

    // When the manager is shut down
    manager.shutdown()

    // Then tracked is drained, no meters are inflated (cancellation is not a
    // failure or completion), and all Yammer metrics are unregistered so the
    // next broker start in the same JVM can re-register them.
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(
      failedBefore,
      meterCount(
        InitDisklessLogManager.InitFailedPerSecMetricName,
        defaultIfMissing = failedBefore
      )
    )
    assertEquals(
      completedBefore,
      meterCount(
        InitDisklessLogManager.InitCompletedPerSecMetricName,
        defaultIfMissing = completedBefore
      )
    )
    assertFalse(initDisklessMetricsRegistered,
      "all InitDisklessLogManager metrics should be unregistered after shutdown")

    // And shutdown is idempotent
    manager.shutdown()
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertFalse(initDisklessMetricsRegistered)
  }

  @Test
  def testRemovePartitionBeforeRetriableResponseDoesNotEnqueueOrphanRetry(): Unit = {
    // Given a partition with an in-flight controller request
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    assertEquals(1, channelManager.requests.size())

    val retriedBefore = meterCount(InitDisklessLogManager.InitRetriedPerSecMetricName)

    // When the partition is removed (e.g., leadership loss) and the
    // controller subsequently responds with a retriable error
    manager.removePartition(tp0)
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))

    // Then no retry is enqueued: the queue's `enqueueRetryOrFail` must
    // observe the now-removed promise and skip both re-queueing and the
    // onRetry callback. Otherwise the queue would keep firing orphan
    // controller requests on every backoff tick.
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(
      retriedBefore,
      meterCount(InitDisklessLogManager.InitRetriedPerSecMetricName),
      "removed partition's retriable response must not increment the retried meter"
    )

    // Advance well past the maximum backoff to confirm no retry task fires.
    mockTime.sleep(manager.maxRetryTimeMs + 1)
    scheduler.tick()
    assertTrue(channelManager.requests.isEmpty,
      "no request should be issued for a partition that was removed before the response")
  }

  @Test
  def testRemovePartitionBeforeTransportFailureDoesNotEnqueueOrphanRetry(): Unit = {
    // Same race as above, but exercising the `Left(reason)` path of
    // `enqueueRetryOrFail` (transport-level failure -- here, a request
    // timeout) which iterates over every in-flight entry.
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    assertEquals(1, channelManager.requests.size())

    val retriedBefore = meterCount(InitDisklessLogManager.InitRetriedPerSecMetricName)

    manager.removePartition(tp0)
    channelManager.requests.poll().timeout()

    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(
      retriedBefore,
      meterCount(InitDisklessLogManager.InitRetriedPerSecMetricName)
    )

    mockTime.sleep(manager.maxRetryTimeMs + 1)
    scheduler.tick()
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testRemovePartitionDuringInFlightSendIsNotCountedAsFailure(): Unit = {
    // Given a partition with an in-flight controller request
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    assertEquals(1, channelManager.requests.size())

    val failedBefore = meterCount(InitDisklessLogManager.InitFailedPerSecMetricName)

    // When the partition is removed before the response arrives
    // (e.g. on leadership loss), `removePartition` calls `queue.remove(tp)`
    // which completes the pending promise with `false`.
    manager.removePartition(tp0)

    // Then the init diskless log operation is no longer tracked and is NOT counted as a failure:
    // the parasitic callback's `accepted = false` branch must distinguish a
    // cancellation (tracked already cleared) from a permanent failure.
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(
      failedBefore,
      meterCount(InitDisklessLogManager.InitFailedPerSecMetricName),
      "removePartition during in-flight send must not increment the failed meter"
    )

    // And a late-arriving controller response remains a no-op for metrics:
    // the queue's promise is already completed, so `completeAndRemovePromise`
    // finds no promise to fulfill.
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertEquals(
      failedBefore,
      meterCount(InitDisklessLogManager.InitFailedPerSecMetricName)
    )
    assertEquals(
      0L,
      meterCount(InitDisklessLogManager.InitCompletedPerSecMetricName)
    )
  }

  private def meterCount(name: String): Long = {
    val (_, metric) = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (mn, _) => mn.getType == "InitDisklessLogManager" && mn.getName == name }
      .getOrElse(throw new AssertionError(
        s"Meter $name not registered on InitDisklessLogManager"))
    metric.asInstanceOf[com.yammer.metrics.core.Meter].count()
  }

  // Variant used after shutdown(), when the meter may already have been removed
  // from the registry. Returns `defaultIfMissing` when the metric is gone.
  private def meterCount(name: String, defaultIfMissing: Long): Long = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (mn, _) => mn.getType == "InitDisklessLogManager" && mn.getName == name }
      .map { case (_, metric) => metric.asInstanceOf[com.yammer.metrics.core.Meter].count() }
      .getOrElse(defaultIfMissing)
  }

  private def initDisklessMetricsRegistered: Boolean = {
    val expected = InitDisklessLogManager.MetricNames
    val present = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collect {
      case (mn, _) if mn.getType == "InitDisklessLogManager" => mn.getName
    }.toSet
    expected.subsetOf(present)
  }

  @Test
  def testPermanentErrorRemovesFromTracking(): Unit = {
    // Given a partition registered with HW = LEO and a batch scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    assertState[SendingToController](tp0)

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
    assertState[SendingToController](tp0)

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
    assertState[SendingToController](tp0)

    // When the scheduled batch fires and the controller returns a retriable error (NOT_CONTROLLER)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))

    // Then the partition stays in SendingToController
    assertState[SendingToController](tp0)

    // When the retry fires and succeeds
    fireRetry()
    pollAndComplete(makeSuccessResponse(topicId, 0))

    // Then state transitions to AwaitingMetadata
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testExceptionSchedulesRetry(): Unit = {
    // Given a partition registered with HW = LEO and a batch scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    assertState[SendingToController](tp0)

    // When the scheduled batch fires and the controller call times out
    fireLinger()
    channelManager.requests.poll().timeout()

    // Then the partition stays in SendingToController
    assertState[SendingToController](tp0)

    // When the retry fires and succeeds
    fireRetry()
    pollAndComplete(makeSuccessResponse(topicId, 0))

    // Then state transitions to AwaitingMetadata
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testRetryCancelledWhenNotLeader(): Unit = {
    // Given a partition that received a retriable error with a retry scheduled
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertState[SendingToController](tp0)

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
    assertState[SendingToController](tp0)

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
    triggerFailed(tp0)

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
    triggerDeleted(tp0)

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
  def testRetriesUseExponentialBackoffCappedByMaxRetryTime(): Unit = {
    // Given a partition that always receives retriable errors
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))

    // When retries continue, delay grows exponentially and is capped by maxRetryTimeMs
    for (attemptNumber <- 1 to 6) {
      mockTime.sleep(expectedBackoffMs(attemptNumber) + 1)
      scheduler.tick()
      pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
      assertState[SendingToController](tp0)
    }

    // Then retriable errors never exhaust; partition remains tracked
    assertState[SendingToController](tp0)
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
    assertState[SendingToController](tp0)
    assertState[WaitingForReplication](tp1)
    assertState[SendingToController](tp2)
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
    assertState[AwaitingMetadata](tp0)
    assertState[WaitingForReplication](tp1)
    assertState[AwaitingMetadata](tp2)

    // When the third partition's HW catches up
    val log1 = partition1.log.get
    when(log1.highWatermark).thenReturn(200L)
    triggerHighWatermarkUpdate(tp1, 200)

    // Then it transitions to SendingToController
    assertState[SendingToController](tp1)

    // And after lingerMs elapses, it is sent to the controller on its own
    fireLinger()
    val lateRequest = channelManager.requests.peek().requestData
    assertEquals(1, lateRequest.topics().get(0).partitions().size())
    assertEquals(1, lateRequest.topics().get(0).partitions().get(0).partitionId())
    pollAndComplete(makeSuccessResponse(topicId, 1))
    assertState[AwaitingMetadata](tp1)
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
    assertState[SendingToController](tp0)
    assertState[SendingToController](tp1)
    assertState[SendingToController](tp2)
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
    assertState[AwaitingMetadata](tp0)
    assertState[AwaitingMetadata](tp1)
    assertState[AwaitingMetadata](tp2)
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
    assertState[SendingToController](tp0)
    assertState[SendingToController](tp1)
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, a single controller call is made with both topics
    fireLinger()
    val request = channelManager.requests.peek().requestData
    assertEquals(2, request.topics().size())
    val requestTopicIds = request.topics().asScala.map(_.topicId()).toSet
    assertEquals(Set(topicId, topicId2), requestTopicIds)
    pollAndComplete(successResponse)

    // And both partitions transition to AwaitingMetadata
    assertState[AwaitingMetadata](tp0)
    assertState[AwaitingMetadata](tp1)
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
    assertState[AwaitingMetadata](tp0)
    assertEquals(None, manager.getInitState(tp1))
    assertState[SendingToController](tp2)

    // When the retry fires for the retriable-error partition
    fireRetry()
    val retryRequest = channelManager.requests.peek().requestData
    assertEquals(1, retryRequest.topics().get(0).partitions().size())
    assertEquals(2, retryRequest.topics().get(0).partitions().get(0).partitionId())
    pollAndComplete(makeSuccessResponse(topicId, 2))

    // Then only that partition is retried and transitions to AwaitingMetadata
    assertState[AwaitingMetadata](tp2)
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
    assertState[SendingToController](tp0)
    assertState[SendingToController](tp1)

    // When the retry fires and succeeds
    fireRetry()
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))

    // Then both partitions transition to AwaitingMetadata
    assertState[AwaitingMetadata](tp0)
    assertState[AwaitingMetadata](tp1)
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
    triggerHighWatermarkUpdate(tp0, 100)
    triggerHighWatermarkUpdate(tp1, 100)

    // Then both transition to SendingToController and a batch is scheduled
    assertState[SendingToController](tp0)
    assertState[SendingToController](tp1)
    assertTrue(channelManager.requests.isEmpty)

    // And after lingerMs elapses, a single controller call is made with both partitions
    fireLinger()
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))
    assertState[AwaitingMetadata](tp0)
    assertState[AwaitingMetadata](tp1)
  }

  @Test
  def testNewPartitionPreemptsRetryBackoff(): Unit = {
    // Given a partition in SendingToController with a periodic retry pending
    val tp1 = new TopicPartition("test-topic", 1)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    manager.registerPartition(partition0, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertState[SendingToController](tp0)

    // When a new ready partition is registered within the lingerMs window
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)
    manager.registerPartition(partition1, topicId)

    // Then both partitions are in SendingToController
    assertState[SendingToController](tp0)
    assertState[SendingToController](tp1)

    // And after lingerMs elapses, both partitions are sent together
    fireLinger()
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))
    assertState[AwaitingMetadata](tp0)
    assertState[AwaitingMetadata](tp1)
  }

  @Test
  def testFirstRetryFiresWithFixedPeriod(): Unit = {
    // Given a partition that receives a retriable error on first attempt
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER))
    assertState[SendingToController](tp0)

    // When the first retry fires
    fireRetry()
    pollAndComplete(makeSuccessResponse(topicId, 0))

    // Then retry uses fixed period and succeeds
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testRetryBackoffUsesLargestAttemptAcrossQueuedPartitions(): Unit = {
    // Given one partition that has already accumulated retriable attempts
    val tp1 = new TopicPartition("test-topic", 1)
    val partition0 = mockPartition(tp = tp0, hw = 100, leo = 100)
    val partition1 = mockPartition(tp = tp1, hw = 200, leo = 200)
    manager.registerPartition(partition0, topicId)
    fireLinger()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER)) // tp0 -> attempt 1 queued
    fireRetry()
    pollAndComplete(makeErrorResponse(topicId, 0, Errors.NOT_CONTROLLER)) // tp0 -> attempt 2 queued

    // When a new partition arrives and both are sent in one batch, then both fail retriably:
    // tp0 becomes attempt 3, tp1 becomes attempt 1.
    manager.registerPartition(partition1, topicId)
    fireLinger()
    pollAndComplete(makeBatchErrorResponse(topicId, Seq(0, 1), Errors.NOT_CONTROLLER))

    // Then retry scheduling uses the largest attempt in queue (tp0 attempt 3), not tp1 attempt 1.
    // So a tick after the smaller backoff should not fire any request.
    mockTime.sleep(expectedBackoffMs(1) + 1)
    scheduler.tick()
    assertTrue(channelManager.requests.isEmpty)

    // But once the remaining delay to attempt-3 backoff elapses, a single retry batch is sent.
    mockTime.sleep(expectedBackoffMs(3) - expectedBackoffMs(1) + 1)
    scheduler.tick()
    val retryRequest = channelManager.requests.peek().requestData
    val partitionIds = retryRequest.topics().get(0).partitions().asScala.map(_.partitionId()).toSet
    assertEquals(Set(0, 1), partitionIds)
    pollAndComplete(makeBatchSuccessResponse(topicId, Seq(0, 1)))
    assertState[AwaitingMetadata](tp0)
    assertState[AwaitingMetadata](tp1)
  }

  @Test
  def testHWCatchesUpBetweenCheckAndListenerRegistration(): Unit = {
    // Simulate HW advancing to LEO between the initial maybeAdvanceState()
    // check and the maybeAddListener() call, with no further HW updates.
    val partition = mock(classOf[Partition])
    val log = mock(classOf[UnifiedLog])
    val producerStateManager = mock(classOf[ProducerStateManager])

    when(partition.topicPartition).thenReturn(tp0)
    when(partition.isSealed).thenReturn(true)
    when(partition.isLeader).thenReturn(true)
    when(partition.getLeaderEpoch).thenReturn(1)
    when(partition.log).thenReturn(Some(log))
    when(log.logEndOffset).thenReturn(100L)
    when(log.producerStateManager()).thenReturn(producerStateManager)
    when(producerStateManager.activeProducers()).thenReturn(new util.HashMap())

    // HW starts below LEO for the initial evaluation
    when(log.highWatermark).thenReturn(50L)

    // When maybeAddListener is called, HW has caught up — simulating
    // the race where replicas finish between check and listener registration
    doAnswer { invocation =>
      when(log.highWatermark).thenReturn(100L)
      listenersByTp.put(tp0, invocation.getArgument[PartitionListener](0))
      true
    }.when(partition).maybeAddListener(any(classOf[PartitionListener]))

    manager.registerPartition(partition, topicId)

    // Then the partition advances to SendingToController despite no explicit
    // HW listener callback, because the post-listener re-evaluation catches it
    assertState[SendingToController](tp0)

    // And the flow completes normally
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testStaleListenerCallbackDoesNotRegressState(): Unit = {
    // Given a partition registered with HW < LEO (WaitingForReplication)
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertState[WaitingForReplication](tp0)

    // When HW catches up to LEO, advancing state to SendingToController
    val log = partition.log.get
    when(log.highWatermark).thenReturn(100L)
    triggerHighWatermarkUpdate(tp0, 100)
    assertState[SendingToController](tp0)

    // And a stale HW listener callback fires after the state has already advanced
    triggerHighWatermarkUpdate(tp0, 100)

    // Then the state is NOT regressed back to SendingToController (a new instance) —
    // the existing SendingToController state is preserved
    assertState[SendingToController](tp0)

    // And the flow completes normally
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)
  }

  @Test
  def testStaleListenerCallbackDoesNotRegressFromAwaitingMetadata(): Unit = {
    // Given a partition registered with HW < LEO so a listener is captured
    val partition = mockPartition(hw = 50, leo = 100)
    manager.registerPartition(partition, topicId)
    assertState[WaitingForReplication](tp0)

    // When HW catches up, advancing to SendingToController, then to AwaitingMetadata
    val log = partition.log.get
    when(log.highWatermark).thenReturn(100L)
    triggerHighWatermarkUpdate(tp0, 100)
    assertState[SendingToController](tp0)
    fireLinger()
    pollAndComplete(makeSuccessResponse(topicId, 0))
    assertState[AwaitingMetadata](tp0)

    // And a stale HW listener callback fires after the state has already advanced
    triggerHighWatermarkUpdate(tp0, 100)

    // Then the state remains AwaitingMetadata and no spurious controller call is made
    assertState[AwaitingMetadata](tp0)
    assertTrue(channelManager.requests.isEmpty)
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

  private def makeBatchErrorResponse(
    topicId: Uuid,
    partitionIds: Seq[Int],
    error: Errors
  ): InitDisklessLogResponseData = {
    val partitions = new util.ArrayList[InitDisklessLogResponseData.PartitionResponse]()
    partitionIds.foreach { id =>
      partitions.add(new InitDisklessLogResponseData.PartitionResponse()
        .setPartitionId(id)
        .setErrorCode(error.code()))
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

    manager.initOnControlPlane(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      classicToDisklessStartOffset = 100L,
      producerStates = util.List.of(new CpProducerState(1L, 0.toShort, 0, 1, 100L, 1000L)),
      classicPrefixInRemoteStorage = true
    )

    fireLinger()

    verify(controlPlane).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testMetadataAppliedAlreadyInitializedIsTerminalSuccess(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any())).thenReturn(util.List.of(CpInitResponse.alreadyInitialized()))

    manager.initOnControlPlane(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      classicToDisklessStartOffset = 100L,
      producerStates = util.List.of(),
      classicPrefixInRemoteStorage = true
    )

    fireLinger()

    verify(controlPlane).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testMetadataAppliedRetriableErrorSchedulesRetry(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any()))
      .thenReturn(util.List.of(new CpInitResponse(Errors.NOT_CONTROLLER)))
      .thenReturn(util.List.of(CpInitResponse.success()))

    manager.initOnControlPlane(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      classicToDisklessStartOffset = 100L,
      producerStates = util.List.of(),
      classicPrefixInRemoteStorage = true
    )

    fireLinger()
    assertState[AwaitingMetadata](tp0)
    verify(controlPlane, times(1)).initDisklessLog(any())

    fireRetry()
    verify(controlPlane, times(2)).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testMetadataAppliedRepeatedCallbacksAreDeduplicated(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    when(controlPlane.initDisklessLog(any())).thenReturn(util.List.of(CpInitResponse.success()))

    manager.initOnControlPlane(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      classicToDisklessStartOffset = 100L,
      producerStates = util.List.of(),
      classicPrefixInRemoteStorage = true
    )
    manager.initOnControlPlane(
      partition = partition,
      topicId = topicId,
      topicName = tp0.topic(),
      classicToDisklessStartOffset = 100L,
      producerStates = util.List.of(),
      classicPrefixInRemoteStorage = true
    )

    fireLinger()

    verify(controlPlane, times(1)).initDisklessLog(any())
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  private val classicSeal = 100L

  // Makes the control plane accept every request in a batch.
  private def stubControlPlaneInitSuccess(): Unit = {
    doAnswer((invocation: InvocationOnMock) =>
      util.Collections.nCopies(invocation.getArgument[util.List[_]](0).size(), CpInitResponse.success())
    ).when(controlPlane).initDisklessLog(any())
  }

  // Hands the partition to the manager as the become-leader hook does, with the flag unset.
  private def startInitWithFlagUnset(partition: Partition, seal: Long = classicSeal, topicId: Uuid = topicId): Unit = {
    stubControlPlaneInitSuccess()
    manager.initOnControlPlane(
      partition = partition,
      topicId = topicId,
      topicName = partition.topicPartition.topic(),
      classicToDisklessStartOffset = seal,
      producerStates = util.List.of(),
      classicPrefixInRemoteStorage = false
    )
  }

  // Runs the control-plane init to completion, so the partition lands in the state that follows AwaitingMetadata.
  private def initWithFlagUnset(partition: Partition, seal: Long = classicSeal): Unit = {
    startInitWithFlagUnset(partition, seal)
    fireLinger()
  }

  private def setRemoteProgress(partition: Partition, logStartOffset: Long = 0L, highestRemoteOffset: Long): Unit = {
    val log = partition.log.get
    when(log.logStartOffset).thenReturn(logStartOffset)
    when(log.highestOffsetInRemoteStorage).thenReturn(highestRemoteOffset)
  }

  // Leaves the partition in SendingFlagToController with the flag request still lingering.
  private def driveToSendingFlag(partition: Partition, seal: Long = classicSeal): Unit = {
    setRemoteProgress(partition, highestRemoteOffset = seal - 1)
    initWithFlagUnset(partition, seal)
    assertState[SendingFlagToController](partition.topicPartition)
  }

  private def makeSetFlagResponse(
    topicId: Uuid,
    partitionIds: Seq[Int],
    error: Errors = Errors.NONE
  ): SetClassicPrefixInRemoteStorageResponseData = {
    val partitions = partitionIds.map { id =>
      new SetClassicPrefixInRemoteStorageResponseData.PartitionResponse()
        .setPartitionId(id)
        .setErrorCode(error.code())
    }
    new SetClassicPrefixInRemoteStorageResponseData().setTopics(util.List.of(
      new SetClassicPrefixInRemoteStorageResponseData.TopicResponse()
        .setTopicId(topicId)
        .setPartitions(partitions.asJava)
    ))
  }

  private def pollFlagRequest(): MockInitDisklessLogChannelManager#CapturedRequest = {
    assertEquals(1, channelManager.requests.size())
    val captured = channelManager.requests.poll()
    assertEquals(ApiKeys.SET_CLASSIC_PREFIX_IN_REMOTE_STORAGE, captured.apiKey)
    captured
  }

  private def gaugeValue(name: String): Long = {
    val (_, metric) = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (mn, _) => mn.getType == "InitDisklessLogManager" && mn.getName == name }
      .getOrElse(throw new AssertionError(s"Gauge $name not registered on InitDisklessLogManager"))
    metric.asInstanceOf[com.yammer.metrics.core.Gauge[_]].value().asInstanceOf[Number].longValue()
  }

  @Test
  def testInitSuccessWithFlagUnsetWaitsForUpload(): Unit = {
    // Given a partition whose classic prefix isn't in remote storage
    val partition = mockPartition(hw = 100, leo = 100)

    // When the control-plane init succeeds
    initWithFlagUnset(partition)

    // Then the partition stays tracked, waiting for the upload, and no flag request is sent
    assertState[WaitingForUpload](tp0)
    fireLinger()
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testWaitingForUploadAdvancesOnlyWhenHighestRemoteOffsetReachesSealMinusOne(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)

    // When the remote log manager has copied less than the whole prefix
    setRemoteProgress(partition, highestRemoteOffset = classicSeal - 2)
    fireUploadCheck()
    fireUploadCheck()

    // Then the partition keeps waiting and the poller keeps running
    assertState[WaitingForUpload](tp0)
    assertTrue(channelManager.requests.isEmpty)

    // When the last prefix offset reaches remote storage
    setRemoteProgress(partition, highestRemoteOffset = classicSeal - 1)
    fireUploadCheck()

    // Then the partition moves on and the flag request goes out after the linger
    assertState[SendingFlagToController](tp0)
    assertTrue(channelManager.requests.isEmpty)
    fireLinger()
    pollFlagRequest()
  }

  @ParameterizedTest(name = "testPrefixWithNothingToUploadAdvancesImmediately with seal: {0}")
  @ValueSource(longs = Array(0L, 100L))
  def testPrefixWithNothingToUploadAdvancesImmediately(seal: Long): Unit = {
    // Given a leader whose log start offset already reached the seal: a switch sealed at offset 0,
    // or a prefix that retention removed
    val partition = mockPartition(hw = seal, leo = seal)
    setRemoteProgress(partition, logStartOffset = seal, highestRemoteOffset = -1L)

    // When the control-plane init succeeds
    initWithFlagUnset(partition, seal)

    // Then the first evaluation advances the partition with no poll period and no upload
    assertState[SendingFlagToController](tp0)
  }

  @Test
  def testFlagRequestContentsAndBatching(): Unit = {
    // Given two partitions of one topic and one partition of another, all with their prefix uploaded
    val tp1 = new TopicPartition(tp0.topic(), 1)
    val otherTopicId = Uuid.randomUuid()
    val otherTp = new TopicPartition("other-topic", 0)
    val p0 = mockPartition(tp = tp0, hw = 100, leo = 100, leaderEpoch = 3)
    val p1 = mockPartition(tp = tp1, hw = 100, leo = 100, leaderEpoch = 4)
    val p2 = mockPartition(tp = otherTp, hw = 100, leo = 100, leaderEpoch = 5)
    Seq(p0, p1, p2).foreach(p => setRemoteProgress(p, highestRemoteOffset = classicSeal - 1))

    // When all three finish the control-plane init in the same linger window
    startInitWithFlagUnset(p0)
    startInitWithFlagUnset(p1)
    startInitWithFlagUnset(p2, topicId = otherTopicId)
    fireLinger()
    assertState[SendingFlagToController](tp0)
    assertState[SendingFlagToController](tp1)
    assertState[SendingFlagToController](otherTp)

    // Then one request carries all of them with the broker identity, seal, and current leader epochs
    fireLinger()
    val request = pollFlagRequest().setFlagRequestData
    assertEquals(brokerId, request.brokerId())
    assertEquals(brokerEpoch, request.brokerEpoch())
    val partitionsByTopic = request.topics().asScala.map(t => t.topicId() -> t.partitions().asScala.toList).toMap
    assertEquals(Set(topicId, otherTopicId), partitionsByTopic.keySet)

    val sameTopic = partitionsByTopic(topicId).sortBy(_.partitionId())
    assertEquals(List(0, 1), sameTopic.map(_.partitionId()))
    assertEquals(List(3, 4), sameTopic.map(_.leaderEpoch()))
    assertEquals(List(classicSeal, classicSeal), sameTopic.map(_.classicToDisklessStartOffset()))

    val other = partitionsByTopic(otherTopicId).head
    assertEquals(0, other.partitionId())
    assertEquals(5, other.leaderEpoch())
    assertEquals(classicSeal, other.classicToDisklessStartOffset())
  }

  @Test
  def testFlagAcceptedUntracksPartition(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)
    val completedBefore = meterCount(InitDisklessLogManager.FlagRequestCompletedPerSecMetricName)
    val failedBefore = meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName)

    // When the controller accepts the flag
    fireLinger()
    pollFlagRequest().complete(makeSetFlagResponse(topicId, Seq(0)))

    // Then the partition is no longer tracked, the completion is counted, and nothing counts as a failure
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(completedBefore + 1, meterCount(InitDisklessLogManager.FlagRequestCompletedPerSecMetricName))
    assertEquals(failedBefore, meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName))
  }

  @Test
  def testFlagInvalidRequestUntracksAndCountsFailure(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)
    val failedBefore = meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName)
    val initFailedBefore = meterCount(InitDisklessLogManager.InitFailedPerSecMetricName)

    // When the controller rejects the request as inconsistent with the partition's switch state
    fireLinger()
    pollFlagRequest().complete(makeSetFlagResponse(topicId, Seq(0), Errors.INVALID_REQUEST))

    // Then the partition is dropped, the flag failure is counted apart from init failures, and no retry follows
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(failedBefore + 1, meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName))
    assertEquals(initFailedBefore, meterCount(InitDisklessLogManager.InitFailedPerSecMetricName))
    fireRetry()
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testFlagFencedLeaderEpochRetriesWithRefreshedEpoch(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100, leaderEpoch = 1)
    driveToSendingFlag(partition)
    fireLinger()
    val first = pollFlagRequest()
    assertEquals(1, first.setFlagRequestData.topics().get(0).partitions().get(0).leaderEpoch())

    // When an epoch bump with the same leader fences the in-flight request
    when(partition.getLeaderEpoch).thenReturn(2)
    first.complete(makeSetFlagResponse(topicId, Seq(0), Errors.FENCED_LEADER_EPOCH))

    // Then the partition keeps its place and the retry carries the current epoch
    assertState[SendingFlagToController](tp0)
    fireRetry()
    val retry = pollFlagRequest()
    assertEquals(2, retry.setFlagRequestData.topics().get(0).partitions().get(0).leaderEpoch())
    retry.complete(makeSetFlagResponse(topicId, Seq(0)))
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @ParameterizedTest(name = "testFlagRequestRetriesAfterNotControllerOrTimeout with timeout: {0}")
  @ValueSource(booleans = Array(true, false))
  def testFlagRequestRetriesAfterNotControllerOrTimeout(timeout: Boolean): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)
    val retriedBefore = meterCount(InitDisklessLogManager.FlagRequestRetriedPerSecMetricName)
    val initRetriedBefore = meterCount(InitDisklessLogManager.InitRetriedPerSecMetricName)
    fireLinger()
    val first = pollFlagRequest()
    if (timeout) first.timeout()
    else first.complete(makeSetFlagResponse(topicId, Seq(0), Errors.NOT_CONTROLLER))
    assertState[SendingFlagToController](tp0)
    assertEquals(retriedBefore + 1, meterCount(InitDisklessLogManager.FlagRequestRetriedPerSecMetricName))
    assertEquals(initRetriedBefore, meterCount(InitDisklessLogManager.InitRetriedPerSecMetricName))

    fireRetry()
    pollFlagRequest().complete(makeSetFlagResponse(topicId, Seq(0)))
    assertTrue(manager.getTrackedPartitions.isEmpty)
  }

  @Test
  def testRemovePartitionWhileWaitingForUploadSendsNothing(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)

    // When leadership is lost and the prefix would otherwise count as uploaded
    manager.removePartition(tp0)
    setRemoteProgress(partition, highestRemoteOffset = classicSeal - 1)
    fireUploadCheck()
    fireLinger()

    // Then nothing is tracked and no flag request goes out
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testRemovePartitionWhileSendingFlagSendsNothing(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)

    // When the partition is removed before the linger elapses
    manager.removePartition(tp0)
    fireLinger()

    // Then no flag request goes out
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testRemovePartitionDuringInFlightFlagRequestIsNotCountedAsFailure(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)
    fireLinger()
    val inFlight = pollFlagRequest()
    val failedBefore = meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName)

    // When the partition is removed and a retriable error arrives afterward
    manager.removePartition(tp0)
    inFlight.complete(makeSetFlagResponse(topicId, Seq(0), Errors.NOT_CONTROLLER))

    // Then neither the failure meter nor the retry path fires
    assertEquals(failedBefore, meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName))
    fireRetry()
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testFailoverReRegistersTheUploadWatch(): Unit = {
    // Given a partition waiting for the upload on the old leader
    val oldLeader = mockPartition(hw = 100, leo = 100)
    initWithFlagUnset(oldLeader)
    assertState[WaitingForUpload](tp0)

    // When the old leader loses leadership and a new leader whose remote offset already covers the prefix registers
    manager.removePartition(tp0)
    assertTrue(manager.getTrackedPartitions.isEmpty)
    val newLeader = mockPartition(hw = 100, leo = 100, leaderEpoch = 2)
    setRemoteProgress(newLeader, highestRemoteOffset = classicSeal - 1)
    doReturn(util.List.of(CpInitResponse.alreadyInitialized())).when(controlPlane).initDisklessLog(any())
    manager.initOnControlPlane(
      partition = newLeader,
      topicId = topicId,
      topicName = tp0.topic(),
      classicToDisklessStartOffset = classicSeal,
      producerStates = util.List.of(),
      classicPrefixInRemoteStorage = false
    )
    fireLinger()

    // Then it converges to the flag request with no other action
    assertState[SendingFlagToController](tp0)
    fireLinger()
    val request = pollFlagRequest().setFlagRequestData
    assertEquals(2, request.topics().get(0).partitions().get(0).leaderEpoch())
  }

  @Test
  def testReInitWhileSendingFlagReplacesTheQueuedRequest(): Unit = {
    // Given a partition queued for the flag request
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)

    // When a new leader-change callback re-inits it before the linger elapses
    initWithFlagUnset(partition)

    // Then the stale queue entry sends nothing and one request follows the fresh state
    assertTrue(channelManager.requests.isEmpty)
    assertState[SendingFlagToController](tp0)
    fireLinger()
    pollFlagRequest()
    assertTrue(channelManager.requests.isEmpty)
  }

  @Test
  def testReInitWithLowerSealRestartsTheUploadWatch(): Unit = {
    // Given a partition waiting for an upload that stopped short of the original seal
    val partition = mockPartition(hw = 100, leo = 100)
    val loweredSeal = classicSeal - 20
    setRemoteProgress(partition, highestRemoteOffset = loweredSeal - 1)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)

    // When an operator re-seal lowers the seal and the manager re-inits the partition
    initWithFlagUnset(partition, seal = loweredSeal)

    // Then the watch checks the lowered seal and the flag request carries it
    assertState[SendingFlagToController](tp0)
    fireLinger()
    val request = pollFlagRequest().setFlagRequestData
    assertEquals(loweredSeal, request.topics().get(0).partitions().get(0).classicToDisklessStartOffset())
  }

  @Test
  def testStaleInitCompletionDuringReInitLeavesTheReplacementToItsOwnInit(): Unit = {
    // Given a queued control-plane init for the original seal, and an upload that already covers a lower seal
    val partition = mockPartition(hw = 100, leo = 100)
    val loweredSeal = classicSeal - 20
    setRemoteProgress(partition, highestRemoteOffset = loweredSeal - 1)
    startInitWithFlagUnset(partition)

    // When the original init completes after a re-seal installs the replacement but before it queues it.
    // The manager reads the clock in that window, when it refreshes the metrics.
    var staleInitCompleted = false
    mockTime.runOnNextRead(() => {
      fireLinger()
      staleInitCompleted = true
    })
    startInitWithFlagUnset(partition, seal = loweredSeal)
    assertTrue(staleInitCompleted)
    verify(controlPlane, times(1)).initDisklessLog(any())

    // Then the replacement waits for its own init, and the flag request that follows carries the lower seal
    assertState[AwaitingMetadata](tp0)
    fireLinger()
    verify(controlPlane, times(2)).initDisklessLog(any())
    assertState[SendingFlagToController](tp0)
    fireLinger()
    val request = pollFlagRequest().setFlagRequestData
    assertEquals(loweredSeal, request.topics().get(0).partitions().get(0).classicToDisklessStartOffset())
  }

  @Test
  def testLostInitDisklessLogResponseAfterSealCommitSendsNoRetry(): Unit = {
    // Given a leader whose InitDisklessLog request is still in flight when the seal commit reaches it
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    val sealRequest = channelManager.requests.poll()
    assertEquals(ApiKeys.INIT_DISKLESS_LOG, sealRequest.apiKey)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)
    val failedBefore = meterCount(InitDisklessLogManager.InitFailedPerSecMetricName)

    // When the lost response times out
    sealRequest.timeout()
    fireRetry()

    // Then no retry goes out, because the controller would reject it, and the upload watch survives
    assertTrue(channelManager.requests.isEmpty)
    assertState[WaitingForUpload](tp0)
    assertEquals(failedBefore, meterCount(InitDisklessLogManager.InitFailedPerSecMetricName))
  }

  @Test
  def testStaleInitDisklessLogRejectionAfterSealCommitKeepsTheUploadWatch(): Unit = {
    // Given a leader whose InitDisklessLog request is still in flight when the seal commit reaches it
    val partition = mockPartition(hw = 100, leo = 100)
    manager.registerPartition(partition, topicId)
    fireLinger()
    val sealRequest = channelManager.requests.poll()
    assertEquals(ApiKeys.INIT_DISKLESS_LOG, sealRequest.apiKey)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)
    val failedBefore = meterCount(InitDisklessLogManager.InitFailedPerSecMetricName)

    // When the controller rejects that request because the partition is no longer switch-pending
    sealRequest.complete(makeErrorResponse(topicId, 0, Errors.INVALID_REQUEST))

    // Then the upload watch survives and nothing counts as a failed init
    assertState[WaitingForUpload](tp0)
    assertEquals(failedBefore, meterCount(InitDisklessLogManager.InitFailedPerSecMetricName))
  }

  @Test
  def testLeadershipLostAtPollTimeDropsThePartition(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)

    // When the broker stops being the leader before the next poll
    when(partition.isLeader).thenReturn(false)
    setRemoteProgress(partition, highestRemoteOffset = classicSeal - 1)
    fireUploadCheck()
    fireLinger()

    // Then the partition is dropped and no flag request goes out
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertTrue(channelManager.requests.isEmpty)
  }

  @ParameterizedTest(name = "testSkippedFlagRequestIsNotCountedAsFailure with leadershipLost: {0}")
  @ValueSource(booleans = Array(true, false))
  def testSkippedFlagRequestIsNotCountedAsFailure(leadershipLost: Boolean): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    driveToSendingFlag(partition)
    val failedBefore = meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName)

    // When the broker stops being the leader, or a topic deletion removes the log, before the linger elapses
    if (leadershipLost) when(partition.isLeader).thenReturn(false)
    else when(partition.log).thenReturn(None)
    fireLinger()

    // Then the request is skipped, the partition is dropped, and nothing counts as a failure
    assertTrue(channelManager.requests.isEmpty)
    assertTrue(manager.getTrackedPartitions.isEmpty)
    assertEquals(failedBefore, meterCount(InitDisklessLogManager.FlagRequestFailedPerSecMetricName))
  }

  @Test
  def testUploadCheckFailureKeepsWatchingAndRetriesOnNextPoll(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)
    initWithFlagUnset(partition)
    assertState[WaitingForUpload](tp0)

    // When reading the log throws during a poll
    val log = partition.log.get
    when(log.logStartOffset).thenThrow(new RuntimeException("log read failed"))
    fireUploadCheck()

    // Then the partition stays tracked and the next poll still runs
    assertState[WaitingForUpload](tp0)
    doReturn(0L).when(log).logStartOffset
    setRemoteProgress(partition, highestRemoteOffset = classicSeal - 1)
    fireUploadCheck()
    assertState[SendingFlagToController](tp0)
  }

  @Test
  def testUploadStateGaugesAndInFlightPartitionsExcludeUploadStates(): Unit = {
    val partition = mockPartition(hw = 100, leo = 100)

    // When a partition waits for the upload
    initWithFlagUnset(partition)
    mockTime.sleep(5000)

    // Then the upload gauge counts it with its age, and InFlightPartitions leaves it out
    assertEquals(1L, gaugeValue(InitDisklessLogManager.WaitingForUploadPartitionsMetricName))
    assertTrue(gaugeValue(InitDisklessLogManager.OldestWaitingForUploadAgeMsMetricName) >= 5000L)
    assertEquals(0L, gaugeValue(InitDisklessLogManager.InFlightPartitionsMetricName))
    assertEquals(0L, gaugeValue(InitDisklessLogManager.SendingFlagToControllerPartitionsMetricName))

    // When the prefix is uploaded
    setRemoteProgress(partition, highestRemoteOffset = classicSeal - 1)
    fireUploadCheck()

    // Then the partition moves to the flag gauge, and InFlightPartitions still leaves it out
    assertEquals(0L, gaugeValue(InitDisklessLogManager.WaitingForUploadPartitionsMetricName))
    assertEquals(0L, gaugeValue(InitDisklessLogManager.OldestWaitingForUploadAgeMsMetricName))
    assertEquals(1L, gaugeValue(InitDisklessLogManager.SendingFlagToControllerPartitionsMetricName))
    assertEquals(0L, gaugeValue(InitDisklessLogManager.InFlightPartitionsMetricName))

    // When the controller accepts the flag
    fireLinger()
    pollFlagRequest().complete(makeSetFlagResponse(topicId, Seq(0)))

    // Then every gauge is back to zero
    assertEquals(0L, gaugeValue(InitDisklessLogManager.SendingFlagToControllerPartitionsMetricName))
    assertEquals(0L, gaugeValue(InitDisklessLogManager.OldestSendingFlagToControllerAgeMsMetricName))
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
    def apiKey: ApiKeys = requestBuilder.apiKey

    def requestData: InitDisklessLogRequestData = {
      requestBuilder.build(ApiKeys.INIT_DISKLESS_LOG.latestVersion()).asInstanceOf[InitDisklessLogRequest].data()
    }

    def setFlagRequestData: SetClassicPrefixInRemoteStorageRequestData = {
      requestBuilder.build(ApiKeys.SET_CLASSIC_PREFIX_IN_REMOTE_STORAGE.latestVersion())
        .asInstanceOf[SetClassicPrefixInRemoteStorageRequest].data()
    }

    def complete(responseData: InitDisklessLogResponseData): Unit = {
      val response = new InitDisklessLogResponse(responseData)
      val header = new RequestHeader(ApiKeys.INIT_DISKLESS_LOG, ApiKeys.INIT_DISKLESS_LOG.latestVersion(), "", 0)
      val clientResponse = new ClientResponse(header, callback, "-1", 0L, 0L, false, null, null, response)
      callback.onComplete(clientResponse)
    }

    def complete(responseData: SetClassicPrefixInRemoteStorageResponseData): Unit = {
      val response = new SetClassicPrefixInRemoteStorageResponse(responseData)
      val header = new RequestHeader(
        ApiKeys.SET_CLASSIC_PREFIX_IN_REMOTE_STORAGE, ApiKeys.SET_CLASSIC_PREFIX_IN_REMOTE_STORAGE.latestVersion(), "", 0)
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

/**
 * `MockTime` that runs a one-shot hook on the next clock read, so a test can deliver a response at a
 * point inside a manager call that no other seam reaches.
 */
private[server] class HookedMockTime extends MockTime {
  @volatile private var onNextRead: Option[() => Unit] = None

  def runOnNextRead(hook: () => Unit): Unit = onNextRead = Some(hook)

  override def milliseconds(): Long = {
    val hook = onNextRead
    onNextRead = None
    hook.foreach(_())
    super.milliseconds()
  }
}
