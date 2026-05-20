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

import io.aiven.inkless.control_plane.{ControlPlane, InitDisklessLogProducerState => CpProducerState}
import kafka.cluster.Partition
import kafka.server.InitDisklessLogManager._
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.NodeToControllerChannelManager
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class InitDisklessLogManager(
  controllerChannelManager: NodeToControllerChannelManager,
  controlPlane: ControlPlane,
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long,
  time: Time = Time.SYSTEM
) extends Logging {

  this.logIdent = s"[InitDisklessLogManager broker=$brokerId] "

  private val tracked = new ConcurrentHashMap[TopicPartition, InitDisklessLogState]()
  private val metrics = new Metrics(time, () => tracked.size)

  // Notify metrics that `tp`'s tracked state has changed. Call after every
  // mutation of `tracked`. Best-effort: a tiny race with concurrent mutations
  // is tolerated.
  private def refreshMetrics(tp: TopicPartition): Unit = metrics.onStateChange(tp, tracked.get(tp))

  // Delay before firing a batch send, allowing multiple partitions that become ready
  // around the same time (e.g., when an entire topic is sealed) to be coalesced into one request.
  private[server] val lingerMs = 500L
  // Initial retry period for retriable failures.
  private[server] val retryPeriodMs = 1000L
  // Maximum exponential backoff delay used between retriable attempts.
  private[server] val maxRetryTimeMs = 10000L
  private val sendingToControllerQueue = new SendingToControllerBatchQueue(
    controllerChannelManager = controllerChannelManager,
    scheduler = scheduler,
    brokerId = brokerId,
    brokerEpochSupplier = brokerEpochSupplier,
    lingerMs = lingerMs,
    retryPeriodMs = retryPeriodMs,
    maxRetryTimeMs = maxRetryTimeMs,
    onRetry = () => metrics.markRetried()
  )
  private val awaitingMetadataQueue = new AwaitingMetadataBatchQueue(
    controlPlane = controlPlane,
    scheduler = scheduler,
    brokerId = brokerId,
    brokerEpochSupplier = brokerEpochSupplier,
    lingerMs = lingerMs,
    retryPeriodMs = retryPeriodMs,
    maxRetryTimeMs = maxRetryTimeMs,
    onRetry = () => metrics.markRetried()
  )

  private[server] def getTrackedPartitions: Set[TopicPartition] = tracked.keySet().asScala.toSet

  private[server] def getInitState(tp: TopicPartition): Option[InitDisklessLogState] = Option(tracked.get(tp))

  /**
   * Handles already-applied diskless init metadata for a partition.
   * This keeps/moves the partition to AwaitingMetadata and triggers a prompt
   * control-plane init send, since metadata is committed and visible.
   */
  def initOnControlPlane(
    partition: Partition,
    topicId: Uuid,
    topicName: String,
    classicToDisklessStartOffset: Long,
    producerStates: java.util.List[CpProducerState]
  ): Unit = {
    if (classicToDisklessStartOffset < 0) {
      warn(s"Received negative classicToDisklessStartOffset ($classicToDisklessStartOffset) for $topicName:${partition.topicPartition}, skipping control-plane init")
      return
    }

    val tp = partition.topicPartition
    val payload = DisklessInitMetadata(topicName, classicToDisklessStartOffset, producerStates)
    val newState = AwaitingMetadata(partition, topicId, Some(payload))
    if (tracked.putIfAbsent(tp, newState) != null) {
      tracked.computeIfPresent(tp, (_, _) => newState)
    }
    refreshMetrics(tp)
    enqueueAwaitingMetadata(tp, newState)
  }

  /**
   * Register a sealed partition for init diskless log. Registers this manager as a
   * PartitionListener to receive HW advancement notifications. If HW already
   * equals LEO, immediately marks the partition ready and schedules a batch
   * send. Otherwise, waits for HW advancement notifications.
   */
  def registerPartition(partition: Partition, topicId: Uuid): Unit = {
    val waitingState = WaitingForReplication(partition, topicId, onPartitionUpdate = (tp, outcome) => {
      tracked.computeIfPresent(tp, (_, currentState) => currentState match {
        case _: WaitingForReplication => handleWaitingOutcome(tp, outcome)
        case other => other
      })
      refreshMetrics(tp)
    })

    val tp = partition.topicPartition
    val inserted = tracked.putIfAbsent(tp, waitingState) == null

    // Evaluate immediately for both new and already tracked entries:
    // - new entries may already be ready
    // - duplicate registrations can re-drive a waiting or queued state
    tracked.computeIfPresent(tp, (_, currentState) => currentState match {
      case waitingForReplication: WaitingForReplication =>
        handleWaitingOutcome(tp, waitingForReplication.maybeAdvanceState())
      case sendingToController: SendingToController =>
        enqueueSendingToController(tp, sendingToController)
        sendingToController
      case awaitingMetadata: AwaitingMetadata => awaitingMetadata
      case done: Done => done
      case _: Failed => null
    })
    refreshMetrics(tp)

    if (inserted) {
      Option(tracked.get(tp)).foreach {
        case waitingForReplication: WaitingForReplication =>
          partition.maybeAddListener(waitingForReplication)
          // Re-evaluate after adding the listener to catch HW updates that
          // occurred between the initial maybeAdvanceState() and addListener().
          tracked.computeIfPresent(tp, (_, currentState) => currentState match {
            case w: WaitingForReplication => handleWaitingOutcome(tp, w.maybeAdvanceState())
            case other => other
          })
          refreshMetrics(tp)
        case _ =>
      }
    }
    info(s"Registered new partition $tp in state ${Option(tracked.get(tp))}")
  }

  private def handleWaitingOutcome(tp: TopicPartition, outcome: WaitingForReplicationOutcome): InitDisklessLogState = {
    outcome match {
      case sendingToController: SendingToController =>
        enqueueSendingToController(tp, sendingToController)
        sendingToController
      case _: Failed =>
        // remove from tracking and count as a failed init diskless log operation.
        metrics.markFailed()
        null
      case waitingForReplication: WaitingForReplication => waitingForReplication
    }
  }

  private def enqueueSendingToController(tp: TopicPartition, state: SendingToController): Unit = {
    sendingToControllerQueue.enqueue(tp, state).foreach { accepted =>
      if (accepted) {
        tracked.computeIfPresent(tp, (_, initDisklessLogState) =>
          initDisklessLogState match {
            case sendingToController: SendingToController => sendingToController.onSuccess
            case _ => initDisklessLogState
          }
        )
      } else {
        // PermanentFailure on controller call (e.g. FENCED_LEADER_EPOCH /
        // INVALID_REQUEST) or local pre-flight rejection (leader/log lost):
        // the init diskless log operation won't progress.
        // Only count a failure when this callback observed `tp` as still
        // tracked. `removePartition` completes the queue promise with `false`
        // (via `queue.remove`) AFTER clearing `tracked`, so the cancellation
        // path lands here with `tracked.remove(tp) == null` and must not
        // increment the failed meter.
        if (tracked.remove(tp) != null) {
          metrics.markFailed()
        }
      }
      refreshMetrics(tp)
    }(ExecutionContext.parasitic)
  }

  private def enqueueAwaitingMetadata(tp: TopicPartition, state: AwaitingMetadata): Unit = {
    awaitingMetadataQueue.enqueue(tp, state).foreach { accepted =>
      if (accepted) {
        tracked.computeIfPresent(tp, (_, initDisklessLogState) =>
          initDisklessLogState match {
            case awaitingMetadata: AwaitingMetadata => awaitingMetadata.onSuccess
            case other => other
          }
        )
        // Only count completion when this callback observed `tp` as still
        // tracked. If `removePartition` cleared `tracked` (and/or the queue
        // promise) concurrently, the init diskless log operation was cancelled externally and
        // we must not inflate the completed meter.
        if (tracked.remove(tp) != null) {
          metrics.markCompleted()
        }
      } else {
        if (tracked.remove(tp) != null) {
          // Only count failure when this callback observed `tp` as still tracked
          metrics.markFailed()
        }
      }
      refreshMetrics(tp)
    }(ExecutionContext.parasitic)
  }

  /**
   * Remove a partition from tracking (e.g., when leadership is lost).
   */
  def removePartition(tp: TopicPartition): Unit = {
    if (tracked.remove(tp) != null) {
      sendingToControllerQueue.remove(tp)
      awaitingMetadataQueue.remove(tp)
      refreshMetrics(tp)
      info(s"Removed partition $tp from diskless init tracking")
    }
  }

  def shutdown(): Unit = {
    tracked.keySet().asScala.toList.foreach(removePartition)
    removeMetrics()
  }

  def removeMetrics(): Unit = metrics.removeMetrics()
}

object InitDisklessLogManager {
  private val MetricsPackage = "kafka.server"
  private val MetricsClassName = "InitDisklessLogManager"

  private[server] val InitDisklessLogInFlightMetricName = "InitDisklessLogInFlight"
  private[server] val WaitingForReplicationCountMetricName = "InitDisklessLogWaitingForReplicationCount"
  private[server] val SendingToControllerCountMetricName = "InitDisklessLogSendingToControllerCount"
  private[server] val AwaitingMetadataCountMetricName = "InitDisklessLogAwaitingMetadataCount"

  // Oldest-age-per-state gauges. Reads 0 when no partition is currently in the corresponding state.
  private[server] val OldestWaitingForReplicationAgeMsMetricName = "InitDisklessLogOldestWaitingForReplicationAgeMs"
  private[server] val OldestSendingToControllerAgeMsMetricName = "InitDisklessLogOldestSendingToControllerAgeMs"
  private[server] val OldestAwaitingMetadataAgeMsMetricName = "InitDisklessLogOldestAwaitingMetadataAgeMs"

  private[server] val InitDisklessLogCompletedPerSecMetricName = "InitDisklessLogCompletedPerSec"
  private[server] val InitDisklessLogFailedPerSecMetricName = "InitDisklessLogFailedPerSec"
  private[server] val InitDisklessLogRetriedPerSecMetricName = "InitDisklessLogRetriedPerSec"

  private[server] val GaugeMetricNames = Set(
    InitDisklessLogInFlightMetricName,
    WaitingForReplicationCountMetricName,
    SendingToControllerCountMetricName,
    AwaitingMetadataCountMetricName,
    OldestWaitingForReplicationAgeMsMetricName,
    OldestSendingToControllerAgeMsMetricName,
    OldestAwaitingMetadataAgeMsMetricName,
  )

  private[server] val MeterMetricNames = Set(
    InitDisklessLogCompletedPerSecMetricName,
    InitDisklessLogFailedPerSecMetricName,
    InitDisklessLogRetriedPerSecMetricName,
  )

  private[server] val MetricNames: Set[String] = GaugeMetricNames union MeterMetricNames

  // JMX wiring: per-state count and oldest-age gauges plus completed/failed/
  // retried meters. The manager calls `onStateChange` after every mutation of
  // its `tracked` map and `markCompleted` / `markFailed` / `markRetried` at
  // the relevant state-machine transitions.
  private[server] final class Metrics(time: Time, trackedSize: () => Int) {
    private val metricsGroup = new KafkaMetricsGroup(MetricsPackage, MetricsClassName)

    // Per-state counters incrementally maintained by `onStateChange`
    private val counters: Map[Class[_], AtomicInteger] = Map(
      classOf[WaitingForReplication] -> new AtomicInteger(0),
      classOf[SendingToController]   -> new AtomicInteger(0),
      classOf[AwaitingMetadata]      -> new AtomicInteger(0)
    )

    // Per-state TP -> enteredAtMs registry; walked by `oldestAgeMs` on read.
    // Scan cost is negligible: same pattern as `AbstractFetcherManager.MaxLag`.
    private val enteredAtByState: Map[Class[_], ConcurrentHashMap[TopicPartition, java.lang.Long]] = Map(
      classOf[WaitingForReplication] -> new ConcurrentHashMap[TopicPartition, java.lang.Long](),
      classOf[SendingToController]   -> new ConcurrentHashMap[TopicPartition, java.lang.Long](),
      classOf[AwaitingMetadata]      -> new ConcurrentHashMap[TopicPartition, java.lang.Long]()
    )

    metricsGroup.newGauge(InitDisklessLogInFlightMetricName, () => trackedSize())
    metricsGroup.newGauge(WaitingForReplicationCountMetricName, () => counters(classOf[WaitingForReplication]).get)
    metricsGroup.newGauge(SendingToControllerCountMetricName,   () => counters(classOf[SendingToController]).get)
    metricsGroup.newGauge(AwaitingMetadataCountMetricName,      () => counters(classOf[AwaitingMetadata]).get)
    metricsGroup.newGauge(OldestWaitingForReplicationAgeMsMetricName, () => oldestAgeMs(classOf[WaitingForReplication]))
    metricsGroup.newGauge(OldestSendingToControllerAgeMsMetricName,   () => oldestAgeMs(classOf[SendingToController]))
    metricsGroup.newGauge(OldestAwaitingMetadataAgeMsMetricName,      () => oldestAgeMs(classOf[AwaitingMetadata]))

    private val completedMeter = metricsGroup.newMeter(InitDisklessLogCompletedPerSecMetricName, "completed", TimeUnit.SECONDS)
    private val failedMeter    = metricsGroup.newMeter(InitDisklessLogFailedPerSecMetricName,    "failed",    TimeUnit.SECONDS)
    private val retriedMeter   = metricsGroup.newMeter(InitDisklessLogRetriedPerSecMetricName,   "retries",   TimeUnit.SECONDS)

    def markCompleted(): Unit = completedMeter.mark()
    def markFailed(): Unit    = failedMeter.mark()
    def markRetried(): Unit   = retriedMeter.mark()

    // Update metrics for `tp` in `state`, or remove it from tracking when `state == null`.
    def onStateChange(tp: TopicPartition, state: InitDisklessLogState): Unit = {
      val newClass: Class[_] = if (state == null) null else state.getClass
      val nowMs = time.milliseconds()
      enteredAtByState.foreach { case (cls, m) =>
        if (cls eq newClass) {
          // Keep the original timestamp on same-class re-entries.
          if (m.putIfAbsent(tp, nowMs) == null) counters(cls).incrementAndGet()
        } else if (m.remove(tp) != null) counters(cls).decrementAndGet()
      }
    }

    def removeMetrics(): Unit = MetricNames.foreach(metricsGroup.removeMetric)

    private def oldestAgeMs(stateClass: Class[_]): Long = {
      val nowMs = time.milliseconds()
      var oldest = 0L
      val it = enteredAtByState(stateClass).values().iterator()
      while (it.hasNext) {
        val age = nowMs - it.next().longValue()
        if (age > oldest) oldest = age
      }
      oldest
    }
  }
}
