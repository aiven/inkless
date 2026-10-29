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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

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
  private val metrics = new Metrics(time)

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
  // Period between upload checks for partitions in WaitingForUpload. A faster poll doesn't help because
  // the remote log manager copies on a much slower interval.
  private[server] val uploadCheckPeriodMs = 1000L

  private val uploadCheckScheduled = new AtomicBoolean(false)

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
  private val sendingFlagToControllerQueue = new SendingFlagToControllerBatchQueue(
    controllerChannelManager = controllerChannelManager,
    scheduler = scheduler,
    brokerId = brokerId,
    brokerEpochSupplier = brokerEpochSupplier,
    lingerMs = lingerMs,
    retryPeriodMs = retryPeriodMs,
    maxRetryTimeMs = maxRetryTimeMs,
    onRetry = () => metrics.markFlagRetried()
  )

  private[server] def getTrackedPartitions: Set[TopicPartition] = tracked.keySet().asScala.toSet

  private[server] def getInitState(tp: TopicPartition): Option[InitDisklessLogState] = Option(tracked.get(tp))

  /**
   * Handles already-applied diskless init metadata for a partition.
   * This keeps/moves the partition to AwaitingMetadata and triggers a prompt
   * control-plane init send, since metadata is committed and visible.
   * After the control-plane init succeeds and `classicPrefixInRemoteStorage` is unset, the
   * partition moves on to WaitingForUpload, then SendingFlagToController. `ReplicaManager` calls
   * this method again when this broker becomes leader or an operator re-seals the partition, so
   * the upload watch always uses the current seal.
   */
  def initOnControlPlane(
    partition: Partition,
    topicId: Uuid,
    topicName: String,
    classicToDisklessStartOffset: Long,
    producerStates: java.util.List[CpProducerState],
    classicPrefixInRemoteStorage: Boolean
  ): Unit = {
    if (classicToDisklessStartOffset < 0) {
      warn(s"Received negative classicToDisklessStartOffset ($classicToDisklessStartOffset) for $topicName:${partition.topicPartition}, skipping control-plane init")
      return
    }

    val tp = partition.topicPartition
    val payload = DisklessInitMetadata(topicName, classicToDisklessStartOffset, producerStates, classicPrefixInRemoteStorage)
    val newState = AwaitingMetadata(partition, topicId, Some(payload))
    if (tracked.put(tp, newState) != null) {
      // A replaced state may have queued an InitDisklessLog retry or a flag request, and removing it
      // stops the queue from sending a request that nothing awaits.
      sendingToControllerQueue.remove(tp)
      sendingFlagToControllerQueue.remove(tp)
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
      case waitingForUpload: WaitingForUpload => waitingForUpload
      case sendingFlagToController: SendingFlagToController => sendingFlagToController
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
        // (via `queue.remove`) AFTER clearing `tracked`, so a cancellation
        // finds nothing to remove and must not increment the failed meter.
        // A stale rejection after a seal commit must not remove the state that
        // replaced this one.
        if (removeIfStillTracked(tp, state)) {
          metrics.markFailed()
        }
      }
      refreshMetrics(tp)
    }(ExecutionContext.parasitic)
  }

  // Removes `tp` only while it still holds `state`, compared by reference as in `enqueueAwaitingMetadata`.
  private def removeIfStillTracked(tp: TopicPartition, state: InitDisklessLogState): Boolean = {
    var removed = false
    tracked.computeIfPresent(tp, (_, currentState) =>
      if (currentState eq state) {
        removed = true
        null
      } else {
        currentState
      }
    )
    removed
  }

  private def enqueueAwaitingMetadata(tp: TopicPartition, state: AwaitingMetadata): Unit = {
    awaitingMetadataQueue.enqueue(tp, state).foreach { accepted =>
      if (accepted) {
        // Advance and count only the state that queued this init. A concurrent `removePartition`
        // clears `tracked` to cancel the operation, and a cancellation must not inflate the completed
        // meter. A re-init installs its replacement before it queues it, so a stale completion can land
        // in between, and the replacement must wait for its own init. The check uses reference
        // equality because a replacement with the same payload is an equal case class.
        var completed = false
        var enteredWaitingForUpload = false
        tracked.computeIfPresent(tp, (_, initDisklessLogState) =>
          initDisklessLogState match {
            case awaitingMetadata: AwaitingMetadata if awaitingMetadata eq state =>
              completed = true
              awaitingMetadata.onSuccess match {
                case waitingForUpload: WaitingForUpload =>
                  enteredWaitingForUpload = true
                  waitingForUpload
                case _ => null
              }
            case other => other
          }
        )
        if (completed) {
          metrics.markCompleted()
        }
        refreshMetrics(tp)
        if (enteredWaitingForUpload) {
          onEnteredWaitingForUpload(tp)
        }
      } else {
        if (tracked.remove(tp) != null) {
          // Only count failure when this callback observed `tp` as still tracked
          metrics.markFailed()
        }
        refreshMetrics(tp)
      }
    }(ExecutionContext.parasitic)
  }

  private def onEnteredWaitingForUpload(tp: TopicPartition): Unit = {
    // Evaluate right away so an empty prefix (or one that was already uploaded before a failover)
    // doesn't wait a full poll period.
    evaluateUpload(tp)
    scheduleUploadCheckIfNeeded()
  }

  // Moves `tp` out of WaitingForUpload once the classic prefix is in remote storage, or drops it
  // when this broker can no longer lead. The enqueue runs inside `computeIfPresent`, as in
  // `handleWaitingOutcome`, to avoid interleaving with `removePartition`.
  private def evaluateUpload(tp: TopicPartition): Unit = {
    tracked.computeIfPresent(tp, (_, currentState) => currentState match {
      case waitingForUpload: WaitingForUpload =>
        try {
          waitingForUpload.check() match {
            case UploadCheck.Pending => waitingForUpload
            case UploadCheck.Uploaded =>
              val sendingFlagToController = waitingForUpload.onUploaded
              enqueueSendingFlagToController(tp, sendingFlagToController)
              sendingFlagToController
            case UploadCheck.Gone =>
              info(s"Dropping the upload watch for $tp because this broker is no longer its leader or the log is gone")
              null
          }
        } catch {
          case NonFatal(e) =>
            warn(s"Failed to check the classic prefix upload for $tp, will retry", e)
            waitingForUpload
        }
      case other => other
    })
    refreshMetrics(tp)
  }

  private def checkUploads(): Unit = {
    // Clear before scanning: a partition that enters WaitingForUpload after this point schedules its own check.
    uploadCheckScheduled.set(false)
    try {
      tracked.entrySet().asScala.toList.foreach { entry =>
        if (entry.getValue.isInstanceOf[WaitingForUpload]) {
          evaluateUpload(entry.getKey)
        }
      }
    } finally {
      if (tracked.values().asScala.exists(_.isInstanceOf[WaitingForUpload])) {
        scheduleUploadCheckIfNeeded()
      }
    }
  }

  private def scheduleUploadCheckIfNeeded(): Unit = {
    if (uploadCheckScheduled.compareAndSet(false, true)) {
      try {
        scheduler.scheduleOnce("init-diskless-log-upload-check", () => checkUploads(), uploadCheckPeriodMs)
      } catch {
        case NonFatal(e) =>
          uploadCheckScheduled.set(false)
          warn("Failed to schedule the classic prefix upload check", e)
      }
    }
  }

  private def enqueueSendingFlagToController(tp: TopicPartition, state: SendingFlagToController): Unit = {
    sendingFlagToControllerQueue.enqueue(tp, state).foreach { accepted =>
      // Removes `tp` only while it still holds the state that queued this request, compared by reference as in
      // `enqueueAwaitingMetadata`. `removePartition` clears `tracked` before it completes the queue promise with
      // `false`, and that cancellation must not count as a failure. The queue completes a detached promise outside
      // its lock, so a late response for a replaced state must leave the replacement alone.
      var removed = false
      tracked.computeIfPresent(tp, (_, currentState) => currentState match {
        case sendingFlagToController: SendingFlagToController if sendingFlagToController eq state =>
          removed = true
          null
        case other => other
      })
      if (removed) {
        if (accepted) {
          info(s"Controller accepted the classic prefix flag for $tp")
          metrics.markFlagCompleted()
        } else if (state.partition.isLeader && state.partition.log.isDefined) {
          // Only a controller rejection, such as `INVALID_REQUEST`, counts as a failure. The queue also completes
          // with `false` when it skips the request after a leadership change or a topic deletion. That's routine,
          // as with `UploadCheck.Gone`. Either way the partition remains in phase 1 until a new leader registers.
          metrics.markFlagFailed()
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
      sendingFlagToControllerQueue.remove(tp)
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

  private[server] val InFlightPartitionsMetricName = "InFlightPartitions"
  private[server] val WaitingForReplicationPartitionsMetricName = "WaitingForReplicationPartitions"
  private[server] val SendingToControllerPartitionsMetricName = "SendingToControllerPartitions"
  private[server] val AwaitingMetadataPartitionsMetricName = "AwaitingMetadataPartitions"
  private[server] val WaitingForUploadPartitionsMetricName = "WaitingForUploadPartitions"
  private[server] val SendingFlagToControllerPartitionsMetricName = "SendingFlagToControllerPartitions"

  // Oldest-age-per-state gauges. Reads 0 when no partition is currently in the corresponding state.
  private[server] val OldestWaitingForReplicationAgeMsMetricName = "OldestWaitingForReplicationAgeMs"
  private[server] val OldestSendingToControllerAgeMsMetricName = "OldestSendingToControllerAgeMs"
  private[server] val OldestAwaitingMetadataAgeMsMetricName = "OldestAwaitingMetadataAgeMs"
  private[server] val OldestWaitingForUploadAgeMsMetricName = "OldestWaitingForUploadAgeMs"
  private[server] val OldestSendingFlagToControllerAgeMsMetricName = "OldestSendingFlagToControllerAgeMs"

  private[server] val InitCompletedPerSecMetricName = "InitCompletedPerSec"
  private[server] val InitFailedPerSecMetricName = "InitFailedPerSec"
  private[server] val InitRetriedPerSecMetricName = "InitRetriedPerSec"
  // Per-partition outcomes of the SetClassicPrefixInRemoteStorage request, kept apart from the init
  // meters so a stuck flag request doesn't look like a failing init.
  private[server] val FlagRequestCompletedPerSecMetricName = "FlagRequestCompletedPerSec"
  private[server] val FlagRequestFailedPerSecMetricName = "FlagRequestFailedPerSec"
  private[server] val FlagRequestRetriedPerSecMetricName = "FlagRequestRetriedPerSec"

  private[server] val GaugeMetricNames = Set(
    InFlightPartitionsMetricName,
    WaitingForReplicationPartitionsMetricName,
    SendingToControllerPartitionsMetricName,
    AwaitingMetadataPartitionsMetricName,
    WaitingForUploadPartitionsMetricName,
    SendingFlagToControllerPartitionsMetricName,
    OldestWaitingForReplicationAgeMsMetricName,
    OldestSendingToControllerAgeMsMetricName,
    OldestAwaitingMetadataAgeMsMetricName,
    OldestWaitingForUploadAgeMsMetricName,
    OldestSendingFlagToControllerAgeMsMetricName,
  )

  private[server] val MeterMetricNames = Set(
    InitCompletedPerSecMetricName,
    InitFailedPerSecMetricName,
    InitRetriedPerSecMetricName,
    FlagRequestCompletedPerSecMetricName,
    FlagRequestFailedPerSecMetricName,
    FlagRequestRetriedPerSecMetricName,
  )

  private[server] val MetricNames: Set[String] = GaugeMetricNames union MeterMetricNames

  // JMX wiring: per-state count and oldest-age gauges plus completed/failed/
  // retried meters for the init and for the flag request. The manager calls
  // `onStateChange` after every mutation of its `tracked` map and the `mark*`
  // methods at the relevant state-machine transitions.
  private[server] final class Metrics(time: Time) {
    private val metricsGroup = new KafkaMetricsGroup(MetricsPackage, MetricsClassName)

    // Per-state counters incrementally maintained by `onStateChange`
    private val counters: Map[Class[_], AtomicInteger] = Map(
      classOf[WaitingForReplication]    -> new AtomicInteger(0),
      classOf[SendingToController]      -> new AtomicInteger(0),
      classOf[AwaitingMetadata]         -> new AtomicInteger(0),
      classOf[WaitingForUpload]         -> new AtomicInteger(0),
      classOf[SendingFlagToController]  -> new AtomicInteger(0)
    )

    // Per-state TP -> enteredAtMs registry; walked by `oldestAgeMs` on read.
    // Scan cost is negligible: same pattern as `AbstractFetcherManager.MaxLag`.
    private val enteredAtByState: Map[Class[_], ConcurrentHashMap[TopicPartition, java.lang.Long]] = Map(
      classOf[WaitingForReplication]    -> new ConcurrentHashMap[TopicPartition, java.lang.Long](),
      classOf[SendingToController]      -> new ConcurrentHashMap[TopicPartition, java.lang.Long](),
      classOf[AwaitingMetadata]         -> new ConcurrentHashMap[TopicPartition, java.lang.Long](),
      classOf[WaitingForUpload]         -> new ConcurrentHashMap[TopicPartition, java.lang.Long](),
      classOf[SendingFlagToController]  -> new ConcurrentHashMap[TopicPartition, java.lang.Long]()
    )

    // Counts only the init states. A partition can wait in WaitingForUpload for hours, and operators
    // and system tests read this gauge to see when the init work has drained.
    metricsGroup.newGauge(InFlightPartitionsMetricName, () =>
      counters(classOf[WaitingForReplication]).get +
        counters(classOf[SendingToController]).get +
        counters(classOf[AwaitingMetadata]).get)
    metricsGroup.newGauge(WaitingForReplicationPartitionsMetricName, () => counters(classOf[WaitingForReplication]).get)
    metricsGroup.newGauge(SendingToControllerPartitionsMetricName, () => counters(classOf[SendingToController]).get)
    metricsGroup.newGauge(AwaitingMetadataPartitionsMetricName, () => counters(classOf[AwaitingMetadata]).get)
    metricsGroup.newGauge(WaitingForUploadPartitionsMetricName, () => counters(classOf[WaitingForUpload]).get)
    metricsGroup.newGauge(SendingFlagToControllerPartitionsMetricName, () => counters(classOf[SendingFlagToController]).get)
    metricsGroup.newGauge(OldestWaitingForReplicationAgeMsMetricName, () => oldestAgeMs(classOf[WaitingForReplication]))
    metricsGroup.newGauge(OldestSendingToControllerAgeMsMetricName,   () => oldestAgeMs(classOf[SendingToController]))
    metricsGroup.newGauge(OldestAwaitingMetadataAgeMsMetricName,      () => oldestAgeMs(classOf[AwaitingMetadata]))
    metricsGroup.newGauge(OldestWaitingForUploadAgeMsMetricName,      () => oldestAgeMs(classOf[WaitingForUpload]))
    metricsGroup.newGauge(OldestSendingFlagToControllerAgeMsMetricName, () => oldestAgeMs(classOf[SendingFlagToController]))

    private val completedMeter = metricsGroup.newMeter(InitCompletedPerSecMetricName, "inits", TimeUnit.SECONDS)
    private val failedMeter    = metricsGroup.newMeter(InitFailedPerSecMetricName,    "inits", TimeUnit.SECONDS)
    private val retriedMeter   = metricsGroup.newMeter(InitRetriedPerSecMetricName,   "inits", TimeUnit.SECONDS)

    private val flagCompletedMeter = metricsGroup.newMeter(FlagRequestCompletedPerSecMetricName, "partitions", TimeUnit.SECONDS)
    private val flagFailedMeter    = metricsGroup.newMeter(FlagRequestFailedPerSecMetricName,    "partitions", TimeUnit.SECONDS)
    private val flagRetriedMeter   = metricsGroup.newMeter(FlagRequestRetriedPerSecMetricName,   "partitions", TimeUnit.SECONDS)

    def markCompleted(): Unit = completedMeter.mark()
    def markFailed(): Unit    = failedMeter.mark()
    def markRetried(): Unit   = retriedMeter.mark()

    def markFlagCompleted(): Unit = flagCompletedMeter.mark()
    def markFlagFailed(): Unit    = flagFailedMeter.mark()
    def markFlagRetried(): Unit   = flagRetriedMeter.mark()

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
