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

import kafka.cluster.Partition
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{InitDisklessLogRequestData, InitDisklessLogResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{InitDisklessLogRequest, InitDisklessLogResponse}
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.partition.PartitionListener
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.UnifiedLog

import scala.collection.mutable
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

sealed trait InitState

object InitState {
  /** HW has not yet caught up with LEO; waiting for replica fetch cycles to advance it. */
  case object WaitingForHW extends InitState
  /** HW == LEO; partition is queued for the next batched InitDisklessLog controller call. */
  case object SendingToController extends InitState
  /** Controller accepted the request; waiting for the PartitionChangeRecord with disklessStartOffset to propagate. */
  case object AwaitingMetadata extends InitState
}

private[server] case class InitPartitionState(
  partition: Partition,
  topicId: Uuid,
  state: InitState,
  retryAttempt: Int = 0
)

class InitDisklessLogManager(
  controllerChannelManager: NodeToControllerChannelManager,
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long
) extends PartitionListener with Logging {

  this.logIdent = s"[InitDisklessLogManager broker=$brokerId] "

  private val tracked = new ConcurrentHashMap[TopicPartition, InitPartitionState]()

  // Delay (ms) of the currently pending scheduled send. Long.MaxValue means no send is pending.
  // A new call with a shorter delay preempts the pending one (e.g. lingerMs preempts a retry
  // backoff). Stale (preempted) tasks are harmless: sendBatch() returns early when nothing is ready.
  private val pendingDelayMs = new AtomicLong(Long.MaxValue)

  // Delay before firing a batch send, allowing multiple partitions that become ready
  // around the same time (e.g., when an entire topic is sealed) to be coalesced into one request.
  private[server] val lingerMs = 100L
  // Exponential backoff parameters for retrying failed controller requests.
  // Retries are infinite until the request succeeds or a non-retriable error is returned.
  private[server] val initialRetryBackoffMs = 1000L
  private[server] val maxRetryBackoffMs = 30000L

  def trackedPartitions: Set[TopicPartition] = tracked.keySet().asScala.toSet

  def initState(tp: TopicPartition): Option[InitState] =
    Option(tracked.get(tp)).map(_.state)

  /**
   * Register a sealed partition for migration. Registers this manager as a
   * PartitionListener to receive HW advancement notifications. If HW already
   * equals LEO, immediately marks the partition ready and schedules a batch
   * send. Otherwise, waits for HW advancement notifications.
   */
  def registerPartition(partition: Partition, topicId: Uuid): Unit = {
    val tp = partition.topicPartition
    if (!partition.isSealed) {
      error(s"Partition $tp is not sealed, which should never happen. Skipping migration.")
      return
    }

    val log = partition.log.getOrElse {
      warn(s"Partition $tp sealed but has no log, skipping migration")
      return
    }

    val hw = log.highWatermark
    val leo = log.logEndOffset
    if (hw > leo) {
      error(s"Partition $tp has HW ($hw) > LEO ($leo), which should never happen. Skipping migration.")
      return
    }
    
    val newState = InitPartitionState(partition, topicId, InitState.WaitingForHW)
    if (tracked.putIfAbsent(tp, newState) == null) {
      partition.maybeAddListener(this)
    }

    maybeAdvanceState(tp)
  }

  /**
   * PartitionListener callback: called when HW advances on a partition.
   */
  override def onHighWatermarkUpdated(topicPartition: TopicPartition, offset: Long): Unit = {
    maybeAdvanceState(topicPartition)
  }

  /**
   * Atomically advance a tracked partition's state if conditions are met.
   * Single place where HW/LEO is evaluated to decide state transitions.
   */
  private def maybeAdvanceState(tp: TopicPartition): Unit = {
    var shouldSchedule = false
    tracked.computeIfPresent(tp, (_, initPartitionState) => {
      initPartitionState.state match {
        case InitState.WaitingForHW =>
          initPartitionState.partition.log match {
            case None => initPartitionState
            case Some(log) =>
              val hw = log.highWatermark
              val leo = log.logEndOffset
              if (hw > leo) {
                error(s"Partition $tp has HW ($hw) > LEO ($leo). Removing from tracking.")
                null
              } else if (hw == leo) {
                info(s"Partition $tp HW ($hw) caught up with LEO ($leo), ready for InitDisklessLog")
                shouldSchedule = true
                initPartitionState.copy(state = InitState.SendingToController, retryAttempt = 0)
              } else initPartitionState
          }
        case InitState.SendingToController =>
          shouldSchedule = true
          initPartitionState
        case InitState.AwaitingMetadata => initPartitionState
      }
    })
    if (shouldSchedule) scheduleBatchSend()
  }

  /**
   * PartitionListener callback: called when the partition fails.
   */
  override def onFailed(topicPartition: TopicPartition): Unit = {
    removePartition(topicPartition)
  }

  /**
   * PartitionListener callback: called when the partition is deleted.
   */
  override def onDeleted(topicPartition: TopicPartition): Unit = {
    removePartition(topicPartition)
  }

  /**
   * Remove a partition from tracking (e.g., when leadership is lost).
   */
  def removePartition(tp: TopicPartition): Unit = {
    if (tracked.remove(tp) != null) {
      info(s"Removed partition $tp from diskless init tracking")
    }
  }

  /**
   * Schedule a batched send of all partitions in SendingToController state.
   * Coalesces multiple calls: only a call with a strictly shorter delay than the
   * pending one actually schedules a new task.
   */
  private[server] def scheduleBatchSend(delayMs: Long = lingerMs): Unit = {
    val current = pendingDelayMs.get()
    if (delayMs < current && pendingDelayMs.compareAndSet(current, delayMs)) {
      scheduler.scheduleOnce("init-diskless-log-batch-send", () => {
        pendingDelayMs.set(Long.MaxValue)
        sendBatch()
      }, delayMs)
    }
  }

  /**
   * Collect all partitions in SendingToController state, group them by topic,
   * and send a single InitDisklessLog request to the controller.
   */
  private[server] def sendBatch(): Unit = {
    val ready = tracked.asScala.filter { case (_, initPartitionState) =>
      initPartitionState.state == InitState.SendingToController
    }.toMap

    if (ready.isEmpty) return

    val validPartitions = ready.filter { case (tp, mps) =>
      if (!mps.partition.isLeader) {
        info(s"Partition $tp is no longer leader, removing from migration tracking")
        tracked.remove(tp)
        false
      } else if (mps.partition.log.isEmpty) {
        warn(s"Partition $tp has no log during migration, removing from tracking")
        tracked.remove(tp)
        false
      } else {
        true
      }
    }

    if (validPartitions.isEmpty) return

    val topicDataMap = new util.LinkedHashMap[Uuid, util.List[InitDisklessLogRequestData.PartitionData]]()

    validPartitions.foreach { case (tp, mps) =>
      val log = mps.partition.log.get
      val hw = log.highWatermark
      val producerStates = extractProducerStates(log)
      val leaderEpoch = mps.partition.getLeaderEpoch

      val partitionData = new InitDisklessLogRequestData.PartitionData()
        .setPartitionId(tp.partition)
        .setDisklessStartOffset(hw)
        .setLeaderEpoch(leaderEpoch)
        .setProducerStates(producerStates)

      topicDataMap.computeIfAbsent(mps.topicId, _ => new util.ArrayList[InitDisklessLogRequestData.PartitionData]())
        .add(partitionData)
    }

    val topicDataList = new util.ArrayList[InitDisklessLogRequestData.TopicData]()
    topicDataMap.forEach { (topicId, partitions) =>
      topicDataList.add(new InitDisklessLogRequestData.TopicData()
        .setTopicId(topicId)
        .setPartitions(partitions))
    }

    val request = new InitDisklessLogRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier())
      .setTopics(topicDataList)

    val partitionCount = validPartitions.size
    val topicCount = topicDataMap.size()
    info(s"Sending batched InitDisklessLog for $partitionCount partition(s) across $topicCount topic(s)")

    val requestBuilder = new InitDisklessLogRequest.Builder(request)
    val partitionKeys = validPartitions.keys.toSet

    controllerChannelManager.sendRequest(requestBuilder, new ControllerRequestCompletionHandler {
      override def onComplete(response: ClientResponse): Unit = {
        if (response.authenticationException != null) {
          handleBatchException(partitionKeys, response.authenticationException)
        } else if (response.versionMismatch != null) {
          handleBatchException(partitionKeys, response.versionMismatch)
        } else {
          val initDisklessLogResponse = response.responseBody.asInstanceOf[InitDisklessLogResponse]
          handleBatchResponse(initDisklessLogResponse.data())
        }
      }

      override def onTimeout(): Unit = {
        handleBatchException(partitionKeys, new RuntimeException("InitDisklessLog request timed out"))
      }
    })
  }

  private def extractProducerStates(
    log: UnifiedLog
  ): util.List[InitDisklessLogRequestData.ProducerState] = {
    val states = new util.ArrayList[InitDisklessLogRequestData.ProducerState]()
    log.producerStateManager().activeProducers().forEach { (producerId, entry) =>
      if (!entry.isEmpty) {
        states.add(new InitDisklessLogRequestData.ProducerState()
          .setProducerId(producerId)
          .setProducerEpoch(entry.producerEpoch())
          .setBaseSequence(entry.firstSeq())
          .setLastSequence(entry.lastSeq())
          .setAssignedOffset(entry.lastDataOffset())
          .setBatchMaxTimestamp(entry.lastTimestamp()))
      }
    }
    states
  }

  /**
   * Process the batched response, handling each partition's result individually.
   */
  private def handleBatchResponse(response: InitDisklessLogResponseData): Unit = {
    val retriableAttempts = mutable.Map[TopicPartition, Int]()

    for (topicResponse <- response.topics().asScala) {
      for (partitionResponse <- topicResponse.partitions().asScala) {
        val tp = findTopicPartition(topicResponse.topicId(), partitionResponse.partitionId())
        if (tp != null) {
          val error = Errors.forCode(partitionResponse.errorCode())
          error match {
            case Errors.NONE =>
              info(s"InitDisklessLog succeeded for partition $tp, transitioning to AwaitingMetadata")
              tracked.computeIfPresent(tp, (_, mps) =>
                mps.copy(state = InitState.AwaitingMetadata, retryAttempt = 0))

            case Errors.FENCED_LEADER_EPOCH | Errors.INVALID_REQUEST =>
              info(s"InitDisklessLog for partition $tp returned permanent error $error, removing from tracking")
              tracked.remove(tp)

            case _ =>
              warn(s"InitDisklessLog for partition $tp returned retriable error $error")
              val updated = tracked.computeIfPresent(tp, (_, mps) =>
                mps.copy(retryAttempt = mps.retryAttempt + 1))
              if (updated != null) retriableAttempts.put(tp, updated.retryAttempt)
          }
        }
      }
    }

    if (retriableAttempts.nonEmpty) {
      val minBackoff = retriableAttempts.values.map(computeBackoff).min
      warn(s"Scheduling batch retry for ${retriableAttempts.size} partition(s) in ${minBackoff}ms")
      scheduleBatchSend(minBackoff)
    }
  }

  /**
   * Handle an exception on the entire batch request (e.g., controller unavailable).
   * All partitions in the batch are treated as retriable failures.
   */
  private def handleBatchException(partitions: Set[TopicPartition], cause: Throwable): Unit = {
    var maxAttempt = 0
    partitions.foreach { tp =>
      tracked.computeIfPresent(tp, (_, mps) => {
        if (mps.state != InitState.SendingToController) mps
        else {
          val attempt = mps.retryAttempt + 1
          if (attempt > maxAttempt) maxAttempt = attempt
          if (!mps.partition.isLeader) {
            info(s"Partition $tp is no longer leader during retry, removing from tracking")
            null
          } else {
            mps.copy(retryAttempt = attempt)
          }
        }
      })
    }

    val backoffMs = computeBackoff(maxAttempt)
    warn(s"Batched InitDisklessLog failed for ${partitions.size} partition(s) (max attempt $maxAttempt), " +
      s"retrying in ${backoffMs}ms", cause)
    scheduleBatchSend(backoffMs)
  }

  /**
   * Find the TopicPartition in tracked state that matches the given topicId and partitionId.
   */
  private def findTopicPartition(topicId: Uuid, partitionId: Int): TopicPartition = {
    tracked.asScala.collectFirst {
      case (tp, mps) if mps.topicId == topicId && tp.partition == partitionId => tp
    }.orNull
  }

  private def computeBackoff(attempt: Int): Long = {
    Math.min(initialRetryBackoffMs * (1L << Math.min(Math.max(attempt - 1, 0), 14)), maxRetryBackoffMs)
  }
}
