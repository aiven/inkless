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
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.NodeToControllerChannelManager
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class InitDisklessLogManager(
  controllerChannelManager: NodeToControllerChannelManager,
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long
) extends Logging {

  this.logIdent = s"[InitDisklessLogManager broker=$brokerId] "

  private val tracked = new ConcurrentHashMap[TopicPartition, InitDisklessLogState]()

  // Delay before firing a batch send, allowing multiple partitions that become ready
  // around the same time (e.g., when an entire topic is sealed) to be coalesced into one request.
  private[server] val lingerMs = 500L
  // Initial retry period for retriable controller failures.
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
    maxRetryTimeMs = maxRetryTimeMs
  )

  private[server] def getTrackedPartitions: Set[TopicPartition] = tracked.keySet().asScala.toSet

  private[server] def getInitState(tp: TopicPartition): Option[InitDisklessLogState] = Option(tracked.get(tp))

  /**
   * Register a sealed partition for migration. Registers this manager as a
   * PartitionListener to receive HW advancement notifications. If HW already
   * equals LEO, immediately marks the partition ready and schedules a batch
   * send. Otherwise, waits for HW advancement notifications.
   */
  def registerPartition(partition: Partition, topicId: Uuid): Unit = {
    val newState = WaitingForReplication(partition, topicId, onPartitionUpdate = (tp, outcome) => {
      tracked.computeIfPresent(tp, (_, _) => outcome match {
        case waitingForReplication: WaitingForReplication => waitingForReplication // HW not caught with LEO yet, continue tracking
        case sendingToController: SendingToController =>
          // HW caught up with LEO, put a new controller request in the queue
          enqueueSendingToController(tp, sendingToController)
          // advance tracked partition
          sendingToController
        case _: Failed => null // failed update, remove from tracking
      })
    })
    newState.validate() match {
      case Left(errorString) =>
        error(s"Validation error when registering a new partition: $errorString")
        return
      case _ =>
    }
    if (tracked.putIfAbsent(partition.topicPartition, newState) == null) {
      partition.maybeAddListener(newState)
    }
    info(s"Registered new partition ${partition.topicPartition} in state $newState")
    maybeAdvanceState(partition.topicPartition)
  }

  /**
   * Atomically advance a tracked partition's state if conditions are met.
   */
  private def maybeAdvanceState(tp: TopicPartition): Unit = {
    tracked.computeIfPresent(tp, (_, initDisklessLogState) => {
      initDisklessLogState match {
        case waitingForReplication: WaitingForReplication =>
          waitingForReplication.maybeAdvanceState() match {
            case sendingToController: SendingToController =>
              enqueueSendingToController(tp, sendingToController)
              sendingToController
            case _: Failed => null // remove from tracking
            case initDisklessLogState => initDisklessLogState
          }
        case sendingToController: SendingToController =>
          enqueueSendingToController(tp, sendingToController)
          sendingToController
        case awaitingMetadata: AwaitingMetadata => awaitingMetadata
        case _: Failed => null // remove from tracking
      }
    })
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
        tracked.remove(tp)
      }
    }(ExecutionContext.parasitic)
  }

  /**
   * Remove a partition from tracking (e.g., when leadership is lost).
   */
  def removePartition(tp: TopicPartition): Unit = {
    if (tracked.remove(tp) != null) {
      sendingToControllerQueue.remove(tp)
      info(s"Removed partition $tp from diskless init tracking")
    }
  }
}
