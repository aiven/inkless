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
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.NodeToControllerChannelManager
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class InitDisklessLogManager(
  controllerChannelManager: NodeToControllerChannelManager,
  controlPlane: ControlPlane,
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long
) extends Logging {

  this.logIdent = s"[InitDisklessLogManager broker=$brokerId] "

  private val tracked = new ConcurrentHashMap[TopicPartition, InitDisklessLogState]()

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
    maxRetryTimeMs = maxRetryTimeMs
  )
  private val awaitingMetadataQueue = new AwaitingMetadataBatchQueue(
    controlPlane = controlPlane,
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
   * Handles already-applied diskless init metadata for a partition.
   * This keeps/moves the partition to AwaitingMetadata and triggers a prompt
   * control-plane init send, since metadata is committed and visible.
   */
  def initOnControlPlane(
    partition: Partition,
    topicId: Uuid,
    topicName: String,
    firstDisklessOffset: Long,
    producerStates: java.util.List[CpProducerState]
  ): Unit = {
    if (firstDisklessOffset < 0) {
      warn(s"Received negative firstDisklessOffset ($firstDisklessOffset) for $topicName:${partition.topicPartition}, skipping control-plane init")
      return
    }

    val tp = partition.topicPartition
    val payload = DisklessInitMetadata(topicName, firstDisklessOffset, producerStates)
    val newState = AwaitingMetadata(partition, topicId, Some(payload))
    if (tracked.putIfAbsent(tp, newState) != null) {
      tracked.computeIfPresent(tp, (_, _) => newState)
    }
    enqueueAwaitingMetadata(tp, newState)
  }

  /**
   * Register a sealed partition for migration. Registers this manager as a
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
      case _: Failed => null // remove from tracking
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
        tracked.remove(tp)
      }
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
        tracked.remove(tp)
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
      awaitingMetadataQueue.remove(tp)
      info(s"Removed partition $tp from diskless init tracking")
    }
  }
}
