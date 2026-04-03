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

import kafka.server.InitDisklessLogBatchQueue.ParsedResponse
import io.aiven.inkless.control_plane.ControlPlane
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.InitDisklessLogResponseData
import org.apache.kafka.common.requests.{InitDisklessLogRequest, InitDisklessLogResponse}
import org.apache.kafka.common.utils.ExponentialBackoff
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.util.Scheduler

import java.util
import scala.concurrent.{Future, Promise}
import java.util.concurrent.ScheduledFuture
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object InitDisklessLogBatchQueue {
  /** Classification for a partition result in a batch response. */
  sealed trait ResponseDisposition
  /** Partition was accepted by destination and can advance state. */
  case object Success extends ResponseDisposition
  /** Partition failed permanently and should be completed as rejected. */
  case object PermanentFailure extends ResponseDisposition
  /** Partition failed transiently and should be retried. */
  case object RetriableFailure extends ResponseDisposition

  /**
   * Parsed partition result extracted from a batched response payload.
   * `topicId` + `partitionId` are used to match outcomes to sent states.
   */
  final case class ParsedResponse(
    topicId: org.apache.kafka.common.Uuid,
    partitionId: Int,
    error: org.apache.kafka.common.protocol.Errors,
    disposition: ResponseDisposition
  )
}

abstract class RetriableInitDisklessLogBatchQueue[S <: InitDisklessLogState](
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long,
  lingerMs: Long,
  retryPeriodMs: Long,
  maxRetryTimeMs: Long
) extends Logging {
  import InitDisklessLogBatchQueue._

  private case class Attempt(state: S, attemptNumber: Int)
  private var queuedByTp = new java.util.LinkedHashMap[TopicPartition, Attempt]()
  private val resultPromiseByTp = new java.util.HashMap[TopicPartition, Promise[Boolean]]()
  private val retryBackoff = new ExponentialBackoff(retryPeriodMs, 2, maxRetryTimeMs, 0.0)
  
  private sealed trait TaskStatus
  private case object NoTask extends TaskStatus
  private case class TaskScheduled(task: ScheduledFuture[_]) extends TaskStatus
  private case object TaskRunning extends TaskStatus

  @volatile private var taskStatus: TaskStatus = NoTask

  private val queueLock = new AnyRef
  private def withQueueLock[T](f: => T): T = {
    queueLock.synchronized(f)
  }

  protected def shouldSend(state: S): Boolean

  /**
   * Sends one batch and returns parsed per-partition outcomes.
   *
   * Implementations must invoke `onBatchComplete` exactly once:
   *  - `Right(outcomes)` for a successfully received/decoded batch response
   *  - `Left(reason)` for transport/protocol-level failures
   */
  protected def sendBatch(
    states: Iterable[S],
    brokerId: Int,
    brokerEpoch: Long,
    onBatchComplete: Either[String, Iterable[ParsedResponse]] => Unit
  ): Unit

  def enqueue(tp: TopicPartition, state: S): Future[Boolean] = {
    val promise = withQueueLock {
      val currentPromise = resultPromiseByTp.computeIfAbsent(tp, _ => Promise[Boolean]())
      val duplicateQueued = Option(queuedByTp.get(tp)).exists(existing => existing.state == state)
      if (duplicateQueued) {
        taskStatus match {
          case TaskScheduled(scheduledTask) =>
            // Reschedule when receiving a duplicate so it's immediately sent
            scheduledTask.cancel(false)
            scheduleNewTask(lingerMs)
          case _ =>
        }
        currentPromise
      } else {
        val preservedAttemptNumber = Option(queuedByTp.get(tp)).map(_.attemptNumber).getOrElse(0)
        // Keep retry progression for already queued partitions, but always refresh the state payload.
        queuedByTp.put(tp, Attempt(state, attemptNumber = preservedAttemptNumber))

        taskStatus match {
          case NoTask =>
            // Schedule a new run, allowing linger for additional ready partitions.
            scheduleNewTask(lingerMs)
          case TaskScheduled(scheduledTask) =>
            // Re-schedule using linger so newly ready partitions can batch together.
            scheduledTask.cancel(false)
            scheduleNewTask(lingerMs)
          case TaskRunning =>
            // A task is in-flight. Newly queued work is already in queuedByTp
            // and will be picked up by finishTask.
        }
        currentPromise
      }
    }
    
    promise.future
  }

  def remove(tp: TopicPartition): Unit = withQueueLock {
    queuedByTp.remove(tp)
    Option(resultPromiseByTp.remove(tp)).foreach(_.trySuccess(false))
  }
  
  private def task(): Unit = {
    val toSend: util.LinkedHashMap[TopicPartition, Attempt] = withQueueLock {
      taskStatus = TaskRunning
      // Consume the items in the queue, create a new empty queue
      val queued = queuedByTp
      queuedByTp = new util.LinkedHashMap[TopicPartition, Attempt]()
      queued
    }

    if (toSend.isEmpty) {
      finishTask()
      return
    }

    val sendableByTp = new util.LinkedHashMap[TopicPartition, Attempt]()
    toSend.entrySet().asScala.foreach { entry =>
      if (shouldSend(entry.getValue.state)) {
        sendableByTp.put(entry.getKey, entry.getValue)
      } else {
        completeAndRemovePromise(entry.getKey, accepted = false)
      }
    }

    if (sendableByTp.isEmpty) {
      finishTask()
      return
    }

    val sendableStates: Seq[S] = sendableByTp.values().asScala.map(_.state).toList
    val keyToTp = sendableByTp.asScala.map { case (tp, attempt) =>
      ((attempt.state.topicId, tp.partition), tp)
    }.toMap

    try {
      sendBatch(
        states = sendableStates,
        brokerId = brokerId,
        brokerEpoch = brokerEpochSupplier(),
        onBatchComplete = { responseOrError =>
          try {
            responseOrError match {
              case Right(parsedResponses) =>
                val outcomesByTp = parsedResponses
                  .flatMap(outcome => keyToTp.get((outcome.topicId, outcome.partitionId)).map(_ -> outcome.disposition))
                  .toMap

                sendableByTp.asScala.foreach { case (tp, attempt) =>
                  outcomesByTp.get(tp) match {
                    case Some(Success) =>
                      info(s"Successful response for $tp")
                      completeAndRemovePromise(tp, accepted = true)
                    case Some(PermanentFailure) =>
                      info(s"Permanent failure response for $tp")
                      completeAndRemovePromise(tp, accepted = false)
                    case Some(RetriableFailure) =>
                      info(s"Retriable failure response for $tp")
                      enqueueRetryOrFail(tp, attempt)
                    case None => enqueueRetryOrFail(tp, attempt)
                  }
                }

              case Left(reason) =>
                warn(s"Batch send failed, scheduling retry for all states in batch. reason=$reason")
                sendableByTp.asScala.foreach { case (tp, attempt) =>
                  enqueueRetryOrFail(tp, attempt)
                }
            }
          } finally {
            finishTask()
          }
        }
      )
    } catch {
      case NonFatal(_) =>
        sendableByTp.asScala.foreach { case (tp, attempt) =>
          enqueueRetryOrFail(tp, attempt)
        }
        finishTask()
    }
  }

  private def finishTask(): Unit = withQueueLock {
    if (queuedByTp.isEmpty) {
      taskStatus = NoTask
    } else {
      val hasFreshAttempts = queuedByTp.values().asScala.exists(_.attemptNumber == 0)
      val delayMs = if (hasFreshAttempts) lingerMs else computeRetryDelayMs()
      scheduleNewTask(delayMs)
    }
  }

  private def scheduleNewTask(delayMs: Long): Unit = {
    taskStatus = TaskScheduled(scheduler.scheduleOnce("init-diskless-log-batch-queue", () => task(), delayMs))
  }

  private def enqueueRetryOrFail(tp: TopicPartition, attempt: Attempt): Unit = withQueueLock {
    val retryAttemptNumber = attempt.attemptNumber + 1
    Option(queuedByTp.get(tp)) match {
      case Some(existing) =>
        // Keep the already queued state (it may be fresher), but ensure retry progression is not lost.
        queuedByTp.put(tp, Attempt(existing.state, Math.max(existing.attemptNumber, retryAttemptNumber)))
      case None =>
        queuedByTp.put(tp, Attempt(attempt.state, retryAttemptNumber))
    }
  }

  private def completeAndRemovePromise(tp: TopicPartition, accepted: Boolean): Unit = withQueueLock {
    Option(resultPromiseByTp.remove(tp)).foreach(_.trySuccess(accepted))
  }

  private def computeRetryDelayMs(): Long = {
    val maxAttemptNumber = queuedByTp.values().asScala.map(_.attemptNumber).maxOption.getOrElse(0)
    // First retry waits retryPeriodMs, then doubles exponentially up to maxRetryTimeMs.
    val attempts = Math.max(maxAttemptNumber - 1, 0)
    retryBackoff.backoff(attempts)
  }

}

class SendingToControllerBatchQueue(
  controllerChannelManager: NodeToControllerChannelManager,
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long,
  lingerMs: Long,
  retryPeriodMs: Long,
  maxRetryTimeMs: Long
) extends RetriableInitDisklessLogBatchQueue[SendingToController](
  scheduler = scheduler,
  brokerId = brokerId,
  brokerEpochSupplier = brokerEpochSupplier,
  lingerMs = lingerMs,
  retryPeriodMs = retryPeriodMs,
  maxRetryTimeMs = maxRetryTimeMs
) {
  logIdent = s"[SendingToControllerBatchQueue] "

  override protected def shouldSend(state: SendingToController): Boolean = {
    if (!state.partition.isLeader) {
      state.warn(s"Skipping InitDisklessLog controller request because this broker is no longer leader for ${state.tp}")
      false
    } else if (state.partition.log.isEmpty) {
      state.warn(s"Skipping InitDisklessLog controller request because log is no longer present for ${state.tp}")
      false
    } else {
      true
    }
  }

  override protected def sendBatch(
    states: Iterable[SendingToController],
    brokerId: Int,
    brokerEpoch: Long,
    onBatchComplete: Either[String, Iterable[ParsedResponse]] => Unit
  ): Unit = {
    val requestData = SendingToController.buildRequestData(states, brokerId, brokerEpoch)
    val request = new InitDisklessLogRequest.Builder(requestData)
    controllerChannelManager.sendRequest(request, new ControllerRequestCompletionHandler {
      override def onComplete(response: ClientResponse): Unit = {
        onBatchComplete(extractControllerResponse(response).map(SendingToController.parseBatchResponse))
      }

      override def onTimeout(): Unit = onBatchComplete(Left("timeout"))
    })
  }

  private def extractControllerResponse(response: ClientResponse): Either[String, InitDisklessLogResponseData] = {
    if (response.authenticationException != null) {
      Left("authentication exception")
    } else if (response.versionMismatch != null) {
      Left("version mismatch")
    } else {
      response.responseBody match {
        case initDisklessLogResponse: InitDisklessLogResponse => Right(initDisklessLogResponse.data())
        case _ => Left("unexpected response body type")
      }
    }
  }
}

class AwaitingMetadataBatchQueue(
  controlPlane: ControlPlane,
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long,
  lingerMs: Long,
  retryPeriodMs: Long,
  maxRetryTimeMs: Long
) extends RetriableInitDisklessLogBatchQueue[AwaitingMetadata](
  scheduler = scheduler,
  brokerId = brokerId,
  brokerEpochSupplier = brokerEpochSupplier,
  lingerMs = lingerMs,
  retryPeriodMs = retryPeriodMs,
  maxRetryTimeMs = maxRetryTimeMs
) {
  logIdent = s"[AwaitingMetadataBatchQueue] "

  override protected def shouldSend(state: AwaitingMetadata): Boolean = {
    if (state.metadataPayload.isDefined) {
      true
    } else {
      state.warn(s"Skipping InitDisklessLog control-plane request because metadata payload is missing for ${state.tp}")
      false
    }
  }

  override protected def sendBatch(
    states: Iterable[AwaitingMetadata],
    brokerId: Int,
    brokerEpoch: Long,
    onBatchComplete: Either[String, Iterable[ParsedResponse]] => Unit
  ): Unit = {
    AwaitingMetadata.sendBatch(
      states = states,
      destination = controlPlane,
      brokerId = brokerId,
      brokerEpoch = brokerEpoch,
      onBatchComplete = onBatchComplete
    )
  }
}

