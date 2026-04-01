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

import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.InitDisklessLogResponseData
import org.apache.kafka.server.common.NodeToControllerChannelManager
import org.apache.kafka.server.util.Scheduler

import java.util
import scala.concurrent.{Future, Promise}
import java.util.concurrent.ScheduledFuture
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
 * Protocol that defines batched send/parse behavior for a specific state kind `S`
 * and destination type `D`.
 *
 * @tparam S state type handled by this protocol.
 * @tparam D destination type used to send batched requests.
 * @tparam Resp parsed response type produced by the destination.
 */
trait SendableBatchProtocol[S <: InitDisklessLogState, D, Resp] {
  /**
   * Whether the state should be included in a batch at send time.
   * Returning false indicates that the state should be excluded from this send attempt.
   */
  def shouldSend(state: S): Boolean

  /**
   * Sends one batch containing all currently selected states.
   *
   * Implementations must invoke `onBatchComplete` exactly once:
   *  - `Right(response)` for a successfully received/decoded batch response
   *  - `Left(reason)` for transport/protocol-level failures
   */
  def sendBatch(
    states: Iterable[S],
    destination: D,
    brokerId: Int,
    brokerEpoch: Long,
    onBatchComplete: Either[String, Resp] => Unit
  ): Unit

  /**
   * Converts a successful batch response into per-partition outcomes.
   * Any state sent but missing from this parsed output is treated as retriable by callers.
   */
  def parseBatchResponse(response: Resp): Iterable[SendableBatchProtocol.ParsedResponse]
}

object SendableBatchProtocol {
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

/** A queue that is able to batch multiple requests and send them all together in the same batch. */
trait InitDisklessLogBatchQueue[S <: InitDisklessLogState]{

  /* Enqueue a sendable state into the queue.
  * The returned future contains the result of the operation (true if it was accepted by the destination, false if permanently failed).
  * The future is completed only when a final response is given.
  * */
  def enqueue(tp: TopicPartition, state: S): Future[Boolean]

  /**
   * Remove the partition from this queue.
   */
  def remove(tp: TopicPartition): Unit

}

abstract class RetriableInitDisklessLogBatchQueue[S <: InitDisklessLogState, D, Resp](
  destination: D,
  protocol: SendableBatchProtocol[S, D, Resp],
  scheduler: Scheduler,
  brokerId: Int,
  brokerEpochSupplier: () => Long,
  lingerMs: Long,
  retryPeriodMs: Long,
  maxRetryTimeMs: Long
) extends InitDisklessLogBatchQueue[S] with Logging {
  private case class Attempt(state: S, attemptNumber: Int)
  private var queuedByTp = new java.util.LinkedHashMap[TopicPartition, Attempt]()
  private val resultPromiseByTp = new java.util.HashMap[TopicPartition, Promise[Boolean]]()
  
  private sealed trait TaskStatus
  private case object NoTask extends TaskStatus
  private case class TaskScheduled(task: ScheduledFuture[_]) extends TaskStatus
  private case object TaskRunning extends TaskStatus

  @volatile private var taskStatus: TaskStatus = NoTask

  private val queueLock = new AnyRef
  private def withQueueLock[T](f: => T): T = {
    queueLock.synchronized(f)
  }

  override def enqueue(tp: TopicPartition, state: S): Future[Boolean] = {
    val promise = withQueueLock{
      val preservedAttemptNumber = Option(queuedByTp.get(tp)).map(_.attemptNumber).getOrElse(0)
      // Keep retry progression for already queued partitions, but always refresh the state payload.
      queuedByTp.put(tp, Attempt(state, attemptNumber = preservedAttemptNumber))
      val currentPromise = resultPromiseByTp.computeIfAbsent(tp, _ => Promise[Boolean]())
      
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
    
    promise.future
  }

  override def remove(tp: TopicPartition): Unit = withQueueLock {
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
      if (protocol.shouldSend(entry.getValue.state)) {
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
      protocol.sendBatch(
        states = sendableStates,
        destination = destination,
        brokerId = brokerId,
        brokerEpoch = brokerEpochSupplier(),
        onBatchComplete = { responseOrError =>
          try {
            responseOrError match {
              case Right(parsedResponse) =>
                val outcomesByTp = protocol.parseBatchResponse(parsedResponse)
                  .flatMap(outcome => keyToTp.get((outcome.topicId, outcome.partitionId)).map(_ -> outcome.disposition))
                  .toMap

                sendableByTp.asScala.foreach { case (tp, attempt) =>
                  outcomesByTp.get(tp) match {
                    case Some(SendableBatchProtocol.Success) => 
                      info(s"Successful response for $tp")
                      completeAndRemovePromise(tp, accepted = true)
                    case Some(SendableBatchProtocol.PermanentFailure) =>
                      info(s"Permanent failure response for $tp")
                      completeAndRemovePromise(tp, accepted = false)
                    case Some(SendableBatchProtocol.RetriableFailure) =>
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
    if (maxAttemptNumber <= 0) {
      retryPeriodMs
    } else {
      // First retry waits retryPeriodMs, then doubles exponentially up to maxRetryTimeMs.
      var delay = retryPeriodMs
      var exponent = maxAttemptNumber - 1
      while (exponent > 0 && delay < maxRetryTimeMs) {
        if (delay >= (maxRetryTimeMs + 1L) / 2L) {
          delay = maxRetryTimeMs
        } else {
          delay = delay * 2L
        }
        exponent -= 1
      }
      Math.min(delay, maxRetryTimeMs)
    }
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
) extends RetriableInitDisklessLogBatchQueue[
  SendingToController,
  NodeToControllerChannelManager,
  InitDisklessLogResponseData
](
  destination = controllerChannelManager,
  protocol = SendingToControllerBatchProtocol,
  scheduler = scheduler,
  brokerId = brokerId,
  brokerEpochSupplier = brokerEpochSupplier,
  lingerMs = lingerMs,
  retryPeriodMs = retryPeriodMs,
  maxRetryTimeMs = maxRetryTimeMs
) {
  logIdent = s"[SendingToControllerBatchQueue] "
}
