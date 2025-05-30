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

import com.yammer.metrics.core.Meter
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.server.ListOffsetsPartitionStatus
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.purgatory.DelayedOperation
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError

import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

class DelayedRemoteListOffsets(delayMs: Long,
                               version: Int,
                               statusByPartition: mutable.Map[TopicPartition, ListOffsetsPartitionStatus],
                               replicaManager: ReplicaManager,
                               isInklessTopic: String => Boolean = _ => false,
                               responseCallback: List[ListOffsetsTopicResponse] => Unit)
  extends DelayedOperation(delayMs) with Logging {
  // Mark the status as completed, if there is no async task to track.
  // If there is a task to track, then build the response as REQUEST_TIMED_OUT by default.
  statusByPartition.foreachEntry { (topicPartition, status) =>
    status.completed(status.futureHolderOpt.isEmpty)
    if (status.futureHolderOpt.isPresent) {
      status.responseOpt(Optional.of(buildErrorResponse(Errors.REQUEST_TIMED_OUT, topicPartition.partition())))
    }
    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   */
  override def onExpiration(): Unit = {
    statusByPartition.foreachEntry { (topicPartition, status) =>
      if (!status.completed) {
        debug(s"Expiring list offset request for partition $topicPartition with status $status")
        status.futureHolderOpt.ifPresent(futureHolder => futureHolder.jobFuture.cancel(true))
        DelayedRemoteListOffsetsMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   */
  override def onComplete(): Unit = {
    val responseTopics = statusByPartition.groupBy(e => e._1.topic()).map {
      case (topic, status) =>
        new ListOffsetsTopicResponse().setName(topic).setPartitions(status.values.flatMap(s => Some(s.responseOpt.get())).toList.asJava)
    }.toList
    responseCallback(responseTopics)
  }

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  override def tryComplete(): Boolean = {
    var completable = true
    statusByPartition.foreachEntry { (partition, status) =>
      if (!status.completed) {
        try {
          if (!isInklessTopic(partition.topic()))
            replicaManager.getPartitionOrException(partition)
        } catch {
          case e: ApiException =>
            status.futureHolderOpt.ifPresent { futureHolder =>
              futureHolder.jobFuture.cancel(false)
              futureHolder.taskFuture.complete(new FileRecordsOrError(Optional.of(e), Optional.empty()))
            }
        }

        status.futureHolderOpt.ifPresent { futureHolder =>
          if (futureHolder.taskFuture.isDone) {
            val taskFuture = futureHolder.taskFuture.get()
            val response = {
              if (taskFuture.hasException) {
                buildErrorResponse(Errors.forException(taskFuture.exception().get()), partition.partition())
              } else if (!taskFuture.hasTimestampAndOffset) {
                val error = status.maybeOffsetsError
                  .map(e => if (version >= 5) Errors.forException(e) else Errors.LEADER_NOT_AVAILABLE)
                  .orElse(Errors.NONE)
                buildErrorResponse(error, partition.partition())
              } else {
                var partitionResponse = buildErrorResponse(Errors.NONE, partition.partition())
                val found = taskFuture.timestampAndOffset().get()
                if (status.lastFetchableOffset.isPresent && found.offset >= status.lastFetchableOffset.get) {
                  if (status.maybeOffsetsError.isPresent) {
                    val error = if (version >= 5) Errors.forException(status.maybeOffsetsError.get) else Errors.LEADER_NOT_AVAILABLE
                    partitionResponse.setErrorCode(error.code())
                  }
                } else {
                  partitionResponse = new ListOffsetsPartitionResponse()
                    .setPartitionIndex(partition.partition())
                    .setErrorCode(Errors.NONE.code())
                    .setTimestamp(found.timestamp)
                    .setOffset(found.offset)

                  if (found.leaderEpoch.isPresent && version >= 4) {
                    partitionResponse.setLeaderEpoch(found.leaderEpoch.get)
                  }
                }
                partitionResponse
              }
            }
            status.responseOpt(Optional.of(response))
            status.completed(true)
          }
          completable = completable && futureHolder.taskFuture.isDone
        }
      }
    }
    if (completable) {
      forceComplete()
    } else {
      false
    }
  }

  private def buildErrorResponse(e: Errors, partitionIndex: Int): ListOffsetsPartitionResponse = {
    new ListOffsetsPartitionResponse()
      .setPartitionIndex(partitionIndex)
      .setErrorCode(e.code)
      .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
      .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
  }
}

object DelayedRemoteListOffsetsMetrics {
  private val metricsGroup = new KafkaMetricsGroup(DelayedRemoteListOffsetsMetrics.getClass)
  private[server] val aggregateExpirationMeter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)
  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    metricsGroup.newMeter("ExpiresPerSec",
      "requests",
      TimeUnit.SECONDS,
      Map("topic" -> key.topic, "partition" -> key.partition.toString).asJava)
  private[server] val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}