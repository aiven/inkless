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
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.util
import scala.jdk.CollectionConverters._

sealed trait InitDisklessLogState extends Logging {
  def partition: Partition
  val topicId: Uuid
  val tp: TopicPartition = partition.topicPartition
}

final case class Failed(partition: Partition, topicId: Uuid) extends WaitingForReplicationOutcome

sealed trait WaitingForReplicationOutcome extends InitDisklessLogState

/** HW has not yet caught up with LEO; waiting for replica fetch cycles to advance it. */
final case class WaitingForReplication(
  partition: Partition,
  topicId: Uuid
) extends WaitingForReplicationOutcome with PartitionListener {
  logIdent = s"[InitDisklessLog->WaitingForReplication $tp] "

  validate()
  
  def validate(): Unit = {
    if (!partition.isSealed) {
      error(s"Partition is not sealed, which should never happen. Skipping migration.")
      throw new IllegalArgumentException()
    }
    val log: UnifiedLog = partition.log.getOrElse {
      warn(s"Partition sealed but has no log. Skipping migration")
      throw new IllegalArgumentException()
    }
    val hw: Long = log.highWatermark
    val leo: Long = log.logEndOffset
    if (hw > leo) {
      error(s"HW ($hw) > LEO ($leo), which should never happen. Skipping migration.")
      throw new IllegalArgumentException()
    }
  }
  
  def maybeAdvanceState(): WaitingForReplicationOutcome = {
    partition.log match {
      case None => this
      case Some(log) =>
        val hw = log.highWatermark
        val leo = log.logEndOffset
        if (hw > leo) {
          error(s"HW ($hw) > LEO ($leo). Removing from tracking.")
          Failed(partition, topicId)
        } else if (hw == leo) {
          info(s"HW ($hw) caught up with LEO ($leo). Advancing to SendingToController")
          SendingToController(partition, topicId)
        } else {
          info(s"HW ($hw) still < LEO ($leo). Remaining in WaitingForReplication")
          this
        }
    }
  }

}

/** HW == LEO; partition is queued for the next batched InitDisklessLog controller call. */
final case class SendingToController(
  partition: Partition,
  topicId: Uuid
) extends WaitingForReplicationOutcome {
  logIdent = s"[InitDisklessLog->SendingToController $tp] "

  def onSuccess: AwaitingMetadata = {
    info(s"Request successful. Advancing to AwaitingMetadata.")
    AwaitingMetadata(partition, topicId)
  }
}

object SendingToControllerBatchProtocol extends SendableBatchProtocol[
  SendingToController,
  NodeToControllerChannelManager,
  InitDisklessLogResponseData
] {
  override def shouldSend(state: SendingToController): Boolean = {
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

  private def buildRequestData(
    states: Iterable[SendingToController],
    brokerId: Int,
    brokerEpoch: Long
  ): InitDisklessLogRequestData = {
    val topicDataById = new util.LinkedHashMap[Uuid, util.List[InitDisklessLogRequestData.PartitionData]]()
    states.foreach { state =>
      toPartitionData(state).foreach { partitionData =>
        topicDataById.computeIfAbsent(state.topicId, _ => new util.ArrayList[InitDisklessLogRequestData.PartitionData]())
          .add(partitionData)
      }
    }

    val topicDataList = new util.ArrayList[InitDisklessLogRequestData.TopicData]()
    topicDataById.asScala.foreach { case (stateTopicId, partitions) =>
      topicDataList.add(new InitDisklessLogRequestData.TopicData()
        .setTopicId(stateTopicId)
        .setPartitions(partitions))
    }

    new InitDisklessLogRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch)
      .setTopics(topicDataList)
  }

  override def sendBatch(
    states: Iterable[SendingToController],
    destination: NodeToControllerChannelManager,
    brokerId: Int,
    brokerEpoch: Long,
    onBatchComplete: Either[String, InitDisklessLogResponseData] => Unit
  ): Unit = {
    val requestData = buildRequestData(states, brokerId, brokerEpoch)
    val request = new InitDisklessLogRequest.Builder(requestData)
    destination.sendRequest(request, new ControllerRequestCompletionHandler {
      override def onComplete(response: ClientResponse): Unit = {
        onBatchComplete(extractControllerResponse(response))
      }

      override def onTimeout(): Unit = onBatchComplete(Left("timeout"))
    })
  }

  override def parseBatchResponse(response: InitDisklessLogResponseData): Iterable[SendableBatchProtocol.ParsedResponse] = {
    val outcomes = scala.collection.mutable.ArrayBuffer[SendableBatchProtocol.ParsedResponse]()
    for (topicResponse <- response.topics().asScala) {
      for (partitionResponse <- topicResponse.partitions().asScala) {
        val error = Errors.forCode(partitionResponse.errorCode())
        val disposition = error match {
          case Errors.NONE => SendableBatchProtocol.Success
          case Errors.FENCED_LEADER_EPOCH | Errors.INVALID_REQUEST => SendableBatchProtocol.PermanentFailure
          case _ => SendableBatchProtocol.RetriableFailure
        }
        outcomes += SendableBatchProtocol.ParsedResponse(
          topicId = topicResponse.topicId(),
          partitionId = partitionResponse.partitionId(),
          error = error,
          disposition = disposition
        )
      }
    }
    outcomes.toSeq
  }

  private def toPartitionData(state: SendingToController): Option[InitDisklessLogRequestData.PartitionData] = {
    state.partition.log.map { log =>
      val hw = log.highWatermark
      val leaderEpoch = state.partition.getLeaderEpoch
      new InitDisklessLogRequestData.PartitionData()
        .setPartitionId(state.tp.partition)
        .setDisklessStartOffset(hw)
        .setLeaderEpoch(leaderEpoch)
        .setProducerStates(extractProducerStates(log))
    }.orElse {
      state.warn(s"Skipping InitDisklessLog request entry because log disappeared for ${state.tp}")
      None
    }
  }

  private def extractProducerStates(log: UnifiedLog): util.List[InitDisklessLogRequestData.ProducerState] = {
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

/** Controller accepted the request; waiting for the PartitionChangeRecord with disklessStartOffset to propagate. */
final case class AwaitingMetadata(
  partition: Partition,
  topicId: Uuid
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->AwaitingMetadata $tp] "

}   