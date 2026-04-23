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

import io.aiven.inkless.control_plane.{ControlPlane, InitDisklessLogProducerState => CpProducerState, InitDisklessLogRequest => CpInitRequest, InitDisklessLogResponse => CpInitResponse}
import kafka.cluster.Partition
import kafka.server.InitDisklessLogBatchQueue.ParsedResponse
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{InitDisklessLogRequestData, InitDisklessLogResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.partition.PartitionListener
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.util
import scala.jdk.CollectionConverters._

sealed trait InitDisklessLogState extends Logging {
  def partition: Partition
  val topicId: Uuid
  val tp: TopicPartition = partition.topicPartition
}

final case class DisklessInitMetadata(
  topicName: String,
  firstDisklessOffset: Long,
  producerStates: util.List[CpProducerState]
)

final case class Failed(partition: Partition, topicId: Uuid) extends WaitingForReplicationOutcome

sealed trait WaitingForReplicationOutcome extends InitDisklessLogState

/** HW has not yet caught up with LEO; waiting for replica fetch cycles to advance it. */
final case class WaitingForReplication(
  partition: Partition,
  topicId: Uuid,
  // called whenever there's a new partition event sent by the PartitionListener
  onPartitionUpdate: (TopicPartition, WaitingForReplicationOutcome) => Unit  
) extends WaitingForReplicationOutcome with PartitionListener {
  logIdent = s"[InitDisklessLog->WaitingForReplication $tp] "

  private def validate(): Either[String, Unit] = {
    if (!partition.isSealed) {
      return Left("Partition is not sealed, which should never happen. Skipping migration.")
    }
    val log: UnifiedLog = partition.log.getOrElse {
      return Left("Partition sealed but has no log. Skipping migration")
    }
    val hw: Long = log.highWatermark
    val leo: Long = log.logEndOffset
    if (hw > leo) {
      Left(s"HW ($hw) > LEO ($leo), which should never happen. Skipping migration.")
    } else {
      Right(())
    }
  }

  override def onHighWatermarkUpdated(tp: TopicPartition, offset: Long): Unit =
    onPartitionUpdate(tp, maybeAdvanceState())

  override def onFailed(tp: TopicPartition): Unit =
    onPartitionUpdate(tp, Failed(partition, topicId))

  override def onDeleted(tp: TopicPartition): Unit =
    onPartitionUpdate(tp, Failed(partition, topicId))
  
  def maybeAdvanceState(): WaitingForReplicationOutcome = {
    validate() match {
      case Left(errorString) =>
        error(errorString)
        return Failed(partition, topicId)
      case Right(_) =>
    }
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
    AwaitingMetadata(partition, topicId, metadataPayload = None)
  }
}

object SendingToController {

  def buildRequestData(
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

  def parseBatchResponse(response: InitDisklessLogResponseData): Iterable[ParsedResponse] = {
    val outcomes = scala.collection.mutable.ArrayBuffer[ParsedResponse]()
    for (topicResponse <- response.topics().asScala) {
      for (partitionResponse <- topicResponse.partitions().asScala) {
        val error = Errors.forCode(partitionResponse.errorCode())
        val disposition = error match {
          case Errors.NONE => InitDisklessLogBatchQueue.Success
          case Errors.FENCED_LEADER_EPOCH | Errors.INVALID_REQUEST => InitDisklessLogBatchQueue.PermanentFailure
          case _ => InitDisklessLogBatchQueue.RetriableFailure
        }
        outcomes += ParsedResponse(
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

}

/** Controller accepted the request; waiting for the PartitionChangeRecord with firstDisklessOffset to propagate. */
final case class AwaitingMetadata(
  partition: Partition,
  topicId: Uuid,
  metadataPayload: Option[DisklessInitMetadata] = None
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->AwaitingMetadata $tp] "

  def onSuccess: Done = {
    info(s"Metadata application successful. Advancing to Done.")
    Done(partition, topicId)
  }
}

/** Terminal state once metadata init succeeds. */
final case class Done(
  partition: Partition,
  topicId: Uuid
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->Done $tp] "
}

object AwaitingMetadata {

  def sendBatch(
    states: Iterable[AwaitingMetadata],
    destination: ControlPlane,
    brokerId: Int,
    brokerEpoch: Long,
    onBatchComplete: Either[String, Iterable[ParsedResponse]] => Unit
  ): Unit = {
    def retriable(state: AwaitingMetadata): ParsedResponse =
      ParsedResponse(state.topicId, state.tp.partition(), Errors.NOT_CONTROLLER, InitDisklessLogBatchQueue.RetriableFailure)

    // Keep stable ordering to align final outcomes with input states.
    val stateSeq = states.toSeq
    // Retry outcomes computed locally (without a control-plane round-trip), keyed by input index.
    val retryOutcomeByIndex = scala.collection.mutable.Map[Int, ParsedResponse]()
    // Batched control-plane requests for states that are ready to be applied.
    val requests = new util.ArrayList[CpInitRequest]()

    // Validate each state and either precompute a retry outcome or queue a batched request.
    stateSeq.zipWithIndex.foreach { case (state, index) =>
      state.metadataPayload match {
        case None =>
          state.warn(s"Missing metadata payload for ${state.tp} while awaiting metadata; scheduling retry")
          retryOutcomeByIndex += index -> retriable(state)
        case Some(metadata) =>
          state.partition.log match {
            case None =>
              state.warn(s"Partition ${state.tp} has no log while applying diskless metadata, scheduling retry")
              retryOutcomeByIndex += index -> retriable(state)
            case Some(log) =>
              requests.add(new CpInitRequest(
                state.topicId,
                metadata.topicName,
                state.tp.partition(),
                log.logStartOffset,
                metadata.firstDisklessOffset,
                metadata.producerStates
              ))
          }
      }
    }

    // Issue one Control Plane call for the whole batch (if any requests exist).
    val responseResult: Either[Throwable, Seq[CpInitResponse]] =
      if (requests.isEmpty) Right(Seq.empty)
      else {
        try Right(Option(destination.initDisklessLog(requests)).map(_.asScala.toSeq).getOrElse(Seq.empty))
        catch {
          case t: Throwable => Left(t)
        }
      }

    // Rebuild outcomes in original order.
    val responseIterator = responseResult.getOrElse(Seq.empty).iterator
    val outcomes = stateSeq.zipWithIndex.map { case (state, index) =>
      retryOutcomeByIndex.getOrElse(index, {
        responseResult match {
          case Left(t) =>
            // If the single batched call fails, retry all.
            state.warn(s"Control-plane InitDisklessLog for ${state.tp} failed, scheduling retry", t)
            retriable(state)
          case Right(_) =>
            (if (responseIterator.hasNext) Some(responseIterator.next()) else None) match {
              // INVALID_REQUEST = partition already initialized (idempotent success)
              case Some(r) if r.error() == Errors.NONE || r.error() == Errors.INVALID_REQUEST =>
                ParsedResponse(state.topicId, state.tp.partition(), r.error(), InitDisklessLogBatchQueue.Success)
              case Some(r) =>
                ParsedResponse(state.topicId, state.tp.partition(), r.error(), InitDisklessLogBatchQueue.RetriableFailure)
              case None =>
                // Missing response entry is treated as retriable to avoid dropping work.
                state.warn(s"Control-plane InitDisklessLog response missing for ${state.tp}, scheduling retry")
                retriable(state)
            }
        }
      })
    }

    onBatchComplete(Right(outcomes))
  }
}