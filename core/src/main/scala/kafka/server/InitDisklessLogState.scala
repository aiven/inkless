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
import org.apache.kafka.common.message.{InitDisklessLogRequestData, InitDisklessLogResponseData, SetClassicPrefixInRemoteStorageRequestData, SetClassicPrefixInRemoteStorageResponseData}
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
  classicToDisklessStartOffset: Long,
  producerStates: util.List[CpProducerState],
  classicPrefixInRemoteStorage: Boolean
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
      if (!entry.batchMetadata().isEmpty) {
        entry.batchMetadata().asScala.foreach { batch =>
          states.add(new InitDisklessLogRequestData.ProducerState()
            .setProducerId(producerId)
            .setProducerEpoch(entry.producerEpoch())
            .setBaseSequence(batch.firstSeq())
            .setLastSequence(batch.lastSeq())
            .setAssignedOffset(batch.firstOffset())
            .setBatchMaxTimestamp(batch.timestamp()))
        }
      }
    }
    states
  }

}

/** Controller accepted the request; waiting for the PartitionChangeRecord with classicToDisklessStartOffset to propagate. */
final case class AwaitingMetadata(
  partition: Partition,
  topicId: Uuid,
  metadataPayload: Option[DisklessInitMetadata] = None
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->AwaitingMetadata $tp] "

  /**
   * Advances to WaitingForUpload while the classic prefix isn't in remote storage yet (phase 1),
   * and to Done otherwise.
   */
  def onSuccess: InitDisklessLogState = metadataPayload match {
    case Some(metadata) if !metadata.classicPrefixInRemoteStorage =>
      info(s"Metadata application successful. Advancing to WaitingForUpload.")
      WaitingForUpload(partition, topicId, metadata.classicToDisklessStartOffset)
    case _ =>
      info(s"Metadata application successful. Advancing to Done.")
      Done(partition, topicId)
  }
}

/** Terminal state once metadata init succeeds and no classic-prefix upload remains to watch. */
final case class Done(
  partition: Partition,
  topicId: Uuid
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->Done $tp] "
}

/** Result of evaluating the upload detection rule for a partition in WaitingForUpload. */
sealed trait UploadCheck
object UploadCheck {
  /** The classic prefix isn't fully in remote storage yet. */
  case object Pending extends UploadCheck
  /** The classic prefix is in remote storage, or retention already removed it. */
  case object Uploaded extends UploadCheck
  /** This broker is no longer the leader or has no local log, so the watch is moot. */
  case object Gone extends UploadCheck
}

/**
 * The controller committed the seal and `classicPrefixInRemoteStorage` is unset (switch phase 1).
 * The leader waits until the classic prefix `[0, seal)` is in remote storage.
 */
final case class WaitingForUpload(
  partition: Partition,
  topicId: Uuid,
  seal: Long
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->WaitingForUpload $tp] "

  /**
   * Checks the detection rule `logStartOffset >= seal || highestOffsetInRemoteStorage >= seal - 1`.
   * The first term covers a prefix that retention or `DeleteRecords` already removed, where the
   * remote gauge stays at `-1`. Remote copying follows offset order, and a leader change seeds the
   * gauge from the remote log metadata. The gauge alone proves coverage.
   */
  def check(): UploadCheck = {
    if (!partition.isLeader) {
      UploadCheck.Gone
    } else {
      partition.log match {
        case None => UploadCheck.Gone
        case Some(log) =>
          val logStartOffset = log.logStartOffset
          val highestRemoteOffset = log.highestOffsetInRemoteStorage
          if (logStartOffset >= seal || highestRemoteOffset >= seal - 1) {
            info(s"Classic prefix is uploaded (seal=$seal, logStartOffset=$logStartOffset, " +
              s"highestOffsetInRemoteStorage=$highestRemoteOffset). Advancing to SendingFlagToController.")
            UploadCheck.Uploaded
          } else {
            UploadCheck.Pending
          }
      }
    }
  }

  def onUploaded: SendingFlagToController = SendingFlagToController(partition, topicId, seal)
}

/** The classic prefix is in remote storage. The partition waits for the next batched `SetClassicPrefixInRemoteStorage` call. */
final case class SendingFlagToController(
  partition: Partition,
  topicId: Uuid,
  seal: Long
) extends InitDisklessLogState {
  logIdent = s"[InitDisklessLog->SendingFlagToController $tp] "
}

object SendingFlagToController {

  def buildRequestData(
    states: Iterable[SendingFlagToController],
    brokerId: Int,
    brokerEpoch: Long
  ): SetClassicPrefixInRemoteStorageRequestData = {
    val topicDataById = new util.LinkedHashMap[Uuid, util.List[SetClassicPrefixInRemoteStorageRequestData.PartitionData]]()
    states.foreach { state =>
      // Reading the leader epoch at build time makes a retry after a same-leader epoch bump send the current one.
      topicDataById.computeIfAbsent(state.topicId, _ => new util.ArrayList[SetClassicPrefixInRemoteStorageRequestData.PartitionData]())
        .add(new SetClassicPrefixInRemoteStorageRequestData.PartitionData()
          .setPartitionId(state.tp.partition)
          .setClassicToDisklessStartOffset(state.seal)
          .setLeaderEpoch(state.partition.getLeaderEpoch))
    }

    val topicDataList = new util.ArrayList[SetClassicPrefixInRemoteStorageRequestData.TopicData]()
    topicDataById.asScala.foreach { case (topicId, partitions) =>
      topicDataList.add(new SetClassicPrefixInRemoteStorageRequestData.TopicData()
        .setTopicId(topicId)
        .setPartitions(partitions))
    }

    new SetClassicPrefixInRemoteStorageRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch)
      .setTopics(topicDataList)
  }

  def parseBatchResponse(response: SetClassicPrefixInRemoteStorageResponseData): Iterable[ParsedResponse] = {
    val outcomes = scala.collection.mutable.ArrayBuffer[ParsedResponse]()
    for (topicResponse <- response.topics().asScala) {
      for (partitionResponse <- topicResponse.partitions().asScala) {
        val error = Errors.forCode(partitionResponse.errorCode())
        val disposition = error match {
          case Errors.NONE => InitDisklessLogBatchQueue.Success
          // The partition doesn't have the switch state the request assumed, or the topic no longer exists.
          // A retry can't change that.
          case Errors.INVALID_REQUEST | Errors.UNKNOWN_TOPIC_ID | Errors.UNKNOWN_TOPIC_OR_PARTITION =>
            InitDisklessLogBatchQueue.PermanentFailure
          // `FENCED_LEADER_EPOCH` is retriable, unlike in `InitDisklessLog`. A same-leader epoch bump, such as
          // an ISR shrink, fences an in-flight request but doesn't re-register the watch, because the broker
          // didn't become leader. The retry sends the current epoch. If leadership moved, `shouldSend`
          // and `removePartition` stop the retry.
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
                metadata.classicToDisklessStartOffset,
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