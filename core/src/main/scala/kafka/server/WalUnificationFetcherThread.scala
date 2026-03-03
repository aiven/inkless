/*
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

import java.util.Optional

import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.server.ReplicaState
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.util.ShutdownableThread
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

/**
 * Fetcher thread for WAL unification (Option A): extends ShutdownableThread, owns partition state,
 * calls WalUnificationFetchHelper.fetchFromWal and appends to local log via the same path as replica fetcher.
 */
class WalUnificationFetcherThread(
  name: String,
  val fetcherId: Int,
  walFetchHelper: WalUnificationFetchHelper,
  brokerConfig: KafkaConfig,
  failedPartitions: FailedPartitions,
  replicaMgr: ReplicaManager
) extends ShutdownableThread(name, false) with Logging {

  private val partitionState = mutable.Map.empty[TopicPartition, (Option[Uuid], Long, Int)]
  private val partitionMapLock = new Object
  private val partitionsWithNewHighWatermark = mutable.Set.empty[TopicPartition]

  private val maxBytesPerPartition = brokerConfig.replicaFetchResponseMaxBytes

  def addPartitions(initialFetchStates: Map[TopicPartition, InitialFetchState]): Set[TopicPartition] = {
    partitionMapLock.synchronized {
      failedPartitions.removeAll(initialFetchStates.keySet)
      initialFetchStates.foreach { case (tp, state) =>
        partitionState(tp) = (state.topicId, state.initOffset, state.currentLeaderEpoch)
      }
      initialFetchStates.keySet
    }
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    partitionMapLock.synchronized {
      val result = mutable.Map.empty[TopicPartition, PartitionFetchState]
      topicPartitions.foreach { tp =>
        partitionState.remove(tp).foreach { case (topicId, fetchOffset, leaderEpoch) =>
          result(tp) = new PartitionFetchState(
            topicId.toJava,
            fetchOffset,
            Optional.empty[java.lang.Long](),
            leaderEpoch,
            ReplicaState.FETCHING,
            Optional.empty()
          )
        }
      }
      result.toMap
    }
  }

  def partitionCount: Int = partitionMapLock.synchronized(partitionState.size)
  def partitions: Set[TopicPartition] = partitionMapLock.synchronized(partitionState.keySet.toSet)

  override def doWork(): Unit = {
    val fetchData = partitionMapLock.synchronized {
      buildFetchData()
    }
    if (fetchData.isEmpty) {
      // No partitions to fetch or all missing topicId; back off briefly
      Thread.sleep(100)
      return
    }

    val response = walFetchHelper.fetchFromWal(fetchData.asJava)
    processResponse(response.asScala.toMap)
    if (partitionsWithNewHighWatermark.nonEmpty) {
      replicaMgr.completeDelayedFetchRequests(partitionsWithNewHighWatermark.toSeq)
      partitionsWithNewHighWatermark.clear()
    }
  }

  private def buildFetchData(): mutable.Map[org.apache.kafka.common.TopicIdPartition, FetchRequest.PartitionData] = {
    val data = mutable.Map.empty[org.apache.kafka.common.TopicIdPartition, FetchRequest.PartitionData]
    partitionState.foreach { case (tp, (topicIdOpt, fetchOffset, leaderEpoch)) =>
      topicIdOpt.foreach { topicId =>
        val tidp = new org.apache.kafka.common.TopicIdPartition(topicId, tp)
        data(tidp) = new FetchRequest.PartitionData(
          topicId,
          fetchOffset,
          0L,
          maxBytesPerPartition,
          Optional.of(leaderEpoch)
        )
      }
    }
    data
  }

  private def processResponse(response: Map[TopicPartition, FetchResponseData.PartitionData]): Unit = {
    val toUpdate = mutable.Map.empty[TopicPartition, Long]
    response.foreach { case (topicPartition, partitionData) =>
      Errors.forCode(partitionData.errorCode) match {
        case Errors.NONE =>
          try {
            val (topicId, fetchOffset, leaderEpoch) = partitionMapLock.synchronized(partitionState(topicPartition))
            val partition = replicaMgr.getPartitionOrException(topicPartition)
            val log = partition.localLogOrException
            val records = FetchResponse.recordsOrFail(partitionData)
            if (records.sizeInBytes() == 0) {
              // no data for this partition, skip
            } else {
              val memoryRecords = records match {
                case r: org.apache.kafka.common.record.MemoryRecords => r
                case r: org.apache.kafka.common.record.FileRecords =>
                  val buffer = java.nio.ByteBuffer.allocate(r.sizeInBytes())
                  r.readInto(buffer, 0)
                  org.apache.kafka.common.record.MemoryRecords.readableRecords(buffer)
                case r: io.aiven.inkless.consume.ConcatenatedRecords =>
                  r.toMemoryRecords()
                case other =>
                  throw new IllegalStateException(
                    s"Unsupported Records type from WAL fetch: ${other.getClass.getName}. Expected MemoryRecords, FileRecords, or ConcatenatedRecords.")
              }
              if (fetchOffset != log.logEndOffset) {
                throw new IllegalStateException(
                  s"Offset mismatch for partition $topicPartition: fetched offset = $fetchOffset, log end offset = ${log.logEndOffset}")
              }
              val logAppendInfoOpt = partition.appendRecordsToFollowerOrFutureReplica(
                memoryRecords, isFuture = false, leaderEpoch)
              val leaderLogStartOffset = partitionData.logStartOffset
              log.maybeUpdateHighWatermark(partitionData.highWatermark).ifPresent { _ =>
                partitionsWithNewHighWatermark += topicPartition
              }
              log.maybeIncrementLogStartOffset(leaderLogStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)
              replicaMgr.brokerTopicStats.updateReplicationBytesIn(memoryRecords.sizeInBytes())
              logAppendInfoOpt.foreach { logAppendInfo =>
                if (logAppendInfo.validBytes > 0) {
                  toUpdate(topicPartition) = logAppendInfo.lastOffset + 1
                }
              }
            }
          } catch {
            case e: KafkaStorageException =>
              error(s"Error while processing data for partition $topicPartition", e)
              failedPartitions.add(topicPartition)
            case t: Throwable =>
              error(s"Unexpected error while processing data for partition $topicPartition", t)
              failedPartitions.add(topicPartition)
          }
        case _ =>
          debug(s"Fetch error for partition $topicPartition: ${Errors.forCode(partitionData.errorCode)}")
      }
    }
    partitionMapLock.synchronized {
      toUpdate.foreach { case (tp, nextOffset) =>
        partitionState.get(tp).foreach { case (topicId, _, leaderEpoch) =>
          partitionState(tp) = (topicId, nextOffset, leaderEpoch)
        }
      }
    }
  }
}
