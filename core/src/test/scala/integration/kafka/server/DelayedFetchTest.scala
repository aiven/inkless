/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import io.aiven.inkless.control_plane.{BatchInfo, BatchMetadata, FindBatchResponse}

import java.util.{Optional, OptionalLong}
import scala.collection.Seq
import kafka.cluster.Partition
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, TimestampType}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.LogReadResult
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogOffsetMetadata, LogOffsetSnapshot}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, when}

import java.util.concurrent.CompletableFuture

class DelayedFetchTest {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

  @Test
  def testFetchWithFencedEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
        .thenReturn(partition)
    when(partition.fetchOffsetSnapshot(
        currentLeaderEpoch,
        fetchOnlyFromLeader = true))
        .thenThrow(new FencedLeaderEpochException("Requested epoch has been fenced"))
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    when(replicaManager.fetchParamsWithNewMaxBytes(any(), any())).thenAnswer(_.getArgument(0))
    when(replicaManager.fetchInklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))

    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.FENCED_LEADER_EPOCH)

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.error)
  }

  @Test
  def testNotLeaderOrFollower(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenThrow(new NotLeaderOrFollowerException(s"Replica for $topicIdPartition not available"))
    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.NOT_LEADER_OR_FOLLOWER)
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    when(replicaManager.fetchParamsWithNewMaxBytes(any(), any())).thenAnswer(_.getArgument(0))
    when(replicaManager.fetchInklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.error)
  }

  @Test
  def testDivergingEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val lastFetchedEpoch = Optional.of[Integer](9)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])
    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition)).thenReturn(partition)
    val endOffsetMetadata = new LogOffsetMetadata(500L, 0L, 500)
    when(partition.fetchOffsetSnapshot(
      currentLeaderEpoch,
      fetchOnlyFromLeader = true))
      .thenReturn(new LogOffsetSnapshot(0L, endOffsetMetadata, endOffsetMetadata, endOffsetMetadata))
    when(partition.lastOffsetForLeaderEpoch(currentLeaderEpoch, lastFetchedEpoch.get, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(topicIdPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(lastFetchedEpoch.get)
        .setEndOffset(fetchOffset - 1))
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    when(replicaManager.fetchParamsWithNewMaxBytes(any(), any())).thenAnswer(_.getArgument(0))
    when(replicaManager.fetchInklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))
    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.NONE)

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.NONE, fetchResult.error)
  }

  @ParameterizedTest(name = "testDelayedFetchWithMessageOnlyHighWatermark endOffset={0}")
  @ValueSource(longs = Array(0, 500))
  def testDelayedFetchWithMessageOnlyHighWatermark(endOffset: Long): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 450L
    val logStartOffset = 5L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])
    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition)).thenReturn(partition)
    // Note that the high-watermark does not contain the complete metadata
    val endOffsetMetadata = new LogOffsetMetadata(endOffset, -1L, -1)
    when(partition.fetchOffsetSnapshot(
      currentLeaderEpoch,
      fetchOnlyFromLeader = true))
      .thenReturn(new LogOffsetSnapshot(0L, endOffsetMetadata, endOffsetMetadata, endOffsetMetadata))
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    when(replicaManager.fetchParamsWithNewMaxBytes(any(), any())).thenAnswer(_.getArgument(0))
    when(replicaManager.fetchInklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))
    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.NONE)

    // 1. When `endOffset` is 0, it refers to the truncation case
    // 2. When `endOffset` is 500, we won't complete because it doesn't contain offset metadata
    val expected = endOffset == 0
    assertEquals(expected, delayedFetch.tryComplete())
    assertEquals(expected, delayedFetch.isCompleted)
    assertEquals(expected, fetchResultOpt.isDefined)
    if (fetchResultOpt.isDefined) {
      assertEquals(Errors.NONE, fetchResultOpt.get.error)
    }
  }

  @Test
  def testWithInkless(): Unit = {
    val classicTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val inklessTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "inkless-topic")
    val classicBytesAvailable = 100
    val inklessBytesAvailable = 100
    val minBytes  = classicBytesAvailable + inklessBytesAvailable
    val fetchOffset = 0L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val lastFetchedEpoch = Optional.empty[Integer]()
    val replicaId = 1
    val inklessBatches = java.util.List.of(
      new BatchInfo(0L, "object", BatchMetadata.of(inklessTopicIdPartition, 0L, inklessBytesAvailable, 0L, 123L, 1L, 1L, TimestampType.CREATE_TIME))
    )

    val classicFetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset, 0L, 0),
      fetchInfo = new FetchRequest.PartitionData(classicTopicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch))
    val inklessFetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(inklessTopicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500, minBytes = minBytes)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(classicTopicIdPartition -> classicFetchStatus, inklessTopicIdPartition -> inklessFetchStatus),
      replicaManager = replicaManager,
      isInklessTopic = t => t.equals(inklessTopicIdPartition.topic()),
      quota = replicaQuota,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])
    when(replicaManager.getPartitionOrException(classicTopicIdPartition.topicPartition)).thenReturn(partition)
    val endOffsetMetadata = new LogOffsetMetadata(500L, 0L, classicBytesAvailable)
    when(partition.fetchOffsetSnapshot(
      currentLeaderEpoch,
      fetchOnlyFromLeader = true))
      .thenReturn(new LogOffsetSnapshot(0L, endOffsetMetadata, endOffsetMetadata, endOffsetMetadata))
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    when(replicaManager.fetchParamsWithNewMaxBytes(any(), any())).thenAnswer(_.getArgument(0))
    when(replicaManager.findInklessBatches(any(), any())).thenReturn(Some(java.util.List.of(
      new FindBatchResponse(Errors.NONE, inklessBatches, 0L, 100L)
    )))
    when(replicaManager.fetchInklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))
    expectReadFromReplica(fetchParams, classicTopicIdPartition, classicFetchStatus.fetchInfo, Errors.NONE)

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.NONE, fetchResult.error)
  }

  private def buildFollowerFetchParams(
    replicaId: Int,
    maxWaitMs: Int,
    minBytes: Int = 1,
  ): FetchParams = {
    new FetchParams(
      replicaId,
      1,
      maxWaitMs,
      minBytes,
      maxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )
  }

  private def expectReadFromReplica(
    fetchParams: FetchParams,
    topicIdPartition: TopicIdPartition,
    fetchPartitionData: FetchRequest.PartitionData,
    error: Errors
  ): Unit = {
    when(replicaManager.readFromLog(
      fetchParams,
      readPartitionInfo = Seq((topicIdPartition, fetchPartitionData)),
      quota = replicaQuota,
      readFromPurgatory = true
    )).thenReturn(Seq((topicIdPartition, buildReadResult(error))))
  }

  private def buildReadResult(error: Errors): LogReadResult = {
    new LogReadResult(
      new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      Optional.empty(),
      -1L,
      -1L,
      -1L,
      -1L,
      -1L,
      OptionalLong.empty(),
      if (error != Errors.NONE) Optional.of[Throwable](error.exception) else Optional.empty[Throwable]())
  }

}
