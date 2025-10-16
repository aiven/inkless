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

import io.aiven.inkless.control_plane.{BatchInfo, BatchMetadata, FindBatchRequest, FindBatchResponse}

import java.util.{Collections, Optional, OptionalLong}
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
import org.junit.jupiter.api.{Nested, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.{any, anyFloat, anyInt, anyLong}
import org.mockito.Mockito.{mock, never, verify, when}

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
      classicFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
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
    when(replicaManager.fetchDisklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))

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
      classicFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenThrow(new NotLeaderOrFollowerException(s"Replica for $topicIdPartition not available"))
    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.NOT_LEADER_OR_FOLLOWER)
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    when(replicaManager.fetchParamsWithNewMaxBytes(any(), any())).thenAnswer(_.getArgument(0))
    when(replicaManager.fetchDisklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))

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
      classicFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
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
    when(replicaManager.fetchDisklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))
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
      classicFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
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
    when(replicaManager.fetchDisklessMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(Seq.empty))
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

  @Nested
  class Inkless {

    @Test
    def testCompletionWhenLogIsTruncated(): Unit = {
      // Case A: When fetchOffset > endOffset (high watermark), it means the log has been truncated
      // and the fetch should complete immediately
      val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 500L
      val endOffset = 450L // endOffset < fetchOffset indicates truncation
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](10)
      val minBytes = 100

      val fetchStatus = FetchPartitionStatus(
        startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
        fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val fetchParams = new FetchParams(
        -1, // consumer replica id
        1,
        500L, // maxWaitMs
        minBytes,
        maxBytes,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      @volatile var fetchResultOpt: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None

      def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        fetchResultOpt = Some(responses)
      }

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = Seq.empty,
        disklessFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
        replicaManager = replicaManager,
        quota = replicaQuota,
        responseCallback = callback
      )

      // Create proper BatchMetadata
      val batchMetadata = BatchMetadata.of(
        topicIdPartition,
        0L, // byteOffset
        100L, // byteSize
        endOffset, // baseOffset
        endOffset + 10, // lastOffset
        System.currentTimeMillis(), // logAppendTimestamp
        System.currentTimeMillis(), // batchMaxTimestamp
        TimestampType.CREATE_TIME
      )

      // Mock successful diskless batch finding with truncation scenario
      val mockResponse = mock(classOf[FindBatchResponse])
      when(mockResponse.errors()).thenReturn(Errors.NONE)
      when(mockResponse.batches()).thenReturn(Collections.singletonList(new BatchInfo(
        1L, // batchId
        "test-batch-id", // objectKey
        batchMetadata
      )))
      when(mockResponse.highWatermark()).thenReturn(endOffset) // endOffset < fetchOffset (truncation)

      val future = Some(Collections.singletonList(mockResponse))
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]], anyInt())).thenReturn(future)

      // Mock fetchDisklessMessages for onComplete
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], any[Float])).thenAnswer(_.getArgument(0))
      when(replicaManager.fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]))
        .thenReturn(CompletableFuture.completedFuture(Seq((topicIdPartition, mock(classOf[FetchPartitionData])))))

      when(replicaManager.readFromLog(
        fetchParams,
        readPartitionInfo = Seq.empty,
        quota = replicaQuota,
        readFromPurgatory = true
      )).thenReturn(Seq.empty)

      // Test that tryComplete returns false but force completion (Case A)
      // The truncation case (fetchOffset > endOffset) should cause tryCompleteDiskless to return None,
      // and accumulated bytes won't exceed minBytes
      assertFalse(delayedFetch.tryComplete())
      assertTrue(delayedFetch.isCompleted)
      assertTrue(fetchResultOpt.isDefined)

      // Verify that estimatedByteSize is never called since we hit the truncation case
      verify(mockResponse, never()).estimatedByteSize(anyLong())
    }

    @Test
    def testNoCompletionWhenFetchOffsetEqualsHighWatermark(): Unit = {
      // Case D: When fetchOffset == endOffset (high watermark), no new data is available
      // and accumulated bytes won't exceed minBytes, so completion should not occur
      val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 500L
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](10)
      val minBytes = 100
      val estimatedBatchSize = 150L // This would exceed minBytes if counted, but won't be since fetchOffset == endOffset

      val fetchStatus = FetchPartitionStatus(
        startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
        fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val fetchParams = new FetchParams(
        -1, // not a replica
        1, // not a replica
        500L, // maxWaitMs
        minBytes,
        maxBytes,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      @volatile var fetchResultOpt: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None

      def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        fetchResultOpt = Some(responses)
      }

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = Seq.empty,
        disklessFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
        replicaManager = replicaManager,
        quota = replicaQuota,
        responseCallback = callback
      )

      // Create proper BatchMetadata
      val batchMetadata = BatchMetadata.of(
        topicIdPartition,
        0L, // byteOffset
        100L, // byteSize
        fetchOffset, // baseOffset
        fetchOffset + 10, // lastOffset
        System.currentTimeMillis(), // logAppendTimestamp
        System.currentTimeMillis(), // batchMaxTimestamp
        TimestampType.CREATE_TIME
      )

      // Mock successful diskless batch finding
      val mockResponse = mock(classOf[FindBatchResponse])
      when(mockResponse.errors()).thenReturn(Errors.NONE)
      when(mockResponse.batches()).thenReturn(Collections.singletonList(new BatchInfo(
        1L, // batchId
        "test-batch-id", // objectKey
        batchMetadata
      )))
      when(mockResponse.highWatermark()).thenReturn(fetchOffset) // fetchOffset == endOffset (no new data)
      when(mockResponse.estimatedByteSize(fetchOffset)).thenReturn(estimatedBatchSize)

      val future = Some(Collections.singletonList(mockResponse))
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]], anyInt())).thenReturn(future)

      when(replicaManager.readFromLog(
        fetchParams,
        readPartitionInfo = Seq.empty,
        quota = replicaQuota,
        readFromPurgatory = true
      )).thenReturn(Seq.empty)

      // Test that tryComplete returns false since fetchOffset == endOffset means no new data
      // and accumulated bytes won't exceed minBytes
      assertFalse(delayedFetch.tryComplete())
      assertFalse(delayedFetch.isCompleted)
      assertFalse(fetchResultOpt.isDefined)

      // Verify that estimatedByteSize is never called since fetchOffset == endOffset
      verify(replicaManager, never()).fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]])
      verify(mockResponse, never()).estimatedByteSize(anyLong())
    }

    @Test
    def testNoCompletionWhenAccumulatedBytesUnderThreshold(): Unit = {
      // Case B (partial): When fetchOffset < endOffset (data available) but accumulated bytes < minBytes
      // The fetch should not complete and wait for more data or timeout
      val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 500L
      val endOffset = 600L // endOffset > fetchOffset, so data is available
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](10)
      val minBytes = 200 // Set threshold higher than available bytes
      val estimatedBatchSize = 50L // This will NOT exceed minBytes

      val fetchStatus = FetchPartitionStatus(
        startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
        fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val fetchParams = new FetchParams(
        -1, // consumer replica id
        1,
        500L, // maxWaitMs
        minBytes,
        maxBytes,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      @volatile var fetchResultOpt: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None

      def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        fetchResultOpt = Some(responses)
      }

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = Seq.empty,
        disklessFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
        replicaManager = replicaManager,
        quota = replicaQuota,
        responseCallback = callback
      )

      // Create proper BatchMetadata
      val batchMetadata = BatchMetadata.of(
        topicIdPartition,
        0L, // byteOffset
        100L, // byteSize
        fetchOffset, // baseOffset
        fetchOffset + 10, // lastOffset
        System.currentTimeMillis(), // logAppendTimestamp
        System.currentTimeMillis(), // batchMaxTimestamp
        TimestampType.CREATE_TIME
      )

      // Mock successful diskless batch finding with available data but insufficient bytes
      val mockResponse = mock(classOf[FindBatchResponse])
      when(mockResponse.errors()).thenReturn(Errors.NONE)
      when(mockResponse.batches()).thenReturn(Collections.singletonList(new BatchInfo(
        1L, // batchId
        "test-batch-id", // objectKey
        batchMetadata
      )))
      when(mockResponse.highWatermark()).thenReturn(endOffset) // endOffset > fetchOffset (data available)
      when(mockResponse.estimatedByteSize(fetchOffset)).thenReturn(estimatedBatchSize)

      val future = Some(Collections.singletonList(mockResponse))
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]], anyInt())).thenReturn(future)

      when(replicaManager.readFromLog(
        fetchParams,
        readPartitionInfo = Seq.empty,
        quota = replicaQuota,
        readFromPurgatory = true
      )).thenReturn(Seq.empty)

      // Test that tryComplete returns false since accumulated bytes (50) < minBytes (200)
      assertFalse(delayedFetch.tryComplete())
      assertFalse(delayedFetch.isCompleted)
      assertFalse(fetchResultOpt.isDefined)

      // Verify that estimatedByteSize is called since fetchOffset < endOffset
      verify(mockResponse).estimatedByteSize(fetchOffset)
    }

    @Test
    def testCompletionWhenAccumulatedBytesExceedsMinBytes(): Unit = {
      // Case B: When accumulated bytes from available batches exceeds minBytes, fetch should complete
      val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 500L
      val endOffset = 600L // endOffset > fetchOffset, so data is available
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](10)
      val minBytes = 100
      val estimatedBatchSize = 150L // This will exceed minBytes

      val fetchStatus = FetchPartitionStatus(
        startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
        fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val fetchParams = new FetchParams(
        -1, // consumer replica id
        1,
        500L, // maxWaitMs
        minBytes,
        maxBytes,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      @volatile var fetchResultOpt: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None

      def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        fetchResultOpt = Some(responses)
      }

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = Seq.empty,
        disklessFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
        replicaManager = replicaManager,
        quota = replicaQuota,
        responseCallback = callback
      )

      // Create proper BatchMetadata
      val batchMetadata = BatchMetadata.of(
        topicIdPartition,
        0L, // byteOffset
        100L, // byteSize
        fetchOffset, // baseOffset
        fetchOffset + 10, // lastOffset
        System.currentTimeMillis(), // logAppendTimestamp
        System.currentTimeMillis(), // batchMaxTimestamp
        TimestampType.CREATE_TIME
      )

      // Mock successful diskless batch finding with available data
      val mockResponse = mock(classOf[FindBatchResponse])
      when(mockResponse.errors()).thenReturn(Errors.NONE)
      when(mockResponse.batches()).thenReturn(Collections.singletonList(new BatchInfo(
        1L, // batchId
        "test-batch-id", // objectKey
        batchMetadata
      )))
      when(mockResponse.highWatermark()).thenReturn(endOffset) // endOffset > fetchOffset (data available)
      when(mockResponse.estimatedByteSize(fetchOffset)).thenReturn(estimatedBatchSize)

      val future = Some(Collections.singletonList(mockResponse))
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]], anyInt())).thenReturn(future)

      // Mock fetchDisklessMessages for onComplete
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], anyFloat())).thenAnswer(_.getArgument(0))
      when(replicaManager.fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]))
        .thenReturn(CompletableFuture.completedFuture(Seq((topicIdPartition, mock(classOf[FetchPartitionData])))))

      when(replicaManager.readFromLog(
        fetchParams,
        readPartitionInfo = Seq.empty,
        quota = replicaQuota,
        readFromPurgatory = true
      )).thenReturn(Seq.empty)

      // Test that tryComplete returns true (Case B: accumulated bytes exceeds min bytes)
      assertTrue(delayedFetch.tryComplete())
      assertTrue(delayedFetch.isCompleted)
      assertTrue(fetchResultOpt.isDefined)

      // Verify that estimatedByteSize is called since fetchOffset < endOffset
      verify(mockResponse).estimatedByteSize(fetchOffset)
    }

    @Test
    def testCompletionWhenErrorOccursDuringDisklessBatchFinding(): Unit = {
      // Case C: When an error occurs while trying to find diskless batches, fetch should complete immediately
      val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 500L
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](10)
      val minBytes = 100

      val fetchStatus = FetchPartitionStatus(
        startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
        fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val fetchParams = new FetchParams(
        -1, // consumer replica id
        1,
        500L, // maxWaitMs
        minBytes,
        maxBytes,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      @volatile var fetchResultOpt: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None

      def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        fetchResultOpt = Some(responses)
      }

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = Seq.empty,
        disklessFetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
        replicaManager = replicaManager,
        quota = replicaQuota,
        responseCallback = callback
      )

      // Create proper BatchMetadata
      val batchMetadata = BatchMetadata.of(
        topicIdPartition,
        0L, // byteOffset
        100L, // byteSize
        fetchOffset, // baseOffset
        fetchOffset + 10, // lastOffset
        System.currentTimeMillis(), // logAppendTimestamp
        System.currentTimeMillis(), // batchMaxTimestamp
        TimestampType.CREATE_TIME
      )

      // Mock error response from diskless batch finding
      val mockResponse = mock(classOf[FindBatchResponse])
      when(mockResponse.errors()).thenReturn(Errors.UNKNOWN_SERVER_ERROR) // Non-NONE error triggers Case C
      when(mockResponse.batches()).thenReturn(Collections.singletonList(new BatchInfo(
        1L, // batchId
        "test-batch-id", // objectKey
        batchMetadata
      )))
      when(mockResponse.highWatermark()).thenReturn(600L)

      val future = Some(Collections.singletonList(mockResponse))
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]], anyInt())).thenReturn(future)

      // Mock fetchDisklessMessages for onComplete
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], anyFloat())).thenAnswer(_.getArgument(0))
      when(replicaManager.fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]))
        .thenReturn(CompletableFuture.completedFuture(Seq((topicIdPartition, mock(classOf[FetchPartitionData])))))

      when(replicaManager.readFromLog(
        fetchParams,
        readPartitionInfo = Seq.empty,
        quota = replicaQuota,
        readFromPurgatory = true
      )).thenReturn(Seq.empty)

      // Test that tryComplete returns true (Case C: error forces completion)
      assertFalse(delayedFetch.tryComplete())
      assertTrue(delayedFetch.isCompleted)
      assertTrue(fetchResultOpt.isDefined)

      // Verify that estimatedByteSize is never called since we hit the error case
      verify(mockResponse, never()).estimatedByteSize(anyLong())
      verify(mockResponse, never()).highWatermark()
    }

    @Test
    def testX(): Unit = {

    }
  }
}
