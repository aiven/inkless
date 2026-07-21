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
import kafka.utils.TestUtils

import java.util.{Collections, Optional, OptionalInt, OptionalLong}
import scala.collection.Seq
import kafka.cluster.Partition
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, TimestampType}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, FetchPartitionStatus, LogOffsetMetadata, LogOffsetSnapshot, LogReadResult}
import org.junit.jupiter.api.{BeforeEach, Nested, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.{any, anyFloat, anyInt, anyLong}
import org.mockito.Mockito.{mock, never, times, verify, when}

import java.util.concurrent.CompletableFuture

import java.util

class DelayedFetchTest {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

  @BeforeEach
  def setUp(): Unit = {
    when(replicaManager.buildConsolidationSupplementFetchInfos(any(), any(), any())).thenReturn(Seq.empty)
  }

  @Test
  def testFetchWithFencedEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = new FetchPartitionStatus(
      new LogOffsetMetadata(fetchOffset),
      new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      classicFetchPartitionStatus = createFetchPartitionStatusMap(topicIdPartition, fetchStatus),
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

    val fetchStatus = new FetchPartitionStatus(
      new LogOffsetMetadata(fetchOffset),
      new FetchRequest.PartitionData(topicIdPartition.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      classicFetchPartitionStatus = createFetchPartitionStatusMap(topicIdPartition, fetchStatus),
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

    val fetchStatus = new FetchPartitionStatus(
      new LogOffsetMetadata(fetchOffset),
      new FetchRequest.PartitionData(topicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      classicFetchPartitionStatus = createFetchPartitionStatusMap(topicIdPartition, fetchStatus),
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

    val fetchStatus = new FetchPartitionStatus(
      new LogOffsetMetadata(fetchOffset),
      new FetchRequest.PartitionData(topicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      classicFetchPartitionStatus = createFetchPartitionStatusMap(topicIdPartition, fetchStatus),
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
      error)
  }

  private def createFetchPartitionStatusMap(tpId: TopicIdPartition, status: FetchPartitionStatus): util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus] = {
    val statusMap = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]
    statusMap.put(tpId, status)
    statusMap
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

      val fetchStatus = new FetchPartitionStatus(
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

      val disklessFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      disklessFetchPartitionStatus.put(topicIdPartition, fetchStatus)

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus](),
        disklessFetchPartitionStatus = disklessFetchPartitionStatus,
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
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]])).thenReturn(future)

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

      val fetchStatus = new FetchPartitionStatus(
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

      val disklessFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      disklessFetchPartitionStatus.put(topicIdPartition, fetchStatus)
      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus](),
        disklessFetchPartitionStatus = disklessFetchPartitionStatus,
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
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]])).thenReturn(future)

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

      val fetchStatus = new FetchPartitionStatus(
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

      val disklessFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]
      disklessFetchPartitionStatus.put(topicIdPartition, fetchStatus)
      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus](),
        disklessFetchPartitionStatus = disklessFetchPartitionStatus,
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
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]])).thenReturn(future)

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

      val fetchStatus = new FetchPartitionStatus(
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

      val disklessFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      disklessFetchPartitionStatus.put(topicIdPartition, fetchStatus)

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus](),
        disklessFetchPartitionStatus = disklessFetchPartitionStatus,
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
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]])).thenReturn(future)

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
    def testCompletionWithConsolidationSupplement(): Unit = {
      // Verifies that a consolidating partition's local data is supplemented with diskless data
      // and that a pure-diskless partition's data also arrives in the response.
      val consolidatingTp = new TopicIdPartition(Uuid.randomUuid(), 0, "consolidating-topic")
      val disklessTp = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 100L
      val logEndOffset = 150L
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](1)
      val minBytes = 1024 // high enough that local data alone won't satisfy it

      // Classic (consolidating) partition status
      val classicFetchStatus = new FetchPartitionStatus(
        new LogOffsetMetadata(fetchOffset),
        new FetchRequest.PartitionData(consolidatingTp.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val classicStatusMap = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      classicStatusMap.put(consolidatingTp, classicFetchStatus)

      // Diskless partition status
      val disklessFetchStatus = new FetchPartitionStatus(
        new LogOffsetMetadata(fetchOffset),
        new FetchRequest.PartitionData(disklessTp.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
      )
      val disklessStatusMap = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      disklessStatusMap.put(disklessTp, disklessFetchStatus)

      val fetchParams = new FetchParams(
        -1, // consumer
        1,
        500L,
        minBytes,
        maxBytes,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      @volatile var callbackResult: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None
      def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        callbackResult = Some(responses)
      }

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = classicStatusMap,
        disklessFetchPartitionStatus = disklessStatusMap,
        replicaManager = replicaManager,
        quota = replicaQuota,
        consolidatingSupplements = Map(consolidatingTp -> logEndOffset),
        responseCallback = callback
      )

      // Stub readFromLog to return a small local read (below minBytes)
      val localReadResult = new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(fetchOffset), MemoryRecords.EMPTY),
        Optional.empty(), -1L, -1L, -1L, -1L, -1L, OptionalLong.empty(), Errors.NONE)
      when(replicaManager.readFromLog(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]], any[ReplicaQuota], any[Boolean]))
        .thenReturn(Seq((consolidatingTp, localReadResult)))

      // Stub buildConsolidationSupplementFetchInfos to return non-empty supplement info
      val supplementFetchInfo = (consolidatingTp, new FetchRequest.PartitionData(
        consolidatingTp.topicId(), logEndOffset, logStartOffset, maxBytes, currentLeaderEpoch))
      when(replicaManager.buildConsolidationSupplementFetchInfos(any(), any(), any()))
        .thenReturn(Seq(supplementFetchInfo))

      // Prepare supplement and diskless FetchPartitionData
      val supplementRecords = mock(classOf[MemoryRecords])
      when(supplementRecords.sizeInBytes).thenReturn(512)
      val supplementData = new FetchPartitionData(Errors.NONE, logEndOffset + 100, logEndOffset,
        supplementRecords, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)

      val disklessRecords = mock(classOf[MemoryRecords])
      when(disklessRecords.sizeInBytes).thenReturn(256)
      val disklessData = new FetchPartitionData(Errors.NONE, 200L, 0L,
        disklessRecords, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)

      // Stub fetchDisklessMessages: first call returns supplement, second call returns diskless
      when(replicaManager.fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]))
        .thenReturn(CompletableFuture.completedFuture(Seq((consolidatingTp, supplementData))))
        .thenReturn(CompletableFuture.completedFuture(Seq((disklessTp, disklessData))))

      // Stub fetchParamsWithNewMaxBytes
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], anyFloat())).thenAnswer(_.getArgument(0))

      // Stub mergeConsolidationSupplement to return a merged result
      val mergedRecords = mock(classOf[MemoryRecords])
      when(mergedRecords.sizeInBytes).thenReturn(768)
      val mergedData = new FetchPartitionData(Errors.NONE, logEndOffset + 100, logEndOffset,
        mergedRecords, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
      when(replicaManager.mergeConsolidationSupplement(any[TopicIdPartition], any[FetchPartitionData], any[FetchPartitionData]))
        .thenReturn(mergedData)

      when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)

      // Trigger onComplete
      delayedFetch.forceComplete()

      TestUtils.waitUntilTrue(() => callbackResult.isDefined, "responseCallback should have been called")
      val results = callbackResult.get
      assertEquals(2, results.size)

      val resultMap = results.toMap
      assertEquals(mergedData, resultMap(consolidatingTp))
      assertEquals(disklessData, resultMap(disklessTp))

      // Verify mergeConsolidationSupplement was called exactly once
      verify(replicaManager, times(1)).mergeConsolidationSupplement(
        any[TopicIdPartition], any[FetchPartitionData], any[FetchPartitionData])
    }

    // When the local read for a consolidating partition returns an error, the supplement fetch
    // must not be issued — buildConsolidationSupplementFetchInfos must not receive that partition.
    @Test
    def testConsolidationSupplementNotIssuedWhenLocalReadHasError(): Unit = {
      val consolidatingTp = new TopicIdPartition(Uuid.randomUuid(), 0, "consolidating")
      val fetchOffset = 50L
      val logStartOffset = 0L
      val logEndOffset = 100L
      val currentLeaderEpoch = Optional.of[Integer](1)

      val classicFetchStatus = new FetchPartitionStatus(
        new LogOffsetMetadata(fetchOffset),
        new FetchRequest.PartitionData(consolidatingTp.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
      val classicStatusMap = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      classicStatusMap.put(consolidatingTp, classicFetchStatus)

      val fetchParams = new FetchParams(-1, 1, 500L, 1, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty())

      @volatile var callbackResult: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None
      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = classicStatusMap,
        replicaManager = replicaManager,
        quota = replicaQuota,
        consolidatingSupplements = Map(consolidatingTp -> logEndOffset),
        responseCallback = responses => callbackResult = Some(responses)
      )

      // Local read returns an error — supplement must not be issued for this partition
      val errorReadResult = new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(fetchOffset), MemoryRecords.EMPTY),
        Optional.empty(), -1L, -1L, -1L, -1L, -1L, OptionalLong.empty(), Errors.NOT_LEADER_OR_FOLLOWER)
      when(replicaManager.readFromLog(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]], any[ReplicaQuota], any[Boolean]))
        .thenReturn(Seq((consolidatingTp, errorReadResult)))
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], anyFloat())).thenAnswer(_.getArgument(0))
      when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)

      delayedFetch.forceComplete()

      TestUtils.waitUntilTrue(() => callbackResult.isDefined, "responseCallback should have been called")
      // The erroring partition must be filtered out — supplement fetch and merge must not fire.
      verify(replicaManager, never()).mergeConsolidationSupplement(any(), any(), any())
    }

    // When the local read for a consolidating partition returns an error, the supplement must not
    // be applied — mergeConsolidationSupplement must never be called for an erroring local result.
    @Test
    def testConsolidationSupplementSkippedWhenLocalReadHasError(): Unit = {
      val consolidatingTp = new TopicIdPartition(Uuid.randomUuid(), 0, "consolidating")
      val fetchOffset = 50L
      val logStartOffset = 0L
      val logEndOffset = 100L
      val currentLeaderEpoch = Optional.of[Integer](1)

      val classicFetchStatus = new FetchPartitionStatus(
        new LogOffsetMetadata(fetchOffset),
        new FetchRequest.PartitionData(consolidatingTp.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
      val classicStatusMap = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      classicStatusMap.put(consolidatingTp, classicFetchStatus)

      val fetchParams = new FetchParams(-1, 1, 500L, 1, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty())

      @volatile var callbackResult: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None
      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = classicStatusMap,
        replicaManager = replicaManager,
        quota = replicaQuota,
        consolidatingSupplements = Map(consolidatingTp -> logEndOffset),
        responseCallback = responses => callbackResult = Some(responses)
      )

      // Local read returns an error
      val errorReadResult = new LogReadResult(
        new FetchDataInfo(new LogOffsetMetadata(fetchOffset), MemoryRecords.EMPTY),
        Optional.empty(), -1L, -1L, -1L, -1L, -1L, OptionalLong.empty(), Errors.NOT_LEADER_OR_FOLLOWER)
      when(replicaManager.readFromLog(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]], any[ReplicaQuota], any[Boolean]))
        .thenReturn(Seq((consolidatingTp, errorReadResult)))

      val supplementFetchInfo = (consolidatingTp, new FetchRequest.PartitionData(
        consolidatingTp.topicId(), logEndOffset, logStartOffset, maxBytes, currentLeaderEpoch))
      when(replicaManager.buildConsolidationSupplementFetchInfos(any(), any(), any()))
        .thenReturn(Seq(supplementFetchInfo))

      val supplementRecords = mock(classOf[MemoryRecords])
      when(supplementRecords.sizeInBytes).thenReturn(512)
      val supplementData = new FetchPartitionData(Errors.NONE, logEndOffset + 100, logEndOffset,
        supplementRecords, Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
      when(replicaManager.fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]))
        .thenReturn(CompletableFuture.completedFuture(Seq((consolidatingTp, supplementData))))
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], anyFloat())).thenAnswer(_.getArgument(0))
      when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)

      delayedFetch.forceComplete()

      TestUtils.waitUntilTrue(() => callbackResult.isDefined, "responseCallback should have been called")
      val results = callbackResult.get.toMap
      assertEquals(1, results.size)
      // Error from the local read must be preserved — supplement must not overwrite it
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, results(consolidatingTp).error)
      verify(replicaManager, never()).mergeConsolidationSupplement(any(), any(), any())
    }

    @Test
    def testCompletionWhenErrorOccursDuringDisklessBatchFinding(): Unit = {
      // Case C: When an error occurs while trying to find diskless batches, fetch should complete immediately
      val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 500L
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](10)
      val minBytes = 100

      val fetchStatus = new FetchPartitionStatus(
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

      val disklessFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      disklessFetchPartitionStatus.put(topicIdPartition, fetchStatus)

      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus](),
        disklessFetchPartitionStatus = disklessFetchPartitionStatus,
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
      when(replicaManager.findDisklessBatches(any[Seq[FindBatchRequest]])).thenReturn(future)

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

    // When the diskless fetch future fails, the response must include per-partition error entries
    // rather than silently dropping those partitions from the response.
    @Test
    def testDisklessFetchFailureReturnsPerPartitionErrors(): Unit = {
      val disklessTp = new TopicIdPartition(Uuid.randomUuid(), 0, "diskless-topic")
      val fetchOffset = 200L
      val logStartOffset = 0L
      val currentLeaderEpoch = Optional.of[Integer](1)
      val minBytes = 100

      val disklessFetchStatus = new FetchPartitionStatus(
        new LogOffsetMetadata(fetchOffset),
        new FetchRequest.PartitionData(disklessTp.topicId(), fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
      val disklessStatusMap = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]()
      disklessStatusMap.put(disklessTp, disklessFetchStatus)

      val fetchParams = new FetchParams(-1, 1, 500L, minBytes, maxBytes, FetchIsolation.HIGH_WATERMARK, Optional.empty())

      @volatile var callbackResult: Option[Seq[(TopicIdPartition, FetchPartitionData)]] = None
      val delayedFetch = new DelayedFetch(
        params = fetchParams,
        classicFetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus](),
        disklessFetchPartitionStatus = disklessStatusMap,
        replicaManager = replicaManager,
        quota = replicaQuota,
        responseCallback = responses => callbackResult = Some(responses)
      )

      when(replicaManager.readFromLog(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]], any[ReplicaQuota], any[Boolean]))
        .thenReturn(Seq.empty)
      when(replicaManager.fetchParamsWithNewMaxBytes(any[FetchParams], anyFloat())).thenAnswer(_.getArgument(0))
      when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)

      // Diskless fetch fails with an exception
      val failedFuture = new CompletableFuture[Seq[(TopicIdPartition, FetchPartitionData)]]()
      failedFuture.completeExceptionally(new RuntimeException("Object storage unavailable"))
      when(replicaManager.fetchDisklessMessages(any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]))
        .thenReturn(failedFuture)

      delayedFetch.forceComplete()

      TestUtils.waitUntilTrue(() => callbackResult.isDefined, "responseCallback should have been called")
      val results = callbackResult.get.toMap
      assertEquals(1, results.size, "Response must include the diskless partition even on failure")
      val partitionData = results(disklessTp)
      assertNotEquals(Errors.NONE, partitionData.error, "Error must be set for the failed partition")
      assertEquals(MemoryRecords.EMPTY, partitionData.records)
    }
  }
}
