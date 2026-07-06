/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.inkless.consolidation

import io.aiven.inkless.consume.FetchHandler
import io.aiven.inkless.control_plane.{BatchInfo, BatchMetadata, FindBatchResponse}
import kafka.server.ReplicaManager
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, TimestampType}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

import java.time.Duration
import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.Optional

class DelayedConsolidationFetchTest {
  private val topicId = Uuid.randomUuid()
  private val tip = new TopicIdPartition(topicId, 0, "test-topic")
  private val secondTip = new TopicIdPartition(topicId, 1, "test-topic")

  private def newFetchParams(maxWaitMs: Long, minBytes: Int): FetchParams =
    new FetchParams(
      FetchRequest.FUTURE_LOCAL_REPLICA_ID,
      -1,
      maxWaitMs,
      minBytes,
      1024 * 1024,
      FetchIsolation.LOG_END,
      Optional.empty()
    )

  private def newFetchInfo(offset: Long, maxBytes: Int): util.Map[TopicIdPartition, FetchRequest.PartitionData] = {
    val map = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    map.put(tip, new FetchRequest.PartitionData(topicId, offset, 0L, maxBytes, Optional.of(0)))
    map
  }

  private def newFetchInfos(entries: (TopicIdPartition, Long)*): util.Map[TopicIdPartition, FetchRequest.PartitionData] = {
    val map = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    entries.foreach { case (tp, offset) =>
      map.put(tp, new FetchRequest.PartitionData(tp.topicId, offset, 0L, 1024, Optional.of(0)))
    }
    map
  }

  private def batch(baseOffset: Long, lastOffset: Long, byteSize: Long): BatchInfo =
    batchFor(tip, baseOffset, lastOffset, byteSize)

  private def batchFor(topicIdPartition: TopicIdPartition, baseOffset: Long, lastOffset: Long, byteSize: Long): BatchInfo =
    new BatchInfo(
      baseOffset,
      "object-key",
      new BatchMetadata(
        2.toByte, topicIdPartition,
        baseOffset * 100, byteSize,
        baseOffset, lastOffset,
        0L, 0L,
        TimestampType.CREATE_TIME
      )
    )

  private def emptyFetchPartitionData: FetchPartitionData =
    new FetchPartitionData(
      Errors.NONE,
      0L, 0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      java.util.OptionalLong.empty(),
      Optional.empty(),
      java.util.OptionalInt.empty(),
      false
    )

  @Test
  def tryCompleteCompletesWhenAccumulatedAtLeastMinBytes(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    // control plane returns 2 batches of 500 bytes each, totaling 1000 bytes -- above minBytes
    val batches = util.List.of(batch(0, 4, 500), batch(5, 9, 500))
    val response = util.List.of(FindBatchResponse.success(batches, 0L, 100L))
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(response))
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of(tip, emptyFetchPartitionData)))

    val captured = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 100),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = r => captured.complete(r)
    )

    assertTrue(op.tryComplete(), "tryComplete should succeed when bytes >= minBytes")
    assertTrue(op.isCompleted, "operation must be completed after tryComplete returns true")
    assertNotNull(captured.get(1, TimeUnit.SECONDS))
    verify(fetchHandler, times(1)).handle(any(), any())
  }

  @Test
  def tryCompleteCompletesWhenAccumulatedAcrossPartitionsReachesMinBytes(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    val firstResponse = FindBatchResponse.success(util.List.of(batchFor(tip, 0, 4, 100)), 0L, 100L)
    val secondResponse = FindBatchResponse.success(util.List.of(batchFor(secondTip, 0, 4, 125)), 0L, 100L)
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(util.List.of(firstResponse, secondResponse)))
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of(tip, emptyFetchPartitionData, secondTip, emptyFetchPartitionData)))

    val captured = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 200),
      fetchInfos = newFetchInfos(tip -> 0L, secondTip -> 0L),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = r => captured.complete(r)
    )

    assertTrue(op.tryComplete(), "tryComplete should use aggregate bytes across partitions")
    assertEquals(2, captured.get(1, TimeUnit.SECONDS).size)
    verify(replicaManager, times(1)).findDisklessBatches(any())
  }

  @Test
  def tryCompleteForcesCompletionWhenFindBatchResponseCountDiffers(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    val firstResponse = FindBatchResponse.success(util.List.of(batchFor(tip, 0, 4, 100)), 0L, 100L)
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(util.List.of(firstResponse)))
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of(tip, emptyFetchPartitionData, secondTip, emptyFetchPartitionData)))

    val captured = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 200),
      fetchInfos = newFetchInfos(tip -> 0L, secondTip -> 0L),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = r => captured.complete(r)
    )

    assertTrue(op.tryComplete(), "response-count mismatch should defer to the authoritative fetch")
    assertEquals(2, captured.get(1, TimeUnit.SECONDS).size)
    verify(fetchHandler, times(1)).handle(any(), any())
  }

  @Test
  def tryCompleteParksWhenBelowMinBytes(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    // Empty control-plane response -- accumulated = 0, below minBytes
    val response = util.List.of(FindBatchResponse.success(util.List.of(), 0L, 100L))
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(response))

    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = _ => ()
    )

    assertFalse(op.tryComplete(), "tryComplete should park when bytes < minBytes")
    assertFalse(op.tryComplete(), "duplicate purgatory registration check should not re-probe the control plane")
    assertFalse(op.isCompleted, "operation must remain in purgatory")
    verify(replicaManager, times(1)).findDisklessBatches(any())
    verify(fetchHandler, times(0)).handle(any(), any())
  }

  @Test
  def expirationCompletesParkedFetchViaFinalFetch(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    val response = util.List.of(FindBatchResponse.success(util.List.of(), 0L, 100L))
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(response))
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of(tip, emptyFetchPartitionData)))

    val captured = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = r => captured.complete(r)
    )

    assertFalse(op.tryComplete(), "initial probe should park below minBytes")
    op.run()

    assertEquals(1, captured.get(1, TimeUnit.SECONDS).size)
    verify(replicaManager, times(1)).findDisklessBatches(any())
    verify(fetchHandler, times(1)).handle(any(), any())
  }

  @Test
  def tryCompleteForcesCompletionOnError(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    // control plane returns OFFSET_OUT_OF_RANGE -- should short-circuit the wait
    val response = util.List.of(FindBatchResponse.offsetOutOfRange(0L, 100L))
    when(replicaManager.findDisklessBatches(any())).thenReturn(Some(response))
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of(tip, emptyFetchPartitionData)))

    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = _ => ()
    )

    assertTrue(op.tryComplete(), "tryComplete should force completion on control-plane error")
    assertTrue(op.isCompleted)
  }

  @Test
  def tryCompleteForcesCompletionWhenFindDisklessBatchesThrows(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    when(replicaManager.findDisklessBatches(any())).thenThrow(new RuntimeException("control plane down"))
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of(tip, emptyFetchPartitionData)))

    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = _ => ()
    )

    // Should not propagate the exception -- defer to onComplete
    assertTrue(op.tryComplete())
    assertTrue(op.isCompleted)
  }

  @Test
  def emptyFetchInfosCompletesImmediately(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])
    when(fetchHandler.handle(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, FetchPartitionData]()))

    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData](),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = _ => ()
    )

    assertTrue(op.tryComplete())
  }

  @Test
  def onCompleteSurfacesPerPartitionErrorsOnFailure(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    val failed = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    failed.completeExceptionally(new RuntimeException("storage down"))
    when(fetchHandler.handle(any(), any())).thenReturn(failed)

    val captured = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = r => captured.complete(r)
    )

    op.forceComplete()
    val response = captured.get(1, TimeUnit.SECONDS)
    assertNotNull(response)
    assertFalse(response.isEmpty, "callback should fire with per-partition errors when handle() fails")
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.get(tip).error)
  }

  @Test
  def onCompleteDoesNotBlockOnIncompleteFetchFuture(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val fetchHandler = mock(classOf[FetchHandler])

    val pending = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    when(fetchHandler.handle(any(), any())).thenReturn(pending)

    val captured = new CompletableFuture[util.Map[TopicIdPartition, FetchPartitionData]]()
    val op = new DelayedConsolidationFetch(
      params = newFetchParams(maxWaitMs = 1000, minBytes = 1),
      fetchInfos = newFetchInfo(0L, 1024),
      fetchHandler = fetchHandler,
      replicaManager = replicaManager,
      responseCallback = r => captured.complete(r)
    )

    assertTimeoutPreemptively(Duration.ofSeconds(1), new Executable {
      override def execute(): Unit = assertTrue(op.forceComplete())
    })
    assertFalse(captured.isDone, "callback must wait for the async fetch result")

    pending.complete(util.Map.of(tip, emptyFetchPartitionData))
    assertNotNull(captured.get(1, TimeUnit.SECONDS))
  }
}
