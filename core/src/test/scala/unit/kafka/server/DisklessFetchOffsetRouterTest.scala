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

import io.aiven.inkless.consume.FetchOffsetHandler
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, RecordBatch}
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, DelayedRemoteListOffsets, ListOffsetsPartitionStatus}
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito._

import java.util.Optional
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}
import scala.collection.mutable

class DisklessFetchOffsetRouterTest {

  private val tp = new TopicPartition("diskless-topic", 0)
  private val consumerReplicaId = ListOffsetsRequest.CONSUMER_REPLICA_ID
  private val followerReplicaId = 1

  private val inklessMetadataView: InklessMetadataView = mock(classOf[InklessMetadataView])
  private val purgatory: DelayedOperationPurgatory[DelayedRemoteListOffsets] =
    mock(classOf[DelayedOperationPurgatory[DelayedRemoteListOffsets]])

  // Diskless task future we hand back from job.add(...). Stays incomplete by default;
  // tests that need a result complete this future before invoking route().
  private val disklessTaskFuture: CompletableFuture[FileRecordsOrError] = new CompletableFuture[FileRecordsOrError]()

  private val job: FetchOffsetHandler.Job = {
    val m = mock(classOf[FetchOffsetHandler.Job])
    when(m.add(any(), any())).thenAnswer(_ => disklessTaskFuture)
    when(m.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    m
  }

  // Records the (topicPartition, partition, allowFromFollower) tuple the router passes to the
  // classic path on each call, so tests can assert on call count and call shape.
  private val classicCalls = mutable.ListBuffer.empty[(TopicPartition, ListOffsetsPartition, Boolean)]

  // Default classic-side answer for tests that don't care about the response shape.
  private val defaultClassicResult: ListOffsetsPartitionStatus = resolvedStatus(makeResponse(offset = 0L, timestamp = 0L))
  
  private def newRouter(
    disklessManagedReplicasEnabled: Boolean = true,
    disklessConsolidationEnabled: Boolean = false
  ): DisklessFetchOffsetRouter =
    new DisklessFetchOffsetRouter(inklessMetadataView, disklessManagedReplicasEnabled, disklessConsolidationEnabled, purgatory)

  private def makePartition(timestamp: Long, partitionIndex: Int): ListOffsetsPartition =
    new ListOffsetsPartition().setPartitionIndex(partitionIndex).setTimestamp(timestamp)

  private def makeResponse(offset: Long,
                           timestamp: Long = RecordBatch.NO_TIMESTAMP,
                           errorCode: Short = Errors.NONE.code): ListOffsetsPartitionResponse =
    new ListOffsetsPartitionResponse()
      .setPartitionIndex(tp.partition)
      .setOffset(offset)
      .setTimestamp(timestamp)
      .setErrorCode(errorCode)

  // Status with the answer already resolved.
  private def resolvedStatus(response: ListOffsetsPartitionResponse): ListOffsetsPartitionStatus =
    ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(response)).build()

  // Invoke `route()` with the standard test wiring.
  private def route(router: DisklessFetchOffsetRouter,
                    timestamp: Long,
                    topicPartition: TopicPartition = tp,
                    replicaId: Int = consumerReplicaId,
                    version: Short = 7,
                    classicLogStartOffset: Option[Long] = None,
                    hasCompleteClassicPrefix: Boolean = true,
                    classicResult: ListOffsetsPartitionStatus = defaultClassicResult,
                    newJob: () => FetchOffsetHandler.Job = () =>
                      throw new AssertionError("newJob() should not be called by this routing path")): ListOffsetsPartitionStatus = {
    router.route(
      job = job,
      newJob = newJob,
      topicPartition = topicPartition,
      partition = makePartition(timestamp, topicPartition.partition),
      replicaId = replicaId,
      version = version,
      classicLogStartOffsetProvider = _ => classicLogStartOffset,
      hasCompleteClassicPrefix = (_, _) => hasCompleteClassicPrefix,
      classicFetchOffset = (tpArg, partition, allow) => {
        classicCalls += ((tpArg, partition, allow))
        classicResult
      }
    )
  }

  private def assertClassicCalledWith(allowFromFollower: Boolean): Unit = {
    assertEquals(1, classicCalls.size, "classic path should have been invoked exactly once")
    val (calledTp, _, allow) = classicCalls.head
    assertEquals(tp, calledTp)
    assertEquals(allowFromFollower, allow, "unexpected allowFromFollower")
  }

  // ---------------------------------------------------------------------------
  // Case 1: pure diskless partition.
  // ---------------------------------------------------------------------------

  @Test
  def routesToDisklessWhenNotSwitched(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.LATEST_TIMESTAMP)

    assertTrue(classicCalls.isEmpty, "classic path should not have been invoked")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "pure-diskless routing returns an async holder")
    assertFalse(status.responseOpt.isPresent)
  }

  @Test
  def routesToDisklessWhenManagedReplicasDisabledEvenWithCommittedBoundary(): Unit = {
    // classicToDisklessStartOffset > 0 but managed replicas are disabled: we treat the partition
    // as pure diskless (case 1) and never consult the classic local log.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(disklessManagedReplicasEnabled = false), timestamp = 123L)

    assertTrue(classicCalls.isEmpty, "classic path should not have been invoked")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent)
  }

  // ---------------------------------------------------------------------------
  // Case 3: partition is being switched from classic to diskless.
  // ---------------------------------------------------------------------------

  @Test
  def routesToClassicWithoutFollowerAccessWhenSwitchPending(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.LATEST_TIMESTAMP)

    assertSame(defaultClassicResult, status, "switch-pending must return the classic status as-is")
    assertClassicCalledWith(allowFromFollower = false)
    verify(job, never()).add(any(), any())
  }

  // ---------------------------------------------------------------------------
  // Case 4: follower request on a switched partition.
  // ---------------------------------------------------------------------------

  @Test
  def routesToClassicWithFollowerAccessWhenSwitchedAndFollowerRequest(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.LATEST_TIMESTAMP, replicaId = followerReplicaId)

    assertSame(defaultClassicResult, status)
    // followers may serve from their local log on a sealed (switched) partition.
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def routesToClassicWithoutFollowerAccessWhenSwitchedFollowerClassicPrefixIsIncomplete(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(
      newRouter(),
      timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
      replicaId = followerReplicaId,
      hasCompleteClassicPrefix = false)

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = false)
    verify(job, never()).add(any(), any())
  }

  // ---------------------------------------------------------------------------
  // Case 2: hybrid routing by timestamp.
  // ---------------------------------------------------------------------------

  @Test
  def hybridEarliestLocalAlwaysGoesToClassic(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridLatestTieredAlwaysGoesToClassic(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.LATEST_TIERED_TIMESTAMP)

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridEarliestUsesClassicWhenClassicStillHasData(): Unit = {
    // logStartOffset (0) < classicToDisklessStartOffset (100), so classic side still owns the
    // earliest offset.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP, classicLogStartOffset = Some(0L))

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridEarliestFallsThroughToDisklessWhenClassicLogIsEmpty(): Unit = {
    // logStartOffset (100) >= classicToDisklessStartOffset (100): no classic data left to scan.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP, classicLogStartOffset = Some(100L))

    assertTrue(classicCalls.isEmpty, "classic path should not have been invoked")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent)
  }

  @Test
  def hybridSpecificTimestampReturnsClassicResultOnSyncMatch(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    val matched = resolvedStatus(makeResponse(offset = 42L, timestamp = 123L))

    val status = route(newRouter(), timestamp = 123L, classicResult = matched)

    assertSame(matched, status, "classic must be returned verbatim on a sync match")
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridSpecificTimestampFallsBackToDisklessOnSyncNoMatch(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    // errorCode == NONE && offset < 0  =>  classic "no match" sentinel.
    val noMatch = resolvedStatus(makeResponse(offset = -1L, errorCode = Errors.NONE.code))

    val status = route(newRouter(), timestamp = 123L, classicResult = noMatch)

    assertClassicCalledWith(allowFromFollower = true)
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "fallback to diskless surfaces an async holder")
    assertNotSame(noMatch, status)
  }

  @Test
  def hybridLatestPrefersDisklessAndDoesNotConsultClassicWhenDisklessHits(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    // The diskless task must already be done so withFallback's thenCompose can resolve while we
    // assert the result.
    val disklessHit = new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(456L, 200L, Optional.of[Integer](1))))
    disklessTaskFuture.complete(disklessHit)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.LATEST_TIMESTAMP)

    assertTrue(classicCalls.isEmpty, "classic path should not have been invoked")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent)
    val result = status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
    assertSame(disklessHit, result)
  }

  @Test
  def hybridLatestFallsBackToClassicWhenDisklessIsEmpty(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    // Empty FileRecordsOrError: neither exception nor offset => withFallback should fire its
    // fallback factory and consult the classic path.
    disklessTaskFuture.complete(new FileRecordsOrError(Optional.empty(), Optional.empty()))
    val classicHit = resolvedStatus(makeResponse(offset = 7L, timestamp = 999L))

    val status = route(newRouter(), timestamp = ListOffsetsRequest.LATEST_TIMESTAMP, classicResult = classicHit)

    verify(job).add(eqTo(tp), any())
    assertClassicCalledWith(allowFromFollower = true)
    assertTrue(status.futureHolderOpt.isPresent)
    val result = status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
    assertFalse(result.hasException)
    assertTrue(result.hasTimestampAndOffset)
    assertEquals(7L, result.timestampAndOffset.get.offset)
    assertEquals(999L, result.timestampAndOffset.get.timestamp)
  }

  @Test
  def batchesMultipleDisklessPartitionsIntoTheSameJob(): Unit = {
    val tp1 = new TopicPartition("diskless-topic", 0)
    val tp2 = new TopicPartition("diskless-topic", 1)
    val tp3 = new TopicPartition("diskless-topic", 2)
    Seq(tp1, tp2, tp3).foreach { p =>
      when(inklessMetadataView.getClassicToDisklessStartOffset(p)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    }

    val router = newRouter()
    val statuses = Seq(tp1, tp2, tp3).map { p =>
      route(router, timestamp = ListOffsetsRequest.LATEST_TIMESTAMP, topicPartition = p)
    }

    // Each routed partition must show up on the shared job exactly once, and there must be no
    // other adds (so no stray duplicates or extra calls) and no start (the caller is responsible
    // for invoking start()).
    verify(job, times(3)).add(any(), any())
    Seq(tp1, tp2, tp3).foreach { p => verify(job).add(eqTo(p), any()) }
    verify(job, never()).start()

    assertTrue(statuses.forall(_.futureHolderOpt.isPresent))
    assertTrue(classicCalls.isEmpty, "classic path should not have been invoked")
  }

  @Test
  def mixesBatchedAndFreshJobsInTheSameRequest(): Unit = {
    // batchedTp: case 1 (pure diskless). fallbackTps: case 2 (t >= 0) with async-incomplete classic.
    val batchedTp = new TopicPartition("diskless-topic", 0)
    val fallbackTps = Seq(new TopicPartition("diskless-topic", 1), new TopicPartition("diskless-topic", 2))
    when(inklessMetadataView.getClassicToDisklessStartOffset(batchedTp))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    fallbackTps.foreach(p => when(inklessMetadataView.getClassicToDisklessStartOffset(p)).thenReturn(100L))

    val emptyResult = new FileRecordsOrError(Optional.empty(), Optional.empty())

    val freshJobs = mutable.ListBuffer.empty[FetchOffsetHandler.Job]
    def newFreshJob(): FetchOffsetHandler.Job = {
      val m = mock(classOf[FetchOffsetHandler.Job])
      when(m.add(any(), any())).thenReturn(CompletableFuture.completedFuture(emptyResult))
      when(m.cancelHandler()).thenReturn(new CompletableFuture[Void]())
      freshJobs += m
      m
    }

    // Wraps a still-pending classic future as a `futureHolderOpt`-shaped status.
    def asyncClassicStatus(f: CompletableFuture[FileRecordsOrError]): ListOffsetsPartitionStatus =
      ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(
        new AsyncOffsetReadFutureHolder[FileRecordsOrError](new CompletableFuture[Void](), f))).build()

    val router = newRouter()

    // batchedTp routes through case 1 → adds directly to the batched `job`.
    route(router, timestamp = ListOffsetsRequest.LATEST_TIMESTAMP, topicPartition = batchedTp)

    // Each fallbackTp routes through case 2 (t >= 0) with an incomplete classic future, priming
    // a fresh-job fallback factory. The fallback only fires when the classic future completes.
    val classicFutures = fallbackTps.map(_ => new CompletableFuture[FileRecordsOrError]())
    fallbackTps.zip(classicFutures).foreach { case (p, f) =>
      route(router, timestamp = 123L, topicPartition = p,
        classicResult = asyncClassicStatus(f), newJob = () => newFreshJob())
    }

    // Trigger each fallback (empty FileRecordsOrError = primary missed → run fallback).
    classicFutures.foreach(_.complete(emptyResult))

    // The batched `job` got exactly one add (for batchedTp) and was not started by the router.
    verify(job).add(eqTo(batchedTp), any())
    verify(job, times(1)).add(any(), any())
    verify(job, never()).start()

    // Each fallback created its own distinct fresh job, started by the router, and carrying
    // its own partition. No spillover between fresh jobs or onto the batched `job`.
    assertEquals(fallbackTps.size, freshJobs.size, "each async-classic fallback must create its own fresh job")
    assertNotSame(freshJobs(0), freshJobs(1), "fallbacks must not share a fresh job")
    fallbackTps.zip(freshJobs).foreach { case (p, fresh) =>
      verify(fresh).add(eqTo(p), any())
      verify(fresh).start()
    }
  }

  @Test
  def routesToDisklessAndForwardsControlPlaneFailureWhenSwitchPendingAndManagedReplicasDisabled(): Unit = {
    // Switch to diskless is pending while managed replicas are disabled. Switch can only have been 
    // initiated with managed replicas enabled, so this is a misconfiguration.
    // The control plane is not expected to have a committed diskless start offset for the partition yet, so we
    // simulate the realistic outcome by completing the diskless task future exceptionally and
    // assert the router forwards that failure verbatim.
    
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    val controlPlaneFailure = new RuntimeException("partition not found in the diskless control plane")
    disklessTaskFuture.completeExceptionally(controlPlaneFailure)

    val status = route(newRouter(disklessManagedReplicasEnabled = false), timestamp = ListOffsetsRequest.LATEST_TIMESTAMP)

    assertTrue(classicCalls.isEmpty, "classic path must not be invoked")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "must surface an async holder backed by the diskless control plane")
    assertFalse(status.responseOpt.isPresent)
    val ex = assertThrows(classOf[ExecutionException],
      () => status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS))
    assertSame(controlPlaneFailure, ex.getCause)
  }

  @Test
  def routesToDisklessWhenSwitchPendingButConsolidatingTopic(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.LATEST_TIMESTAMP
    )

    assertTrue(classicCalls.isEmpty, "consolidating topics must not use switch-pending classic-only ListOffsets")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent)
    assertFalse(status.responseOpt.isPresent)
  }

  @Test
  def hybridConsolidatingWithoutCommittedBoundaryAllowsFollowerOnEarliestLocal(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP
    )

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridConsolidatingWithoutCommittedBoundaryAllowsFollowerOnLatestTiered(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.LATEST_TIERED_TIMESTAMP
    )

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridConsolidatingEarliestAlwaysTriesClassicFirstWithDisklessFallback(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(100L)
    )

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridConsolidatingEarliestFallsBackToDisklessWhenClassicReturnsNoMatchEvenIfLogPastBoundary(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)
    val noMatch = resolvedStatus(makeResponse(offset = -1L, errorCode = Errors.NONE.code))

    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(100L),
      classicResult = noMatch
    )

    assertClassicCalledWith(allowFromFollower = true)
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "fallback to diskless surfaces an async holder")
    assertNotSame(noMatch, status)
  }

  @Test
  def hybridConsolidatingWithCommittedBoundaryRequiresCompleteClassicPrefixForFollowerAccess(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(0L),
      hasCompleteClassicPrefix = false
    )

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = false)
    verify(job, never()).add(any(), any())
  }

  @Test
  def hybridConsolidatingEarliestReturnsClassicWhenClassicHitsEvenIfLogPastBoundary(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(100L)
    )

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }
  
  @Test
  def consolidatingFollowerOnLatestTimestampRoutesThroughDisklessFirst(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
      replicaId = followerReplicaId
    )

    // Should go through hybrid Case 2 LATEST_TIMESTAMP path (diskless first, classic fallback)
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent)
  }

}
