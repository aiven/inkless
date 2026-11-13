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
import kafka.server.DisklessFetchOffsetRouter.ClassicSearchRange
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{LeaderNotAvailableException, OffsetNotAvailableException}
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.internal.{FileRecords, RecordBatch}
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
  // WAL start future we hand back from job.addWalStart(...), incomplete by default like disklessTaskFuture.
  private val walStartFuture: CompletableFuture[FileRecordsOrError] = new CompletableFuture[FileRecordsOrError]()

  private val job: FetchOffsetHandler.Job = {
    val m = mock(classOf[FetchOffsetHandler.Job])
    when(m.add(any(), any())).thenAnswer(_ => disklessTaskFuture)
    when(m.addWalStart(any())).thenAnswer(_ => walStartFuture)
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

  // A diskless leg result carrying a concrete offset.
  private def offsetResult(offset: Long, timestamp: Long = 0L, leaderEpoch: Int = 1): FileRecordsOrError =
    new FileRecordsOrError(
      Optional.empty(),
      Optional.of(new FileRecords.TimestampAndOffset(timestamp, offset, Optional.of[Integer](leaderEpoch))))

  // Invoke `route()` with the standard test wiring.
  private def route(router: DisklessFetchOffsetRouter,
                    timestamp: Long,
                    topicPartition: TopicPartition = tp,
                    replicaId: Int = consumerReplicaId,
                    version: Short = 7,
                    classicLogStartOffset: Option[Long] = None,
                    hasCompleteClassicPrefix: Boolean = true,
                    classicSearchableEnd: TopicPartition => Long = _ => Long.MaxValue,
                    searchRangeIsCurrent: () => Boolean = () => true,
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
      classicSearchRange = tpArg => ClassicSearchRange(classicSearchableEnd(tpArg), searchRangeIsCurrent),
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
  def hybridEarliestPendingUploadAlwaysGoesToClassic(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)

    assertSame(defaultClassicResult, status)
    assertClassicCalledWith(allowFromFollower = true)
    verify(job, never()).add(any(), any())
  }

  // Consolidation requires managed replicas, and a follower request on a switched partition then takes
  // the direct classic route. A tiered diskless topic whose switch is pending has no committed seal, and
  // the router already applies the consolidating rules to it, so a follower request takes the hybrid
  // route while the classic fetcher still replicates the prefix. The local log is the only source of
  // the earliest pending upload offset.
  @Test
  def hybridConsolidatingFollowerEarliestPendingUploadGoesToClassicWhileSwitchPending(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP,
      replicaId = followerReplicaId
    )

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
  def hybridConsolidatingWithoutCommittedBoundaryAllowsFollowerOnEarliestPendingUpload(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    val status = route(
      newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP
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
  def consolidatingBornEarliestRoutesToDiskless(): Unit = {
    // Born-consolidated topic: no committed switch offset (NO_CLASSIC_TO_DISKLESS_START_OFFSET),
    // so the classic local log never owns the earliest. EARLIEST must go straight to the
    // control-plane leg, whose list_offsets_v1 returns COALESCE(remote_log_start_offset,
    // log_start_offset) and therefore advances above 0 once cross-tier retention reclaims the
    // remote prefix. The classic leg must NOT be consulted (it would pin earliest at a stale 0).
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)
    disklessTaskFuture.complete(offsetResult(50L))

    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP
    )

    assertTrue(classicCalls.isEmpty, "classic path must not be invoked for a born-consolidated topic")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "diskless leg surfaces an async holder")
    val result = status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
    assertEquals(50L, result.timestampAndOffset.get.offset)
  }

  @Test
  def consolidatingSwitchedEarliestRoutesToDisklessWhilePrefixPresent(): Unit = {
    // Switched-then-consolidated topic whose classic prefix is still on disk
    // (classicLogStartOffset 0 < classicToDisklessStartOffset 100). Even so, EARLIEST goes to the
    // broker-agnostic control plane, not the local classic log (which is frozen at the switch on
    // followers and would differ per broker): the diskless leg is consulted, the classic leg is not.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)
    disklessTaskFuture.complete(offsetResult(0L))

    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(0L)
    )

    assertTrue(classicCalls.isEmpty, "classic path must not be invoked for a consolidating topic's EARLIEST")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "diskless leg surfaces an async holder")
    val result = status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
    assertEquals(0L, result.timestampAndOffset.get.offset)
  }

  @Test
  def consolidatingSwitchedEarliestIsConsistentAcrossBrokersRegardlessOfLocalClassicLogStart(): Unit = {
    // Cross-broker EARLIEST consistency guard: two brokers with different local classic log starts (leader at 60,
    // follower still frozen at 0, both below the seal of 100) must return the SAME earliest. Both
    // route to the control plane, so neither consults its local classic log.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)

    Seq(Some(0L), Some(60L)).foreach { localClassicLogStart =>
      classicCalls.clear()
      val fresh = new CompletableFuture[FileRecordsOrError]()
      fresh.complete(offsetResult(42L))
      val jobForBroker = {
        val m = mock(classOf[FetchOffsetHandler.Job])
        when(m.add(any(), any())).thenAnswer(_ => fresh)
        when(m.cancelHandler()).thenReturn(new CompletableFuture[Void]())
        m
      }
      val status = new DisklessFetchOffsetRouter(
        inklessMetadataView, true, true, purgatory).route(
        job = jobForBroker,
        newJob = () => throw new AssertionError("newJob() should not be called by this routing path"),
        topicPartition = tp,
        partition = makePartition(ListOffsetsRequest.EARLIEST_TIMESTAMP, tp.partition),
        replicaId = consumerReplicaId,
        version = 7,
        classicLogStartOffsetProvider = _ => localClassicLogStart,
        hasCompleteClassicPrefix = (_, _) => true,
        classicSearchRange = _ => ClassicSearchRange(Long.MaxValue, () => true),
        classicFetchOffset = (tpArg, partition, allow) => {
          classicCalls += ((tpArg, partition, allow))
          defaultClassicResult
        }
      )
      assertTrue(classicCalls.isEmpty,
        s"classic path must not be invoked (localClassicLogStart=$localClassicLogStart)")
      assertTrue(status.futureHolderOpt.isPresent)
      assertEquals(42L, status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS).timestampAndOffset.get.offset)
    }
  }

  @Test
  def consolidatingSwitchedEarliestFallsToDisklessWhenPrefixGone(): Unit = {
    // Switched-then-consolidated topic whose classic prefix has been fully reclaimed
    // (classicLogStartOffset 100 >= classicToDisklessStartOffset 100): no classic data left, so
    // EARLIEST is served by the control-plane leg and the classic path is NOT consulted.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)
    disklessTaskFuture.complete(offsetResult(120L))

    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(100L)
    )

    assertTrue(classicCalls.isEmpty, "classic path must not be invoked once the prefix is gone")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent, "diskless leg surfaces an async holder")
    val result = status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
    assertEquals(120L, result.timestampAndOffset.get.offset)
  }

  @Test
  def consolidatingEarliestWithCommittedBoundaryRoutesToDisklessEvenWhenPrefixIncomplete(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)
    disklessTaskFuture.complete(offsetResult(5L))
    // The local classic prefix is incomplete here, but EARLIEST for a consolidating topic is served
    // by the control plane, so local completeness is irrelevant and the classic leg is never consulted.
    val status = route(
      newRouter(disklessConsolidationEnabled = true),
      timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
      classicLogStartOffset = Some(0L),
      hasCompleteClassicPrefix = false
    )

    assertTrue(classicCalls.isEmpty, "classic path must not be invoked for a consolidating topic's EARLIEST")
    verify(job).add(eqTo(tp), any())
    assertTrue(status.futureHolderOpt.isPresent)
    assertEquals(5L, status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS).timestampAndOffset.get.offset)
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

  // ---------------------------------------------------------------------------
  // Specific timestamp on a replica whose local log ends below the diskless log.
  // ---------------------------------------------------------------------------

  private val syncNoMatch: ListOffsetsPartitionStatus = resolvedStatus(makeResponse(offset = -1L))

  private def consolidatingRouter(): DisklessFetchOffsetRouter = {
    when(inklessMetadataView.isConsolidatingDisklessTopic(tp.topic)).thenReturn(true)
    newRouter(disklessManagedReplicasEnabled = true, disklessConsolidationEnabled = true)
  }

  private def resultOf(status: ListOffsetsPartitionStatus): FileRecordsOrError = {
    assertTrue(status.futureHolderOpt.isPresent)
    status.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
  }

  private def assertPrefixNotSearchable(status: ListOffsetsPartitionStatus,
                                        expected: Class[_ <: Exception] = classOf[OffsetNotAvailableException]): Unit = {
    val result = resultOf(status)
    assertTrue(result.hasException, s"expected ${expected.getSimpleName}, got $result")
    assertEquals(expected, result.exception.get.getClass)
  }

  @Test
  def switchedSpecificTimestampFailsWithoutDisklessLookupWhenLocalLogEndsBelowSeal(): Unit = {
    // A recreated phase 2 leader: empty local log, seal at 100.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = 123L, classicResult = syncNoMatch, classicSearchableEnd = _ => 0L)

    assertPrefixNotSearchable(status)
    verify(job, never()).add(any(), any())
    verify(job, never()).addWalStart(any())
  }

  @Test
  def switchedSpecificTimestampMapsBelowSealErrorToLeaderNotAvailableBeforeVersion5(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = 123L, version = 4, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 0L)

    assertPrefixNotSearchable(status, classOf[LeaderNotAvailableException])
  }

  @Test
  def switchedSpecificTimestampReturnsClassicMatchWhenLocalLogEndsBelowSeal(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    val matched = resolvedStatus(makeResponse(offset = 42L, timestamp = 123L))

    val status = route(newRouter(), timestamp = 123L, classicResult = matched, classicSearchableEnd = _ => 50L)

    assertSame(matched, status, "a match is in the searched range, so it stands")
    verify(job, never()).add(any(), any())
  }

  @Test
  def switchedSpecificTimestampFailsWhenAsyncClassicMissesBelowSeal(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    val classicFuture = new CompletableFuture[FileRecordsOrError]()
    val asyncClassic = ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(
      new AsyncOffsetReadFutureHolder[FileRecordsOrError](new CompletableFuture[Void](), classicFuture))).build()

    // The default newJob fails the test if the fallback opens a control-plane job.
    val status = route(newRouter(), timestamp = 123L, classicResult = asyncClassic, classicSearchableEnd = _ => 0L)
    classicFuture.complete(new FileRecordsOrError(Optional.empty(), Optional.empty()))

    assertPrefixNotSearchable(status)
  }

  @Test
  def consolidatingSpecificTimestampFailsWhenLocalLogEndsBelowWalStart(): Unit = {
    // A reassignment target of a born-diskless topic: empty local log, WAL pruned up to 800.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    disklessTaskFuture.complete(offsetResult(offset = 800L, timestamp = 10800L))
    walStartFuture.complete(offsetResult(offset = 800L))

    val status = route(consolidatingRouter(), timestamp = 10100L, replicaId = followerReplicaId,
      classicResult = syncNoMatch, classicSearchableEnd = _ => 0L)

    assertPrefixNotSearchable(status)
    verify(job).add(eqTo(tp), any())
    verify(job).addWalStart(tp)
    verify(job, never()).start()
  }

  @Test
  def consolidatingSpecificTimestampReturnsDisklessResultWhenLocalLogReachesWalStart(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val disklessHit = offsetResult(offset = 1000L, timestamp = 11000L)
    disklessTaskFuture.complete(disklessHit)
    walStartFuture.complete(offsetResult(offset = 800L))

    val status = route(consolidatingRouter(), timestamp = 11000L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 900L)

    assertSame(disklessHit, resultOf(status))
  }

  @Test
  def consolidatingSpecificTimestampFailsWhenTheDisklessLegAlsoMissesBelowWalStart(): Unit = {
    // The local log didn't search [0, 800), so the diskless "no match" doesn't show that no record matches.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    disklessTaskFuture.complete(new FileRecordsOrError(Optional.empty(), Optional.empty()))
    walStartFuture.complete(offsetResult(offset = 800L))

    val status = route(consolidatingRouter(), timestamp = 99999L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 0L)

    assertPrefixNotSearchable(status)
  }

  @Test
  def consolidatingSpecificTimestampReadsTheSearchRangeOnceBeforeTheClassicLookup(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    disklessTaskFuture.complete(offsetResult(offset = 1000L, timestamp = 11000L))
    walStartFuture.complete(offsetResult(offset = 800L))
    val events = mutable.ListBuffer.empty[String]

    val status = route(consolidatingRouter(), timestamp = 11000L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => { events += s"range after ${classicCalls.size} classic calls"; 900L })
    resultOf(status)

    assertEquals(List("range after 0 classic calls"), events.toList)
  }

  @Test
  def consolidatingSpecificTimestampFailsWhenTheSearchRangeStopsBeingCurrent(): Unit = {
    // A rebuild or recreation runs after the range read, so the local search might have read less than the range.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    disklessTaskFuture.complete(offsetResult(offset = 1000L, timestamp = 11000L))
    walStartFuture.complete(offsetResult(offset = 800L))

    val status = route(consolidatingRouter(), timestamp = 11000L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 900L, searchRangeIsCurrent = () => false)

    assertPrefixNotSearchable(status)
  }

  @Test
  def switchedSpecificTimestampFailsWhenTheSearchRangeStopsBeingCurrent(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)

    val status = route(newRouter(), timestamp = 123L, classicResult = syncNoMatch, classicSearchableEnd = _ => 150L,
      searchRangeIsCurrent = () => false)

    assertPrefixNotSearchable(status)
    verify(job, never()).add(any(), any())
  }

  @Test
  def consolidatingSpecificTimestampChecksTheSearchRangeWhenTheControlPlaneAnswers(): Unit = {
    // The range is current at routing time and stops being current before the control plane answers.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    var current = true

    val status = route(consolidatingRouter(), timestamp = 11000L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 900L, searchRangeIsCurrent = () => current)
    current = false
    disklessTaskFuture.complete(offsetResult(offset = 1000L, timestamp = 11000L))
    walStartFuture.complete(offsetResult(offset = 800L))

    assertPrefixNotSearchable(status)
  }

  @Test
  def consolidatingSwitchedSpecificTimestampComparesWithTheSealBeforeTheFirstPrune(): Unit = {
    // Before the first prune the WAL start of a switched partition is its classic log start, below the seal.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    val disklessHit = offsetResult(offset = 100L, timestamp = 10100L)
    disklessTaskFuture.complete(disklessHit)
    walStartFuture.complete(offsetResult(offset = 0L))

    val status = route(consolidatingRouter(), timestamp = 10100L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 100L)

    assertSame(disklessHit, resultOf(status))
    verify(job).addWalStart(tp)
  }

  @Test
  def consolidatingSwitchedSpecificTimestampFailsWhenThePrunePassedTheLocalLog(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    disklessTaskFuture.complete(offsetResult(offset = 300L, timestamp = 10300L))
    walStartFuture.complete(offsetResult(offset = 300L))

    val status = route(consolidatingRouter(), timestamp = 10150L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 200L)

    assertPrefixNotSearchable(status)
  }

  @Test
  def consolidatingSpecificTimestampForwardsADisklessErrorBeforeCheckingTheWalStart(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val disklessError = new FileRecordsOrError(Optional.of(new RuntimeException("control plane failure")), Optional.empty())
    disklessTaskFuture.complete(disklessError)
    walStartFuture.complete(offsetResult(offset = 800L))

    val status = route(consolidatingRouter(), timestamp = 10100L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 0L)

    assertSame(disklessError, resultOf(status))
  }

  @Test
  def consolidatingSpecificTimestampForwardsAWalStartError(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    disklessTaskFuture.complete(offsetResult(offset = 1000L, timestamp = 11000L))
    val walStartError = new FileRecordsOrError(Optional.of(new RuntimeException("control plane failure")), Optional.empty())
    walStartFuture.complete(walStartError)

    val status = route(consolidatingRouter(), timestamp = 11000L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 900L)

    assertSame(walStartError, resultOf(status))
  }

  @Test
  def consolidatingSpecificTimestampAsyncFallbackReadsTheWalStartOnTheFreshJob(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    val fresh = mock(classOf[FetchOffsetHandler.Job])
    when(fresh.add(any(), any())).thenReturn(CompletableFuture.completedFuture(offsetResult(offset = 800L, timestamp = 10800L)))
    when(fresh.addWalStart(any())).thenReturn(CompletableFuture.completedFuture(offsetResult(offset = 800L)))
    when(fresh.cancelHandler()).thenReturn(new CompletableFuture[Void]())
    val classicFuture = new CompletableFuture[FileRecordsOrError]()
    val asyncClassic = ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(
      new AsyncOffsetReadFutureHolder[FileRecordsOrError](new CompletableFuture[Void](), classicFuture))).build()

    val status = route(consolidatingRouter(), timestamp = 10100L, classicResult = asyncClassic,
      classicSearchableEnd = _ => 0L, newJob = () => fresh)
    classicFuture.complete(new FileRecordsOrError(Optional.empty(), Optional.empty()))

    assertPrefixNotSearchable(status)
    val order = inOrder(fresh)
    order.verify(fresh).add(eqTo(tp), any())
    order.verify(fresh).addWalStart(tp)
    order.verify(fresh).start()
    verify(job, never()).add(any(), any())
  }

  @Test
  def consolidatingSwitchedSpecificTimestampSkipsTheWalStartCheckInPhase1(): Unit = {
    // In phase 1 the local log stays at the seal, and a DeleteRecords past the seal moved the WAL start to 300.
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isClassicPrefixWaitingForUpload(tp)).thenReturn(true)
    val disklessHit = offsetResult(offset = 300L, timestamp = 10300L)
    disklessTaskFuture.complete(disklessHit)

    val status = route(consolidatingRouter(), timestamp = 10150L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 100L)

    assertSame(disklessHit, resultOf(status))
    verify(job, never()).addWalStart(any())
  }

  @Test
  def consolidatingSwitchedSpecificTimestampKeepsTheSealCheckInPhase1(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    when(inklessMetadataView.isClassicPrefixWaitingForUpload(tp)).thenReturn(true)

    val status = route(consolidatingRouter(), timestamp = 10150L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 0L)

    assertPrefixNotSearchable(status)
    verify(job, never()).add(any(), any())
  }

  @Test
  def consolidatingSpecificTimestampFailsWhenTheWalStartIsNegative(): Unit = {
    when(inklessMetadataView.getClassicToDisklessStartOffset(tp)).thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    disklessTaskFuture.complete(offsetResult(offset = 1000L, timestamp = 11000L))
    walStartFuture.complete(offsetResult(offset = -1L))

    val status = route(consolidatingRouter(), timestamp = 11000L, classicResult = syncNoMatch,
      classicSearchableEnd = _ => 900L)

    assertPrefixNotSearchable(status)
  }

}
