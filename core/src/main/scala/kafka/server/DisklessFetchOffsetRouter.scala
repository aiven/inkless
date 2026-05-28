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
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, RecordBatch}
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, DelayedRemoteListOffsets, ListOffsetsPartitionStatus, TopicPartitionOperationKey}
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError

import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CompletableFuture, CopyOnWriteArrayList, Future}
import scala.jdk.OptionConverters.RichOptional

/**
 * Routes ListOffsets queries for diskless-managed topics among the classic UnifiedLog path,
 * the diskless control-plane path, and hybrid configurations that can answer from either.
 *
 * Distinguishes among:
 *   1. Pure diskless partition (`classicToDisklessStartOffset <= 0` and managed replicas disabled
 *      and not consolidating diskless topics): fetch offset in the diskless path.
 *   2. Hybrid diskless partition (`classicToDisklessStartOffset > 0` and managed replicas enabled,
 *      or a consolidating diskless topic): fetch offset in classic and/or diskless path, depending
 *      on the requested timestamp. Consolidating topics also allow followers on the classic leg
 *      (same rationale as sealed switched replicas: clients may be routed to any ISR replica).
 *   3. Partition that is being switched from classic to diskless
 *      (`classicToDisklessStartOffset == CLASSIC_TO_DISKLESS_SWITCH_PENDING` and managed
 *      replicas enabled, and not a consolidating diskless topic): fetch offset only in the
 *      classic path.
 *   4. Follower requests on a switched partition: ListOffsets requests (used by ReplicaFetcher
 *      for truncation / initial offset bootstrap) must never see diskless offsets, otherwise the
 *      follower would try to truncate to or fetch from offsets that live only in object storage:
 *      fetch offset only in the classic path.
 *
 * Per request, the caller obtains a per-request `FetchOffsetHandler.Job` via [[createJob]],
 * routes each diskless-managed partition through [[route]] (passing classic-side callbacks),
 * and finally calls `job.start()` to fire the underlying control plane job.
 */
class DisklessFetchOffsetRouter(
  inklessMetadataView: InklessMetadataView,
  disklessManagedReplicasEnabled: Boolean,
  disklessConsolidationEnabled: Boolean,
  delayedRemoteListOffsetsPurgatory: DelayedOperationPurgatory[DelayedRemoteListOffsets]
) {
  import DisklessFetchOffsetRouter._

  /**
   * Route a single partition.
   *
   * @param job                           the per-request control-plane job; partitions that go
   *                                      to the diskless path are added to it (and the caller
   *                                      starts it once after routing all partitions).
   * @param newJob                        factory for a fresh, immediately-started job used by
   *                                      async fallbacks that fire after the caller has already
   *                                      started `job`.
   * @param classicLogStartOffsetProvider returns the local UnifiedLog's `logStartOffset` for the
   *                                      given partition, used to decide whether the EARLIEST
   *                                      timestamp can be served by the classic path.
   * @param hasCompleteClassicPrefix      returns whether the local UnifiedLog's high watermark has
   *                                      reached the classic-to-diskless switch offset.
   * @param classicFetchOffset            runs the standard Kafka classic-path lookup for the
   *                                      given `(topicPartition, partition, allowFromFollower)`.
   */
  def route(
    job: FetchOffsetHandler.Job,
    newJob: () => FetchOffsetHandler.Job,
    topicPartition: TopicPartition,
    partition: ListOffsetsPartition,
    replicaId: Int,
    version: Short,
    classicLogStartOffsetProvider: TopicPartition => Option[Long],
    hasCompleteClassicPrefix: (TopicPartition, Long) => Boolean,
    classicFetchOffset: (TopicPartition, ListOffsetsPartition, Boolean) => ListOffsetsPartitionStatus
  ): ListOffsetsPartitionStatus = {
    val classicToDisklessStartOffset = inklessMetadataView.getClassicToDisklessStartOffset(topicPartition)
    val switchPending = classicToDisklessStartOffset == PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING
    val hasCommittedSwitchOffset = classicToDisklessStartOffset > 0
    val isSwitchedWithClassicAccess = hasCommittedSwitchOffset && disklessManagedReplicasEnabled
    val isConsolidatingPartition = disklessConsolidationEnabled && inklessMetadataView.isConsolidatingDisklessTopic(topicPartition.topic)

    // Switched partitions seal their classic local log: once classicToDisklessStartOffset is
    // committed the LEO can no longer grow. Any replica whose local HW has reached the seal
    // can therefore safely answer ListOffsets from its local log, so we let those followers
    // serve the classic-side query as well.
    // Consolidating partitions can also allow follower requests, except when they are switched
    // and this broker has not caught up to the sealed classic prefix.
    val switchedAllowsFollower =
      isSwitchedWithClassicAccess && hasCompleteClassicPrefix(topicPartition, classicToDisklessStartOffset)
    val consolidatingAllowsFollower =
      isConsolidatingPartition && (!hasCommittedSwitchOffset || hasCompleteClassicPrefix(topicPartition, classicToDisklessStartOffset))
    val allowFromFollower = switchedAllowsFollower || consolidatingAllowsFollower
    val isFollowerRequest = replicaId >= 0

    def classicLookup(): ListOffsetsPartitionStatus = classicFetchOffset(topicPartition, partition, allowFromFollower)
    def disklessLookup: Lookup = disklessLookupOnJob(job, topicPartition, partition)
    def disklessLookupOnNewJob: Lookup = disklessLookupOnJob(newJob(), topicPartition, partition, startNow = true)

    // Consolidating diskless topics can still carry CLASSIC_TO_DISKLESS_SWITCH_PENDING in
    // metadata while the leader serves (or is catching up) from object storage; ListOffsets must
    // not be forced down the classic-only path in that case (see ReplicaManager diskless fetch
    // routing for the analogous consolidating carve-out).
    if (switchPending && disklessManagedReplicasEnabled && !isConsolidatingPartition) {
      // Case 3.
      classicFetchOffset(topicPartition, partition, false)
    } else if (isSwitchedWithClassicAccess && isFollowerRequest) {
      // Case 4.
      classicLookup()
    } else if (isSwitchedWithClassicAccess || isConsolidatingPartition) {
      // Case 2: hybrid, route by timestamp.
      partition.timestamp() match {
        case ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP | ListOffsetsRequest.LATEST_TIERED_TIMESTAMP =>
          // These timestamps reference the local UnifiedLog / tiered storage; never route to diskless.
          classicLookup()

        case ListOffsetsRequest.EARLIEST_TIMESTAMP =>
          // Always do classic first and then fall back to diskless with consolidating partitions
          if (isConsolidatingPartition) {
            classicWithDisklessFallbackLookup(topicPartition, version, classicLookup _, disklessLookup _, disklessLookupOnNewJob _)
          // Try classic first when classic data is still on disk, otherwise go straight to diskless.
          } else if (classicLogStartOffsetProvider(topicPartition).exists(_ < classicToDisklessStartOffset)) {
            classicLookup()
          } else {
            asStatus(topicPartition, disklessLookup)
          }

        case t if t >= 0 =>
          // Specific timestamp: classic first, fall back to diskless on a "no match".
          classicWithDisklessFallbackLookup(topicPartition, version, classicLookup _, disklessLookup _, disklessLookupOnNewJob _)

        case _ =>
          // LATEST_TIMESTAMP / MAX_TIMESTAMP: diskless first, classic fallback on empty.
          withFallback(topicPartition, disklessLookup, () => classicLookupOf(classicLookup(), version))
      }
    } else {
      // Case 1.
      asStatus(topicPartition, disklessLookup)
    }
  }

  private def classicWithDisklessFallbackLookup(topicPartition: TopicPartition,
                                                version: Short,
                                                classicLookup: () => ListOffsetsPartitionStatus,
                                                disklessLookup: () => Lookup,
                                                disklessLookupOnNewJob: () => Lookup) = {
    val classic = classicLookup()
    if (classic.responseOpt.isPresent) {
      if (isSyncNoMatch(classic.responseOpt.get)) asStatus(topicPartition, disklessLookup())
      else classic
    } else if (classic.futureHolderOpt.isPresent) {
      // If the classic future is already complete at routing time, withFallback's thenCompose
      // invokes the fallback factory synchronously on this thread, i.e. inside route(), which
      // runs before the caller calls job.start() (see ReplicaManager.fetchOffset). So we can
      // still batch the diskless lookup into the per-request job. Otherwise the fallback
      // fires asynchronously after start(), and must use a fresh, immediately-started job.
      val fallback: () => Lookup =
        if (classic.futureHolderOpt.get.taskFuture.isDone) () => disklessLookup()
        else () => disklessLookupOnNewJob()
      withFallback(topicPartition, classicLookupOf(classic, version), fallback)
    } else classic
  }

  /**
   * Wrap a single async lookup as a `ListOffsetsPartitionStatus`, ensuring the purgatory wakes
   * up to deliver the response when the lookup completes.
   */
  private def asStatus(tp: TopicPartition, lookup: Lookup): ListOffsetsPartitionStatus = {
    lookup.taskFuture.whenComplete { (_, _) =>
      delayedRemoteListOffsetsPurgatory.checkAndComplete(new TopicPartitionOperationKey(tp.topic, tp.partition))
    }
    ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(lookup)).build()
  }

  /**
   * Run `primary`; if it returns a "no match" (no exception, no timestamp+offset), invoke
   * `fallback` and forward its outcome instead. The purgatory's single cancel hook is fanned
   * out to both legs so an expiration cancels whichever underlying job(s) are still in flight,
   * forwarding the original `mayInterruptIfRunning` flag (the purgatory uses `cancel(false)` on
   * the in-flight-error path and `cancel(true)` on expiration; both must reach the underlying
   * remote-storage / inkless job futures unchanged).
   */
  private def withFallback(
    tp: TopicPartition,
    primary: Lookup,
    fallback: () => Lookup
  ): ListOffsetsPartitionStatus = {
    val cancelSignal = new FanOutCancelFuture
    cancelSignal.addTarget(primary.jobFuture)

    val combined = primary.taskFuture.thenCompose[FileRecordsOrError] { result =>
      if (result.hasException || result.hasTimestampAndOffset) {
        // Primary answered authoritatively (hit or error). No fallback needed.
        CompletableFuture.completedFuture(result)
      } else {
        try {
          val secondary = fallback()
          cancelSignal.addTarget(secondary.jobFuture)
          secondary.taskFuture
        } catch {
          case e: Exception =>
            CompletableFuture.completedFuture(new FileRecordsOrError(Optional.of(e), Optional.empty()))
        }
      }
    }

    asStatus(tp, new AsyncOffsetReadFutureHolder(cancelSignal, combined))
  }
}

private[server] object DisklessFetchOffsetRouter {

  /**
   * The shape every code path produces: a result future plus a cancel hook the purgatory can
   * pull on. This is the same record `ListOffsetsPartitionStatus` carries around, which means
   * we don't need a separate intermediate type and `asStatus` doesn't have to convert.
   */
  private type Lookup = AsyncOffsetReadFutureHolder[FileRecordsOrError]

  /**
   * A `CompletableFuture[Void]` used as a single cancel hook for several underlying job futures.
   * When `cancel(mayInterruptIfRunning)` is invoked the same flag is forwarded to every target,
   * preserving the purgatory's choice between `cancel(true)` (expiration: interrupt the running
   * worker) and `cancel(false)` (in-flight error: just mark cancelled, don't interrupt). Targets
   * added after a cancel has already fired are cancelled immediately with the recorded flag, so
   * the chained-in fallback leg cannot leak past a cancellation that races with it.
   *
   * Concurrent `cancel(...)` calls are made deterministic with a single CAS on `cancelFlag`:
   * the first caller atomically transitions it from `null` to its own `mayInterruptIfRunning`
   * and is the unique fan-out winner; later callers (and `addTarget` after the fact) read back
   * that same flag, so every target is cancelled exactly once with the winner's value.
   */
  private final class FanOutCancelFuture extends CompletableFuture[Void] {
    // Use CopyOnWriteArrayList because addTarget() and cancel()'s forEach can race on different threads,
    // and we need iteration to be snapshot-safe (no ConcurrentModificationException) and
    // lock-free, so a target's `cancel(...)` callback can't deadlock us by reentering. Writes
    // are tiny and rare (typically 1-2 targets per request), so the copy-on-write cost is negligible.
    private val targets = new CopyOnWriteArrayList[Future[_]]()
    // Records the mayInterruptIfRunning flag of the first cancel() to land; `null` while no cancel has run yet.
    private val cancelFlag = new AtomicReference[java.lang.Boolean]()

    def addTarget(target: Future[_]): Unit = {
      targets.add(target)
      // Add first, *then* read cancelFlag. A racing cancel either already
      // published its flag (we observe it here and cancel ourselves) or will iterate `targets` in `cancel()`
      // next and find us there. So no concurrent cancel can leave us un-cancelled.
      val f = cancelFlag.get
      if (f != null) target.cancel(f)
    }

    override def cancel(mayInterruptIfRunning: Boolean): Boolean = {
      // `won` == true if this call is the first cancel() and its flag is the one now stored.
      val won = cancelFlag.compareAndSet(null, java.lang.Boolean.valueOf(mayInterruptIfRunning))
      // Always propagate the *winning* flag (never our own arg) so a losing caller's
      // mayInterruptIfRunning cannot leak through to targets and contradict the winner's choice.
      val flag = cancelFlag.get.booleanValue
      val result = super.cancel(flag)
      // Only the CAS winner fans out: Future#cancel is idempotent, but a second fan-out from a
      // losing caller would still re-invoke targets unnecessarily.
      if (won) targets.forEach(_.cancel(flag))
      result
    }
  }

  private val EmptyResult = new FileRecordsOrError(Optional.empty(), Optional.empty())
  private val NoOpCancel: CompletableFuture[Void] = CompletableFuture.completedFuture(null)

  /**
   * Diskless lookup against the given job. Set `startNow = true` for a one-off fallback job
   * (for fallbacks discovered after the caller's job has already been started); leave it
   * `false` when adding to the caller's per-request job (the caller will start it).
   */
  private def disklessLookupOnJob(job: FetchOffsetHandler.Job,
                                  tp: TopicPartition,
                                  partition: ListOffsetsPartition,
                                  startNow: Boolean = false): Lookup = {
    val task = job.add(tp, partition)
    if (startNow) job.start()
    new AsyncOffsetReadFutureHolder(job.cancelHandler(), task)
  }

  /**
   * Wrap a `ListOffsetsPartitionStatus` produced by the classic path. For async (tiered storage)
   * results, applies the `lastFetchableOffset` / `maybeOffsetsError` translation up-front so the
   * resulting future already accounts for clamp / pending error and the status we eventually
   * hand to the purgatory does not need to carry classic metadata.
   */
  private def classicLookupOf(status: ListOffsetsPartitionStatus, version: Short): Lookup = {
    if (status.responseOpt.isPresent) {
      new AsyncOffsetReadFutureHolder(NoOpCancel, CompletableFuture.completedFuture(syncResponseToResult(status.responseOpt.get)))
    } else if (status.futureHolderOpt.isPresent) {
      val holder = status.futureHolderOpt.get
      val translated = holder.taskFuture.thenApply[FileRecordsOrError](r => translateClassicRemoteResult(r, status, version))
      new AsyncOffsetReadFutureHolder(holder.jobFuture, translated)
    } else {
      new AsyncOffsetReadFutureHolder(NoOpCancel, CompletableFuture.completedFuture(EmptyResult))
    }
  }

  /** Sync classic responses with `errorCode == NONE && offset < 0` are the "no match" sentinel. */
  private def isSyncNoMatch(r: ListOffsetsPartitionResponse): Boolean =
    r.errorCode == Errors.NONE.code && r.offset < 0

  private def syncResponseToResult(r: ListOffsetsPartitionResponse): FileRecordsOrError = {
    if (r.errorCode != Errors.NONE.code) {
      new FileRecordsOrError(Optional.of(Errors.forCode(r.errorCode).exception()), Optional.empty())
    } else if (r.offset >= 0) {
      val leaderEpoch =
        if (r.leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) Optional.empty[Integer]()
        else Optional.of[Integer](r.leaderEpoch)
      new FileRecordsOrError(
        Optional.empty(),
        Optional.of(new FileRecords.TimestampAndOffset(r.timestamp, r.offset, leaderEpoch)))
    } else {
      EmptyResult
    }
  }

  /**
   * Apply the same `lastFetchableOffset` / `maybeOffsetsError` evaluation that
   * [[org.apache.kafka.server.purgatory.DelayedRemoteListOffsets]] performs when it inspects a
   * partition status that carries classic-side metadata.
   *
   *   - exception → forward as-is.
   *   - empty taskFuture → if `maybeOffsetsError` is set, surface the version-mapped error; else
   *     forward the empty result so the consumer reports NONE+UNKNOWN.
   *   - hit with offset >= lastFetchableOffset → if `maybeOffsetsError` is set, surface the
   *     version-mapped error; otherwise treat as empty (the original behaviour returns
   *     NONE+UNKNOWN, encoded here as an empty `FileRecordsOrError`).
   *   - hit otherwise → forward as-is.
   *
   * The version mapping mirrors classicFetchOffset's catch on `OffsetNotAvailableException`:
   * v>=5 surfaces the exception's actual error code, older versions collapse to
   * `LEADER_NOT_AVAILABLE` for back-compat.
   */
  private def translateClassicRemoteResult(remoteResult: FileRecordsOrError,
                                           classic: ListOffsetsPartitionStatus,
                                           version: Short): FileRecordsOrError = {
    def mapPendingError(e: ApiException): FileRecordsOrError = {
      val mapped: Exception = if (version >= 5) e else Errors.LEADER_NOT_AVAILABLE.exception()
      new FileRecordsOrError(Optional.of(mapped), Optional.empty())
    }

    if (remoteResult.hasException) remoteResult
    else if (!remoteResult.hasTimestampAndOffset) {
      classic.maybeOffsetsError.toScala.map(mapPendingError).getOrElse(remoteResult)
    } else {
      val found = remoteResult.timestampAndOffset.get
      // Clamped == true: found offset is at/past the visibility boundary (LSO/HW/LEO per isolation level), so it isn't readable yet.
      val clamped = classic.lastFetchableOffset.toScala.exists(found.offset >= _)
      if (!clamped) remoteResult
      else classic.maybeOffsetsError.toScala.map(mapPendingError).getOrElse(EmptyResult)
    }
  }
}
