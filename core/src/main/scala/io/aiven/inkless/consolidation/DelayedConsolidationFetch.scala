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
import io.aiven.inkless.control_plane.FindBatchRequest
import kafka.server.ReplicaManager
import kafka.utils.Logging
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.purgatory.DelayedOperation
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._

/**
 * Long-poll wrapper for a consolidation fetch, honoring `minBytes` and `maxWaitMs`.
 *
 * `tryComplete` runs one cheap metadata probe before parking, completing early only if it already meets `minBytes`.
 * It does not re-probe on later wake-ups (none for this delayed operation), so a request below `minBytes` completes
 * via the `maxWaitMs` timeout -- trading up to `maxWaitMs` latency for a hard bound on control-plane pressure.
 *
 * `onComplete` runs the full `FetchHandler.handle` (findBatches + storage fetch) once, then invokes
 * the response callback when that future completes.
 */
class DelayedConsolidationFetch(
  params: FetchParams,
  fetchInfos: util.Map[TopicIdPartition, FetchRequest.PartitionData],
  fetchHandler: FetchHandler,
  replicaManager: ReplicaManager,
  responseCallback: util.Map[TopicIdPartition, FetchPartitionData] => Unit
) extends DelayedOperation(params.maxWaitMs) with Logging {

  private val initialProbeDone = new AtomicBoolean(false)

  override def toString: String =
    s"DelayedConsolidationFetch(numPartitions=${fetchInfos.size}, minBytes=${params.minBytes}, maxWaitMs=${params.maxWaitMs})"

  /**
   * Cheap probe: sum the bytes [[ReplicaManager.findDisklessBatches]] reports across the requested partitions,
   * and force completion if the estimate meets `minBytes`.
   *
   * With the batch coordinate cache enabled this reads only local, this-broker appends, so it can miss
   * data committed on other brokers and under-count.
   * This is acceptable here: a miss just keeps the op parked until [[onComplete]] runs the real fetch
   * (findBatches through the control plane), which sees the authoritative view.
   */
  override def tryComplete(): Boolean = {
    if (fetchInfos.isEmpty) return forceComplete()

    // Probe once per op. tryCompleteElseWatch calls tryComplete twice: once before registering the
    // watch, then again right after as a race re-check.
    // This purgatory has no wake-ups, so that second call is the only repeat, and the CAS collapses it
    // to a cheap false, keeping each op at one probe plus the final fetch.
    if (!initialProbeDone.compareAndSet(false, true)) return false

    val requests = fetchInfos.entrySet().asScala.toSeq.map { e =>
      new FindBatchRequest(e.getKey, e.getValue.fetchOffset, e.getValue.maxBytes)
    }

    val maybeFindBatchResponses = try {
      replicaManager.findDisklessBatches(requests)
    } catch {
      case e: Throwable =>
        warn(s"$this partitions=$samplePartitions: error during tryComplete findDisklessBatches; deferring to onComplete", e)
        return forceComplete()
    }

    val findBatchResponses = maybeFindBatchResponses match {
      case Some(r) => r
      case None => return forceComplete() // no shared state -- should not happen for consolidation, fail-safe
    }

    if (findBatchResponses.size() != fetchInfos.size) {
      warn(s"$this partitions=$samplePartitions: findDisklessBatches returned ${findBatchResponses.size()} responses " +
        s"for ${fetchInfos.size} requests; deferring to onComplete")
      return forceComplete()
    }

    var accumulated = 0L
    var index = 0
    val it = fetchInfos.entrySet().iterator()
    while (it.hasNext && index < findBatchResponses.size()) {
      val entry = it.next()
      val req = entry.getValue
      val resp = findBatchResponses.get(index)
      resp.errors() match {
        case Errors.NONE =>
          accumulated += resp.estimatedByteSize(req.fetchOffset)
        case _ =>
          // any error short-circuits the wait -- let onComplete surface it through the real fetch
          return forceComplete()
      }
      index += 1
    }

    // Estimated only: this sums the probe's per-partition byte estimates with no total cap.
    // The real fetch in onComplete applies the per-partition and response.max.bytes limits (via findBatches),
    // so accumulated here may exceed what the fetch returns -- that is fine for a >= minBytes check.
    if (accumulated >= params.minBytes) forceComplete()
    else false
  }

  override def onExpiration(): Unit = {
    debug(s"$this expired at maxWaitMs=${params.maxWaitMs}")
  }

  override def onComplete(): Unit = {
    try {
      fetchHandler.handle(params, fetchInfos).whenComplete { (response, throwable) =>
        if (throwable == null) {
          // handle never completes with a null response (it substitutes an error map on failure).
          responseCallback(response)
        } else {
          warn(s"$this partitions=$samplePartitions: fetchHandler.handle failed in onComplete; returning per-partition errors", throwable)
          responseCallback(errorResponse(throwable))
        }
      }
    } catch {
      case e: Throwable =>
        warn(s"$this partitions=$samplePartitions: fetchHandler.handle failed before completion; returning per-partition errors", e)
        responseCallback(errorResponse(e))
    }
  }

  private def samplePartitions: String = {
    val sample = fetchInfos.keySet().asScala.take(5).map(_.topicPartition).mkString(",")
    if (fetchInfos.size > 5) s"[$sample,...]" else s"[$sample]"
  }

  private def errorResponse(e: Throwable): util.Map[TopicIdPartition, FetchPartitionData] = {
    // Errors.forException unwraps CompletionException/ExecutionException to map the real cause.
    val error = Errors.forException(e)
    val response = new util.LinkedHashMap[TopicIdPartition, FetchPartitionData](fetchInfos.size)
    fetchInfos.keySet().forEach { tp =>
      response.put(tp, new FetchPartitionData(
        error,
        -1L,
        -1L,
        MemoryRecords.EMPTY,
        util.Optional.empty(),
        util.OptionalLong.empty(),
        util.Optional.empty(),
        util.OptionalInt.empty(),
        false
      ))
    }
    response
  }
}
