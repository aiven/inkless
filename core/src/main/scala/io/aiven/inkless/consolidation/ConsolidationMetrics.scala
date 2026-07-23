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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

/**
 * Tracks consolidation pipeline lag per partition and aggregated at broker level.
 * All lag values are in offsets (messages).
 *
 * Pipeline: Diskless WAL → Local Log → Remote/Tiered Storage
 *
 * - ConsolidationLocalLag: disklessLEO - localLogEndOffset (first hop: diskless → local)
 * - ConsolidationTotalLag: disklessLEO - remoteLogEndOffset (full pipeline: diskless → remote).
 *   Only updated when remote storage is active (highestOffsetInRemoteStorage ≥ 0); stays at 0 otherwise.
 * - ConsolidationDeletableMessages: messages already in remote storage eligible for WAL pruning
 * - ConsolidationOversizedBatch: count of consolidation append attempts rejected because the block held a
 *   batch larger than the partition's segment.bytes (RecordBatchTooLargeException). The partition is parked
 *   and retried with backoff, so this increments once per retry while the condition persists. Non-zero
 *   indicates a partition holding a batch larger than the current segment.bytes (e.g. max.message.bytes
 *   lowered below segment.bytes over time, or a coalesced diskless unit); raise segment.bytes above the
 *   batch size to resume. See DisklessLeaderEndPoint.clampRecordsToSegment.
 */
class ConsolidationMetrics extends Closeable {
  private val TotalLag = "ConsolidationTotalLag"
  private val LocalLag = "ConsolidationLocalLag"
  private val DeletableMessages = "ConsolidationDeletableMessages"
  private val OversizedBatch = "ConsolidationOversizedBatch"

  private val metricsGroup = new KafkaMetricsGroup("io.aiven.inkless.consolidation", "ConsolidationMetrics")

  private val totalLagByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()
  private val localLagByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()
  private val deletableByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()
  private val oversizedBatchByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()

  // Broker-level aggregate gauges (sum across all partitions)
  metricsGroup.newGauge(TotalLag, () => sumValues(totalLagByPartition))
  metricsGroup.newGauge(LocalLag, () => sumValues(localLagByPartition))
  metricsGroup.newGauge(DeletableMessages, () => sumValues(deletableByPartition))
  metricsGroup.newGauge(OversizedBatch, () => sumValues(oversizedBatchByPartition))

  def registerPartition(tp: TopicPartition): Unit = {
    val tags = Map("topic" -> tp.topic, "partition" -> tp.partition.toString).asJava

    totalLagByPartition.computeIfAbsent(tp, _ => {
      val value = new AtomicLong(0)
      metricsGroup.newGauge(TotalLag, () => value.get, tags)
      value
    }).set(0)
    localLagByPartition.computeIfAbsent(tp, _ => {
      val value = new AtomicLong(0)
      metricsGroup.newGauge(LocalLag, () => value.get, tags)
      value
    }).set(0)
    deletableByPartition.computeIfAbsent(tp, _ => {
      val value = new AtomicLong(0)
      metricsGroup.newGauge(DeletableMessages, () => value.get, tags)
      value
    }).set(0)
    // Monotonic event count; do not reset an existing value on re-registration.
    oversizedBatchByPartition.computeIfAbsent(tp, _ => {
      val value = new AtomicLong(0)
      metricsGroup.newGauge(OversizedBatch, () => value.get, tags)
      value
    })
  }

  def updateTotalLag(tp: TopicPartition, lag: Long): Unit =
    Option(totalLagByPartition.get(tp)).foreach(_.set(lag))

  def updateLocalLag(tp: TopicPartition, lag: Long): Unit =
    Option(localLagByPartition.get(tp)).foreach(_.set(lag))

  def updateDeletableMessages(tp: TopicPartition, count: Long): Unit =
    Option(deletableByPartition.get(tp)).foreach(_.set(count))

  def recordOversizedBatch(tp: TopicPartition): Unit =
    Option(oversizedBatchByPartition.get(tp)).foreach(_.incrementAndGet())

  def unregisterPartition(tp: TopicPartition): Unit = {
    val tags = Map("topic" -> tp.topic, "partition" -> tp.partition.toString).asJava
    totalLagByPartition.remove(tp)
    localLagByPartition.remove(tp)
    deletableByPartition.remove(tp)
    oversizedBatchByPartition.remove(tp)
    metricsGroup.removeMetric(TotalLag, tags)
    metricsGroup.removeMetric(LocalLag, tags)
    metricsGroup.removeMetric(DeletableMessages, tags)
    metricsGroup.removeMetric(OversizedBatch, tags)
  }

  override def close(): Unit = {
    // Using same keys to unregister all partition-level metrics
    totalLagByPartition.keys.asScala.toList.foreach(unregisterPartition)
    // Unregistering aggregated metrics
    metricsGroup.removeMetric(TotalLag)
    metricsGroup.removeMetric(LocalLag)
    metricsGroup.removeMetric(DeletableMessages)
    metricsGroup.removeMetric(OversizedBatch)
  }

  private def sumValues(map: ConcurrentHashMap[TopicPartition, AtomicLong]): Long =
    map.values.asScala.foldLeft(0L)(_ + _.get)
}
