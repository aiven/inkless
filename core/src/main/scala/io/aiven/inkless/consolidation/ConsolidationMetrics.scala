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

class ConsolidationMetrics extends Closeable {
  private val Lag = "inkless.remote.consolidation.lag"
  private val LocalLag = "inkless.remote.consolidation.local.lag"
  private val DeletableMessages = "inkless.remote.consolidation.deletable.messages"

  private val metricsGroup = new KafkaMetricsGroup("io.aiven.inkless.consolidation", "ConsolidationMetrics")

  private val lagByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()
  private val localLagByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()
  private val deletableByPartition = new ConcurrentHashMap[TopicPartition, AtomicLong]()

  def registerPartition(tp: TopicPartition): Unit = {
    val tags = Map("topic" -> tp.topic, "partition" -> tp.partition.toString).asJava

    lagByPartition.computeIfAbsent(tp, _ => {
      val value = new AtomicLong(0)
      metricsGroup.newGauge(Lag, () => value.get, tags)
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
  }

  def updateLag(tp: TopicPartition, lag: Long): Unit =
    Option(lagByPartition.get(tp)).foreach(_.set(lag))

  def updateLocalLag(tp: TopicPartition, lag: Long): Unit =
    Option(localLagByPartition.get(tp)).foreach(_.set(lag))

  def updateDeletableMessages(tp: TopicPartition, count: Long): Unit =
    Option(deletableByPartition.get(tp)).foreach(_.set(count))

  def unregisterPartition(tp: TopicPartition): Unit = {
    val tags = Map("topic" -> tp.topic, "partition" -> tp.partition.toString).asJava
    lagByPartition.remove(tp)
    localLagByPartition.remove(tp)
    deletableByPartition.remove(tp)
    metricsGroup.removeMetric(Lag, tags)
    metricsGroup.removeMetric(LocalLag, tags)
    metricsGroup.removeMetric(DeletableMessages, tags)
  }

  override def close(): Unit = {
    lagByPartition.keys.asScala.toList.foreach(unregisterPartition)
  }
}
