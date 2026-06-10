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

import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
import kafka.server.{KafkaConfig, ReplicaManager, ReplicationQuotaManager}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.Uuid
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.quota.QuotaType
import org.apache.kafka.server.{PartitionFetchState, ReplicaState}
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.{mock, when}

import java.util
import java.util.{Collections, Optional}

class ConsolidationQuotaManagerTest {
  private val brokerEndPoint = new BrokerEndPoint(1, "localhost", 9092)
  private val tp = new TopicPartition("consolidating-topic", 0)
  private val topicId = Uuid.randomUuid()

  private var time: MockTime = _
  private var metrics: Metrics = _

  @BeforeEach
  def setUp(): Unit = {
    time = new MockTime
    metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
  }

  @AfterEach
  def tearDown(): Unit = {
    metrics.close()
  }

  private def createConsolidationQuota(rateBytesPerSec: Long): ReplicationQuotaManager = {
    val config = new ReplicationQuotaManagerConfig(10, 1)
    val manager = new ReplicationQuotaManager(config, metrics, QuotaType.DISKLESS_CONSOLIDATION_FETCH, time)
    manager.updateQuota(new Quota(rateBytesPerSec.toDouble, true))
    manager.markThrottled("consolidating-topic")
    manager
  }

  private def createEndPoint(quota: ReplicationQuotaManager): DisklessLeaderEndPoint = {
    val fetchHandler = mock(classOf[FetchHandler])
    val fetchOffsetHandler = mock(classOf[FetchOffsetHandler])
    val replicaManager = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(0L)
    when(replicaManager.localLogOrException(tp)).thenReturn(log)

    val props = TestUtils.createBrokerConfig(brokerEndPoint.id, port = brokerEndPoint.port)
    val kafkaConfig = KafkaConfig.fromProps(props)

    new DisklessLeaderEndPoint(
      brokerEndPoint,
      fetchHandler,
      fetchOffsetHandler,
      replicaManager,
      kafkaConfig,
      quota,
      () => MetadataVersion.LATEST_PRODUCTION,
      () => 7L
    )
  }

  private def buildFetchSucceeds(endPoint: DisklessLeaderEndPoint): Boolean = {
    val fetchState = new PartitionFetchState(
      Optional.of(topicId),
      0L,
      Optional.empty(),
      1,
      Optional.empty(),
      ReplicaState.FETCHING,
      Optional.empty()
    )
    endPoint.buildFetch(util.Map.of(tp, fetchState)).result.isPresent
  }

  @Test
  def shouldNotExceedQuotaWhenUnlimited(): Unit = {
    val quota = createConsolidationQuota(Long.MaxValue)

    quota.record(100 * 1024 * 1024)
    assertFalse(quota.isQuotaExceeded)
    assertTrue(buildFetchSucceeds(createEndPoint(quota)))
  }

  @Test
  def shouldExceedQuotaWhenRateExceeded(): Unit = {
    val quota = createConsolidationQuota(100) // 100 bytes/sec

    assertFalse(quota.isQuotaExceeded)

    // First window is fixed, skip it
    time.sleep(1000)

    // Record 150 bytes in 1.5 seconds → rate = 100 B/s (at quota)
    time.sleep(500)
    quota.record(1)
    quota.record(149)
    assertFalse(quota.isQuotaExceeded)

    // One more byte pushes over quota: 151B / 1.5s > 100 B/s
    quota.record(1)
    assertTrue(quota.isQuotaExceeded)
    assertFalse(buildFetchSucceeds(createEndPoint(quota)))
  }

  @Test
  def shouldRecoverAfterTimePasses(): Unit = {
    val quota = createConsolidationQuota(100) // 100 bytes/sec
    val endPoint = createEndPoint(quota)

    time.sleep(1000)

    // Exceed the quota
    time.sleep(500)
    quota.record(1)
    quota.record(150)
    assertTrue(quota.isQuotaExceeded)
    assertFalse(buildFetchSucceeds(endPoint))

    // Sleep enough for the sample containing the burst to expire
    time.sleep(10000)
    assertFalse(quota.isQuotaExceeded)
    assertTrue(buildFetchSucceeds(endPoint))
  }

  @Test
  def shouldBeIndependentFromFollowerReplicationQuota(): Unit = {
    val consolidationQuota = createConsolidationQuota(100) // 100 bytes/sec

    val followerQuota = new ReplicationQuotaManager(
      new ReplicationQuotaManagerConfig(10, 1), metrics, QuotaType.FOLLOWER_REPLICATION, time
    )
    followerQuota.updateQuota(new Quota(Long.MaxValue.toDouble, true))
    followerQuota.markThrottled("consolidating-topic")

    time.sleep(1000)

    // Exceed consolidation quota
    time.sleep(500)
    consolidationQuota.record(151)
    assertTrue(consolidationQuota.isQuotaExceeded)

    // Follower quota should be unaffected
    assertFalse(followerQuota.isQuotaExceeded)
  }

  @Test
  def shouldOnlyRecordForThrottledPartitions(): Unit = {
    val quota = createConsolidationQuota(1024)

    assertTrue(quota.isThrottled(tp))
    assertFalse(quota.isThrottled(new TopicPartition("other-topic", 0)))
  }

  @Test
  def shouldExposeByteRateMetric(): Unit = {
    val quota = createConsolidationQuota(10 * 1024 * 1024)

    // Trigger sensor creation (sensors are lazily initialized)
    quota.record(1)

    val metricName = metrics.metricName("byte-rate", QuotaType.DISKLESS_CONSOLIDATION_FETCH.toString,
      "Tracking byte-rate for " + QuotaType.DISKLESS_CONSOLIDATION_FETCH)
    val metric = metrics.metrics.get(metricName)
    assertNotNull(metric, "DisklessConsolidationFetch byte-rate metric should exist")
  }

  @Test
  def shouldSupportDynamicQuotaUpdate(): Unit = {
    val quota = createConsolidationQuota(1000) // 1000 bytes/sec
    val endPoint = createEndPoint(quota)

    time.sleep(1000)

    // Record 500B in 1s → under 1000 B/s
    time.sleep(500)
    quota.record(500)
    time.sleep(500)
    assertFalse(quota.isQuotaExceeded)
    assertTrue(buildFetchSucceeds(endPoint))

    // Tighten quota to 100 B/s
    quota.updateQuota(new Quota(100, true))

    // Record another 500B → now well over 100 B/s
    time.sleep(500)
    quota.record(500)
    time.sleep(500)
    assertTrue(quota.isQuotaExceeded)
    assertFalse(buildFetchSucceeds(endPoint))
  }

  @Test
  def shouldBlockFetchWhenRecordedBytesExceedQuota(): Unit = {
    val quota = createConsolidationQuota(100) // 100 bytes/sec
    val endPoint = createEndPoint(quota)

    time.sleep(1000)

    // Simulate what processPartitionData does: check isThrottled then record
    assertTrue(quota.isThrottled(tp))
    time.sleep(500)
    quota.record(151)
    assertTrue(quota.isQuotaExceeded)

    // Next buildFetch call should be blocked
    assertFalse(buildFetchSucceeds(endPoint))
  }
}
