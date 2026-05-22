/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import io.aiven.inkless.common.SharedState
import io.aiven.inkless.config.InklessConfig
import io.aiven.inkless.control_plane.ControlPlane
import kafka.server.metadata.InklessMetadataView
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.metadata.{PartitionRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image._
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.Answers
import kafka.server.metadata.KRaftMetadataCache

import java.io.File
import java.util
import java.util.{Collections, Optional, Properties}
import scala.jdk.CollectionConverters._

/**
 * Invariant tests for the diskless switch recovery paths.
 *
 * After applyDelta completes for a sealed partition, certain invariants must
 * hold regardless of the checkpoint state at restart time. These tests verify
 * those invariants across a matrix of scenarios:
 *
 *   - Role after restart: leader vs follower
 *   - Checkpoint HW: stale (0, partial) vs fresh (== seal)
 *   - Seal state: committed (>= 0) vs pending (-2) vs none (-1)
 */
class DisklessSwitchInvariantsTest {

  private val time = new MockTime()
  private val metrics = new Metrics()
  private val brokerId = 1
  private var replicaManager: ReplicaManager = _

  @AfterEach
  def tearDown(): Unit = {
    if (replicaManager != null) {
      replicaManager.shutdown(checkpointHW = false)
    }
    metrics.close()
  }

  // --- Invariant: Sealed leader HW >= seal offset ---

  @ParameterizedTest(name = "leader checkpoint hw={0}, seal={1}")
  @MethodSource(Array("sealedLeaderScenarios"))
  def testSealedLeaderHwInvariant(checkpointHw: Long, sealOffset: Long, expectedHw: Long): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val leo = sealOffset // LEO == seal for a fully-replicated partition

    replicaManager = createReplicaManager(List(topicName))
    val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
    populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo, checkpointHw)

    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
      .thenReturn(sealOffset)

    val delta = leaderDelta(topicName, topicId)
    replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

    // --- Assert invariant ---
    val partition = replicaManager.getPartition(tp).asInstanceOf[HostedPartition.Online].partition
    assertTrue(partition.isSealed, "Partition must be sealed")
    assertTrue(partition.isLeader, "Partition must be leader")
    assertEquals(expectedHw, partition.localLogOrException.highWatermark,
      s"Sealed leader invariant: HW must be >= sealOffset ($sealOffset) " +
      s"regardless of checkpoint state ($checkpointHw)")
  }

  // --- Invariant: Sealed follower with HW < seal triggers catch-up fetcher ---

  @ParameterizedTest(name = "follower checkpoint hw={0}, seal={1}, expectFetcher={2}")
  @MethodSource(Array("sealedFollowerScenarios"))
  def testSealedFollowerFetcherInvariant(checkpointHw: Long, sealOffset: Long, expectFetcher: Boolean): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val leaderId = 2
    val leo = sealOffset

    val mockFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(mockFetcherManager.removeFetcherForPartitions(any())).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    replicaManager = spy(createReplicaManager(
      List(topicName),
      mockReplicaFetcherManager = Some(mockFetcherManager)
    ))
    val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
    populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, leo, checkpointHw)

    when(replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(tp))
      .thenReturn(sealOffset)

    val delta = followerDelta(topicName, topicId, leaderId)
    replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))

    // --- Assert invariant ---
    val partition = replicaManager.getPartition(tp).asInstanceOf[HostedPartition.Online].partition
    assertTrue(partition.isSealed, "Partition must be sealed")

    if (expectFetcher) {
      verify(mockFetcherManager).addFetcherForPartitions(any())
    } else {
      verify(mockFetcherManager, never()).addFetcherForPartitions(any())
    }
  }

  // --- Test data ---

  private def leaderDelta(topicName: String, topicId: Uuid): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
    delta.replay(new PartitionRecord()
      .setPartitionId(0)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
      .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
      .setLeader(brokerId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
    )
    delta
  }

  private def followerDelta(topicName: String, topicId: Uuid, leaderId: Int): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
    delta.replay(new PartitionRecord()
      .setPartitionId(0)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(brokerId, leaderId))
      .setIsr(util.Arrays.asList(brokerId, leaderId))
      .setLeader(leaderId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
    )
    delta
  }

  private def populateLocalLogAtLeoAndCheckpointedHwm(
      replicaManager: ReplicaManager, tp: TopicPartition,
      log: UnifiedLog, leo: Long, hw: Long): Unit = {
    if (leo > 0) {
      val simpleRecords = (0L until leo).map(i => new SimpleRecord(s"msg-$i".getBytes)).toArray
      val records = MemoryRecords.withRecords(Compression.NONE, 0, simpleRecords: _*)
      log.appendAsFollower(records, 0)
    }
    val checkpoint = replicaManager.highWatermarkCheckpoints(log.parentDir)
    checkpoint.write(Collections.singletonMap(tp, java.lang.Long.valueOf(hw)))
    log.maybeUpdateHighWatermark(hw)
  }

  private def createReplicaManager(
    disklessTopics: Seq[String],
    mockReplicaFetcherManager: Option[ReplicaFetcherManager] = None
  ): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(brokerId, logDirCount = 2)
    props.put(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(new Properties()))
    val sharedState = mock(classOf[SharedState], Answers.RETURNS_DEEP_STUBS)
    when(sharedState.time()).thenReturn(Time.SYSTEM)
    when(sharedState.config()).thenReturn(new InklessConfig(new util.HashMap[String, Object]()))
    when(sharedState.controlPlane()).thenReturn(mock(classOf[ControlPlane]))
    val inklessMetadata = mock(classOf[InklessMetadataView])
    when(inklessMetadata.isDisklessTopic(any())).thenReturn(false)
    disklessTopics.foreach(t => when(inklessMetadata.isDisklessTopic(t)).thenReturn(true))
    when(sharedState.metadata()).thenReturn(inklessMetadata)

    val logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = time.scheduler,
      logManager = mockLogMgr,
      quotaManagers = mock(classOf[QuotaFactory.QuotaManagers]),
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = logDirFailureChannel,
      alterPartitionManager = mock(classOf[AlterPartitionManager]),
      inklessSharedState = Some(sharedState),
      inklessMetadataView = Some(inklessMetadata),
    ) {
      override protected def createReplicaFetcherManager(
        metrics: Metrics,
        time: Time,
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = {
        mockReplicaFetcherManager.getOrElse(super.createReplicaFetcherManager(metrics, time, quotaManager))
      }
    }
  }

  private def imageFromTopics(topicsImage: TopicsImage): MetadataImage = {
    new MetadataImage(
      new MetadataProvenance(100L, 10, 1000L, true),
      new FeaturesImage(Collections.emptyMap(), MetadataVersion.latestProduction()),
      ClusterImageTest.IMAGE1,
      topicsImage,
      ConfigurationsImage.EMPTY,
      ClientQuotasImage.EMPTY,
      ProducerIdsImage.EMPTY,
      AclsImage.EMPTY,
      ScramImage.EMPTY,
      DelegationTokenImage.EMPTY
    )
  }
}

object DisklessSwitchInvariantsTest {

  def sealedLeaderScenarios(): java.util.stream.Stream[Arguments] = {
    // (checkpointHw, sealOffset, expectedHwAfterApplyDelta)
    java.util.stream.Stream.of(
      Arguments.of(0L: java.lang.Long,  10L: java.lang.Long, 10L: java.lang.Long),  // unclean shutdown
      Arguments.of(5L: java.lang.Long,  10L: java.lang.Long, 10L: java.lang.Long),  // partial flush
      Arguments.of(10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long),  // clean shutdown
    )
  }

  def sealedFollowerScenarios(): java.util.stream.Stream[Arguments] = {
    // (checkpointHw, sealOffset, expectFetcher)
    java.util.stream.Stream.of(
      Arguments.of(0L: java.lang.Long,  10L: java.lang.Long, true: java.lang.Boolean),   // stale
      Arguments.of(5L: java.lang.Long,  10L: java.lang.Long, true: java.lang.Boolean),   // partial
      Arguments.of(10L: java.lang.Long, 10L: java.lang.Long, false: java.lang.Boolean),  // fresh
    )
  }
}
