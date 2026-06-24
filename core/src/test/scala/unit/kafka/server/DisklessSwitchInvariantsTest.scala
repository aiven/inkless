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
import org.apache.kafka.metadata.{InitDisklessLogFields, PartitionRegistration}
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
 *   - Seal state: committed (>= 0) vs pending (CLASSIC_TO_DISKLESS_SWITCH_PENDING = -2)
 *                 vs none (NO_CLASSIC_TO_DISKLESS_START_OFFSET = -1)
 *
 * Key invariants:
 *   - Committed seal → leader HW is advanced to seal offset regardless of checkpoint
 *   - Committed seal → follower fetcher starts only when HW < seal
 *   - Pending seal → partition is sealed, HW not advanced (seal offset unknown);
 *                     follower starts fetcher on new leader epoch
 *   - No switch → partition is sealed, HW not advanced, no fetcher
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

  @ParameterizedTest(name = "leader checkpoint hw={0}, seal={1}, leo={2}")
  @MethodSource(Array("sealedLeaderScenarios"))
  def testSealedLeaderHwInvariant(
      checkpointHw: Long,
      sealOffset: Long,
      localLeo: Long,
      expectedHw: Long,
      expectedLeo: Long): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    replicaManager = createReplicaManager(List(topicName))
    val log = replicaManager.logManager.getOrCreateLog(tp, isNew = true, topicId = Optional.of(topicId))
    populateLocalLogAtLeoAndCheckpointedHwm(replicaManager, tp, log, localLeo, checkpointHw)

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
    assertEquals(expectedLeo, partition.localLogOrException.logEndOffset,
      s"Sealed leader invariant: LEO must be reconciled to expected offset $expectedLeo")
  }

  // --- Invariant: Sealed follower with HW < seal triggers catch-up fetcher ---

  @ParameterizedTest(name = "follower checkpoint hw={0}, seal={1}, expectFetcher={2}")
  @MethodSource(Array("sealedFollowerScenarios"))
  def testSealedFollowerFetcherInvariant(checkpointHw: Long, sealOffset: Long, expectFetcher: Boolean): Unit = {
    val topicName = "switched-topic"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)
    val leaderId = 2
    val leo = if (sealOffset >= 0) sealOffset else 10L

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
    assertFalse(partition.isLeader, "Partition must be follower")

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
    val partitionRecord = new PartitionRecord()
      .setPartitionId(0)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(brokerId, brokerId + 1))
      .setIsr(util.Arrays.asList(brokerId, brokerId + 1))
      .setLeader(brokerId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
    addClassicToDisklessStartOffsetIfPresent(topicName, partitionRecord)
    delta.replay(partitionRecord)
    delta
  }

  private def followerDelta(topicName: String, topicId: Uuid, leaderId: Int): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
    val partitionRecord = new PartitionRecord()
      .setPartitionId(0)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(brokerId, leaderId))
      .setIsr(util.Arrays.asList(brokerId, leaderId))
      .setLeader(leaderId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
    addClassicToDisklessStartOffsetIfPresent(topicName, partitionRecord)
    delta.replay(partitionRecord)
    delta
  }

  private def addClassicToDisklessStartOffsetIfPresent(topicName: String, partitionRecord: PartitionRecord): Unit = {
    val sealOffset = replicaManager.inklessMetadataView().getClassicToDisklessStartOffset(
      new TopicPartition(topicName, partitionRecord.partitionId()))
    if (sealOffset != PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET) {
      partitionRecord.unknownTaggedFields().add(
        InitDisklessLogFields.encodeClassicToDisklessStartOffset(sealOffset))
    }
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
    val props = TestUtils.createBrokerConfig(brokerId, logDirCount = 1)
    props.put(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(new Properties()))
    val sharedState = mock(classOf[SharedState], Answers.RETURNS_DEEP_STUBS)
    when(sharedState.time()).thenReturn(Time.SYSTEM)
    val inklessConfigMap = new util.HashMap[String, Object]()
    // Disable lagging consumer feature — not relevant for these tests
    inklessConfigMap.put("fetch.lagging.consumer.thread.pool.size", Integer.valueOf(0))
    when(sharedState.config()).thenReturn(new InklessConfig(inklessConfigMap))
    when(sharedState.controlPlane()).thenReturn(mock(classOf[ControlPlane]))
    when(sharedState.maybeLaggingFetchStorage()).thenReturn(Optional.empty())
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
        threadNamePrefix: Option[String],
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = {
        mockReplicaFetcherManager.getOrElse(super.createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManager))
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

  import org.apache.kafka.metadata.PartitionRegistration._

  def sealedLeaderScenarios(): java.util.stream.Stream[Arguments] = {
    // (checkpointHw, sealOffset, localLeo, expectedHwAfterApplyDelta, expectedLeoAfterApplyDelta)
    // Committed seal (>= 0): HW is always advanced to sealOffset regardless of checkpoint.
    // The post-restart path (getOrCreatePartition + makeLeader) creates a new Partition object
    // that re-initializes HW from the checkpoint via LazyOffsetCheckpoints. However, since the
    // log already exists, LogManager returns it without re-reading the checkpoint. The seal
    // offset advancement in applyLocalLeadersDelta (sealOffset >= 0 && HW < sealOffset guard)
    // compensates by forcing HW = sealOffset, even when an uncommitted tail must first be
    // truncated from LEO > seal to LEO == seal.
    java.util.stream.Stream.of(
      Arguments.of(0L: java.lang.Long,  10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long),  // unclean shutdown
      Arguments.of(5L: java.lang.Long,  10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long),  // partial flush
      Arguments.of(5L: java.lang.Long,  10L: java.lang.Long, 13L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long),  // stale HW with uncommitted tail
      Arguments.of(10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long, 10L: java.lang.Long),  // clean shutdown
      // Pending seal (-2): partition is sealed. The HW is NOT advanced because the seal
      // offset is not yet committed. The post-restart path does not restore HW from
      // checkpoint for pending seals — the pending fetcher (follower side) will eventually
      // propagate the correct HW when the seal commits.
      Arguments.of(0L: java.lang.Long,  CLASSIC_TO_DISKLESS_SWITCH_PENDING: java.lang.Long, 10L: java.lang.Long, 0L: java.lang.Long, 10L: java.lang.Long),
      Arguments.of(5L: java.lang.Long,  CLASSIC_TO_DISKLESS_SWITCH_PENDING: java.lang.Long, 10L: java.lang.Long, 0L: java.lang.Long, 10L: java.lang.Long),
      // No switch (-1): not realistic today but documents defensive behavior of the
      // post-restart path which seals unconditionally. Relevant if diskless topics
      // eventually use local logs as a read cache.
      Arguments.of(0L: java.lang.Long,  NO_CLASSIC_TO_DISKLESS_START_OFFSET: java.lang.Long, 10L: java.lang.Long, 0L: java.lang.Long, 10L: java.lang.Long),
      Arguments.of(5L: java.lang.Long,  NO_CLASSIC_TO_DISKLESS_START_OFFSET: java.lang.Long, 10L: java.lang.Long, 0L: java.lang.Long, 10L: java.lang.Long),
    )
  }

  def sealedFollowerScenarios(): java.util.stream.Stream[Arguments] = {
    // (checkpointHw, sealOffset, expectFetcher)
    // Committed seal (>= 0): fetcher starts when HW < seal
    java.util.stream.Stream.of(
      Arguments.of(0L: java.lang.Long,  10L: java.lang.Long, true: java.lang.Boolean),   // stale
      Arguments.of(5L: java.lang.Long,  10L: java.lang.Long, true: java.lang.Boolean),   // partial
      Arguments.of(10L: java.lang.Long, 10L: java.lang.Long, false: java.lang.Boolean),  // fresh
      // Pending seal (-2): fetcher starts (new leader epoch triggers catch-up)
      Arguments.of(0L: java.lang.Long,  CLASSIC_TO_DISKLESS_SWITCH_PENDING: java.lang.Long, true: java.lang.Boolean),
      Arguments.of(5L: java.lang.Long,  CLASSIC_TO_DISKLESS_SWITCH_PENDING: java.lang.Long, true: java.lang.Boolean),
      // No switch (-1): defensive — documents post-restart seal behavior (see leader scenarios)
      Arguments.of(0L: java.lang.Long,  NO_CLASSIC_TO_DISKLESS_START_OFFSET: java.lang.Long, false: java.lang.Boolean),
      Arguments.of(5L: java.lang.Long,  NO_CLASSIC_TO_DISKLESS_START_OFFSET: java.lang.Long, false: java.lang.Boolean),
    )
  }
}
