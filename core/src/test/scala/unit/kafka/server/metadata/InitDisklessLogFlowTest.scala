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

package kafka.server.metadata

import io.aiven.inkless.control_plane.{ControlPlane, InitDisklessLogResponse => CpInitResponse}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{AwaitingMetadata, HostedPartition, InitDisklessLogManager, InitDisklessLogState, MockInitDisklessLogChannelManager, ReplicaManager, SendingToController}
import kafka.server.share.SharePartitionManager
import kafka.utils.TestUtils
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionChangeRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.image.{AclsImage, ClientQuotasImage, ClusterImageTest, ConfigurationsImage, DelegationTokenImage, FeaturesImage, MetadataDelta, MetadataImage, MetadataProvenance, ProducerIdsImage, ScramImage, TopicsDelta, TopicsImage, VirtualClustersImage}
import org.apache.kafka.image.loader.LogDeltaManifest
import org.apache.kafka.metadata.InitDisklessLogFields
import org.apache.kafka.metadata.publisher.{AclPublisher, DelegationTokenPublisher, DynamicClientQuotaPublisher, ScramPublisher}
import org.apache.kafka.raft.LeaderAndEpoch
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.fault.FaultHandler
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, times, verify, when}

import java.util
import scala.jdk.CollectionConverters._

class InitDisklessLogFlowTest {

  private case class TestContext(
    config: kafka.server.KafkaConfig,
    metadataCache: KRaftMetadataCache,
    logManager: LogManager,
    replicaManager: ReplicaManager,
    initDisklessLogManager: InitDisklessLogManager,
    metadataPublisher: BrokerMetadataPublisher,
    controlPlane: ControlPlane,
    channelManager: MockInitDisklessLogChannelManager,
    time: MockTime,
    scheduler: MockScheduler
  )

  @Test
  def testEndToEndFlowFromSealingToControlPlaneInit(): Unit = {
    val ctx = newContext()
    val topicName = "integration-e2e-seal-to-control-plane-init"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(ctx.config.brokerId)
        .setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      ctx.replicaManager.applyDelta(createDelta, createImage)

      val partition = ctx.replicaManager.getPartitionOrException(tp)
      assertFalse(partition.isSealed)

      // Step 1: enable diskless and trigger sealing + controller init request path.
      ctx.metadataPublisher._firstPublish = false
      when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val enableDisklessDelta = new MetadataDelta(createImage)
      enableDisklessDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val disklessImage = withClusterBrokers(enableDisklessDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(enableDisklessDelta, disklessImage, metadataManifest())

      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()
      assertTrue(partition.isSealed)
      assertEquals(1, ctx.channelManager.requests.size())

      // Simulate successful controller response.
      ctx.channelManager.requests.poll().complete(new org.apache.kafka.common.message.InitDisklessLogResponseData().setTopics(util.List.of(
        new org.apache.kafka.common.message.InitDisklessLogResponseData.TopicResponse()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new org.apache.kafka.common.message.InitDisklessLogResponseData.PartitionResponse()
              .setPartitionId(0)
              .setErrorCode(org.apache.kafka.common.protocol.Errors.NONE.code())
          ))
      )))

      // Step 2: apply committed PartitionChangeRecord with diskless fields.
      val pcrDelta = new MetadataDelta(disklessImage)
      val pcr = new PartitionChangeRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setIsr(util.Arrays.asList(0, 1))
      pcr.unknownTaggedFields().add(InitDisklessLogFields.encodeDisklessStartOffset(100L))
      pcr.unknownTaggedFields().add(InitDisklessLogFields.encodeProducerStates(util.List.of()))
      pcrDelta.replay(pcr)
      val pcrImage = withClusterBrokers(pcrDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(pcrDelta, pcrImage, metadataManifest())
      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()

      // Final check: metadata-triggered control-plane init executed.
      verify(ctx.controlPlane, times(1)).initDisklessLog(any())
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateSealsAndRegistersExistingClassicLeader(): Unit = {
    // Given a classic topic where this broker is already the leader.
    val ctx = newContext()
    val topicName = "integration-classic-to-diskless"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(false)

    try {
      // And Given the classic topic partition exists as local leader.
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(ctx.config.brokerId)
        .setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      ctx.replicaManager.applyDelta(createDelta, createImage)

      val partition = ctx.replicaManager.getPartitionOrException(tp)
      assertTrue(partition.isLeader)
      assertFalse(partition.isSealed)
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))

      // When diskless is enabled for the existing topic.
      ctx.metadataPublisher._firstPublish = false
      when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val disklessDelta = new MetadataDelta(createImage)
      disklessDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val updatedImage = withClusterBrokers(disklessDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(disklessDelta, updatedImage, metadataManifest())

      // Then the local leader is sealed and tracked for init in SendingToController.
      assertTrue(partition.isSealed)
      assertTrackedStates(ctx, Map(tp -> classOf[SendingToController]))

      // And when linger elapses.
      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()

      // Then exactly one batched request is emitted and tracking state is unchanged.
      assertEquals(1, ctx.channelManager.requests.size())
      assertSingleTopicRequest(ctx, topicId, Set(0))
      assertTrackedStates(ctx, Map(tp -> classOf[SendingToController]))
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateDoesNotSealOrTrackWhenDisklessAlreadyEnabled(): Unit = {
    // Given a classic topic where this broker is already the leader.
    val ctx = newContext()
    val topicName = "integration-diskless-already-enabled-no-transition"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(false)

    try {
      // And Given the classic partition exists as local leader.
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(ctx.config.brokerId)
        .setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      ctx.replicaManager.applyDelta(createDelta, createImage)

      val partition = ctx.replicaManager.getPartitionOrException(tp)
      assertTrue(partition.isLeader)
      assertFalse(partition.isSealed)
      assertTrackedStates(ctx, Map.empty)

      // And Given previous metadata image already had diskless=true.
      val alreadyDisklessDelta = new MetadataDelta(createImage)
      alreadyDisklessDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val disklessImage = withClusterBrokers(alreadyDisklessDelta.apply(MetadataProvenance.EMPTY))

      // When a config delta keeps diskless=true (no classic->diskless transition).
      ctx.metadataPublisher._firstPublish = false
      val noTransitionDelta = new MetadataDelta(disklessImage)
      noTransitionDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val updatedImage = withClusterBrokers(noTransitionDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(noTransitionDelta, updatedImage, metadataManifest())

      // Then the local leader is not sealed and no init tracking is created.
      assertFalse(partition.isSealed)
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))

      // And after linger, no request is emitted.
      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()
      assertEquals(0, ctx.channelManager.requests.size())
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateDoesNotRegisterBrandNewDisklessTopic(): Unit = {
    // Given a brand-new topic created directly as diskless.
    val ctx = newContext()
    val topicName = "integration-brand-new-diskless"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)

    try {
      // When the topic is created and diskless is enabled in the same metadata delta.
      ctx.metadataPublisher._firstPublish = false
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      delta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(ctx.config.brokerId))
      delta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("false"))
      delta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))

      val image = withClusterBrokers(delta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(delta, image, metadataManifest())

      // Then no local classic partition is created and nothing is tracked for init.
      assertEquals(HostedPartition.None, ctx.replicaManager.getPartition(tp))
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))
      assertEquals(0, ctx.channelManager.requests.size())
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateFollowerTransitionRemovesFromInitTracking(): Unit = {
    // Given a classic topic that starts with this broker as follower.
    val ctx = newContext()
    val topicName = "integration-diskless-follower-transition"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      // And Given the partition exists locally but leader is another broker.
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setPartitionId(0).setTopicId(topicId)
        .setReplicas(util.Arrays.asList(0, 1)).setIsr(util.Arrays.asList(0, 1))
        .setLeader(1).setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      ctx.replicaManager.applyDelta(createDelta, createImage)
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))

      // When diskless is enabled and leadership moves to this broker.
      ctx.metadataPublisher._firstPublish = false
      when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val toLeaderDelta = new MetadataDelta(createImage)
      toLeaderDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      toLeaderDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0).setTopicId(topicId)
        .setLeader(ctx.config.brokerId).setIsr(util.Arrays.asList(0, 1)))
      val leaderImage = withClusterBrokers(toLeaderDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(toLeaderDelta, leaderImage, metadataManifest())

      // Then the partition is tracked in SendingToController.
      assertTrackedStates(ctx, Map(tp -> classOf[SendingToController]))

      // When leadership moves away from this broker.
      val toFollowerDelta = new MetadataDelta(leaderImage)
      toFollowerDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0).setTopicId(topicId)
        .setLeader(1).setIsr(util.Arrays.asList(0, 1)))
      val followerImage = withClusterBrokers(toFollowerDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(toFollowerDelta, followerImage, metadataManifest())

      // Then the partition is removed from tracking.
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))

      // And when linger elapses, no request is emitted because tracking was cleaned up.
      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()
      assertEquals(0, ctx.channelManager.requests.size())
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateEnablesDisklessAndElectsNewLeaderForExistingClassicTopic(): Unit = {
    // Given an existing classic topic where another broker is the current leader.
    val ctx = newContext()
    val topicName = "integration-classic-enable-diskless-and-new-leader"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      // And Given the partition exists on this broker as follower.
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(1)
        .setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      ctx.replicaManager.applyDelta(createDelta, createImage)

      val partition = ctx.replicaManager.getPartitionOrException(tp)
      assertFalse(partition.isLeader)
      assertFalse(partition.isSealed)
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))

      // When diskless is enabled and leadership moves to this broker in the same image.
      ctx.metadataPublisher._firstPublish = false
      when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val migrationDelta = new MetadataDelta(createImage)
      migrationDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      migrationDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0).setTopicId(topicId)
        .setLeader(ctx.config.brokerId).setIsr(util.Arrays.asList(0, 1)))
      val migratedImage = withClusterBrokers(migrationDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(migrationDelta, migratedImage, metadataManifest())

      // Then the new local leader is sealed and tracked in SendingToController.
      assertTrue(partition.isLeader)
      assertTrue(partition.isSealed)
      assertTrackedStates(ctx, Map(tp -> classOf[SendingToController]))

      // And when linger elapses.
      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()

      // Then one init request is emitted and state stays SendingToController until response.
      assertEquals(1, ctx.channelManager.requests.size())
      assertSingleTopicRequest(ctx, topicId, Set(0))
      assertTrackedStates(ctx, Map(tp -> classOf[SendingToController]))
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateTracksOnlyLocalLeadersAcrossTwoBrokersForThreePartitions(): Unit = {
    // Given two brokers and a classic topic with three partitions.
    val broker0Ctx = newContext(brokerId = 0)
    val broker1Ctx = newContext(brokerId = 1)
    val topicName = "integration-two-brokers-three-partitions"
    val topicId = Uuid.randomUuid()
    val tp0 = new TopicPartition(topicName, 0)
    val tp1 = new TopicPartition(topicName, 1)
    val tp2 = new TopicPartition(topicName, 2)

    when(broker0Ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)
    when(broker1Ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      // And Given partition leadership is split: one partition on broker0, two partitions on broker1.
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(0).setLeaderEpoch(0).setPartitionEpoch(0))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(1).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(1).setLeaderEpoch(0).setPartitionEpoch(0))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(2).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(1).setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      broker0Ctx.replicaManager.applyDelta(createDelta, createImage)
      broker1Ctx.replicaManager.applyDelta(createDelta, createImage)

      assertTrackedStates(broker0Ctx, Map.empty)
      assertTrackedStates(broker1Ctx, Map.empty)

      // When diskless is enabled for that existing topic.
      broker0Ctx.metadataPublisher._firstPublish = false
      broker1Ctx.metadataPublisher._firstPublish = false
      when(broker0Ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      when(broker1Ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val migrationDelta = new MetadataDelta(createImage)
      migrationDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val migratedImage = withClusterBrokers(migrationDelta.apply(MetadataProvenance.EMPTY))
      broker0Ctx.metadataPublisher.onMetadataUpdate(migrationDelta, migratedImage, metadataManifest())
      broker1Ctx.metadataPublisher.onMetadataUpdate(migrationDelta, migratedImage, metadataManifest())

      // Then each broker tracks only local-leader partitions, in SendingToController state.
      assertTrackedStates(broker0Ctx, Map(tp0 -> classOf[SendingToController]))
      assertTrackedStates(broker1Ctx, Map(tp1 -> classOf[SendingToController], tp2 -> classOf[SendingToController]))
      assertEquals(None, broker0Ctx.initDisklessLogManager.getInitState(tp1))
      assertEquals(None, broker0Ctx.initDisklessLogManager.getInitState(tp2))
      assertEquals(None, broker1Ctx.initDisklessLogManager.getInitState(tp0))

      // And when linger elapses on both brokers.
      broker0Ctx.time.sleep(broker0Ctx.initDisklessLogManager.lingerMs)
      broker0Ctx.scheduler.tick()
      broker1Ctx.time.sleep(broker1Ctx.initDisklessLogManager.lingerMs)
      broker1Ctx.scheduler.tick()

      // Then both brokers emit one batched request each and tracked states remain SendingToController.
      assertEquals(1, broker0Ctx.channelManager.requests.size())
      assertEquals(1, broker1Ctx.channelManager.requests.size())
      assertSingleTopicRequest(broker0Ctx, topicId, Set(0))
      assertSingleTopicRequest(broker1Ctx, topicId, Set(1, 2))
      assertTrackedStates(broker0Ctx, Map(tp0 -> classOf[SendingToController]))
      assertTrackedStates(broker1Ctx, Map(tp1 -> classOf[SendingToController], tp2 -> classOf[SendingToController]))
    } finally {
      shutdown(broker0Ctx)
      shutdown(broker1Ctx)
    }
  }

  @Test
  def testOnMetadataUpdateEnableDisklessWithLeaderElectionInSameImageAcrossTwoBrokers(): Unit = {
    // Given two brokers and an existing classic topic with broker0 as current leader.
    val broker0Ctx = newContext(brokerId = 0)
    val broker1Ctx = newContext(brokerId = 1)
    val topicName = "integration-two-brokers-enable-diskless-and-new-leader"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(broker0Ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)
    when(broker1Ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      // And Given both brokers apply classic-topic metadata.
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(0).setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      broker0Ctx.replicaManager.applyDelta(createDelta, createImage)
      broker1Ctx.replicaManager.applyDelta(createDelta, createImage)

      assertTrackedStates(broker0Ctx, Map.empty)
      assertTrackedStates(broker1Ctx, Map.empty)

      // When diskless is enabled and leadership moves from broker0 to broker1 in the same image.
      broker0Ctx.metadataPublisher._firstPublish = false
      broker1Ctx.metadataPublisher._firstPublish = false
      when(broker0Ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      when(broker1Ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val migrationDelta = new MetadataDelta(createImage)
      migrationDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      migrationDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0).setTopicId(topicId)
        .setLeader(1).setIsr(util.Arrays.asList(0, 1)))
      val migratedImage = withClusterBrokers(migrationDelta.apply(MetadataProvenance.EMPTY))
      broker0Ctx.metadataPublisher.onMetadataUpdate(migrationDelta, migratedImage, metadataManifest())
      broker1Ctx.metadataPublisher.onMetadataUpdate(migrationDelta, migratedImage, metadataManifest())

      // Then only the new leader broker tracks the partition in SendingToController state.
      assertTrackedStates(broker0Ctx, Map.empty)
      assertEquals(None, broker0Ctx.initDisklessLogManager.getInitState(tp))
      assertTrackedStates(broker1Ctx, Map(tp -> classOf[SendingToController]))

      // And when linger elapses on both brokers.
      broker0Ctx.time.sleep(broker0Ctx.initDisklessLogManager.lingerMs)
      broker0Ctx.scheduler.tick()
      broker1Ctx.time.sleep(broker1Ctx.initDisklessLogManager.lingerMs)
      broker1Ctx.scheduler.tick()

      // Then only broker1 emits an init request.
      assertEquals(0, broker0Ctx.channelManager.requests.size())
      assertEquals(1, broker1Ctx.channelManager.requests.size())
      assertSingleTopicRequest(broker1Ctx, topicId, Set(0))
      assertTrackedStates(broker1Ctx, Map(tp -> classOf[SendingToController]))
    } finally {
      shutdown(broker0Ctx)
      shutdown(broker1Ctx)
    }
  }

  @Test
  def testOnMetadataUpdatePartitionChangeRecordWithDisklessFieldsTriggersControlPlaneInit(): Unit = {
    val ctx = newContext()
    val topicName = "integration-diskless-pcr-triggers-control-plane"
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition(topicName, 0)

    when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(ctx.config.brokerId)
        .setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      ctx.replicaManager.applyDelta(createDelta, createImage)

      ctx.metadataPublisher._firstPublish = false
      when(ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val disklessDelta = new MetadataDelta(createImage)
      disklessDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val disklessImage = withClusterBrokers(disklessDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(disklessDelta, disklessImage, metadataManifest())

      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()
      assertEquals(1, ctx.channelManager.requests.size())
      ctx.channelManager.requests.poll().complete(new org.apache.kafka.common.message.InitDisklessLogResponseData().setTopics(util.List.of(
        new org.apache.kafka.common.message.InitDisklessLogResponseData.TopicResponse()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new org.apache.kafka.common.message.InitDisklessLogResponseData.PartitionResponse()
              .setPartitionId(0)
              .setErrorCode(org.apache.kafka.common.protocol.Errors.NONE.code())
          ))
      )))
      assertTrackedStates(ctx, Map(tp -> classOf[AwaitingMetadata]))

      val pcrDelta = new MetadataDelta(disklessImage)
      val pcr = new PartitionChangeRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setIsr(util.Arrays.asList(0, 1))
      pcr.unknownTaggedFields().add(InitDisklessLogFields.encodeDisklessStartOffset(100L))
      pcr.unknownTaggedFields().add(InitDisklessLogFields.encodeProducerStates(util.List.of()))
      pcrDelta.replay(pcr)
      val pcrImage = withClusterBrokers(pcrDelta.apply(MetadataProvenance.EMPTY))
      ctx.metadataPublisher.onMetadataUpdate(pcrDelta, pcrImage, metadataManifest())

      ctx.time.sleep(ctx.initDisklessLogManager.lingerMs)
      ctx.scheduler.tick()

      verify(ctx.controlPlane, times(1)).initDisklessLog(any())
      assertTrackedStates(ctx, Map.empty)
      assertEquals(None, ctx.initDisklessLogManager.getInitState(tp))
    } finally {
      shutdown(ctx)
    }
  }

  @Test
  def testOnMetadataUpdateOnlyLeaderInvokesControlPlaneAfterCommittedMetadata(): Unit = {
    val broker0Ctx = newContext(brokerId = 0)
    val broker1Ctx = newContext(brokerId = 1)
    val topicName = "integration-follower-can-init-control-plane"
    val topicId = Uuid.randomUuid()

    when(broker0Ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)
    when(broker1Ctx.replicaManager.inklessMetadataView().isDisklessTopic(anyString())).thenReturn(false)

    try {
      val createDelta = new TopicsDelta(TopicsImage.EMPTY)
      createDelta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))
      createDelta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(0).setReplicas(util.Arrays.asList(0, 1))
        .setIsr(util.Arrays.asList(0, 1)).setLeader(0).setLeaderEpoch(0).setPartitionEpoch(0))
      val createImage = imageFromTopics(createDelta.apply())
      broker0Ctx.replicaManager.applyDelta(createDelta, createImage)
      broker1Ctx.replicaManager.applyDelta(createDelta, createImage)

      broker0Ctx.metadataPublisher._firstPublish = false
      broker1Ctx.metadataPublisher._firstPublish = false
      when(broker0Ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      when(broker1Ctx.replicaManager.inklessMetadataView().isDisklessTopic(topicName)).thenReturn(true)
      val disklessDelta = new MetadataDelta(createImage)
      disklessDelta.replay(new ConfigRecord()
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setResourceName(topicName)
        .setName(TopicConfig.DISKLESS_ENABLE_CONFIG)
        .setValue("true"))
      val disklessImage = withClusterBrokers(disklessDelta.apply(MetadataProvenance.EMPTY))
      broker0Ctx.metadataPublisher.onMetadataUpdate(disklessDelta, disklessImage, metadataManifest())
      broker1Ctx.metadataPublisher.onMetadataUpdate(disklessDelta, disklessImage, metadataManifest())

      broker0Ctx.time.sleep(broker0Ctx.initDisklessLogManager.lingerMs)
      broker0Ctx.scheduler.tick()
      assertEquals(1, broker0Ctx.channelManager.requests.size())
      broker0Ctx.channelManager.requests.poll().complete(new org.apache.kafka.common.message.InitDisklessLogResponseData().setTopics(util.List.of(
        new org.apache.kafka.common.message.InitDisklessLogResponseData.TopicResponse()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new org.apache.kafka.common.message.InitDisklessLogResponseData.PartitionResponse()
              .setPartitionId(0)
              .setErrorCode(org.apache.kafka.common.protocol.Errors.NONE.code())
          ))
      )))

      val pcrDelta = new MetadataDelta(disklessImage)
      val pcr = new PartitionChangeRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setIsr(util.Arrays.asList(0, 1))
      pcr.unknownTaggedFields().add(InitDisklessLogFields.encodeDisklessStartOffset(100L))
      pcr.unknownTaggedFields().add(InitDisklessLogFields.encodeProducerStates(util.List.of()))
      pcrDelta.replay(pcr)
      val pcrImage = withClusterBrokers(pcrDelta.apply(MetadataProvenance.EMPTY))
      broker0Ctx.metadataPublisher.onMetadataUpdate(pcrDelta, pcrImage, metadataManifest())
      broker1Ctx.metadataPublisher.onMetadataUpdate(pcrDelta, pcrImage, metadataManifest())

      broker0Ctx.time.sleep(broker0Ctx.initDisklessLogManager.lingerMs)
      broker1Ctx.time.sleep(broker1Ctx.initDisklessLogManager.lingerMs)
      broker0Ctx.scheduler.tick()
      broker1Ctx.scheduler.tick()

      verify(broker0Ctx.controlPlane, times(1)).initDisklessLog(any())
      verify(broker1Ctx.controlPlane, times(0)).initDisklessLog(any())
    } finally {
      shutdown(broker0Ctx)
      shutdown(broker1Ctx)
    }
  }

  private def newContext(brokerId: Int = 0): TestContext = {
    val config = kafka.server.KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))
    val metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.getAllTopics()).thenReturn(util.Collections.emptySet())
    when(metadataCache.getAliveBrokerNodes(any())).thenReturn(util.Arrays.asList(new Node(0, "host0", 9092), new Node(1, "host1", 9093)))
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    when(metadataCache.topicConfig(anyString())).thenReturn(new java.util.Properties())

    val time = new MockTime()
    val scheduler = new MockScheduler(time)
    val channelManager = new MockInitDisklessLogChannelManager()
    val controlPlane = mock(classOf[ControlPlane])
    when(controlPlane.initDisklessLog(any())).thenReturn(util.List.of(CpInitResponse.success()))
    val initDisklessLogManager = new InitDisklessLogManager(
      controllerChannelManager = channelManager,
      controlPlane = controlPlane,
      scheduler = scheduler,
      brokerId = config.brokerId,
      brokerEpochSupplier = () => 1L
    )

    val logManager = TestUtils.createLogManager(
      config.logDirs.asScala.map(new java.io.File(_)),
      new LogConfig(new java.util.Properties()))
    val replicaManager = new ReplicaManager(
      config = config,
      metrics = new org.apache.kafka.common.metrics.Metrics(),
      time = time,
      scheduler = scheduler,
      logManager = logManager,
      quotaManagers = mock(classOf[QuotaManagers]),
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = mock(classOf[kafka.server.AlterPartitionManager]),
      inklessMetadataView = Some(mock(classOf[InklessMetadataView])),
      initDisklessLogManager = Some(initDisklessLogManager)
    )
    val faultHandler = mock(classOf[FaultHandler])
    val metadataPublisher = new BrokerMetadataPublisher(
      config,
      metadataCache,
      logManager,
      replicaManager,
      mock(classOf[org.apache.kafka.coordinator.group.GroupCoordinator]),
      mock(classOf[TransactionCoordinator]),
      mock(classOf[org.apache.kafka.coordinator.share.ShareCoordinator]),
      mock(classOf[SharePartitionManager]),
      mock(classOf[DynamicConfigPublisher]),
      mock(classOf[DynamicClientQuotaPublisher]),
      mock(classOf[DynamicTopicClusterQuotaPublisher]),
      mock(classOf[ScramPublisher]),
      mock(classOf[DelegationTokenPublisher]),
      mock(classOf[AclPublisher]),
      faultHandler,
      faultHandler
    )

    TestContext(config, metadataCache, logManager, replicaManager, initDisklessLogManager, metadataPublisher, controlPlane, channelManager, time, scheduler)
  }

  private def shutdown(ctx: TestContext): Unit = {
    ctx.replicaManager.shutdown(checkpointHW = false)
    ctx.logManager.shutdown(-1L)
  }

  private def assertTrackedStates(
    ctx: TestContext,
    expected: Map[TopicPartition, Class[_ <: InitDisklessLogState]]
  ): Unit = {
    assertEquals(expected.keySet, ctx.initDisklessLogManager.getTrackedPartitions)
    expected.foreach { case (tp, expectedStateClass) =>
      val actualState = ctx.initDisklessLogManager.getInitState(tp)
      assertTrue(
        actualState.exists(expectedStateClass.isInstance),
        s"Expected state ${expectedStateClass.getSimpleName} for $tp but was $actualState"
      )
    }
  }

  private def assertSingleTopicRequest(
    ctx: TestContext,
    expectedTopicId: Uuid,
    expectedPartitions: Set[Int]
  ): Unit = {
    val requestData = ctx.channelManager.requests.peek().requestData
    assertEquals(ctx.config.brokerId, requestData.brokerId())
    assertEquals(1, requestData.topics().size())
    val topicData = requestData.topics().get(0)
    assertEquals(expectedTopicId, topicData.topicId())
    val partitionIds = topicData.partitions().asScala.map(_.partitionId()).toSet
    assertEquals(expectedPartitions, partitionIds)
  }

  private def metadataManifest(): LogDeltaManifest = {
    LogDeltaManifest.newBuilder()
      .provenance(MetadataProvenance.EMPTY)
      .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
      .numBatches(1)
      .elapsedNs(100)
      .numBytes(42)
      .build()
  }

  private def withClusterBrokers(image: MetadataImage): MetadataImage = {
    new MetadataImage(
      image.provenance(),
      image.features(),
      ClusterImageTest.IMAGE1,
      image.topics(),
      image.configs(),
      image.clientQuotas(),
      image.producerIds(),
      image.acls(),
      image.scram(),
      image.delegationTokens(),
      image.virtualClusters()
    )
  }

  private def imageFromTopics(topicsImage: TopicsImage): MetadataImage = {
    val featuresImageLatest = new FeaturesImage(
      util.Collections.emptyMap(),
      MetadataVersion.latestProduction())
    new MetadataImage(
      new MetadataProvenance(100L, 10, 1000L, true),
      featuresImageLatest,
      ClusterImageTest.IMAGE1,
      topicsImage,
      ConfigurationsImage.EMPTY,
      ClientQuotasImage.EMPTY,
      ProducerIdsImage.EMPTY,
      AclsImage.EMPTY,
      ScramImage.EMPTY,
      DelegationTokenImage.EMPTY,
      VirtualClustersImage.EMPTY
    )
  }
}
