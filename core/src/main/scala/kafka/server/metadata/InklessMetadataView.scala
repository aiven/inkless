/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package kafka.server.metadata

import io.aiven.inkless.control_plane.MetadataView
import kafka.utils.Logging
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.metadata.{KRaftMetadataCache, PartitionRegistration, Replicas}
import org.apache.kafka.storage.internals.log.LogConfig

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.Properties
import scala.jdk.CollectionConverters._

class InklessMetadataView(val metadataCache: KRaftMetadataCache, val defaultConfig: Supplier[util.Map[String, Object]]) extends MetadataView with Logging {

  /**
   * Cached LogConfig per topic, analogous to how classic Kafka stores a LogConfig in each
   * LocalLog instance (via LogManager -> UnifiedLog -> LocalLog.config).
   *
   * Without this cache, every produce request would reconstruct a LogConfig via
   * LogConfig.fromProps — an expensive operation that parses and validates 40+ config fields.
   *
   * The cache is populated lazily on first access via [[getTopicConfig]] and kept up to date by:
   * - [[updateTopicConfig]]: called from TopicConfigHandler when a topic's config changes.
   * - [[reconfigureDefaultLogConfig]]: called from DynamicInklessLogConfig when broker-level
   *   defaults change, rebuilding all cached entries with new defaults while preserving
   *   topic-specific overrides.
   * - [[removeTopicConfig]]: called from BrokerMetadataPublisher when a topic is deleted.
   */
  private val topicConfigs = new ConcurrentHashMap[String, LogConfig]()

  private[metadata] def getDefaultConfig: util.Map[String, Object] = {
    // Filter out null values as they break LogConfig initialization using Properties.putAll
    defaultConfig.get().asScala.filter(_._2 != null).asJava
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): util.List[Node] = {
    metadataCache.getAliveBrokerNodes(listenerName)
  }

  // Only method requiring specific KRaftMetadataCache functionality.
  // If we could refactor RetentionEnforcement to not require this, we could use the MetadataView interface directly.
  override def getBrokerCount: Integer = metadataCache.currentImage().cluster().brokers().size()

  override def getTopicId(topicName: String): Uuid = {
    metadataCache.getTopicId(topicName)
  }

  override def isDisklessTopic(topicName: String): Boolean =
    isDiskless(topicOverrides(metadataCache.currentImage(), topicName))

  override def isRemoteStorageEnabled(topicName: String): Boolean =
    flag(topicOverrides(metadataCache.currentImage(), topicName), TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)

  override def isConsolidatingDisklessTopic(topicName: String): Boolean =
    isConsolidating(topicOverrides(metadataCache.currentImage(), topicName))

  override def getDisklessTopicPartitions: util.Set[TopicIdPartition] =
    topicPartitions(isDiskless)

  override def getConsolidatingDisklessTopicPartitions: util.Set[TopicIdPartition] =
    topicPartitions(isConsolidating)

  /**
   * Returns the number of diskless partitions whose replicas include the given broker.
   */
  def getDisklessReplicaCount(brokerId: Int): Int = {
    val image = metadataCache.currentImage()
    image.topics().topicsByName().values().stream()
      .filter(topic => isDiskless(topicOverrides(image, topic.name())))
      .mapToInt(_.partitions().values().stream().filter(p => Replicas.contains(p.replicas, brokerId)).count().toInt)
      .sum()
  }

  // Reads the topics, their configs, and their partitions from one image:
  // a topic that a later image deletes can't fail the scan.
  private def topicPartitions(filter: util.Map[String, String] => Boolean): util.Set[TopicIdPartition] = {
    val image = metadataCache.currentImage()
    image.topics().topicsByName().values().stream()
      .filter(topic => filter(topicOverrides(image, topic.name())))
      .flatMap(topic => topic.partitions().keySet().stream()
        .map[TopicIdPartition](p => new TopicIdPartition(topic.id(), p.intValue(), topic.name())))
      .collect(Collectors.toSet[TopicIdPartition]())
  }

  // A read-only view of the image, not a copy like KRaftMetadataCache.topicConfig.
  private def topicOverrides(image: MetadataImage, topicName: String): util.Map[String, String] =
    image.configs().configMapForResource(new ConfigResource(ConfigResource.Type.TOPIC, topicName))

  // Throws on a value other than true or false, ignoring case, such as " true".
  // Neither reading is safe: false misreads a diskless topic, and true misreads a classic one.
  private def flag(overrides: util.Map[String, String], key: String): Boolean =
    overrides.getOrDefault(key, "false").toBoolean

  private def isDiskless(overrides: util.Map[String, String]): Boolean =
    flag(overrides, TopicConfig.DISKLESS_ENABLE_CONFIG)

  private def isConsolidating(overrides: util.Map[String, String]): Boolean =
    isDiskless(overrides) && flag(overrides, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)

  def getClassicToDisklessStartOffset(topicPartition: TopicPartition): Long = {
    Option(metadataCache.currentImage().topics().getTopic(topicPartition.topic()))
      .flatMap(topicImage => Option(topicImage.partitions().get(topicPartition.partition())))
      .map(_.classicToDisklessStartOffset)
      .getOrElse(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
  }

  def isReplicaInIsr(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    partitionRegistration(topicPartition).exists(_.isr.contains(replicaId))
  }

  /**
   * Returns the `classicPrefixInRemoteStorage` flag, which starts phase 2 of the classic-to-diskless
   * switch. The flag is false for a partition that never switched.
   */
  def isClassicPrefixInRemoteStorage(topicPartition: TopicPartition): Boolean = {
    partitionRegistration(topicPartition).exists(_.classicPrefixInRemoteStorage)
  }

  /**
   * Returns true for a partition in phase 1 of the classic-to-diskless switch: the controller committed
   * the seal and the classic prefix isn't in remote storage yet. A pending switch has no committed seal,
   * so it's excluded.
   */
  def isClassicPrefixWaitingForUpload(topicPartition: TopicPartition): Boolean =
    classicPrefixWaitingForUploadSeal(topicPartition).isDefined

  /**
   * Returns the seal of a partition in phase 1 of the classic-to-diskless switch, or `None` outside
   * phase 1. The seal and the phase come from one image read.
   */
  def classicPrefixWaitingForUploadSeal(topicPartition: TopicPartition): Option[Long] =
    partitionRegistration(topicPartition)
      .filter(p => p.classicToDisklessStartOffset >= 0 && !p.classicPrefixInRemoteStorage)
      .map(_.classicToDisklessStartOffset)

  override def isClassicPrefixOnlyOnLocalDisks(topicName: String, partition: Int): Boolean = {
    partitionRegistration(new TopicPartition(topicName, partition)).exists { registration =>
      registration.classicToDisklessStartOffset != PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET &&
        !registration.classicPrefixInRemoteStorage
    }
  }

  private def partitionRegistration(topicPartition: TopicPartition): Option[PartitionRegistration] = {
    Option(metadataCache.currentImage().topics().getTopic(topicPartition.topic()))
      .flatMap(topicImage => Option(topicImage.partitions().get(topicPartition.partition())))
  }

  /**
   * The diskless leader epoch (E_d) captured at the classic-to-diskless switch, or
   * [[PartitionRegistration.NO_DISKLESS_LEADER_EPOCH]] when the partition never switched (born-diskless
   * or switch still pending). It is strictly greater than every classic-prefix leader epoch.
   */
  def getDisklessLeaderEpoch(topicPartition: TopicPartition): Int = {
    Option(metadataCache.currentImage().topics().getTopic(topicPartition.topic()))
      .flatMap(topicImage => Option(topicImage.partitions().get(topicPartition.partition())))
      .map(_.disklessLeaderEpoch)
      .getOrElse(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH)
  }

  /**
   * Returns the leader epoch of the record at `seal - 1` that the phase-1 leader recorded with
   * `classicPrefixInRemoteStorage`, or [[PartitionRegistration.NO_LAST_CLASSIC_LEADER_EPOCH]] when the
   * partition has none.
   */
  def getLastClassicLeaderEpoch(topicPartition: TopicPartition): Int =
    partitionRegistration(topicPartition)
      .map(_.lastClassicLeaderEpoch)
      .getOrElse(PartitionRegistration.NO_LAST_CLASSIC_LEADER_EPOCH)

  /**
   * Returns the diskless leader epoch of the seal commit, or
   * [[PartitionRegistration.NO_DISKLESS_LEADER_EPOCH]] when the partition has none. Unlike
   * [[getDisklessLeaderEpoch]], it doesn't change when phase 2 starts.
   */
  def getInitialDisklessLeaderEpoch(topicPartition: TopicPartition): Int =
    partitionRegistration(topicPartition)
      .map(_.initialDisklessLeaderEpoch)
      .getOrElse(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH)

  override def getTopicConfig(topicName: String): LogConfig = topicConfigs.computeIfAbsent(topicName, t => {
    val props = metadataCache.topicConfig(t)
    if (props.isEmpty) new LogConfig(getDefaultConfig)
    else LogConfig.fromProps(getDefaultConfig, props)
  })

  def updateTopicConfig(topicName: String, topicOverrides: Properties): Unit = {
    warnIfNotBoolean(topicName, topicOverrides, TopicConfig.DISKLESS_ENABLE_CONFIG)
    warnIfNotBoolean(topicName, topicOverrides, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)
    topicConfigs.computeIfPresent(topicName, (_, _) => LogConfig.fromProps(getDefaultConfig, topicOverrides))
  }

  // ConfigDef trims before it validates, so an alter can store a value such as " true".
  // The topic type flags throw on it, and LogConfig reads it as true.
  private def warnIfNotBoolean(topicName: String, topicOverrides: Properties, key: String): Unit = {
    val value = topicOverrides.getProperty(key)
    if (value != null && !value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false"))
      warn(s"Topic $topicName has $key='$value', which isn't true or false. " +
        s"The broker fails the requests and scans that read $key for this topic.")
  }

  def removeTopicConfig(topicName: String): Unit = {
    topicConfigs.remove(topicName)
  }

  def reconfigureDefaultLogConfig(): Unit = {
    val newDefaults = getDefaultConfig
    topicConfigs.replaceAll { (_, existingConfig) =>
      val props = new util.HashMap[String, Object](newDefaults)
      existingConfig.originals.asScala
        .filter { case (k, _) => existingConfig.overriddenConfigs.contains(k) }
        .foreach { case (k, v) => props.put(k, v) }
      new LogConfig(props, existingConfig.overriddenConfigs)
    }
  }
}
