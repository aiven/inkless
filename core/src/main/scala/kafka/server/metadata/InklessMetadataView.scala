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
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Node, TopicIdPartition, Uuid}
import org.apache.kafka.storage.internals.log.LogConfig

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier
import java.util.stream.{Collectors, IntStream}
import scala.jdk.CollectionConverters._

class InklessMetadataView(val metadataCache: KRaftMetadataCache, val defaultConfig: Supplier[util.Map[String, Object]]) extends MetadataView {

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

  override def isDisklessTopic(topicName: String): Boolean = {
    metadataCache.topicConfig(topicName).getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false").toBoolean
  }

  override def isRemoteStorageEnabled(topicName: String): Boolean = {
    metadataCache.topicConfig(topicName).getProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false").toBoolean
  }

  override def getDisklessTopicPartitions: util.Set[TopicIdPartition] = {
    metadataCache.getAllTopics().stream()
      .filter(isDisklessTopic)
      .flatMap(t => IntStream.range(0, metadataCache.numPartitions(t).get())
        .mapToObj(p => new TopicIdPartition(metadataCache.getTopicId(t), p, t)))
      .collect(Collectors.toSet[TopicIdPartition]())
  }

  override def getTopicConfig(topicName: String): LogConfig = topicConfigs.computeIfAbsent(topicName, t => {
    val props = metadataCache.topicConfig(t)
    if (props.isEmpty) new LogConfig(getDefaultConfig)
    else LogConfig.fromProps(getDefaultConfig, props)
  })

  def updateTopicConfig(topicName: String, topicOverrides: Properties): Unit = {
    topicConfigs.computeIfPresent(topicName, (_, _) => LogConfig.fromProps(getDefaultConfig, topicOverrides))
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
