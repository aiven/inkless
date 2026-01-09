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

import java.util.function.Supplier
import java.util.stream.{Collectors, IntStream}
import java.{lang, util}
import scala.jdk.CollectionConverters._

class InklessMetadataView(val metadataCache: KRaftMetadataCache, val defaultConfig: Supplier[util.Map[String, Object]]) extends MetadataView {
  override def getDefaultConfig: util.Map[String, Object] = {
    // Filter out null values as they break LogConfig initialization using Properties.putAll
    val filtered = new util.HashMap[String, Object]()
    defaultConfig.get().entrySet().asScala
      .filter(_.getValue != null)
      .foreach(entry => filtered.put(entry.getKey, entry.getValue))
    filtered
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): lang.Iterable[Node] = {
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

  override def getDisklessTopicPartitions: util.Set[TopicIdPartition] = {
    metadataCache.getAllTopics().stream()
      .filter(isDisklessTopic)
      .flatMap(t => IntStream.range(0, metadataCache.numPartitions(t).get())
        .mapToObj(p => new TopicIdPartition(metadataCache.getTopicId(t), p, t)))
      .collect(Collectors.toSet[TopicIdPartition]())
  }

  override def getTopicConfig(topicName: String): LogConfig = LogConfig.fromProps(getDefaultConfig(), metadataCache.topicConfig(topicName))
}
