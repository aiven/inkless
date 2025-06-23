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
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Node, TopicIdPartition, Uuid}

import java.util.Properties
import java.util.function.Supplier
import java.util.stream.{Collectors, IntStream}
import java.{lang, util}

import scala.jdk.CollectionConverters._

class InklessMetadataView(val metadataCache: KRaftMetadataCache, val defaultConfig: Supplier[util.Map[String, Object]]) extends MetadataView {
  override def getDefaultConfig: util.Map[String, Object] = {
    // Filter out null values as they break LogConfig initialization using Properties.putAll
    defaultConfig.get().entrySet().asScala
      .filter(_.getValue != null)
      .foldLeft(new util.HashMap[String, Object]()) { (filtered, entry) =>
        filtered.put(entry.getKey, entry.getValue)
        filtered
      }
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): lang.Iterable[Node] = {
    metadataCache.getAliveBrokerNodes(listenerName)
  }

  override def getBrokerCount: Integer = metadataCache.currentImage().cluster().brokers().size()

  override def getTopicId(topicName: String): Uuid = {
    metadataCache.getTopicId(topicName)
  }

  override def isInklessTopic(topicName: String): Boolean = {
    metadataCache.isInklessTopic(topicName, defaultConfig)
  }

  override def getTopicConfig(topicName: String): Properties = {
    metadataCache.config(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
  }

  override def getInklessTopicPartitions: util.Set[TopicIdPartition] = {
    metadataCache.getAllTopics().stream()
      .filter(isInklessTopic)
      .flatMap(t => IntStream.range(0, metadataCache.numPartitions(t).get())
        .mapToObj(p => new TopicIdPartition(metadataCache.getTopicId(t), p, t)))
      .collect(Collectors.toSet[TopicIdPartition]())
  }
}
