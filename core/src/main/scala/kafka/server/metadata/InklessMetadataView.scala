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
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{TopicPartition, Uuid}

import java.util.Properties
import java.util.function.Supplier
import java.{lang, util}
import scala.collection.Map
import scala.jdk.CollectionConverters.{IterableHasAsJava, SetHasAsJava}

class InklessMetadataView(val metadataCache: KRaftMetadataCache, val defaultConfig: Supplier[Map[String, _]]) extends MetadataView {
  override def getAliveBrokers: lang.Iterable[BrokerMetadata] = {
    metadataCache.getAliveBrokers().asJava
  }

  override def getTopicPartitions(topicName: String): util.Set[TopicPartition] = {
    metadataCache.getTopicPartitions(topicName).asJava
  }

  override def getTopicId(topicName: String): Uuid = {
    metadataCache.getTopicId(topicName)
  }

  override def isInklessTopic(topicName: String): Boolean = {
    metadataCache.isInklessTopic(topicName, defaultConfig)
  }

  override def getTopicConfig(topicName: String): Properties = {
    metadataCache.config(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
  }
}
