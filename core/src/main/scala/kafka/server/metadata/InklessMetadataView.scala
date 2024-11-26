// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import io.aiven.inkless.control_plane.MetadataView
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.storage.internals.log.LogConfig

import java.util
import scala.jdk.CollectionConverters.SetHasAsJava

class InklessMetadataView(val metadataCache: KRaftMetadataCache, val defaultTopicConfigs: () => LogConfig) extends MetadataView {
  override def getTopicPartitions(topicName: String): util.Set[TopicPartition] = {
    metadataCache.getTopicPartitions(topicName).asJava
  }

  override def getTopicId(topicName: String): Uuid = {
    metadataCache.getTopicId(topicName)
  }

  override def isInklessTopic(topicName: String): Boolean = {
    metadataCache.isInklessTopic(topicName)
  }

  override def getTopicConfig(topicName: String): LogConfig = {
    val overrides = metadataCache.config(new ConfigResource(ConfigResource.Type.TOPIC, topicName))

    if (overrides.isEmpty) {
      defaultTopicConfigs()
    }

    LogConfig.fromProps(defaultTopicConfigs().originals(), overrides)
  }
}
