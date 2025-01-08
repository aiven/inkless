// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import io.aiven.inkless.control_plane.MetadataView
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{TopicPartition, Uuid}

import java.util.Properties
import java.{lang, util}
import scala.jdk.CollectionConverters.{IterableHasAsJava, SetHasAsJava}

class InklessMetadataView(val metadataCache: KRaftMetadataCache) extends MetadataView {
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
    metadataCache.isInklessTopic(topicName)
  }

  override def getTopicConfig(topicName: String): Properties = {
    metadataCache.config(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
  }
}
