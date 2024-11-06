// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import io.aiven.inkless.control_plane.MetadataView
import kafka.server.MetadataCache
import org.apache.kafka.common.{TopicPartition, Uuid}

import java.util
import scala.jdk.CollectionConverters.SetHasAsJava

class InklessMetadataView(val metadataCache: MetadataCache) extends MetadataView {
  override def getTopicPartitions(topicName: String): util.Set[TopicPartition] = {
    metadataCache.getTopicPartitions(topicName).asJava
  }

  override def getTopicId(topicName: String): Uuid = {
    metadataCache.getTopicId(topicName)
  }
}
