// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import io.aiven.inkless.control_plane.{MetadataView, TopicMetadataChangesSubscriber}
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.TopicsDelta

import java.util.Properties
import java.{lang, util}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.{IterableHasAsJava, SetHasAsJava}

class InklessMetadataView(val metadataCache: KRaftMetadataCache) extends MetadataView {
  private val topicMetadataChangesSubscribers = new AtomicReference[List[TopicMetadataChangesSubscriber]](Nil)

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

  override def subscribeToTopicMetadataChanges(subscriber: TopicMetadataChangesSubscriber): Unit = {
    topicMetadataChangesSubscribers.updateAndGet(list => list :+ subscriber)
  }

  def onTopicMetadataUpdate(topicsDelta: TopicsDelta): Unit = {
    topicMetadataChangesSubscribers.get().foreach(_.onTopicMetadataChanges(topicsDelta))
  }
}
