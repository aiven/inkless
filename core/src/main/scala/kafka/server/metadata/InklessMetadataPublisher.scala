// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import kafka.utils.Logging
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.publisher.MetadataPublisher

/**
 * This publisher is basically a bridge to notify [[InklessMetadataView]] about change in topics and partitions.
 */
class InklessMetadataPublisher(metadataView: InklessMetadataView) extends MetadataPublisher with Logging {
  override def name(): String = s"InklessPublisher"

  override def onMetadataUpdate(delta: MetadataDelta,
                                newImage: MetadataImage,
                                manifest: LoaderManifest): Unit = {
    val topicsDelta = delta.topicsDelta()
    if (topicsDelta != null) {
      metadataView.onTopicMetadataUpdate(topicsDelta)
    }
  }
}
