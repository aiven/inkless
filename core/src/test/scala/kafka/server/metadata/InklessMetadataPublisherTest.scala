// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metadata.{PartitionRecord, TopicRecord}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito.verify
import org.mockito.junit.jupiter.{MockitoExtension, MockitoSettings}
import org.mockito.quality.Strictness
import org.mockito.{ArgumentMatchers, Mock}

@ExtendWith(Array(classOf[MockitoExtension]))
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class InklessMetadataPublisherTest {
  val TOPIC_1 = "topic1"
  val TOPIC_ID_1 = new Uuid(1, 1)
  val TOPIC_2 = "topic2"
  val TOPIC_ID_2 = new Uuid(2, 2)

  @Mock
  val metadataView: InklessMetadataView = null
  @Mock
  val loaderManifest: LoaderManifest = null

  @Test
  def topicUpdatesArePropagated(): Unit = {
    val publisher = new InklessMetadataPublisher(metadataView)

    val oldImage = MetadataImage.EMPTY
    val delta = new MetadataDelta.Builder().setImage(oldImage).build
    delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID_1))
    delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_1).setPartitionId(0))
    delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_1).setPartitionId(1))
    delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID_2))
    delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_2).setPartitionId(0))
    val newImage = delta.apply(MetadataProvenance.EMPTY)

    publisher.onMetadataUpdate(delta, newImage, loaderManifest)

    verify(metadataView).onTopicMetadataUpdate(ArgumentMatchers.eq(delta.topicsDelta()))
  }
}
