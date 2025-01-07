// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package kafka.server.metadata

import io.aiven.inkless.control_plane.TopicMetadataChangesSubscriber
import org.apache.kafka.image.TopicsDelta
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.same
import org.mockito.Mockito.{mock, reset, verify}

class InklessMetadataViewTest {
  @Test
  def topicMetadataChangesSubscriptionEmpty(): Unit = {
    val metadataView = new InklessMetadataView(mock(classOf[KRaftMetadataCache]))
    val delta = mock(classOf[TopicsDelta])

    // No exception, all good.
    metadataView.onTopicMetadataUpdate(delta)

    val subscriber1 = mock(classOf[TopicMetadataChangesSubscriber])
    metadataView.subscribeToTopicMetadataChanges(subscriber1)

    metadataView.onTopicMetadataUpdate(delta)
    verify(subscriber1).onTopicMetadataChanges(same(delta))

    reset(subscriber1)

    val subscriber2 = mock(classOf[TopicMetadataChangesSubscriber])
    metadataView.subscribeToTopicMetadataChanges(subscriber2)

    metadataView.onTopicMetadataUpdate(delta)
    verify(subscriber1).onTopicMetadataChanges(same(delta))
    verify(subscriber2).onTopicMetadataChanges(same(delta))
  }
}
