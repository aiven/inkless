package kafka.server

import com.typesafe.scalalogging.Logger
import org.apache.kafka.common.{TopicIdPartition, TopicPartition}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}

import java.util.concurrent.Future
import java.util.concurrent.locks.Lock
import scala.collection.{Map, Seq}
import java.util.{Map => JavaMap}
import scala.jdk.CollectionConverters.MapHasAsScala;

object DelayedInklessFetch {
  private final val logger = Logger(classOf[DelayedInklessFetch])
}

class DelayedInklessFetch(
  params: FetchParams,
  responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit
) extends DelayedOperation(params.maxWaitMs) {

  override def onExpiration(): Unit = {
    // TODO what?
    logger.error("onExpiration")
  }

  override def onComplete(): Unit = {
  }

  override def tryComplete(): Boolean = ???
}
