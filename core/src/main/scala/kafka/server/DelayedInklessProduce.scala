package kafka.server

import com.typesafe.scalalogging.Logger
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import java.util.concurrent.Future
import java.util.concurrent.locks.Lock
import scala.collection.Map
import java.util.{Map => JavaMap}
import scala.jdk.CollectionConverters.MapHasAsScala;

object DelayedInklessProduce {
  private final val logger = Logger(classOf[DelayedInklessProduce])
}

class DelayedInklessProduce(delayMs: Long,
                            future: Future[JavaMap[TopicPartition, PartitionResponse]],
                            responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                            lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  override lazy val logger: Logger = DelayedInklessProduce.logger

  override def onExpiration(): Unit = {
    // TODO what?
    logger.error("onExpiration")
  }

  override def onComplete(): Unit = {
    val result = future.get().asScala
    logger.error("onComplete, result: {}", result)
    responseCallback(future.get().asScala)
  }

  override def tryComplete(): Boolean = {
    logger.error("tryComplete, future.isDone={}", future.isDone)
    if (future.isDone) {
      val forceCompleteResult = forceComplete()
      logger.error("forceCompleteResult={}", forceCompleteResult)
      forceCompleteResult
    } else {
      false
    }
  }
}
