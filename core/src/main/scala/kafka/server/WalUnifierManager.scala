package kafka.server

import io.aiven.inkless.control_plane.{MetadataView, WalUnificationHandler}
import kafka.cluster.Partition
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.ShutdownableThread

import scala.jdk.CollectionConverters.{MapHasAsJava, SetHasAsJava}
import scala.jdk.OptionConverters.RichOptional

class WalUnifierManager(name: String,
                        protected val replicaManager: ReplicaManager,
                        walUnificationHandler: WalUnificationHandler,
                        inklessMetadataView: MetadataView,
                        numWorkers: Int,
                        allPartitionsIterator: => Iterator[Partition]) extends ShutdownableThread(name, true) {

  private val lock = new Object
  private val workers = (0 until numWorkers)
    .map(id => new WalUnifierThread(s"WALUnifierThread-$id", replicaManager, inklessMetadataView)).toList

  // Similar implementation as getFetcherId in AbstractFetcherManager
  private def workerId(tip: TopicIdPartition): Int = {
    lock synchronized {
      Utils.abs(31 * tip.topicId.hashCode + tip.partition) % numWorkers
    }
  }

  override def start(): Unit = {
    updateLatestOffsets()

    workers.foreach(_.start())

    super.start()
  }

  def updateLatestOffsets(): Unit = {
    val offsetMap = allPartitions().map { tp =>
      val lastOffset = replicaManager.getLog(tp.topicPartition()) match {
        case Some(l) => l.logEndOffset()
        case _ => 0L
      }
      (tp -> Long.box(lastOffset))
    }.toMap
    walUnificationHandler.setLastOffsets(offsetMap.asJava)
  }

  private def updateRemoteLogEndOffsets(op: Set[TopicIdPartition]): Unit = {
    val offsetMap = op.map { tp =>
      val lastOffset = replicaManager.getLog(tp.topicPartition()) match {
        case Some(l) => l.highestOffsetInRemoteStorage()
        case _ => 0L
      }
      (tp -> Long.box(lastOffset))
    }.toMap
    walUnificationHandler.setRemoteLogEndOffsets(offsetMap.asJava)
  }

  private def allPartitions(): Set[TopicIdPartition] = {
    allPartitionsIterator
      .filter( p => {
        val topicName = p.topicId.flatMap(id => inklessMetadataView.getTopicName(id).toScala)
        topicName.map(n => inklessMetadataView.isDisklessTopic(p.topic)).getOrElse(false)
      })
      .flatMap(p =>
        p.topicId.map(tid =>
          new TopicIdPartition(tid, p.partitionId, p.topic)))
      .toSet
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  override def doWork(): Unit = {
    try {
      val op = allPartitions()
      println("Online partitions: " + op)
      updateRemoteLogEndOffsets(op);
      updateLatestOffsets()
      val nextWalSegmentData = walUnificationHandler.apply(op.asJava)
      nextWalSegmentData.forEach { case (tip, batchMap) =>
        val wid = workerId(tip)
        workers(wid).submitBatches(batchMap)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def shutdown(): Unit = {
    workers.foreach(_.interrupt())
    workers.foreach(_.shutdown())
    super.shutdown()
  }
}
