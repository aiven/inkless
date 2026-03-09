package kafka.server

import io.aiven.inkless.control_plane.{MetadataView, WalUnificationHandler}
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.ShutdownableThread

import java.util.concurrent.Executors
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava}

class WalUnifierManager(name: String,
                        protected val replicaManager: ReplicaManager,
                        walUnificationHandler: WalUnificationHandler,
                        inklessMetadataView: MetadataView,
                        numWorkers: Int) extends ShutdownableThread(name, true) {

  private val lock = new Object
  private val workerThreadPool = Executors.newFixedThreadPool(numWorkers)
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

    workers.foreach(workerThreadPool.execute)

    super.start()
  }

  def updateLatestOffsets(): Unit = {
    val offsetMap = inklessMetadataView.getDisklessTopicPartitions.asScala.map { tp =>
      val lastOffset = replicaManager.getLog(tp.topicPartition()) match {
        case Some(l) => l.logEndOffset()
        case _ => 0L
      }
      (tp -> Long.box(lastOffset))
    }.toMap
    walUnificationHandler.setLastOffsets(offsetMap.asJava)
  }

  def updateRemoteLogEndOffsets(): Unit = {
    val offsetMap = inklessMetadataView.getDisklessTopicPartitions.asScala.map { tp =>
      val lastOffset = replicaManager.getLog(tp.topicPartition()) match {
        case Some(l) => l.highestOffsetInRemoteStorage()
        case _ => 0L
      }
      (tp -> Long.box(lastOffset))
    }.toMap
    walUnificationHandler.setRemoteLogEndOffsets(offsetMap.asJava)
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  override def doWork(): Unit = {
    updateRemoteLogEndOffsets();
    val nextWalSegmentData = walUnificationHandler.get()
    nextWalSegmentData.forEach { case (tip, batchMap) =>
      val wid = workerId(tip)
      workers(wid).submitBatches(batchMap)
    }
  }

  override def shutdown(): Unit = {
    workers.foreach(_.shutdown())
    super.shutdown()
  }
}
