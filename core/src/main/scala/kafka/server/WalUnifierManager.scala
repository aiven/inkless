package kafka.server

import io.aiven.inkless.control_plane.{MetadataView, WALSplitter}
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.ShutdownableThread

import java.util.concurrent.Executors

class WalUnifierManager(name: String,
                        protected val replicaManager: ReplicaManager,
                        walSplitter: WALSplitter,
                        inklessMetadataView: MetadataView,
                        numWorkers: Int) extends ShutdownableThread(name, true) {

  private val lock = new Object
  private val workerThreadPool = Executors.newFixedThreadPool(numWorkers)
  private val workers = (0 until numWorkers)
    .map(id => new WalUnifierThread(s"WALUnifierThread-$id", replicaManager, inklessMetadataView)).toList
  workers.foreach(workerThreadPool.execute)

  // Similar implementation as getFetcherId in AbstractFetcherManager
  private def workerId(tip: TopicIdPartition): Int = {
    lock synchronized {
      Utils.abs(31 * tip.topicId.hashCode + tip.partition) % numWorkers
    }
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  override def doWork(): Unit = {
    val nextWalSegmentData = walSplitter.get()
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
