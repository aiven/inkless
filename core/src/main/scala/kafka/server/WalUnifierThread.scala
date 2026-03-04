package kafka.server

import io.aiven.inkless.control_plane.{BatchInfo, MetadataView}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.server.util.ShutdownableThread

import java.util
import java.util.concurrent.LinkedBlockingQueue
import scala.jdk.OptionConverters.RichOptional

class WalUnifierThread(name: String,
                       replicaManager: ReplicaManager,
                       inklessMetadataView: MetadataView)
  extends ShutdownableThread(name, true) {

  private val queue = new LinkedBlockingQueue[util.TreeMap[BatchInfo, MemoryRecords]]()

  def submitBatches(batches: util.TreeMap[BatchInfo, MemoryRecords]): Unit = {
    queue.put(batches)
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  override def doWork(): Unit = {
    val nextBatch = queue.take()
    // transforming it to a Scala map is complicated
    nextBatch.forEach((batchInfo: BatchInfo, records: MemoryRecords) => {
      val topicName = inklessMetadataView.getTopicName(batchInfo.metadata.topicIdPartition.topicId)
      val partitionNum = batchInfo.metadata.topicIdPartition.partition
      topicName.toScala.foreach { tn =>
        val tp = new TopicPartition(tn, partitionNum)
        val partition = replicaManager.getPartitionOrException(tp)
        // cheat the system
        val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, 0)
        logAppendInfo.foreach { li =>
          partition.log.foreach{ l =>
            l.updateHighWatermark(li.lastOffset)
          }
        }
      }
    })
  }

  override def shutdown(): Unit = {
    // TODO: should wait until consumed the queue
    super.shutdown()
  }
}
